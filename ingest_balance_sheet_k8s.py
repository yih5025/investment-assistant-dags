from datetime import datetime, timedelta
import os
import requests
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_balance_sheet.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_balance_sheet_sp500_k8s',  # DAG 이름도 명확하게 변경
    default_args=default_args,
    schedule_interval='0 6 * * 2,4,6',  # 화/목/토 오전 6시
    catchup=False,
    description='Fetch S&P 500 balance sheet data 3x per week (Tue/Thu/Sat: 20 companies each)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['balance_sheet', 'alpha_vantage', 'sp500', 'batch', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_balance_sheet_table',
        postgres_conn_id='postgres_default',
        sql='create_balance_sheet.sql',
    )

    def fetch_and_upsert_balance_sheet(**context):
        """SP500 기업 배치 재무제표 데이터 수집 및 저장"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
        
        # 🔧 동적으로 총 기업 수 조회
        total_companies_result = hook.get_first("SELECT COUNT(*) FROM sp500_companies")
        total_companies = total_companies_result[0] if total_companies_result else 0
        
        if total_companies == 0:
            raise AirflowSkipException("sp500_companies 테이블에 데이터가 없습니다")
        
        # 현재 오프셋 확인
        offset = int(Variable.get('BALANCE_SHEET_SP500_OFFSET', default_var=0))
        
        # 🔧 동적 완료 체크
        if offset >= total_companies:
            print(f"🎉 모든 S&P 500 기업 처리 완료! (총 {total_companies}개)")
            # 오프셋 리셋 (다음 주기를 위해)
            Variable.set('BALANCE_SHEET_SP500_OFFSET', 0)
            raise AirflowSkipException("모든 기업 처리 완료 - 다음 주기 대기")
        
        # 🔧 API 제한 고려한 배치 크기 (하루 최대 20개)
        batch_size = min(20, total_companies - offset)
        
        # 배치 기업 조회
        symbols_result = hook.get_records("""
            SELECT symbol FROM sp500_companies 
            WHERE symbol IS NOT NULL
            ORDER BY symbol 
            LIMIT %s OFFSET %s
        """, parameters=[batch_size, offset])
        
        symbols = [row[0] for row in symbols_result]
        
        if not symbols:
            raise AirflowSkipException("처리할 기업이 없습니다")
        
        print(f"📊 S&P 500 배치 처리: {len(symbols)}개 기업")
        print(f"🔢 진행률: {offset}/{total_companies} ({offset/total_companies*100:.1f}%)")
        print(f"📝 처리 기업: {symbols}")
        
        success_count = 0
        error_count = 0
        
        # 각 기업의 재무제표 수집
        for i, symbol in enumerate(symbols):
            try:
                # 🚨 API Rate Limit 준수 (분당 5회 제한)
                if i > 0:
                    print(f"⏳ API 제한 준수를 위해 15초 대기...")
                    time.sleep(15)  # 15초 대기
                
                print(f"📈 {symbol} 재무제표 수집 중... ({i+1}/{len(symbols)})")
                
                # Alpha Vantage API 호출
                resp = requests.get(
                    "https://www.alphavantage.co/query",
                    params={
                        "function": "BALANCE_SHEET",
                        "symbol": symbol,
                        "apikey": api_key
                    },
                    timeout=30
                )
                resp.raise_for_status()
                
                data = resp.json()
                
                # 🔧 API 오류 체크
                if "Error Message" in data:
                    print(f"❌ {symbol} API 오류: {data['Error Message']}")
                    error_count += 1
                    continue
                
                if "Note" in data:
                    print(f"⚠️ {symbol} API 제한: {data['Note']}")
                    error_count += 1
                    continue
                
                quarterly_reports = data.get("quarterlyReports", [])
                
                if not quarterly_reports:
                    print(f"⚠️ {symbol}: 재무제표 데이터 없음")
                    error_count += 1
                    continue
                
                # 각 분기 데이터 저장
                saved_quarters = 0
                for report in quarterly_reports:
                    # None, "None", "" 값 정리
                    params = {
                        k: None if v in (None, "None", "") else v
                        for k, v in report.items()
                    }
                    params["symbol"] = symbol
                    
                    try:
                        hook.run(UPSERT_SQL, parameters=params)
                        saved_quarters += 1
                    except Exception as db_error:
                        print(f"❌ {symbol} DB 저장 실패: {str(db_error)}")
                        continue
                
                print(f"✅ {symbol}: {saved_quarters}개 분기 데이터 저장 완료")
                success_count += 1
                
            except requests.exceptions.RequestException as e:
                print(f"❌ {symbol} 네트워크 오류: {str(e)}")
                error_count += 1
                continue
            except Exception as e:
                print(f"❌ {symbol} 예상치 못한 오류: {str(e)}")
                error_count += 1
                continue
        
        # 🔧 오프셋 업데이트
        next_offset = offset + len(symbols)
        Variable.set('BALANCE_SHEET_SP500_OFFSET', next_offset)
        
        # 최종 결과 통계
        result = hook.get_first("SELECT COUNT(*) FROM balance_sheet")
        total_records = result[0] if result else 0
        
        print(f"\n📊 배치 처리 결과:")
        print(f"  ✅ 성공: {success_count}개 기업")
        print(f"  ❌ 실패: {error_count}개 기업")
        print(f"  🔢 다음 오프셋: {next_offset}")
        print(f"  📈 전체 진행률: {next_offset}/{total_companies} ({next_offset/total_companies*100:.1f}%)")
        print(f"  💾 총 DB 레코드: {total_records}개")
        
        # 🎉 완료 알림
        if next_offset >= total_companies:
            print(f"\n🎉 축하합니다! 모든 S&P 500 기업 처리 완료!")
            print(f"  📊 총 처리 기업: {total_companies}개")
            print(f"  💾 수집된 재무제표: {total_records}개")
        
        return success_count

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_sp500_balance_sheet',
        python_callable=fetch_and_upsert_balance_sheet,
    )

    # Task 의존성
    create_table >> fetch_upsert