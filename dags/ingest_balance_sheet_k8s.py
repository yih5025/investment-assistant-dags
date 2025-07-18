from datetime import datetime, timedelta
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

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
    dag_id='ingest_balance_sheet_k8s',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Fetch balance sheet data in batches (10 companies per day)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['balance_sheet', 'alpha_vantage', 'batch', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_balance_sheet_table',
        postgres_conn_id='postgres_default',
        sql='create_balance_sheet.sql',
    )

    def fetch_and_upsert_balance_sheet(**context):
        """배치로 재무제표 데이터 수집 및 저장"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
        
        # 현재 오프셋 확인
        offset = int(Variable.get('BALANCE_SHEET_OFFSET', default_var=0))
        
        if offset >= 50:
            raise AirflowSkipException("모든 기업 처리 완료")
        
        # 10개 기업 배치 조회
        symbols_result = hook.get_records("""
            SELECT symbol FROM sp500_top50 
            ORDER BY symbol 
            LIMIT 10 OFFSET %s
        """, parameters=[offset])
        
        symbols = [row[0] for row in symbols_result]
        
        if not symbols:
            raise AirflowSkipException("처리할 기업이 없습니다")
        
        print(f"📊 배치 처리: {symbols} (오프셋: {offset})")
        
        # 각 기업의 재무제표 수집
        for symbol in symbols:
            try:
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
                quarterly_reports = data.get("quarterlyReports", [])
                
                # 각 분기 데이터 저장
                for report in quarterly_reports:
                    # None, "None", "" 값 정리
                    params = {
                        k: None if v in (None, "None", "") else v
                        for k, v in report.items()
                    }
                    params["symbol"] = symbol
                    
                    hook.run(UPSERT_SQL, parameters=params)
                
                print(f"✅ {symbol}: {len(quarterly_reports)}개 분기 데이터 저장")
                
            except Exception as e:
                print(f"❌ {symbol} 실패: {str(e)}")
                continue
        
        # 오프셋 업데이트
        next_offset = offset + len(symbols)
        Variable.set('BALANCE_SHEET_OFFSET', next_offset)
        
        # 최종 결과
        result = hook.get_first("SELECT COUNT(*) FROM balance_sheet")
        total_records = result[0] if result else 0
        print(f"✅ 완료. 다음 오프셋: {next_offset}, 총 레코드: {total_records}")
        
        return len(symbols)

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_balance_sheet',
        python_callable=fetch_and_upsert_balance_sheet,
    )

    # Task 의존성
    create_table >> fetch_upsert