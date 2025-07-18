from datetime import datetime, timedelta
from io import StringIO
import csv
import requests
from decimal import Decimal
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Git-Sync 환경에서 SQL 파일 경로
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# upsert SQL 파일 읽기 (dags/sql/ 폴더에서)
with open(os.path.join(DAGS_SQL_DIR, "upsert_earnings_calendar.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='ingest_earnings_calendar_to_db',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    description='Fetch 12-month earnings calendar from Alpha Vantage and upsert into PostgreSQL',
    # create table SQL은 initdb 폴더에서 찾도록 설정
    template_searchpath=[INITDB_SQL_DIR],
    tags=['earnings', 'calendar', 'alpha_vantage'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_earnings_calendar_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_calendar.sql',  # initdb 폴더에서 찾음
    )

    def fetch_calendar(**context):
        """Alpha Vantage API에서 실적 캘린더 데이터 수집"""
        # API 키 가져오기
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        
        # API 요청
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "EARNINGS_CALENDAR",
                "apikey": api_key,
                "horizon": "12month"
            },
            timeout=60
        )
        resp.raise_for_status()
        
        # 응답 검증
        if 'Error Message' in resp.text:
            raise ValueError(f"API 오류: {resp.text}")
        
        # CSV 데이터 파싱
        rows = list(csv.DictReader(StringIO(resp.text)))
        
        if not rows:
            raise ValueError("API에서 데이터를 받지 못했습니다")
        
        print(f"✅ 실적 캘린더 데이터 수집 완료: {len(rows)}개 레코드")
        print(f"📋 샘플 데이터: {rows[0]}")
        
        # XCom에 저장
        context['ti'].xcom_push(key='calendar_rows', value=rows)
        return len(rows)

    def upsert_calendar(**context):
        """데이터를 PostgreSQL에 저장"""
        rows = context['ti'].xcom_pull(task_ids='fetch_earnings_calendar', key='calendar_rows')
        
        if not rows:
            raise ValueError("이전 태스크에서 데이터를 받지 못했습니다")
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        success_count = 0
        error_count = 0
        
        print(f"🚀 데이터 저장 시작: {len(rows)}개 레코드")
        
        for i, row in enumerate(rows):
            try:
                # 필수 필드 검증
                if not row.get('symbol') or not row.get('reportDate'):
                    error_count += 1
                    continue
                
                # estimate 필드 처리
                estimate = None
                if row.get('estimate') and str(row.get('estimate')).strip():
                    try:
                        estimate = Decimal(str(row.get('estimate')))
                    except:
                        estimate = None
                
                # SQL 실행 (파일에서 읽은 UPSERT_SQL 사용)
                hook.run(UPSERT_SQL, parameters={
                    'symbol': row['symbol'],
                    'name': row.get('name', ''),
                    'report_date': row['reportDate'] if row['reportDate'] else None,
                    'fiscal_date_ending': row.get('fiscalDateEnding') if row.get('fiscalDateEnding') else None,
                    'estimate': estimate,
                    'currency': row.get('currency', 'USD')
                })
                
                success_count += 1
                
                # 진행률 표시
                if (i + 1) % 100 == 0:
                    print(f"📊 진행률: {i+1}/{len(rows)}")
                
            except Exception as e:
                print(f"❌ 레코드 저장 실패: {row.get('symbol', 'Unknown')} - {str(e)}")
                error_count += 1
                continue
        
        print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
        
        # 최종 통계
        result = hook.get_first("SELECT COUNT(*) FROM earnings_calendar")
        total_records = result[0] if result else 0
        print(f"📊 총 실적 캘린더 레코드 수: {total_records}")
        
        return success_count

    fetch = PythonOperator(
        task_id='fetch_earnings_calendar',
        python_callable=fetch_calendar,
    )

    upsert = PythonOperator(
        task_id='upsert_earnings_calendar',
        python_callable=upsert_calendar,
    )

    # Task 의존성
    create_table >> fetch >> upsert