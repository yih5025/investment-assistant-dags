from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# 표준 경로 설정
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='load_sp500_top50_k8s',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
    description='Load SP500 top 50 companies data into PostgreSQL',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'master_data', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_sp500_table',
        postgres_conn_id='postgres_default',
        sql='create_sp500_top50.sql',
    )

    insert_data = PostgresOperator(
        task_id='insert_sp500_data',
        postgres_conn_id='postgres_default',
        sql='insert_sp500_top50.sql',
    )

    def verify_data(**context):
        """데이터 로드 검증"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 총 레코드 수 확인
        result = hook.get_first("SELECT COUNT(*) FROM sp500_top50")
        total_count = result[0] if result else 0
        
        print(f"📊 SP500 기업 데이터 로드 완료: {total_count}개 레코드")
        
        # 섹터별 분포 확인
        sector_result = hook.get_records("""
            SELECT sector, COUNT(*) as count 
            FROM sp500_top50 
            GROUP BY sector 
            ORDER BY count DESC
        """)
        
        print("📈 섹터별 기업 분포:")
        for sector, count in sector_result:
            print(f"  - {sector}: {count}개 기업")
        
        # 샘플 데이터 확인
        sample_result = hook.get_records("""
            SELECT symbol, company_name, sector 
            FROM sp500_top50 
            ORDER BY symbol 
            LIMIT 5
        """)
        
        print("🔍 샘플 데이터:")
        for symbol, company_name, sector in sample_result:
            print(f"  - {symbol}: {company_name} ({sector})")
        
        if total_count < 50:
            print(f"⚠️ 경고: 예상보다 적은 레코드 수 ({total_count}/50)")
        else:
            print("✅ 데이터 로드 검증 완료!")
        
        return total_count

    verify = PythonOperator(
        task_id='verify_sp500_data',
        python_callable=verify_data,
    )

    # Task 의존성
    create_table >> insert_data >> verify