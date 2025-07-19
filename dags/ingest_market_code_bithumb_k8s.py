from datetime import datetime, timedelta
import requests
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 표준 경로 설정
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_bithumb_markets_k8s',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 업데이트
    catchup=False,
    description='Update Bithumb market codes daily (truncate and reload)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['bithumb', 'markets', 'master_data', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_market_code_table',
        postgres_conn_id='postgres_default',
        sql='create_market_code_bithumb.sql',
    )

    def update_bithumb_markets(**context):
        """빗썸 마켓코드 수집 및 업데이트 (전체 갱신)"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 빗썸 마켓코드 API 호출
        try:
            resp = requests.get(
                "https://api.bithumb.com/v1/market/all?isDetails=true",
                headers={"accept": "application/json"},
                timeout=30
            )
            resp.raise_for_status()
            markets_data = resp.json()
            
            print(f"📊 조회된 마켓: {len(markets_data)}개")
            
            # 기존 데이터 모두 삭제
            hook.run("TRUNCATE TABLE market_code_bithumb")
            print("🗑️ 기존 마켓코드 삭제 완료")
            
            # KRW 마켓만 새로 삽입
            krw_markets = [m for m in markets_data if m['market'].startswith('KRW-')]
            
            for market in krw_markets:
                hook.run("""
                    INSERT INTO market_code_bithumb (
                        market_code, korean_name, english_name, market_warning
                    ) VALUES (%s, %s, %s, %s)
                """, parameters=[
                    market['market'],
                    market['korean_name'],
                    market['english_name'],
                    market.get('market_warning', '')
                ])
            
            # 최종 통계
            result = hook.get_first("SELECT COUNT(*) FROM market_code_bithumb")
            total_count = result[0] if result else 0
            
            print(f"✅ 완료. 총 마켓: {total_count}개 (KRW 마켓만)")
            
            return total_count
            
        except Exception as e:
            print(f"❌ 마켓코드 업데이트 실패: {str(e)}")
            raise

    update_markets = PythonOperator(
        task_id='update_bithumb_markets',
        python_callable=update_bithumb_markets,
    )

    # Task 의존성
    create_table >> update_markets