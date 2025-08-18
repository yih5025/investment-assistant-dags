from datetime import datetime, timedelta
import requests
import time
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_market_news_finnhub.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_market_news_finnhub_k8s',
    default_args=default_args,
    schedule_interval='30 * * * *',  # 매시간 20분에 실행
    catchup=False,
    description='Fetch Finnhub market news by category',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['market', 'news', 'finnhub', 'categories', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_market_news_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news_finnhub.sql',
    )

    def fetch_and_upsert_market_news(**context):
        """카테고리별 시장 뉴스 수집"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        categories = ["crypto", "forex", "merger", "general"]
        print(f"📊 {len(categories)}개 카테고리 처리 시작")
        
        for category in categories:
            try:
                # 마지막 뉴스 ID 조회
                last_id_result = hook.get_first(
                    "SELECT COALESCE(MAX(news_id), 0) FROM market_news_finnhub WHERE category = %s",
                    parameters=[category]
                )
                last_id = last_id_result[0] if last_id_result else 0
                
                # API 파라미터 설정
                params = {
                    "category": category,
                    "token": api_key
                }
                if last_id > 0:
                    params["minId"] = last_id
                
                # Finnhub API 호출
                resp = requests.get(
                    "https://finnhub.io/api/v1/news",
                    params=params,
                    timeout=30
                )
                
                # Rate Limit 처리
                if resp.status_code == 429:
                    print(f"⏰ {category}: Rate Limit, 60초 대기")
                    time.sleep(60)
                    continue
                
                # 새 데이터 없음
                if resp.status_code == 204:
                    print(f"📰 {category}: 새 뉴스 없음")
                    continue
                
                resp.raise_for_status()
                
                # JSON 파싱
                try:
                    items = resp.json() or []
                except:
                    print(f"❌ {category}: JSON 파싱 실패")
                    continue
                
                # 각 뉴스 저장
                for item in items:
                    if item.get('id') and item.get('datetime'):
                        hook.run(UPSERT_SQL, parameters={
                            'category': category,
                            'news_id': item['id'],
                            'datetime': datetime.fromtimestamp(item['datetime']),
                            'headline': item.get('headline', ''),
                            'image': item.get('image', ''),
                            'related': item.get('related', ''),
                            'source': item.get('source', ''),
                            'summary': item.get('summary', ''),
                            'url': item.get('url', ''),
                        })
                
                print(f"✅ {category}: {len(items)}개 뉴스 처리")
                time.sleep(2)  # API 호출 간격
                
            except Exception as e:
                print(f"❌ {category} 실패: {str(e)}")
                continue
        
        # 최종 결과
        result = hook.get_first("SELECT COUNT(*) FROM market_news_finnhub")
        total_records = result[0] if result else 0
        print(f"✅ 완료. 총 레코드: {total_records}")
        
        return len(categories)

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_market_news',
        python_callable=fetch_and_upsert_market_news,
    )

    # Task 의존성
    create_table >> fetch_upsert