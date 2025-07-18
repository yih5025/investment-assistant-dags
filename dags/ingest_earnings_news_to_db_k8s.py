from datetime import datetime, timedelta
import requests
import os
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_earnings_news_finnhub.sql"), encoding="utf-8") as f:
    UPSERT_NEWS_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_earnings_news_to_db_k8s',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Fetch earnings-related news for companies with upcoming reports',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['earnings', 'news', 'finnhub', 'targeted', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_earnings_news_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_news_finnhub.sql',
    )

    def fetch_and_upsert_earnings_news(**context):
        """실적 발표 예정 기업의 관련 뉴스 수집"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        # 실적 발표 예정 기업 조회
        calendar_entries = hook.get_records("""
            SELECT symbol, report_date
            FROM earnings_calendar
            WHERE report_date BETWEEN current_date - INTERVAL '3 days'
                              AND current_date + INTERVAL '7 days'
        """)
        
        if not calendar_entries:
            raise AirflowSkipException("실적 발표 예정 기업이 없습니다")
        
        print(f"📊 {len(calendar_entries)}개 기업 처리 시작")
        
        for symbol, report_date in calendar_entries:
            try:
                # 실적일 기준 ±14일 뉴스 수집
                to_date = report_date.isoformat()
                from_date = (report_date - timedelta(days=14)).isoformat()
                
                # Finnhub API 호출
                resp = requests.get(
                    "https://finnhub.io/api/v1/company-news",
                    params={
                        "symbol": symbol,
                        "from": from_date,
                        "to": to_date,
                        "token": api_key,
                    },
                    timeout=30
                )
                
                # Rate Limit 처리
                if resp.status_code == 429:
                    print(f"⏰ Rate Limit, 60초 대기")
                    time.sleep(60)
                    continue
                
                resp.raise_for_status()
                articles = resp.json() or []
                
                # 각 기사 저장
                for article in articles:
                    if article.get('url') and article.get('datetime'):
                        published_at = datetime.fromtimestamp(article['datetime']).isoformat()
                        
                        hook.run(UPSERT_NEWS_SQL, parameters={
                            'symbol': symbol,
                            'report_date': report_date,
                            'category': article.get('category', ''),
                            'article_id': article.get('id'),
                            'headline': article.get('headline', ''),
                            'image': article.get('image', ''),
                            'related': article.get('related', ''),
                            'source': article.get('source', ''),
                            'summary': article.get('summary', ''),
                            'url': article['url'],
                            'published_at': published_at,
                        })
                
                time.sleep(1)  # Rate Limit 방지
                
            except Exception as e:
                print(f"❌ {symbol} 실패: {str(e)}")
                continue
        
        # 최종 결과
        result = hook.get_first("SELECT COUNT(*) FROM earnings_news_finnhub")
        total_records = result[0] if result else 0
        print(f"✅ 완료. 총 레코드: {total_records}")
        
        return total_records

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_earnings_news',
        python_callable=fetch_and_upsert_earnings_news,
    )

    # Task 의존성
    create_table >> fetch_upsert