from datetime import datetime, timedelta
import requests
import os

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
with open(os.path.join(DAGS_SQL_DIR, "upsert_market_news.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_market_newsapi_to_db_k8s',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Fetch market-moving news (economics, politics, Fed) via NewsAPI',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['market', 'news', 'newsapi', 'economics', 'politics', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_market_news_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news.sql',
    )

    def fetch_and_upsert_market_news(**context):
        """거시경제 영향 뉴스 수집 (NewsAPI)"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('NEWSAPI_API_KEY')
        
        # 시간 범위 설정 (어제부터 오늘까지)
        now = datetime.utcnow()
        since = (now - timedelta(days=1)).isoformat() + "Z"
        until = now.isoformat() + "Z"
        
        # 효과적인 검색 키워드 (시장에 영향을 주는 주요 이슈들)
        keywords = [
            # 중앙은행 및 통화정책
            "Federal Reserve OR Fed OR interest rate OR inflation OR CPI",
            "ECB OR Bank of Japan OR BOJ OR monetary policy",
            
            # 거시경제 지표
            "GDP OR unemployment OR jobs report OR retail sales",
            "housing market OR consumer confidence OR PMI",
            
            # 정치 및 정책
            "Biden OR Trump OR Congress OR stimulus OR infrastructure",
            "debt ceiling OR government shutdown OR election",
            
            # 국제 정치/외교
            "China trade OR Russia sanctions OR Ukraine war",
            "OPEC OR oil price OR energy crisis",
            
            # 주요 기업 및 시장
            "earnings OR IPO OR merger OR acquisition",
            "stock market OR Wall Street OR S&P 500",
            
            # 암호화폐 및 신기술
            "Bitcoin OR cryptocurrency OR blockchain OR AI regulation"
        ]
        
        print(f"📊 {len(keywords)}개 키워드 그룹으로 뉴스 수집 시작")
        
        total_articles = 0
        
        for i, keyword_group in enumerate(keywords):
            try:
                # NewsAPI 파라미터
                params = {
                    'q': keyword_group,
                    'from': since,
                    'to': until,
                    'sortBy': 'relevancy',
                    'language': 'en',
                    'apiKey': api_key,
                    'pageSize': 10,  # 그룹당 최대 10개 기사
                }
                
                # NewsAPI 호출
                resp = requests.get(
                    "https://newsapi.org/v2/everything",
                    params=params,
                    timeout=30
                )
                
                # Rate Limit 처리
                if resp.status_code == 429:
                    print(f"⏰ Rate Limit 도달, 오늘은 여기까지")
                    raise AirflowSkipException("NewsAPI Rate Limit 초과")
                
                resp.raise_for_status()
                data = resp.json()
                articles = data.get("articles", [])
                
                # 각 기사 저장
                for article in articles:
                    if article.get('url') and article.get('publishedAt'):
                        hook.run(UPSERT_SQL, parameters={
                            'source': article["source"]["name"] if article.get("source") else "",
                            'url': article["url"],
                            'author': article.get("author", ""),
                            'title': article.get("title", ""),
                            'description': article.get("description", ""),
                            'content': article.get("content", ""),
                            'published_at': article["publishedAt"],
                        })
                
                total_articles += len(articles)
                print(f"✅ 그룹 {i+1}: {len(articles)}개 기사 수집")
                
                # API 호출 간격
                if i < len(keywords) - 1:  # 마지막이 아니면 대기
                    import time
                    time.sleep(1)
                
            except Exception as e:
                print(f"❌ 키워드 그룹 {i+1} 실패: {str(e)}")
                continue
        
        # 최종 결과
        result = hook.get_first("SELECT COUNT(*) FROM market_news")
        total_records = result[0] if result else 0
        print(f"✅ 완료. 오늘 수집: {total_articles}개, 총 레코드: {total_records}")
        
        return total_articles

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_market_news',
        python_callable=fetch_and_upsert_market_news,
    )

    # Task 의존성
    create_table >> fetch_upsert