from datetime import datetime, timedelta
import requests, os, logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

BASE_DIR = os.path.dirname(__file__)
SQL_DIR  = os.path.join(BASE_DIR, "sql")

with open(os.path.join(SQL_DIR, "upsert_market_news.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    "owner": "investment_assistant",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ingest_market_news_to_db",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],
    description="Fetch market-moving news (stocks, crypto, commodities) daily",
) as dag:
    
    create_market_news_table = PostgresOperator(
        task_id="create_market_news_table",
        postgres_conn_id="postgres_default",
        sql="create_market_news.sql",
    )

    def fetch_and_upsert_market_news(**ctx):
        pg = PostgresHook(postgres_conn_id="postgres_default")
        api_key = Variable.get("NEWSAPI_API_KEY")
        now      = datetime.utcnow()
        since   = (now - timedelta(days=1)).isoformat() + "Z"
        until   = now.isoformat() + "Z"

        query = (
            # 1) 중앙은행·금리·통화정책
            'Fed OR Federal Reserve OR 금리 OR rate hike OR rate cut OR inflation OR CPI OR PPI '
            'OR quantitative easing OR QE OR tapering OR "interest rate" '
            # 2) 거시경제지표
            'OR GDP OR 실업률 OR unemployment OR ISM OR PMI OR consumer confidence '
            'OR retail sales OR housing OR durable goods '
            # 3) 재정·정치 이슈
            'OR debt ceiling OR fiscal stimulus OR infrastructure bill OR stimulus OR "debt limit" '
            'OR Tariffs OR trade war OR "trade deal" OR Brexit OR election OR impeachment OR sanctions '
            # 4) 주요 기업·섹터 이벤트
            'OR earnings OR 실적 OR merger OR acquisition OR IPO OR bankruptcy OR "stock split" '
            'OR Tesla OR Apple OR Amazon OR Google OR Microsoft '
            # 5) 원자재·에너지·코인
            'OR oil OR crude OR gas OR OPEC OR gold OR silver OR copper '
            'OR bitcoin OR crypto OR ethereum OR cryptocurrency '
            # 6) 시장심리·변동성
            'OR VIX OR volatility OR bull market OR bear OR "market crash"'
        )

        params = {
            'q': query,
            'from': since,
            'to': until,
            'sortBy': 'relevancy',
            'apiKey': api_key,
            'pageSize': 5,
        }

        logging.info(f"[market_news] fetching NewsAPI with params: {params}")
        try:
            resp = requests.get("https://newsapi.org/v2/everything", params=params)
            resp.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                logging.warning("Market News API rate limit(429) 초과, 오늘은 여기까지")
                raise AirflowSkipException("Market News API rate limit(429) 초과, 오늘은 여기까지")
            else:
                raise
        articles = resp.json().get("articles", [])
        logging.info(f"[market_news] received {len(articles)} articles")

        for art in articles:
            upsert_params = {
                "source":        art["source"]["name"],
                "url":           art["url"],
                "author":        art.get("author"),
                "title":         art.get("title"),
                "description":   art.get("description"),
                "content":       art.get("content"),
                "published_at":  art.get("publishedAt"),
            }
            pg.run(UPSERT_SQL, parameters=upsert_params)
            logging.debug(f"[market_news] upserted {upsert_params['url']}")

    fetch_and_upsert = PythonOperator(
        task_id="fetch_and_upsert_market_news",
        python_callable=fetch_and_upsert_market_news,
    )

    create_market_news_table >> fetch_and_upsert