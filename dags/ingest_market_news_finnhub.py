from datetime import datetime, timedelta
import requests, time, logging, os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from json import JSONDecodeError

BASE_DIR = os.path.dirname(__file__)
SQL_DIR  = os.path.join(BASE_DIR, "sql")
with open(os.path.join(SQL_DIR, "upsert_market_news_finnhub.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    "owner": "investment_assistant",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "ingest_market_news_finnhub",
    default_args=default_args,
    schedule_interval="20 * * * *",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    description="Fetch Finnhub market-news per category",
) as dag:

    create_table = PostgresOperator(
        task_id="create_market_news_table",
        postgres_conn_id="postgres_default",
        sql="create_market_news_finnhub.sql",
    )


    def fetch_and_upsert(**ctx):
        categories = ["crypto", "forex", "merger", "general"]
        api_key    = Variable.get("FINNHUB_API_KEY")
        pg         = PostgresHook(postgres_conn_id="postgres_default")

        for cat in categories:
            last_id = pg.get_first(
                "SELECT COALESCE(MAX(news_id), 0) FROM public.market_news_finnhub WHERE category = %s",
                parameters=[cat]
            )[0] or 0

            params = {
                "category": cat,
                "token":    api_key
            }
            if last_id > 0:
                params["minId"] = last_id

            logging.info(f"[market-news] Fetching category={cat} with minId={params.get('minId', 0)}")

            resp = requests.get("https://finnhub.io/api/v1/news", params=params)
            logging.info(f"[market-news] {cat} → HTTP {resp.status_code}")
            logging.debug(f"[market-news] {cat} → body snippet: {resp.text[:200]!r}")

            if resp.status_code == 429:
                logging.warning(f"[market-news] {cat} → rate limit, skipping this category")
                time.sleep(60)
                continue
            if resp.status_code == 204:
                logging.info(f"[market-news] {cat} → no new items (204), skipping")
                continue

            body = resp.text.strip()
            if not body:
                logging.warning(f"[market-news] {cat} → empty body, skipping")
                continue

            try:
                items = resp.json() or []
            except JSONDecodeError:
                logging.warning(f"[market-news] {cat} → invalid JSON, skipping")
                continue

            for item in items:
                up = { 
                    "category":     cat,
                    "news_id":      item["id"],
                    "datetime":     datetime.fromtimestamp(item["datetime"]),
                    "headline":     item["headline"],
                    "image":        item.get("image"),
                    "related":      item.get("related"),
                    "source":       item.get("source"),
                    "summary":      item.get("summary"),
                    "url":          item.get("url"),
                }
                pg.run(UPSERT_SQL, parameters=up)

            time.sleep(2)

    fetch_upsert = PythonOperator(
        task_id="fetch_and_upsert_market_news",
        python_callable=fetch_and_upsert,
    )

    create_table >> fetch_upsert
