from datetime import datetime, timedelta
import requests, os, logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_DIR = os.path.dirname(__file__)
SQL_DIR  = os.path.join(BASE_DIR, "sql")

with open(os.path.join(SQL_DIR, "upsert_company_news.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    "owner": "investment_assistant",
    "start_date": datetime(2025,6,1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ingest_company_news_to_db",
    default_args=default_args,
    schedule_interval="10 * * * *",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_company_news_table",
        postgres_conn_id="postgres_default",
        sql="create_company_news.sql",
    )

    def fetch_and_upsert(**ctx):
        pg      = PostgresHook(postgres_conn_id="postgres_default")
        rows    = pg.get_records("""SELECT symbol FROM public.sp500_top50""")
        symbols = [row[0] for row in rows]
        api_key = Variable.get("FINNHUB_API_KEY")

        for symbol in symbols:
            resp = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={
                    "symbol": symbol,
                    "from":   (datetime.today()-timedelta(hours=1)).strftime("%Y-%m-%d"),
                    "to":     datetime.today().strftime("%Y-%m-%d"),
                    "token":  api_key,
                }
            )
            resp.raise_for_status()
            for art in resp.json() or []:
                published = datetime.fromtimestamp(art["datetime"]).isoformat()
                params = {
                    "symbol":      symbol,
                    "source":      art.get("source"),
                    "url":         art.get("url"),
                    "title":       art.get("headline"),
                    "description": art.get("summary"),
                    "content":     art.get("summary"),
                    "published_at": published,
                }
                pg.run(UPSERT_SQL, parameters=params)

    fetch_upsert = PythonOperator(
        task_id="fetch_and_upsert_company_news",
        python_callable=fetch_and_upsert,
    )

    create_table >> fetch_upsert
