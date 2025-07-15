from datetime import datetime, timedelta
import requests
import os
import logging
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

BASE_DIR = os.path.dirname(__file__)
SQL_DIR  = os.path.join(BASE_DIR, "sql")

with open(os.path.join(SQL_DIR, "upsert_earnings_news_finnhub.sql"), encoding="utf-8") as f:
    UPSERT_NEWS_SQL = f.read()

default_args = {
    "owner": "investment_assistant",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "ingest_earnings_news_to_db",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],
    description='Fetch earnings calendar entries and upsert related news',
) as dag:

    create_news_table = PostgresOperator(
        task_id='create_earnings_news_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_news_finnhub.sql',
    )

    def fetch_calendar_entries(**ctx):
        pg = PostgresHook(postgres_conn_id='postgres_default')
        rows = pg.get_records("""
            SELECT symbol, report_date
            FROM public.earnings_calendar
            WHERE report_date
            BETWEEN current_date - INTERVAL '3 days'
                AND current_date + INTERVAL '7 days'
            ORDER BY report_date;
        """)
        logging.info(f"[fetch_calendar_entries] fetched {len(rows)} calendar rows")
        logging.debug(f"[fetch_calendar_entries] rows={rows}")
        ctx['ti'].xcom_push(key='calendar_entries', value=rows)

    fetch_calendar = PythonOperator(
        task_id='fetch_calendar_entries',
        python_callable=fetch_calendar_entries,
    )


    def fetch_and_upsert_news(**ctx):
        entries = ctx['ti'].xcom_pull(task_ids='fetch_calendar_entries', key='calendar_entries')
        if not entries:
            raise AirflowSkipException("No calendar entries found")

        pg            = PostgresHook(postgres_conn_id='postgres_default')
        api_key       = Variable.get('FINNHUB_API_KEY')
        processed     = []

        for symbol, report_date in entries:
            logging.info(f"[fetch_and_upsert_news] ▶ 처리 시작: {symbol}, report_date={report_date}")
            to_date   = report_date.isoformat()
            from_date = (report_date - timedelta(days=14)).isoformat()

            params = {
                "symbol": symbol,
                "from":   from_date,
                "to":     to_date,
                "token":  api_key,
            }
            try:
                resp = requests.get("https://finnhub.io/api/v1/company-news", params=params)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    logging.warning(f"[fetch_and_upsert_news] {symbol} → rate limit(429), 60초 대기 후 스킵")
                    processed.append(symbol)
                    time.sleep(60)
                    continue
                else:
                    raise

            articles = resp.json() or []
            logging.info(f"[fetch_and_upsert_news] {symbol} → {len(articles)}개 기사 수집")

            for art in articles:
                published_at = datetime.fromtimestamp(art["datetime"]).isoformat()
                upsert_params = {
                    "symbol":       symbol,
                    "report_date":  report_date,
                    "category":     art.get("category"),
                    "article_id":   art.get("id"),
                    "headline":     art.get("headline"),
                    "image":        art.get("image"),
                    "related":      art.get("related"),
                    "source":       art.get("source"),
                    "summary":      art.get("summary"),
                    "url":          art.get("url"),
                    "published_at": published_at,
                }
                pg.run(UPSERT_NEWS_SQL, parameters=upsert_params)

            processed.append(symbol)
            time.sleep(1)

        logging.info(f"[fetch_and_upsert_news] 전체 처리 완료: {processed}")

    upsert_news = PythonOperator(
        task_id='fetch_and_upsert_news',
        python_callable=fetch_and_upsert_news,
    )

    create_news_table >> fetch_calendar >> upsert_news
