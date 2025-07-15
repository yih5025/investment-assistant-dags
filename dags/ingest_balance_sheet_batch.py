from datetime import datetime, timedelta
import os, requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
with open(os.path.join(SQL_DIR, "upsert_balance_sheet.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    "owner": "investment_assistant",
    "start_date": datetime(2025, 6, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingest_balance_sheet_batch",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],
) as dag:

    def fetch_symbols(**ctx):
        offset = int(Variable.get("BALANCE_SHEET_OFFSET", default_var=0))
        if offset >= 50:
            raise AirflowSkipException("All symbols processed")

        pg = PostgresHook(postgres_conn_id="postgres_default")

        if offset > 0:
            prev_records = pg.get_records(
                """
                SELECT symbol
                FROM public.sp500_top50
                ORDER BY symbol
                LIMIT %s
                """,
                parameters=[offset],
            )
            prev_symbols = [r[0] for r in prev_records]
            logging.info(f"[fetch_symbols] already_processed_count={len(prev_symbols)}, symbols={prev_symbols}")
        else:
            logging.info("[fetch_symbols] no symbols processed yet")

        batch_records = pg.get_records(
            """
            SELECT symbol
            FROM public.sp500_top50
            ORDER BY symbol
            LIMIT 10
            OFFSET %s
            """,
            parameters=[offset],
        )
        symbols = [r[0] for r in batch_records]
        
        if not symbols:
            raise AirflowSkipException("No symbols found in this batch")

        logging.info(f"[fetch_symbols] this_batch_count={len(symbols)}, symbols={symbols}")

        next_offset = offset + len(symbols)
        Variable.set("BALANCE_SHEET_OFFSET", next_offset)
        logging.info(f"[fetch_symbols] updated BALANCE_SHEET_OFFSET to {next_offset}")

        ctx["ti"].xcom_push(key="symbols", value=symbols)

    fetch_task = PythonOperator(
        task_id="fetch_symbols",
        python_callable=fetch_symbols,
    )


    def upsert_balance_sheet(**ctx):
        symbols = ctx["ti"].xcom_pull(task_ids="fetch_symbols", key="symbols")
        pg = PostgresHook(postgres_conn_id="postgres_default")
        api_key = Variable.get("ALPHA_VANTAGE_API_KEY")

        for symbol in symbols:
            resp = requests.get(
                "https://www.alphavantage.co/query",
                params={"function": "BALANCE_SHEET", "symbol": symbol, "apikey": api_key},
            )
            resp.raise_for_status()
            for row in resp.json().get("quarterlyReports", []):
                params = {
                    k: None if v in (None, "None", "") else v
                    for k, v in row.items()
                }
                params["symbol"] = symbol
                pg.run(UPSERT_SQL, parameters=params)

    upsert_task = PythonOperator(
        task_id="upsert_balance_sheet",
        python_callable=upsert_balance_sheet,
    )
    
    fetch_task >> upsert_task
