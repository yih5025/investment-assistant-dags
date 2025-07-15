from datetime import datetime, timedelta
from io import StringIO
import csv, requests
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import os

DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
with open(os.path.join(DAGS_SQL_DIR, "upsert_earnings_calendar.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

INITDB_SQL_DIR = "/opt/airflow/initdb"

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ingest_earnings_calendar_to_db',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    description='Fetch 12-month earnings calendar and upsert into Postgres',
    template_searchpath=['/opt/airflow/initdb', '/opt/airflow/dags/sql'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_earnings_calendar_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_calendar.sql',
    )

    def fetch_calendar(**ctx):
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "EARNINGS_CALENDAR",
                "apikey": api_key,
                "horizon": "12month"
            },
        )
        resp.raise_for_status()
        rows = list(csv.DictReader(StringIO(resp.text)))
        ctx['ti'].xcom_push(key='calendar_rows', value=rows)

    fetch = PythonOperator(
        task_id='fetch_earnings_calendar',
        python_callable=fetch_calendar,
    )

    def upsert_calendar(**ctx):
        rows = ctx['ti'].xcom_pull(task_ids='fetch_earnings_calendar', key='calendar_rows')
        hook = PostgresHook(postgres_conn_id='postgres_default')
        for r in rows:
            hook.run(UPSERT_SQL, parameters=(
                r['symbol'],
                r['name'],
                r['reportDate'],
                r['fiscalDateEnding'],
                Decimal(r['estimate']) if r['estimate'] else None,
                r['currency'],
            ))

    upsert = PythonOperator(
        task_id='upsert_earnings_calendar',
        python_callable=upsert_calendar,
    )

    create_table >> fetch >> upsert
