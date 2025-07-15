from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_sp500_top50',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],
) as dag:

    create_sp500_table = PostgresOperator(
        task_id='create_sp500_table',
        postgres_conn_id='postgres_default',
        sql='create_sp500_top50.sql',
    )

    insert_sp500_data = PostgresOperator(
        task_id='insert_sp500_data',
        postgres_conn_id='postgres_default',
        sql='insert_sp500_top50.sql',
    )

    create_sp500_table >> insert_sp500_data
