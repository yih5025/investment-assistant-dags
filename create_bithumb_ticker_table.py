from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# 표준 경로 설정
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='create_bithumb_ticker_table',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
    description='Create Bithumb ticker table for real-time data',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['bithumb', 'ticker', 'table', 'setup'],
) as dag:

    create_ticker_table = PostgresOperator(
        task_id='create_bithumb_ticker_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS bithumb_ticker (
                id                    SERIAL PRIMARY KEY,
                market                TEXT NOT NULL,
                trade_date            TEXT,
                trade_time            TEXT,
                trade_date_kst        TEXT,
                trade_time_kst        TEXT,
                trade_timestamp       BIGINT,
                opening_price         DECIMAL,
                high_price            DECIMAL,
                low_price             DECIMAL,
                trade_price           DECIMAL,
                prev_closing_price    DECIMAL,
                change                TEXT,
                change_price          DECIMAL,
                change_rate           DECIMAL,
                signed_change_price   DECIMAL,
                signed_change_rate    DECIMAL,
                trade_volume          DECIMAL,
                acc_trade_price       DECIMAL,
                acc_trade_price_24h   DECIMAL,
                acc_trade_volume      DECIMAL,
                acc_trade_volume_24h  DECIMAL,
                highest_52_week_price DECIMAL,
                highest_52_week_date  TEXT,
                lowest_52_week_price  DECIMAL,
                lowest_52_week_date   TEXT,
                timestamp_field       BIGINT,
                source                TEXT DEFAULT 'bithumb'
            );
            
            -- 성능 최적화 인덱스
            CREATE INDEX IF NOT EXISTS idx_bithumb_ticker_market 
            ON bithumb_ticker(market);
            
            CREATE INDEX IF NOT EXISTS idx_bithumb_ticker_timestamp 
            ON bithumb_ticker(timestamp_field);
            
            CREATE INDEX IF NOT EXISTS idx_bithumb_ticker_market_time 
            ON bithumb_ticker(market, timestamp_field DESC);
        """,
    )