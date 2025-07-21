# dags/create_finnhub_trades_table.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 7, 20),
    'retries': 0
}

with DAG(
    'create_finnhub_trades_table',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    description='Finnhub trades 테이블 및 트리거 생성',
    tags=['setup', 'finnhub', 'database']
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_finnhub_trades_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS finnhub_trades (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            price NUMERIC(20,8),
            volume BIGINT,
            timestamp_ms BIGINT,
            trade_conditions TEXT[],
            source TEXT DEFAULT 'finnhub_websocket',
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    
    # 2. 인덱스 생성
    create_indexes = PostgresOperator(
        task_id='create_indexes',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE INDEX IF NOT EXISTS idx_finnhub_trades_symbol 
        ON finnhub_trades(symbol);
        
        CREATE INDEX IF NOT EXISTS idx_finnhub_trades_timestamp 
        ON finnhub_trades(timestamp_ms);
        
        CREATE INDEX IF NOT EXISTS idx_finnhub_trades_symbol_timestamp 
        ON finnhub_trades(symbol, timestamp_ms);
        """
    )
    
    # 3. top_gainers 트리거 생성
    create_trigger = PostgresOperator(
        task_id='create_top_gainers_trigger',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE OR REPLACE FUNCTION notify_top_gainers_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('top_gainers_updated', json_build_object(
                'batch_id', NEW.batch_id,
                'timestamp', EXTRACT(EPOCH FROM NOW())
            )::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS top_gainers_update_trigger ON top_gainers;
        CREATE TRIGGER top_gainers_update_trigger
            AFTER INSERT ON top_gainers
            FOR EACH ROW
            WHEN (NEW.category = 'top_gainers')
            EXECUTE FUNCTION notify_top_gainers_update();
        """
    )
    
    # 실행 순서
    create_table >> create_indexes >> create_trigger