import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

with open(os.path.join(DAGS_SQL_DIR, "upsert_institutional_holders.sql")) as f:
    UPSERT_HOLDERS_SQL = f.read()
    
with open(os.path.join(DAGS_SQL_DIR, "upsert_insider_trading.sql")) as f:
    UPSERT_INSIDER_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

def get_target_symbols():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pg_hook.get_pandas_df("SELECT symbol FROM sp500_companies")
    return df['symbol'].tolist()

def fetch_institutional_holders(**context):
    """기관 보유량 수집"""
    symbols = get_target_symbols()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"🐳 기관 보유량 수집 시작: {len(symbols)}개 종목")
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            # yfinance institutional_holders returns DataFrame
            holders = ticker.institutional_holders
            
            if holders is None or holders.empty:
                continue
                
            for _, row in holders.iterrows():
                # yfinance 데이터 컬럼명 확인 필요 (보통 Holder, Shares, Date Reported, % Out, Value)
                # 데이터프레임 컬럼 인덱스로 접근하거나 이름으로 접근
                try:
                    params = {
                        'symbol': symbol,
                        'holder': row.get('Holder'),
                        'shares': int(row.get('Shares')) if pd.notna(row.get('Shares')) else 0,
                        'date_reported': row.get('Date Reported'),
                        'pct_held': float(row.get('% Out')) * 100 if pd.notna(row.get('% Out')) else 0,
                        'value': float(row.get('Value')) if pd.notna(row.get('Value')) else 0
                    }
                    pg_hook.run(UPSERT_HOLDERS_SQL, parameters=params)
                except Exception as row_e:
                    continue
            
            time.sleep(0.5) # Rate Limit
            
        except Exception as e:
            print(f"Failed Holders {symbol}: {e}")

def fetch_insider_trading(**context):
    """내부자 거래 수집"""
    symbols = get_target_symbols()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"🕵️ 내부자 거래 수집 시작: {len(symbols)}개 종목")
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            # insider_transactions returns DataFrame
            insiders = ticker.insider_transactions
            
            if insiders is None or insiders.empty:
                continue
            
            # 최근 6개월 데이터만 저장
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=180)
            
            for _, row in insiders.iterrows():
                # 날짜 파싱 (Start Date 컬럼 사용)
                date_val = row.get('Start Date')
                if pd.isna(date_val) or date_val < cutoff:
                    continue
                    
                params = {
                    'symbol': symbol,
                    'insider': row.get('Insider'),
                    'position': row.get('Position'),
                    'date': date_val,
                    'type': 'Sale' if 'Sale' in str(row.get('Text', '')) else 'Buy', # Text 컬럼 분석 필요
                    'shares': int(row.get('Shares')) if pd.notna(row.get('Shares')) else 0,
                    'price': float(row.get('Value')) / int(row.get('Shares')) if row.get('Shares') else 0, # 단가 추정
                    'shares_after': int(row.get('Shares Held')) if pd.notna(row.get('Shares Held')) else 0
                }
                pg_hook.run(UPSERT_INSIDER_SQL, parameters=params)
                
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Failed Insiders {symbol}: {e}")

with DAG(
    dag_id='ingest_company_ownership_k8s',
    default_args=default_args,
    schedule_interval='@weekly', # 매주 실행
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR],
    tags=['ownership', 'insider', 'holders', 'yfinance'],
) as dag:

    # 테이블 생성 태스크들
    create_holders = PostgresOperator(
        task_id='create_institutional_holders',
        postgres_conn_id='postgres_default',
        sql='create_institutional_holders.sql',
    )
    
    create_insiders = PostgresOperator(
        task_id='create_insider_trading',
        postgres_conn_id='postgres_default',
        sql='create_insider_trading.sql',
    )

    # 데이터 수집 태스크들
    ingest_holders = PythonOperator(
        task_id='fetch_institutional_holders',
        python_callable=fetch_institutional_holders,
    )
    
    ingest_insiders = PythonOperator(
        task_id='fetch_insider_trading',
        python_callable=fetch_insider_trading,
    )

    # 의존성 설정
    [create_holders, create_insiders] >> ingest_holders >> ingest_insiders