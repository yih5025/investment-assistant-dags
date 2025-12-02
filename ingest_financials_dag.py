import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==========================================
# 1. 설정 및 경로
# ==========================================
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 로드 헬퍼 함수
def load_sql(filename):
    path = os.path.join(DAGS_SQL_DIR, filename)
    if os.path.exists(path):
        with open(path, "r", encoding='utf-8') as f:
            return f.read()
    raise FileNotFoundError(f"SQL file not found: {path}")

# Upsert SQL 로드
try:
    UPSERT_BS_SQL = load_sql("upsert_balance_sheet_yfinance.sql")
    UPSERT_INC_SQL = load_sql("upsert_income_stmt.sql")
    UPSERT_CF_SQL = load_sql("upsert_cash_flow.sql")
except Exception as e:
    logging.error(f"SQL 로드 실패: {e}")
    # DAG 파싱 에러를 방지하기 위해 빈 문자열 처리 (실행 시 에러 남)
    UPSERT_BS_SQL = UPSERT_INC_SQL = UPSERT_CF_SQL = ""

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

# ==========================================
# 2. 헬퍼 함수
# ==========================================

def get_sp500_symbols():
    """DB에서 S&P 500 종목 리스트 가져오기"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pg_hook.get_pandas_df("SELECT symbol FROM sp500_companies WHERE symbol IS NOT NULL")
    return df['symbol'].tolist()

def safe_get(df_row, keys):
    """
    여러 키 후보 중 하나라도 존재하는 값을 찾아 반환 (Yahoo 데이터 키 변경 대응)
    예: ['Total Assets', 'TotalAssets'] 중 하나라도 있으면 값 반환
    """
    for key in keys:
        if key in df_row.index:
            val = df_row[key]
            if pd.notna(val):
                return float(val)
    return None

# ==========================================
# 3. 메인 로직
# ==========================================

def fetch_and_store_financials(**context):
    """S&P 500 전 종목의 재무제표 3종 세트 수집 및 DB 저장"""
    symbols = get_sp500_symbols()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"🚀 총 {len(symbols)}개 기업 재무제표 수집 시작 (Source: yfinance)")
    
    success_count = 0
    error_count = 0
    
    for idx, symbol in enumerate(symbols):
        try:
            ticker = yf.Ticker(symbol)
            
            # -------------------------------------------------------
            # A. Balance Sheet (재무상태표)
            # -------------------------------------------------------
            bs = ticker.quarterly_balance_sheet
            if bs is not None and not bs.empty:
                # Transpose: (항목 x 날짜) -> (날짜 x 항목) 으로 변환해야 처리하기 쉬움
                bs_T = bs.T
                for date_idx, row in bs_T.iterrows():
                    fiscal_date = date_idx.date()
                    
                    params = {
                        'symbol': symbol,
                        'fiscal_date_ending': fiscal_date,
                        'total_assets': safe_get(row, ['Total Assets', 'TotalAssets']),
                        'total_liabilities': safe_get(row, ['Total Liabilities Net Minority Interest', 'Total Liabilities', 'TotalLiabilities']),
                        'total_equity': safe_get(row, ['Stockholders Equity', 'Total Stockholder Equity', 'TotalEquity']),
                        'cash_and_equivalents': safe_get(row, ['Cash And Cash Equivalents', 'Cash', 'CashAndCashEquivalents']),
                        'total_debt': safe_get(row, ['Total Debt', 'TotalDebt']),
                        'net_debt': safe_get(row, ['Net Debt', 'NetDebt']),
                        'working_capital': safe_get(row, ['Working Capital', 'WorkingCapital'])
                    }
                    pg_hook.run(UPSERT_BS_SQL, parameters=params)

            # -------------------------------------------------------
            # B. Income Statement (손익계산서)
            # -------------------------------------------------------
            inc = ticker.quarterly_income_stmt
            if inc is not None and not inc.empty:
                inc_T = inc.T
                for date_idx, row in inc_T.iterrows():
                    fiscal_date = date_idx.date()
                    
                    params = {
                        'symbol': symbol,
                        'fiscal_date_ending': fiscal_date,
                        'total_revenue': safe_get(row, ['Total Revenue', 'TotalRevenue']),
                        'gross_profit': safe_get(row, ['Gross Profit', 'GrossProfit']),
                        'operating_income': safe_get(row, ['Operating Income', 'OperatingIncome']),
                        'net_income': safe_get(row, ['Net Income', 'NetIncome']),
                        'ebitda': safe_get(row, ['EBITDA', 'Ebitda']),
                        'basic_eps': safe_get(row, ['Basic EPS', 'BasicEPS']),
                        'diluted_eps': safe_get(row, ['Diluted EPS', 'DilutedEPS'])
                    }
                    pg_hook.run(UPSERT_INC_SQL, parameters=params)

            # -------------------------------------------------------
            # C. Cash Flow (현금흐름표)
            # -------------------------------------------------------
            cf = ticker.quarterly_cashflow
            if cf is not None and not cf.empty:
                cf_T = cf.T
                for date_idx, row in cf_T.iterrows():
                    fiscal_date = date_idx.date()
                    
                    params = {
                        'symbol': symbol,
                        'fiscal_date_ending': fiscal_date,
                        'operating_cashflow': safe_get(row, ['Operating Cash Flow', 'OperatingCashFlow']),
                        'investing_cashflow': safe_get(row, ['Investing Cash Flow', 'InvestingCashFlow']),
                        'financing_cashflow': safe_get(row, ['Financing Cash Flow', 'FinancingCashFlow']),
                        'capital_expenditures': safe_get(row, ['Capital Expenditure', 'CapitalExpenditures']),
                        'free_cash_flow': safe_get(row, ['Free Cash Flow', 'FreeCashFlow'])
                    }
                    pg_hook.run(UPSERT_CF_SQL, parameters=params)
            
            success_count += 1
            
            # 로깅 및 진행률 표시
            if (idx + 1) % 10 == 0:
                print(f"📊 진행률: {idx + 1}/{len(symbols)} 완료 ({symbol})")
                
            # Rate Limit 방지 (짧게 0.5초 대기)
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ {symbol} 처리 실패: {e}")
            error_count += 1
            continue

    print(f"✅ 전체 재무제표 수집 완료: {success_count}개 성공, {error_count}개 실패")

# ==========================================
# 4. DAG 정의
# ==========================================

with DAG(
    dag_id='ingest_financials_dag',  # 요청하신 DAG ID
    default_args=default_args,
    schedule_interval='@weekly',     # 재무제표는 자주 변하지 않으므로 주간 실행 권장
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR],
    tags=['financials', 'yfinance', 'sp500', 'balance_sheet_yfinance', 'income', 'cash_flow'],
) as dag:

    # 1. 테이블 생성 태스크 (Init)
    create_income_table = PostgresOperator(
        task_id='create_income_stmt_table',
        postgres_conn_id='postgres_default',
        sql='create_income_stmt.sql',
    )
    
    create_cashflow_table = PostgresOperator(
        task_id='create_cash_flow_table',
        postgres_conn_id='postgres_default',
        sql='create_cash_flow.sql',
    )
    
    create_balance_table = PostgresOperator(
        task_id='create_balance_sheet_yfinance_table',
        postgres_conn_id='postgres_default',
        sql='create_balance_sheet_yfinance.sql',
    )

    # 2. 데이터 수집 및 저장 태스크
    ingest_task = PythonOperator(
        task_id='fetch_and_upsert_financials',
        python_callable=fetch_and_store_financials,
    )

    # 3. 의존성 설정: 테이블 생성이 모두 끝난 후 데이터 수집 시작
    [create_income_table, create_cashflow_table, create_balance_table] >> ingest_task