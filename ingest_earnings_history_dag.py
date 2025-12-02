from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os
import time
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_earnings_history.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

logger = logging.getLogger(__name__)

def fetch_and_process_earnings_history(**context):
    """
    S&P 500 종목의 과거 실적일과 당시 주가 반응을 수집
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 1. 대상 종목 가져오기 (S&P 500)
    # limit을 걸어 테스트 후 전체로 확장 권장
    symbols_df = pg_hook.get_pandas_df("SELECT symbol FROM sp500_companies")
    symbols = symbols_df['symbol'].tolist()
    
    processed_count = 0
    error_count = 0
    
    print(f"🚀 총 {len(symbols)}개 종목에 대한 실적 히스토리 수집 시작")
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            
            # 2. 실적 발표일 가져오기
            earnings_dates = ticker.earnings_dates
            if earnings_dates is None or earnings_dates.empty:
                continue
                
            # 최근 2년 데이터만 필터링 (너무 오래된 건 패턴 의미 감소)
            cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=730)
            recent_earnings = earnings_dates[earnings_dates.index >= cutoff_date]
            
            for date_idx, row in recent_earnings.iterrows():
                report_dt = date_idx.to_pydatetime()
                report_date = report_dt.date()
                
                # 미래 날짜는 스킵
                if report_date > datetime.now().date():
                    continue
                
                # 3. 발표일 전후 주가 데이터 가져오기 (D-5 ~ D+5)
                # 넉넉하게 가져와서 전일 종가와 당일 시가를 찾음
                start_d = report_date - timedelta(days=5)
                end_d = report_date + timedelta(days=5)
                
                hist = ticker.history(start=start_d, end=end_d)
                
                if hist.empty:
                    continue
                
                # Timezone 제거
                hist.index = hist.index.date
                
                if report_date not in hist.index:
                    # 발표일 당일 데이터가 없으면 (휴장일 등) 스킵
                    continue
                    
                # 데이터 위치 찾기
                try:
                    idx_loc = hist.index.get_loc(report_date)
                    if idx_loc == 0: continue # 전일 데이터 없음
                    
                    # D-1 (발표 전일)
                    prev_day_row = hist.iloc[idx_loc - 1]
                    price_before_close = float(prev_day_row['Close'])
                    
                    # D-Day (발표 당일 - 시장 반응)
                    # 참고: 장 마감 후(AMC) 발표인 경우 다음날 시가를 봐야 하지만,
                    # yfinance에는 시간 정보가 부정확할 때가 많아 일단 당일 변동성으로 근사치 계산
                    day_row = hist.iloc[idx_loc]
                    price_open = float(day_row['Open'])
                    price_close = float(day_row['Close'])
                    
                    # 지표 계산
                    gap_pct = ((price_open - price_before_close) / price_before_close) * 100
                    move_pct = ((price_close - price_before_close) / price_before_close) * 100
                    
                    # EPS 정보
                    eps_est = row.get('EPS Estimate')
                    eps_act = row.get('Reported EPS')
                    surprise = row.get('Surprise(%)')
                    
                    # DB 저장 파라미터
                    params = {
                        'symbol': symbol,
                        'report_date': report_date,
                        'eps_estimate': float(eps_est) if pd.notna(eps_est) else None,
                        'eps_actual': float(eps_act) if pd.notna(eps_act) else None,
                        'surprise_pct': float(surprise) if pd.notna(surprise) else None,
                        'price_before_close': price_before_close,
                        'price_open': price_open,
                        'price_close': price_close,
                        'gap_pct': gap_pct,
                        'move_pct': move_pct
                    }
                    
                    pg_hook.run(UPSERT_SQL, parameters=params)
                    processed_count += 1
                    
                except Exception as e:
                    # 인덱스 에러 등은 스킵
                    continue
            
            # API 제한 방지 딜레이
            time.sleep(1.0)
            
            if processed_count % 50 == 0:
                print(f"📊 진행중: {processed_count}개 레코드 저장 완료")
                
        except Exception as e:
            print(f"❌ {symbol} 처리 중 에러: {e}")
            error_count += 1
            
    print(f"✅ 완료: 총 {processed_count}개 실적 기록 저장, {error_count}개 종목 에러")

with DAG(
    dag_id='ingest_earnings_history_k8s',
    default_args=default_args,
    schedule_interval='@weekly',  # 주 1회 업데이트면 충분 (과거 데이터이므로)
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR],
    tags=['earnings', 'history', 'yfinance', 'analysis'],
) as dag:

    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_earnings_history_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_history.sql',
    )

    # 2. 데이터 수집 및 저장
    ingest_data = PythonOperator(
        task_id='fetch_and_store_earnings_history',
        python_callable=fetch_and_process_earnings_history,
    )

    create_table >> ingest_data