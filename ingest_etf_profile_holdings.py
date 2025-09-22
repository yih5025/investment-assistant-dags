# dags/ingest_etf_alphavantage_data_k8s.py
import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_etf_profile_holdings.sql"), encoding="utf-8") as f:
    UPSERT_ALPHAVANTAGE_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def collect_etf_profile_holdings_data(**context):
    """Alpha Vantage ETF 데이터 수집 (API 응답 그대로 저장)"""
    import requests
    import time
    
    print("🚀 Alpha Vantage ETF 데이터 수집 시작...")
    
    # API 키 확인
    try:
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
        print("🔑 Alpha Vantage API 키 확인 완료")
    except:
        raise ValueError("🔑 ALPHA_VANTAGE_API_KEY가 설정되지 않았습니다")
    
    # 25개 ETF 선정
    hook = PostgresHook(postgres_conn_id='postgres_default')
    priority_etfs = hook.get_records("""
        SELECT symbol FROM etf_basic_info 
        WHERE yahoo_collected = TRUE 
        AND symbol NOT IN (SELECT symbol FROM etf_profile_holdings)
        ORDER BY 
            CASE WHEN aum IS NULL THEN 1 ELSE 0 END,
            aum DESC NULLS LAST,
            popularity_rank 
        LIMIT 25
    """)
    
    print(f"🎯 오늘 수집 대상: {len(priority_etfs)}개 ETF")
    
    collected_count = 0
    
    for i, (symbol,) in enumerate(priority_etfs, 1):
        try:
            print(f"🔍 [{i:2d}/25] {symbol} 수집 중...")
            
            # Alpha Vantage API 호출
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'ETF_PROFILE',
                'symbol': symbol,
                'apikey': api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # API 에러 체크
            if 'Error Message' in data:
                print(f"❌ {symbol}: {data['Error Message']}")
                continue
            
            if 'Note' in data:
                print(f"⚠️ API 제한: {data['Note']}")
                break
            
            # API 응답 데이터 그대로 저장
            etf_data = {
                'symbol': symbol,
                'net_assets': int(data.get('net_assets', 0)) if data.get('net_assets') else None,
                'net_expense_ratio': float(data.get('net_expense_ratio', 0)) if data.get('net_expense_ratio') else None,
                'portfolio_turnover': float(data.get('portfolio_turnover', 0)) if data.get('portfolio_turnover') else None,
                'dividend_yield': float(data.get('dividend_yield', 0)) if data.get('dividend_yield') else None,
                'inception_date': data.get('inception_date'),
                'leveraged': data.get('leveraged'),
                'sectors': json.dumps(data.get('sectors', [])),  # JSON으로 저장
                'holdings': json.dumps(data.get('holdings', []))  # JSON으로 저장
            }
            
            # DB에 저장
            hook.run(UPSERT_ALPHAVANTAGE_SQL, parameters=etf_data)
            
            # etf_basic_info 테이블에 수집 완료 표시
            hook.run("""
                UPDATE etf_basic_info 
                SET alphavantage_collected = TRUE, updated_at = NOW() 
                WHERE symbol = %s
            """, parameters=(symbol,))
            
            collected_count += 1
            
            print(f"✅ {symbol}: 수집 완료")
            print(f"   - Holdings: {len(data.get('holdings', []))}개")
            print(f"   - Sectors: {len(data.get('sectors', []))}개")
            
            # API 제한 준수 (12초 대기)
            if i < len(priority_etfs):
                print(f"⏰ 12초 대기...")
                time.sleep(12)
            
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {e}")
            continue
    
    print(f"\n🎯 수집 완료: {collected_count}/25개 ETF")

def validate_profile_holdings_data(**context):
    """Alpha Vantage 데이터 검증"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 오늘 수집 통계
    today_collected = hook.get_first("""
        SELECT COUNT(*) FROM etf_profile_holdings 
        WHERE DATE(collected_at) = CURRENT_DATE
    """)[0]
    
    # 전체 진행률
    total_etfs = hook.get_first("""
        SELECT COUNT(*) FROM etf_basic_info WHERE yahoo_collected = TRUE
    """)[0]
    
    completed_etfs = hook.get_first("""
        SELECT COUNT(*) FROM etf_profile_holdings
    """)[0]
    
    print(f"📊 Alpha Vantage 수집 현황:")
    print(f"   오늘 수집: {today_collected}개")
    print(f"   전체 진행률: {completed_etfs}/{total_etfs} ({(completed_etfs/total_etfs*100):.1f}%)")
    print(f"   남은 ETF: {total_etfs - completed_etfs}개")
    
    # 데이터 품질 체크
    if today_collected > 0:
        quality_stats = hook.get_records("""
            SELECT 
                symbol,
                CASE WHEN holdings::text != '[]' THEN jsonb_array_length(holdings) ELSE 0 END as holdings_count,
                CASE WHEN sectors::text != '[]' THEN jsonb_array_length(sectors) ELSE 0 END as sectors_count
            FROM etf_profile_holdings 
            WHERE DATE(collected_at) = CURRENT_DATE
            ORDER BY holdings_count DESC
        """)
        
        print(f"\n📈 오늘 수집된 ETF 상세:")
        for symbol, holdings_count, sectors_count in quality_stats:
            print(f"   {symbol}: Holdings {holdings_count}개, Sectors {sectors_count}개")

# DAG 정의
with DAG(
    dag_id='ingest_etf_profile_holdings',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # 매일 오전 8시
    catchup=False,
    description='Alpha Vantage ETF 전체 데이터 수집 (API 응답 그대로 저장)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['etf', 'alphavantage', 'complete_data', 'json_storage', 'k8s'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_etf_profile_holdings_table',
        postgres_conn_id='postgres_default',
        sql='create_etf_profile_holdings.sql',
    )
    
    # 2. Alpha Vantage 데이터 수집
    collect_data = PythonOperator(
        task_id='collect_profile_holdings_data',
        python_callable=collect_etf_profile_holdings_data,
    )
    
    # 3. 데이터 검증
    validate_data = PythonOperator(
        task_id='validate_profile_holdings_data',
        python_callable=validate_profile_holdings_data,
    )
    
    # Task 의존성
    create_table >> collect_data >> validate_data