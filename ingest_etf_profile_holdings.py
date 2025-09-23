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
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY_3')
        print("🔑 Alpha Vantage API 키 확인 완료")
    except:
        raise ValueError("🔑 ALPHA_VANTAGE_API_KEY_3이 설정되지 않았습니다")
    
    # 🔧 수정된 쿼리: 실제 테이블 구조에 맞게 변경
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 기존 수집된 ETF 심볼 확인
    collected_symbols = set()
    try:
        collected_records = hook.get_records("""
            SELECT DISTINCT symbol FROM etf_profile_holdings
        """)
        collected_symbols = {row[0] for row in collected_records}
        print(f"📊 이미 수집된 ETF: {len(collected_symbols)}개")
    except Exception as e:
        print(f"⚠️ 기존 데이터 확인 실패 (테이블이 없을 수 있음): {e}")
    
    # etf_basic_info에서 아직 수집하지 않은 25개 ETF 선정
    priority_etfs = hook.get_records("""
        SELECT symbol FROM etf_basic_info 
        ORDER BY symbol
        LIMIT 52
    """)
    
    # 아직 수집하지 않은 ETF만 필터링
    uncollected_etfs = []
    for (symbol,) in priority_etfs:
        if symbol not in collected_symbols:
            uncollected_etfs.append(symbol)
            if len(uncollected_etfs) >= 25:
                break
    
    print(f"🎯 오늘 수집 대상: {len(uncollected_etfs)}개 ETF")
    for i, symbol in enumerate(uncollected_etfs, 1):
        print(f"   {i:2d}. {symbol}")
    
    if not uncollected_etfs:
        print("✅ 모든 ETF 데이터 수집 완료!")
        return
    
    collected_count = 0
    
    for i, symbol in enumerate(uncollected_etfs, 1):
        try:
            print(f"\n🔍 [{i:2d}/{len(uncollected_etfs)}] {symbol} 수집 중...")
            
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
                print("⏰ API 제한 도달, 오늘 수집 중단")
                break
            
            # 📊 API 응답 데이터 구조 확인 및 안전한 변환
            def safe_numeric(value):
                """안전한 숫자 변환"""
                if value in [None, '', 'None', 'null']:
                    return None
                try:
                    # 퍼센트 제거 후 변환
                    if isinstance(value, str) and '%' in value:
                        value = value.replace('%', '')
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            def safe_int(value):
                """안전한 정수 변환"""
                if value in [None, '', 'None', 'null']:
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None
            
            # API 응답 데이터 그대로 저장
            etf_data = {
                'symbol': symbol,
                'net_assets': safe_int(data.get('net_assets')),
                'net_expense_ratio': safe_numeric(data.get('net_expense_ratio')),
                'portfolio_turnover': safe_numeric(data.get('portfolio_turnover')),
                'dividend_yield': safe_numeric(data.get('dividend_yield')),
                'inception_date': data.get('inception_date') if data.get('inception_date') else None,
                'leveraged': data.get('leveraged'),
                'sectors': json.dumps(data.get('sectors', []), ensure_ascii=False),  # JSON으로 저장
                'holdings': json.dumps(data.get('holdings', []), ensure_ascii=False)  # JSON으로 저장
            }
            
            # 📈 데이터 상세 정보 출력
            holdings_count = len(data.get('holdings', []))
            sectors_count = len(data.get('sectors', []))
            net_assets = etf_data['net_assets']
            
            print(f"   💰 순자산: ${net_assets:,}" if net_assets else "   💰 순자산: N/A")
            print(f"   📊 보유종목: {holdings_count}개")
            print(f"   🏭 섹터: {sectors_count}개")
            print(f"   📅 설정일: {data.get('inception_date', 'N/A')}")
            
            # DB에 저장
            hook.run(UPSERT_ALPHAVANTAGE_SQL, parameters=etf_data)
            collected_count += 1
            
            print(f"✅ {symbol}: 데이터 저장 완료")
            
            # API 제한 준수 (12초 대기)
            if i < len(uncollected_etfs):
                print(f"⏰ 12초 대기...")
                time.sleep(12)
            
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {e}")
            continue
    
    print(f"\n🎯 수집 완료: {collected_count}/{len(uncollected_etfs)}개 ETF")

def validate_profile_holdings_data(**context):
    """Alpha Vantage 데이터 검증"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # 오늘 수집 통계
        today_collected = hook.get_first("""
            SELECT COUNT(*) FROM etf_profile_holdings 
            WHERE DATE(collected_at) = CURRENT_DATE
        """)[0]
        
        # 전체 진행률 (실제 테이블 구조에 맞게 수정)
        total_etfs = hook.get_first("""
            SELECT COUNT(*) FROM etf_basic_info
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
                    CASE 
                        WHEN holdings::text != '[]' AND holdings IS NOT NULL 
                        THEN json_array_length(holdings::json) 
                        ELSE 0 
                    END as holdings_count,
                    CASE 
                        WHEN sectors::text != '[]' AND sectors IS NOT NULL 
                        THEN json_array_length(sectors::json) 
                        ELSE 0 
                    END as sectors_count,
                    net_assets,
                    dividend_yield
                FROM etf_profile_holdings 
                WHERE DATE(collected_at) = CURRENT_DATE
                ORDER BY holdings_count DESC
            """)
            
            print(f"\n📈 오늘 수집된 ETF 상세:")
            for symbol, holdings_count, sectors_count, net_assets, dividend_yield in quality_stats:
                net_assets_str = f"${net_assets:,.0f}" if net_assets else "N/A"
                dividend_str = f"{dividend_yield:.2f}%" if dividend_yield else "N/A"
                print(f"   {symbol}: Holdings {holdings_count}개, Sectors {sectors_count}개, 순자산 {net_assets_str}, 배당률 {dividend_str}")
        
        # 전체 수집 현황 요약
        print(f"\n📋 전체 ETF 수집 현황:")
        print(f"   📊 기본 정보: {total_etfs}개 ETF")
        print(f"   📈 상세 데이터: {completed_etfs}개 ETF ({(completed_etfs/total_etfs*100):.1f}%)")
        
        if completed_etfs >= total_etfs:
            print("🎉 모든 ETF 데이터 수집 완료!")
        else:
            print(f"⏳ 남은 작업: {total_etfs - completed_etfs}개 ETF")
            
    except Exception as e:
        print(f"❌ 데이터 검증 중 오류 발생: {e}")

# DAG 정의
with DAG(
    dag_id='ingest_etf_profile_holdings',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # 매일 오전 8시
    catchup=False,
    description='Alpha Vantage ETF 전체 데이터 수집 (실제 테이블 구조 반영)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['etf', 'alphavantage', 'complete_data', 'json_storage', 'k8s', 'fixed'],
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