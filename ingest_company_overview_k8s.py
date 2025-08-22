# dags/ingest_company_overview_k8s.py
from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_company_overview.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def get_daily_symbols_batch(**context):
    """
    SP500 회사들을 일별로 50개씩 분할하여 조회
    2개 API 키로 하루 50개 → 10일로 500개 완료
    월별 batch_id로 데이터 히스토리 관리
    """
    # 실행 날짜 기준으로 배치 번호 계산 (1-10)
    execution_date = context['execution_date']
    
    # 월의 몇 번째 주기인지 계산 (1일차, 2일차... 10일차)
    day_of_month = execution_date.day
    batch_number = ((day_of_month - 1) % 10) + 1  # 1~10 순환
    
    print(f"📅 실행 날짜: {execution_date.strftime('%Y-%m-%d')}")
    print(f"🔄 배치 번호: {batch_number}/10")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 🆕 월별 batch_id 계산 및 관리
    current_month = execution_date.strftime('%Y-%m')
    print(f"📆 현재 연월: {current_month}")
    
    # 이번 달의 기존 배치 ID가 있는지 확인
    existing_batch_query = """
    SELECT DISTINCT batch_id 
    FROM company_overview 
    WHERE TO_CHAR(created_at, 'YYYY-MM') = %s 
    LIMIT 1
    """
    
    existing_batch = hook.get_first(existing_batch_query, (current_month,))
    
    if existing_batch:
        # 기존 배치 ID 사용 (이번 달에 이미 수집 중)
        batch_id = existing_batch[0]
        print(f"🔄 기존 배치 ID 사용: {batch_id} (이번 달 계속 진행)")
    else:
        # 새로운 배치 ID 생성 (새로운 달 시작)
        max_batch_query = "SELECT COALESCE(MAX(batch_id), 0) FROM company_overview"
        max_batch_result = hook.get_first(max_batch_query)
        batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
        print(f"🆕 새로운 배치 ID 생성: {batch_id} (새로운 달 시작)")
    
    # 전체 심볼을 alphabet 순으로 정렬하여 조회
    query = """
    SELECT symbol, company_name 
    FROM sp500_companies 
    ORDER BY symbol ASC
    """
    
    all_symbols = hook.get_records(query)
    
    if not all_symbols:
        raise ValueError("❌ SP500 companies 테이블에 데이터가 없습니다")
    
    print(f"📊 전체 SP500 기업 수: {len(all_symbols)}개")
    
    # 50개씩 분할 (배치별)
    batch_size = 50
    start_idx = (batch_number - 1) * batch_size
    end_idx = start_idx + batch_size
    
    today_symbols = all_symbols[start_idx:end_idx]
    
    print(f"🎯 오늘 수집 대상: {len(today_symbols)}개 (인덱스 {start_idx}~{end_idx-1})")
    
    if not today_symbols:
        print("⚠️ 오늘 수집할 심볼이 없습니다 (배치 범위 초과)")
        return {
            'batch_id': batch_id,  # 🆕 추가
            'batch_number': batch_number,
            'symbols': [],
            'total_count': 0
        }
    
    # 심볼 리스트 추출
    symbol_list = [row[0] for row in today_symbols]
    
    print(f"📋 오늘의 심볼들: {symbol_list[:5]}{'...' if len(symbol_list) > 5 else ''}")
    print(f"🏷️ 이번 달 배치 ID: {batch_id}")
    
    # XCom에 저장 (batch_id 추가)
    context['ti'].xcom_push(key='daily_symbols', value=symbol_list)
    context['ti'].xcom_push(key='batch_number', value=batch_number)
    context['ti'].xcom_push(key='batch_id', value=batch_id)  # 🆕 추가
    
    return {
        'batch_id': batch_id,        # 🆕 추가
        'batch_number': batch_number,
        'symbols': symbol_list,
        'total_count': len(symbol_list)
    }

def fetch_company_overview_data(**context):
    """
    Alpha Vantage Company Overview API로 회사 상세 정보 수집
    Rate Limit: 25 calls/day per API key → 2개 키로 50 calls/day
    """
    # XCom에서 오늘의 심볼 리스트 가져오기
    symbol_list = context['ti'].xcom_pull(task_ids='get_daily_symbols', key='daily_symbols')
    batch_number = context['ti'].xcom_pull(task_ids='get_daily_symbols', key='batch_number')
    
    if not symbol_list:
        print("⚠️ 오늘 처리할 심볼이 없습니다")
        return {'processed': 0, 'success': 0, 'errors': 0}
    
    # API 키 설정 (2개 번갈아 사용)
    api_keys = [
        Variable.get('ALPHA_VANTAGE_API_KEY_3'),
        Variable.get('ALPHA_VANTAGE_API_KEY_4')  # 두 번째 키
    ]
    
    for key in api_keys:
        if not key:
            raise ValueError("❌ Alpha Vantage API 키가 설정되지 않았습니다")
    
    print(f"🔑 API 키 2개 확인 완료")
    print(f"📊 배치 {batch_number}: {len(symbol_list)}개 심볼 처리 시작")
    
    # 수집된 데이터 저장
    collected_data = []
    success_count = 0
    error_count = 0
    
    # API 요청 URL
    base_url = "https://www.alphavantage.co/query"
    
    for i, symbol in enumerate(symbol_list):
        try:
            # API 키 번갈아 사용 (첫 25개는 key1, 나머지 25개는 key2)
            api_key = api_keys[0] if i < 25 else api_keys[1]
            api_key_index = 1 if i < 25 else 2
            
            print(f"🔍 [{i+1}/{len(symbol_list)}] {symbol} 처리 중... (API키 {api_key_index})")
            
            # API 요청 파라미터
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': api_key
            }
            
            # API 호출
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Rate Limit 체크
            if 'Note' in response.text and 'API call frequency' in response.text:
                print(f"⚠️ API 호출 제한 도달: {symbol}")
                error_count += 1
                time.sleep(60)  # 1분 대기
                continue
            
            # JSON 파싱
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {symbol} - {e}")
                error_count += 1
                continue
            
            # 데이터 유효성 검사
            if 'Error Message' in data:
                print(f"❌ API 오류: {symbol} - {data.get('Error Message')}")
                error_count += 1
                continue
            
            # 필수 필드 확인
            if not data.get('Symbol') or not data.get('Name'):
                print(f"❌ 필수 데이터 누락: {symbol}")
                error_count += 1
                continue
            
            # 데이터 저장
            collected_data.append({
                'symbol': symbol,
                'data': data,
                'api_key_used': api_key_index
            })
            
            success_count += 1
            print(f"✅ {symbol} 수집 완료 - {data.get('Name', 'Unknown')}")
            
            # API Rate Limit 고려하여 딜레이
            if i < len(symbol_list) - 1:  # 마지막이 아니면
                time.sleep(12)  # 12초 딜레이 (하루 25회 제한 고려)
                
        except Exception as e:
            print(f"❌ {symbol} 처리 실패: {str(e)}")
            error_count += 1
            continue
    
    print(f"🎯 배치 {batch_number} 수집 완료:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ❌ 실패: {error_count}개")
    print(f"   📊 총 처리: {len(symbol_list)}개")
    
    # XCom에 저장
    context['ti'].xcom_push(key='company_data', value=collected_data)
    
    return {
        'batch_number': batch_number,
        'processed': len(symbol_list),
        'success': success_count,
        'errors': error_count
    }

def process_and_store_overview_data(**context):
    """
    수집된 Company Overview 데이터를 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    company_data = context['ti'].xcom_pull(task_ids='fetch_company_overview', key='company_data')
    batch_number = context['ti'].xcom_pull(task_ids='get_daily_symbols', key='batch_number')
    batch_id = context['ti'].xcom_pull(task_ids='get_daily_symbols', key='batch_id')
    
    if not company_data:
        print("⚠️ 저장할 데이터가 없습니다")
        return {'stored': 0, 'success': 0, 'errors': 0}
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"💾 배치 {batch_number}: {len(company_data)}개 데이터 저장 시작")
    
    success_count = 0
    error_count = 0
    
    for item in company_data:
        try:
            symbol = item['symbol']
            data = item['data']
            
            # 숫자 데이터 안전하게 변환
            def safe_decimal(value):
                try:
                    if value == 'None' or value is None or value == '-':
                        return None
                    return Decimal(str(value))
                except:
                    return None
            
            def safe_int(value):
                try:
                    if value == 'None' or value is None or value == '-':
                        return None
                    return int(float(str(value)))
                except:
                    return None
            
            def safe_str(value, max_length=None):
                try:
                    if value is None or value == 'None':
                        return None
                    result = str(value).strip()
                    if max_length and len(result) > max_length:
                        result = result[:max_length]
                    return result if result else None
                except:
                    return None
            
            # SQL 파라미터 준비
            params = {
                'batch_id': batch_id,
                'symbol': symbol,
                'asset_type': safe_str(data.get('AssetType'), 50),
                'name': safe_str(data.get('Name'), 200),
                'description': safe_str(data.get('Description')),
                'cik': safe_str(data.get('CIK'), 20),
                'exchange': safe_str(data.get('Exchange'), 20),
                'currency': safe_str(data.get('Currency'), 10),
                'country': safe_str(data.get('Country'), 50),
                'sector': safe_str(data.get('Sector'), 100),
                'industry': safe_str(data.get('Industry'), 200),
                'address': safe_str(data.get('Address'), 300),
                'official_site': safe_str(data.get('OfficialSite'), 200),
                'fiscal_year_end': safe_str(data.get('FiscalYearEnd'), 20),
                'latest_quarter': safe_str(data.get('LatestQuarter'), 20),
                'market_capitalization': safe_int(data.get('MarketCapitalization')),
                'ebitda': safe_int(data.get('EBITDA')),
                'pe_ratio': safe_decimal(data.get('PERatio')),
                'peg_ratio': safe_decimal(data.get('PEGRatio')),
                'book_value': safe_decimal(data.get('BookValue')),
                'dividend_per_share': safe_decimal(data.get('DividendPerShare')),
                'dividend_yield': safe_decimal(data.get('DividendYield')),
                'eps': safe_decimal(data.get('EPS')),
                'revenue_per_share_ttm': safe_decimal(data.get('RevenuePerShareTTM')),
                'profit_margin': safe_decimal(data.get('ProfitMargin')),
                'operating_margin_ttm': safe_decimal(data.get('OperatingMarginTTM')),
                'return_on_assets_ttm': safe_decimal(data.get('ReturnOnAssetsTTM')),
                'return_on_equity_ttm': safe_decimal(data.get('ReturnOnEquityTTM')),
                'revenue_ttm': safe_int(data.get('RevenueTTM')),
                'gross_profit_ttm': safe_int(data.get('GrossProfitTTM')),
                'diluted_eps_ttm': safe_decimal(data.get('DilutedEPSTTM')),
                'quarterly_earnings_growth_yoy': safe_decimal(data.get('QuarterlyEarningsGrowthYOY')),
                'quarterly_revenue_growth_yoy': safe_decimal(data.get('QuarterlyRevenueGrowthYOY')),
                'analyst_target_price': safe_decimal(data.get('AnalystTargetPrice')),
                'trailing_pe': safe_decimal(data.get('TrailingPE')),
                'forward_pe': safe_decimal(data.get('ForwardPE')),
                'price_to_sales_ratio_ttm': safe_decimal(data.get('PriceToSalesRatioTTM')),
                'price_to_book_ratio': safe_decimal(data.get('PriceToBookRatio')),
                'ev_to_revenue': safe_decimal(data.get('EVToRevenue')),
                'ev_to_ebitda': safe_decimal(data.get('EVToEBITDA')),
                'beta': safe_decimal(data.get('Beta')),
                'week_52_high': safe_decimal(data.get('52WeekHigh')),
                'week_52_low': safe_decimal(data.get('52WeekLow')),
                'day_50_moving_average': safe_decimal(data.get('50DayMovingAverage')),
                'day_200_moving_average': safe_decimal(data.get('200DayMovingAverage')),
                'shares_outstanding': safe_int(data.get('SharesOutstanding')),
                'dividend_date': safe_str(data.get('DividendDate'), 20),
                'ex_dividend_date': safe_str(data.get('ExDividendDate'), 20)
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=params)
            success_count += 1
            
            print(f"✅ {symbol} 저장 완료 - {params['name']}")
            
        except Exception as e:
            print(f"❌ {symbol} 저장 실패: {str(e)}")
            error_count += 1
            continue
    
    print(f"💾 배치 {batch_number} 저장 완료:")
    print(f"   ✅ 성공: {success_count}개") 
    print(f"   ❌ 실패: {error_count}개")
    
    return {
        'batch_number': batch_number,
        'stored': len(company_data),
        'success': success_count,
        'errors': error_count
    }

# DAG 정의
with DAG(
    dag_id='ingest_company_overview_k8s',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 매일 새벽 2시 실행 (10일 주기)
    catchup=False,
    description='Alpha Vantage API로 SP500 기업 상세 정보 수집 (일별 50개씩)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'alpha_vantage', 'company_overview', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_company_overview_table',
        postgres_conn_id='postgres_default',
        sql='create_company_overview.sql',
    )
    
    # 2. 일별 심볼 배치 선정
    get_symbols = PythonOperator(
        task_id='get_daily_symbols',
        python_callable=get_daily_symbols_batch,
    )
    
    # 3. Company Overview API 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_company_overview',
        python_callable=fetch_company_overview_data,
    )
    
    # 4. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_data',
        python_callable=process_and_store_overview_data,
    )
    
    # Task 의존성
    create_table >> get_symbols >> fetch_data >> process_data