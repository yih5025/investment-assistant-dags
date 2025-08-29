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
    'start_date': datetime(2025, 8, 26),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def get_or_create_collection_progress(**context):
    """
    수집 진행상황 추적 및 다음 배치 심볼 선정
    실패한 심볼부터 재시작하는 로직
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 진행상황 테이블이 없으면 생성
    create_progress_table = """
    CREATE TABLE IF NOT EXISTS company_overview_progress (
        id SERIAL PRIMARY KEY,
        collection_name TEXT NOT NULL DEFAULT 'sp500_full',
        current_position INTEGER NOT NULL DEFAULT 0,
        total_symbols INTEGER NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'active'
    );
    """
    hook.run(create_progress_table)
    
    # 현재 진행상황 조회
    progress_query = """
    SELECT current_position, total_symbols 
    FROM company_overview_progress 
    WHERE collection_name = 'sp500_full' AND status = 'active'
    ORDER BY last_updated DESC 
    LIMIT 1
    """
    
    progress_result = hook.get_first(progress_query)
    
    # 전체 심볼 조회 (알파벳 순 정렬)
    symbols_query = """
    SELECT symbol, company_name 
    FROM sp500_companies 
    ORDER BY symbol ASC
    """
    all_symbols = hook.get_records(symbols_query)
    
    if not all_symbols:
        raise ValueError("SP500 companies 테이블에 데이터가 없습니다")
    
    total_count = len(all_symbols)
    print(f"전체 S&P 500 기업 수: {total_count}개")
    
    # 진행상황이 없으면 새로 생성
    if not progress_result:
        print("새로운 수집 세션 시작")
        current_position = 0
        
        # 진행상황 초기화
        init_progress = """
        INSERT INTO company_overview_progress (collection_name, current_position, total_symbols)
        VALUES ('sp500_full', 0, %s)
        """
        hook.run(init_progress, parameters=[total_count])
    else:
        current_position = progress_result[0]
        print(f"이전 진행상황에서 재시작: 위치 {current_position}")
    
    # 오늘 수집할 심볼들 선정 (최대 25개, 안전하게)
    max_daily_requests = 20  # Rate Limit 고려하여 보수적으로 설정
    end_position = min(current_position + max_daily_requests, total_count)
    
    if current_position >= total_count:
        print("모든 심볼 수집 완료!")
        # 수집 완료 표시
        complete_query = """
        UPDATE company_overview_progress 
        SET status = 'completed', last_updated = CURRENT_TIMESTAMP
        WHERE collection_name = 'sp500_full' AND status = 'active'
        """
        hook.run(complete_query)
        
        return {
            'symbols': [],
            'current_position': current_position,
            'end_position': current_position,
            'completed': True,
            'total_count': total_count
        }
    
    # 오늘 수집할 심볼들
    today_symbols = all_symbols[current_position:end_position]
    symbol_list = [row[0] for row in today_symbols]
    
    print(f"오늘 수집 범위: {current_position} ~ {end_position-1} (총 {len(symbol_list)}개)")
    print(f"수집 심볼들: {symbol_list}")
    print(f"전체 진행률: {current_position}/{total_count} ({current_position/total_count*100:.1f}%)")
    
    # 배치 ID 생성
    max_batch_query = "SELECT COALESCE(MAX(batch_id), 0) FROM company_overview"
    max_batch_result = hook.get_first(max_batch_query)
    batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
    
    # XCom에 저장
    context['ti'].xcom_push(key='daily_symbols', value=symbol_list)
    context['ti'].xcom_push(key='current_position', value=current_position)
    context['ti'].xcom_push(key='end_position', value=end_position)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    
    return {
        'symbols': symbol_list,
        'current_position': current_position,
        'end_position': end_position,
        'completed': False,
        'total_count': total_count,
        'batch_id': batch_id
    }

def fetch_company_overview_progressive(**context):
    """
    진행형 데이터 수집: 성공한 만큼만 진행상황 업데이트
    """
    symbol_list = context['ti'].xcom_pull(task_ids='get_collection_progress', key='daily_symbols')
    current_position = context['ti'].xcom_pull(task_ids='get_collection_progress', key='current_position')
    
    if not symbol_list:
        print("오늘 처리할 심볼이 없습니다")
        return {'processed': 0, 'success': 0, 'errors': 0, 'successful_symbols': []}
    
    # API 키 설정
    api_keys = [
        Variable.get('ALPHA_VANTAGE_API_KEY_3', default_var=None),
        Variable.get('ALPHA_VANTAGE_API_KEY_4', default_var=None)
    ]
    
    valid_keys = [key for key in api_keys if key]
    if not valid_keys:
        print("사용 가능한 API 키가 없습니다!")
        return {'processed': len(symbol_list), 'success': 0, 'errors': len(symbol_list), 'successful_symbols': []}
    
    print(f"사용 가능한 API 키: {len(valid_keys)}개")
    
    # 수집 시작
    collected_data = []
    successful_symbols = []  # 성공한 심볼들만 추적
    success_count = 0
    error_count = 0
    
    base_url = "https://www.alphavantage.co/query"
    
    for i, symbol in enumerate(symbol_list):
        try:
            # API 키 순환 사용
            api_key = valid_keys[i % len(valid_keys)]
            
            print(f"[{i+1}/{len(symbol_list)}] {symbol} 처리 중... (위치: {current_position + i})")
            
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': api_key
            }
            
            # API 호출
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Rate Limit 체크 (즉시 중단)
            if ('Information' in data and 'rate limit' in data['Information'].lower()) or \
               ('Note' in data and 'API call frequency' in data.get('Note', '')):
                
                print(f"Rate Limit 도달: {symbol}")
                print(f"현재까지 {success_count}개 성공 수집")
                break
            
            # 에러 체크
            if 'Error Message' in data:
                print(f"API 오류: {symbol} - {data.get('Error Message')}")
                error_count += 1
                continue
            
            # 데이터 유효성 검증
            if not data.get('Symbol') or not data.get('Name'):
                print(f"필수 데이터 누락: {symbol}")
                error_count += 1
                continue
            
            # 성공 처리
            collected_data.append({
                'symbol': symbol,
                'data': data,
                'position': current_position + i
            })
            
            successful_symbols.append(symbol)
            success_count += 1
            
            print(f"✅ {symbol} 수집 완료 - {data.get('Name')}")
            
            # 안전한 딜레이
            if i < len(symbol_list) - 1:
                time.sleep(30)  # 30초 대기
                
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {str(e)}")
            error_count += 1
            continue
    
    print(f"\n수집 완료:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ❌ 실패: {error_count}개")
    print(f"   📈 성공률: {success_count/(success_count+error_count)*100:.1f}%")
    
    # XCom에 저장
    context['ti'].xcom_push(key='company_data', value=collected_data)
    context['ti'].xcom_push(key='successful_symbols', value=successful_symbols)
    context['ti'].xcom_push(key='success_count', value=success_count)
    
    return {
        'processed': len(symbol_list),
        'success': success_count,
        'errors': error_count,
        'successful_symbols': successful_symbols,
        'success_rate': round(success_count/(success_count+error_count)*100, 1) if (success_count+error_count) > 0 else 0
    }

def update_progress_and_store_data(**context):
    """
    성공한 심볼들만큼 진행상황 업데이트 + 데이터 저장
    """
    company_data = context['ti'].xcom_pull(task_ids='fetch_progressive_data', key='company_data')
    successful_symbols = context['ti'].xcom_pull(task_ids='fetch_progressive_data', key='successful_symbols')
    success_count = context['ti'].xcom_pull(task_ids='fetch_progressive_data', key='success_count')
    current_position = context['ti'].xcom_pull(task_ids='get_collection_progress', key='current_position')
    batch_id = context['ti'].xcom_pull(task_ids='get_collection_progress', key='batch_id')
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 데이터 저장
    stored_count = 0
    if company_data:
        print(f"데이터 저장 시작: {len(company_data)}개")
        
        for item in company_data:
            try:
                symbol = item['symbol']
                data = item['data']
                
                # 데이터 변환 함수들
                def safe_decimal(value):
                    try:
                        if value == 'None' or value is None or value == '-' or value == '':
                            return None
                        return Decimal(str(value))
                    except:
                        return None
                
                def safe_int(value):
                    try:
                        if value == 'None' or value is None or value == '-' or value == '':
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
                
                # SQL 파라미터 준비 (기존과 동일)
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
                stored_count += 1
                print(f"✅ {symbol} 저장 완료")
                
            except Exception as e:
                print(f"❌ {symbol} 저장 실패: {str(e)}")
                continue
    
    # 진행상황 업데이트: 성공한 만큼만 포인터 이동
    if success_count > 0:
        new_position = current_position + success_count
        
        update_progress = """
        UPDATE company_overview_progress 
        SET current_position = %s, last_updated = CURRENT_TIMESTAMP
        WHERE collection_name = 'sp500_full' AND status = 'active'
        """
        hook.run(update_progress, parameters=[new_position])
        
        print(f"진행상황 업데이트: {current_position} -> {new_position}")
        print(f"성공한 심볼들: {successful_symbols}")
    else:
        print("성공한 수집이 없어 진행상황 유지")
    
    return {
        'stored_count': stored_count,
        'success_count': success_count,
        'updated_position': current_position + success_count if success_count > 0 else current_position,
        'batch_id': batch_id
    }

# DAG 정의
with DAG(
    dag_id='ingest_company_overview_progressive_k8s',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 매일 새벽 6시
    catchup=False,
    description='S&P 500 진행형 수집: 실패한 심볼부터 재시작',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'alpha_vantage', 'company_overview', 'progressive', 'resume'],
    max_active_runs=1,
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_company_overview_table',
        postgres_conn_id='postgres_default',
        sql='create_company_overview.sql',
    )
    
    # 2. 진행상황 확인 및 다음 배치 선정
    get_progress = PythonOperator(
        task_id='get_collection_progress',
        python_callable=get_or_create_collection_progress,
    )
    
    # 3. 진행형 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_progressive_data',
        python_callable=fetch_company_overview_progressive,
        execution_timeout=timedelta(hours=2),
    )
    
    # 4. 진행상황 업데이트 및 데이터 저장
    update_and_store = PythonOperator(
        task_id='update_progress_and_store',
        python_callable=update_progress_and_store_data,
    )
    
    # Task 의존성
    create_table >> get_progress >> fetch_data >> update_and_store