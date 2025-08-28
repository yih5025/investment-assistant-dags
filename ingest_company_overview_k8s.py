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
    'start_date': datetime(2025, 8, 26),  # 오늘부터 시작
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def verify_api_keys():
    """API 키 유효성 검증 및 상태 확인"""
    api_keys = [
        Variable.get('ALPHA_VANTAGE_API_KEY_5', default_var=None),
        Variable.get('ALPHA_VANTAGE_API_KEY_6', default_var=None)
    ]
    
    valid_keys = []
    for i, key in enumerate(api_keys):
        if not key:
            print(f"❌ API 키 {i+1} 설정되지 않음")
            continue
        
        # 간단한 테스트 호출
        test_url = "https://www.alphavantage.co/query"
        params = {
            'function': 'OVERVIEW',
            'symbol': 'AAPL',
            'apikey': key
        }
        
        try:
            response = requests.get(test_url, params=params, timeout=10)
            data = response.json()
            
            if 'Note' in data and 'API call frequency' in data['Note']:
                print(f"⚠️ API 키 {i+1}: 호출 제한 도달 - {data['Note']}")
            elif 'Error Message' in data:
                print(f"❌ API 키 {i+1}: 오류 - {data['Error Message']}")
            elif 'Information' in data:
                print(f"⚠️ API 키 {i+1}: 정보 메시지 - {data['Information']}")
            elif data.get('Symbol') == 'AAPL':
                print(f"✅ API 키 {i+1}: 정상")
                valid_keys.append(key)
            else:
                print(f"❓ API 키 {i+1}: 예상치 못한 응답")
                print(f"   응답 샘플: {str(data)[:200]}...")
                
        except Exception as e:
            print(f"❌ API 키 {i+1}: 네트워크 오류 - {str(e)}")
        
        # 키 간 딜레이
        time.sleep(3)
    
    print(f"🔑 사용 가능한 API 키: {len(valid_keys)}/{len(api_keys)}개")
    return valid_keys

def get_reset_symbols_batch(**context):
    """
    SP500 회사들을 처음부터 다시 50개씩 분할
    새로운 batch_id로 시작
    """
    execution_date = context['execution_date']
    
    # 강제로 배치 1부터 시작하도록 설정
    day_of_month = execution_date.day
    batch_number = ((day_of_month - 1) % 10) + 1
    
    print(f"📅 실행 날짜: {execution_date.strftime('%Y-%m-%d')}")
    print(f"🔄 배치 번호: {batch_number}/10 (처음부터 재시작)")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 새로운 batch_id 강제 생성 (기존 데이터와 구분)
    print("🆕 새로운 배치 ID 생성 (재시작)")
    max_batch_query = "SELECT COALESCE(MAX(batch_id), 0) FROM company_overview"
    max_batch_result = hook.get_first(max_batch_query)
    batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
    
    print(f"🆕 신규 배치 ID: {batch_id}")
    
    # 전체 심볼 조회 (알파벳 순 정렬)
    query = """
    SELECT symbol, company_name 
    FROM sp500_companies 
    ORDER BY symbol ASC
    """
    
    all_symbols = hook.get_records(query)
    
    if not all_symbols:
        raise ValueError("❌ SP500 companies 테이블에 데이터가 없습니다")
    
    print(f"📊 전체 SP500 기업 수: {len(all_symbols)}개")
    
    # 49개씩 분할
    batch_size = 49
    start_idx = (batch_number - 1) * batch_size
    end_idx = start_idx + batch_size
    
    today_symbols = all_symbols[start_idx:end_idx]
    
    print(f"🎯 오늘 수집 대상: {len(today_symbols)}개 (인덱스 {start_idx}~{end_idx-1})")
    
    if not today_symbols:
        print("⚠️ 오늘 수집할 심볼이 없습니다 (배치 범위 초과)")
        return {
            'batch_id': batch_id,
            'batch_number': batch_number,
            'symbols': [],
            'total_count': 0
        }
    
    symbol_list = [row[0] for row in today_symbols]
    
    print(f"📋 수집할 심볼들: {symbol_list}")
    print(f"🏷️ 신규 배치 ID: {batch_id}")
    
    # XCom에 저장
    context['ti'].xcom_push(key='daily_symbols', value=symbol_list)
    context['ti'].xcom_push(key='batch_number', value=batch_number)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    
    return {
        'batch_id': batch_id,
        'batch_number': batch_number,
        'symbols': symbol_list,
        'total_count': len(symbol_list)
    }

def fetch_company_overview_with_detailed_logging(**context):
    """
    상세한 로깅과 함께 Company Overview 데이터 수집
    실패 원인을 자세히 기록
    """
    # XCom에서 데이터 가져오기
    symbol_list = context['ti'].xcom_pull(task_ids='get_reset_symbols', key='daily_symbols')
    batch_number = context['ti'].xcom_pull(task_ids='get_reset_symbols', key='batch_number')
    
    if not symbol_list:
        print("⚠️ 오늘 처리할 심볼이 없습니다")
        return {'processed': 0, 'success': 0, 'errors': 0}
    
    # API 키 검증
    print("🔍 API 키 검증 중...")
    valid_keys = verify_api_keys()
    
    if not valid_keys:
        print("❌ 사용 가능한 API 키가 없습니다!")
        return {'processed': len(symbol_list), 'success': 0, 'errors': len(symbol_list)}
    
    print(f"✅ 사용 가능한 API 키: {len(valid_keys)}개")
    
    # 수집 파라미터
    collected_data = []
    success_count = 0
    error_count = 0
    detailed_errors = {}
    
    base_url = "https://www.alphavantage.co/query"
    
    # 심볼별 처리
    for i, symbol in enumerate(symbol_list):
        try:
            # API 키 선택 (순환)
            api_key = valid_keys[i % len(valid_keys)]
            api_key_index = (i % len(valid_keys)) + 1
            
            print(f"🔍 [{i+1}/{len(symbol_list)}] {symbol} 처리 중... (API키 {api_key_index})")
            
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': api_key
            }
            
            # API 호출
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # 원본 응답 내용 확인 (디버깅용)
            response_text = response.text
            print(f"   📡 응답 크기: {len(response_text)} 바이트")
            
            # Rate Limit 체크
            if 'Note' in response_text and 'API call frequency' in response_text:
                print(f"⚠️ API 호출 제한 도달: {symbol}")
                print(f"   메시지: {response_text[:200]}...")
                detailed_errors[symbol] = "API_RATE_LIMIT"
                error_count += 1
                
                # Rate limit 발생시 더 긴 대기
                wait_time = 60
                print(f"   ⏳ Rate Limit 대기: {wait_time}초...")
                time.sleep(wait_time)
                continue
            
            # JSON 파싱
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {symbol}")
                print(f"   응답 내용: {response_text[:500]}...")
                detailed_errors[symbol] = f"JSON_PARSE_ERROR: {str(e)}"
                error_count += 1
                continue
            
            # 응답 데이터 상세 분석
            print(f"   📊 응답 키들: {list(data.keys())}")
            
            # 다양한 에러 케이스 체크
            if 'Error Message' in data:
                print(f"❌ API 오류: {symbol}")
                print(f"   에러 메시지: {data.get('Error Message')}")
                detailed_errors[symbol] = f"API_ERROR: {data.get('Error Message')}"
                error_count += 1
                continue
                
            elif 'Information' in data:
                print(f"⚠️ 정보 메시지: {symbol}")
                print(f"   메시지: {data.get('Information')}")
                detailed_errors[symbol] = f"INFO_MESSAGE: {data.get('Information')}"
                error_count += 1
                continue
                
            elif 'Note' in data:
                print(f"⚠️ 주의 사항: {symbol}")
                print(f"   메시지: {data.get('Note')}")
                detailed_errors[symbol] = f"NOTE_MESSAGE: {data.get('Note')}"
                error_count += 1
                continue
            
            # 필수 필드 존재 여부 확인
            symbol_field = data.get('Symbol', '')
            name_field = data.get('Name', '')
            
            print(f"   🏷️ Symbol: '{symbol_field}'")
            print(f"   🏢 Name: '{name_field}'")
            
            if not symbol_field or not name_field:
                print(f"❌ 필수 데이터 누락: {symbol}")
                print(f"   Symbol 존재: {'Yes' if symbol_field else 'No'}")
                print(f"   Name 존재: {'Yes' if name_field else 'No'}")
                print(f"   전체 응답: {json.dumps(data, indent=2)[:1000]}...")
                detailed_errors[symbol] = "MISSING_REQUIRED_FIELDS"
                error_count += 1
                continue
            
            # 성공적으로 수집
            collected_data.append({
                'symbol': symbol,
                'data': data,
                'api_key_used': api_key_index
            })
            
            success_count += 1
            print(f"✅ {symbol} 수집 완료 - {name_field}")
            print(f"   섹터: {data.get('Sector', 'N/A')}")
            print(f"   시가총액: {data.get('MarketCapitalization', 'N/A')}")
            
            # 다음 요청 전 충분한 대기 (Rate Limit 방지)
            if i < len(symbol_list) - 1:
                delay = 25  # 25초 대기로 증가
                print(f"   ⏳ 다음 요청까지 {delay}초 대기...")
                time.sleep(delay)
                
        except requests.exceptions.RequestException as e:
            print(f"❌ {symbol} 네트워크 오류: {str(e)}")
            detailed_errors[symbol] = f"NETWORK_ERROR: {str(e)}"
            error_count += 1
            continue
            
        except Exception as e:
            print(f"❌ {symbol} 알 수 없는 오류: {str(e)}")
            detailed_errors[symbol] = f"UNKNOWN_ERROR: {str(e)}"
            error_count += 1
            continue
    
    print(f"🎯 배치 {batch_number} 수집 완료:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ❌ 실패: {error_count}개")
    print(f"   📊 총 처리: {len(symbol_list)}개")
    print(f"   📈 성공률: {success_count/len(symbol_list)*100:.1f}%")
    
    # 실패 원인 상세 분석
    if detailed_errors:
        print("\n📋 실패 원인 분석:")
        error_types = {}
        for symbol, error in detailed_errors.items():
            error_type = error.split(':')[0]
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append(symbol)
        
        for error_type, symbols in error_types.items():
            print(f"   {error_type}: {len(symbols)}개 ({symbols[:5]}{'...' if len(symbols) > 5 else ''})")
    
    # XCom에 저장
    context['ti'].xcom_push(key='company_data', value=collected_data)
    context['ti'].xcom_push(key='detailed_errors', value=detailed_errors)
    
    return {
        'batch_number': batch_number,
        'processed': len(symbol_list),
        'success': success_count,
        'errors': error_count,
        'success_rate': round(success_count/len(symbol_list)*100, 1),
        'error_details': detailed_errors
    }

def process_and_store_reset_data(**context):
    """
    수집된 Company Overview 데이터를 PostgreSQL에 저장
    """
    company_data = context['ti'].xcom_pull(task_ids='fetch_reset_data', key='company_data')
    batch_number = context['ti'].xcom_pull(task_ids='get_reset_symbols', key='batch_number')
    batch_id = context['ti'].xcom_pull(task_ids='get_reset_symbols', key='batch_id')
    
    if not company_data:
        print("⚠️ 저장할 데이터가 없습니다")
        return {'stored': 0, 'success': 0, 'errors': 0}
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"💾 배치 {batch_number}: {len(company_data)}개 데이터 저장 시작")
    print(f"🏷️ 배치 ID: {batch_id}")
    
    success_count = 0
    error_count = 0
    
    for item in company_data:
        try:
            symbol = item['symbol']
            data = item['data']
            
            # 숫자 데이터 안전하게 변환
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
    print(f"   📈 저장률: {success_count/len(company_data)*100:.1f}%")
    
    return {
        'batch_number': batch_number,
        'batch_id': batch_id,
        'stored': len(company_data),
        'success': success_count,
        'errors': error_count,
        'success_rate': round(success_count/len(company_data)*100, 1)
    }

# DAG 정의
with DAG(
    dag_id='ingest_company_overview_reset_k8s',
    default_args=default_args,
    schedule_interval='0 4 * * *',  # 매일 새벽 4시 실행
    catchup=False,
    description='SP500 기업 정보를 처음부터 다시 수집 (상세 로깅 + 25초 딜레이)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'alpha_vantage', 'company_overview', 'reset', 'detailed_logging'],
    max_active_runs=1,  # 동시 실행 방지
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_company_overview_table',
        postgres_conn_id='postgres_default',
        sql='create_company_overview.sql',
    )
    
    # 2. 리셋된 심볼 배치 선정
    get_symbols = PythonOperator(
        task_id='get_reset_symbols',
        python_callable=get_reset_symbols_batch,
    )
    
    # 3. 상세 로깅과 함께 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_reset_data',
        python_callable=fetch_company_overview_with_detailed_logging,
        execution_timeout=timedelta(hours=3),  # 3시간 타임아웃
    )
    
    # 4. 데이터 저장
    process_data = PythonOperator(
        task_id='process_and_store_reset_data',
        python_callable=process_and_store_reset_data,
    )
    
    # Task 의존성
    create_table >> get_symbols >> fetch_data >> process_data