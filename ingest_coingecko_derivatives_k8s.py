from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
import decimal
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_derivatives.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def fetch_derivatives_data(**context):
    """
    CoinGecko Derivatives API에서 파생상품 데이터 수집
    """
    # API 키 가져오기
    api_key = Variable.get('COINGECKO_API_KEY_1')
    if not api_key:
        raise ValueError("COINGECKO_API_KEY_1이 설정되지 않았습니다")
    
    API_URL = "https://api.coingecko.com/api/v3/derivatives"
    
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key
    }
    
    print(f"CoinGecko Derivatives API 요청 시작: {API_URL}")
    
    # 재시도 로직
    for attempt in range(3):
        try:
            response = requests.get(
                API_URL,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"데이터 수집 완료: {len(data)}개 파생상품 데이터")
                
                # 데이터 유효성 검증
                if not data or not isinstance(data, list):
                    raise ValueError("응답 데이터가 비어있거나 올바르지 않습니다")
                
                # 주요 통계 로그 출력
                markets = set([item.get('market', '') for item in data])
                symbols = set([item.get('index_id', '') for item in data])
                
                print(f"파생상품 거래소: {len(markets)}개")
                print(f"기초자산: {len(symbols)}개")
                
                # 상위 5개 거래소별 데이터 개수
                market_counts = {}
                for item in data:
                    market = item.get('market', 'Unknown')
                    market_counts[market] = market_counts.get(market, 0) + 1
                
                top_markets = sorted(market_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                print(f"상위 거래소별 상품 수: {top_markets}")
                
                # XCom에 저장
                context['ti'].xcom_push(key='derivatives_data', value=data)
                
                return {
                    'total_derivatives': len(data),
                    'unique_markets': len(markets),
                    'unique_symbols': len(symbols),
                    'status': 'success'
                }
                
            elif response.status_code == 429:  # Rate limit
                wait_time = 60 * (attempt + 1)
                print(f"Rate limit 도달. {wait_time}초 대기 후 재시도")
                if attempt < 2:
                    import time
                    time.sleep(wait_time)
                    continue
                
            else:
                raise ValueError(f"API 요청 실패: {response.status_code} - {response.text}")
                
        except requests.RequestException as e:
            print(f"요청 중 오류 발생 (시도 {attempt + 1}/3): {str(e)}")
            if attempt < 2:
                import time
                time.sleep(5)
                continue
            raise e
    
    raise ValueError("모든 재시도 실패")

def process_and_store_derivatives(**context):
    """
    수집된 파생상품 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    data = context['ti'].xcom_pull(task_ids='fetch_derivatives_data', key='derivatives_data')
    
    if not data:
        raise ValueError("이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 현재 배치 ID 생성 (실행 시간 기반)
    execution_date = context['execution_date']
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
    
    print(f"배치 {batch_id} 파생상품 데이터 저장 시작")
    
    success_count = 0
    error_count = 0
    
    print(f"전체 파생상품 데이터 처리: {len(data)}개")
    
    # 안전한 데이터 변환 함수들
    def safe_decimal(value, default=None):
        """안전하게 Decimal로 변환"""
        if value is None:
            return default
        try:
            # 문자열로 변환 후 Decimal 생성
            return Decimal(str(value))
        except (TypeError, ValueError, decimal.ConversionSyntax, decimal.InvalidOperation) as e:
            print(f"Decimal 변환 실패: {str(e)}, 값: {value} (타입: {type(value)})")
            return default
    
    def safe_int(value, default=None):
        """안전하게 int로 변환"""
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError) as e:
            print(f"Int 변환 실패: {str(e)}, 값: {value}")
            return default
    
    def parse_timestamp(timestamp):
        """타임스탬프를 datetime으로 변환"""
        if timestamp:
            try:
                return datetime.fromtimestamp(int(timestamp))
            except (TypeError, ValueError, OSError) as e:
                print(f"타임스탬프 변환 실패: {str(e)}, 값: {timestamp}")
                return None
        return None
    
    def safe_execution_date(context_execution_date):
        """Airflow execution_date Proxy 객체를 안전한 datetime으로 변환"""
        try:
            # Proxy 객체인 경우 실제 값을 추출
            if hasattr(context_execution_date, '__wrapped__'):
                return context_execution_date.__wrapped__
            elif hasattr(context_execution_date, 'datetime'):
                return context_execution_date.datetime
            else:
                # 이미 datetime 객체인 경우
                return context_execution_date
        except Exception as e:
            print(f"execution_date 변환 실패: {str(e)}, 현재 시간으로 대체")
            return datetime.utcnow()

    for item in data:
        try:
            # 데이터 검증
            required_fields = ['market', 'symbol', 'index_id']
            if not all(item.get(field) for field in required_fields):
                print(f"필수 데이터 누락: {item}")
                error_count += 1
                continue
            
            # 안전한 execution_date 변환
            safe_collected_at = safe_execution_date(execution_date)
            
            # 파라미터 준비
            params = {
                'batch_id': batch_id,
                'market': item.get('market', '')[:100],  # VARCHAR 제한
                'symbol': item.get('symbol', '')[:50],
                'index_id': item.get('index_id', '').upper()[:20],
                'price': safe_decimal(item.get('price')),
                'price_percentage_change_24h': safe_decimal(item.get('price_percentage_change_24h'), 0),
                'contract_type': item.get('contract_type', '')[:20],
                'index_price': safe_decimal(item.get('index')),
                'basis': safe_decimal(item.get('basis'), 0),
                'spread': safe_decimal(item.get('spread'), 0),
                'funding_rate': safe_decimal(item.get('funding_rate'), 0),
                'open_interest_usd': safe_decimal(item.get('open_interest')),
                'volume_24h_usd': safe_decimal(item.get('volume_24h')),
                'last_traded_at': parse_timestamp(item.get('last_traded_at')),
                'expired_at': safe_int(item.get('expired_at')) if item.get('expired_at') and item.get('expired_at') != 'null' else None,
                'collected_at': safe_collected_at
            }
            
            # SQL 실행
            try:
                hook.run(UPSERT_SQL, parameters=params)
                success_count += 1
                if success_count % 50 == 0:  # 50개마다 진행 상황 로그
                    print(f"진행 상황: {success_count}개 성공 처리됨")
            except Exception as sql_error:
                print(f"❌ SQL 실행 실패 - {item.get('symbol', 'Unknown')}: {str(sql_error)}")
                print(f"문제가 된 파라미터: {params}")
                error_count += 1
                continue
            
        except Exception as e:
            print(f"❌ 데이터 처리 실패 - {item.get('symbol', 'Unknown')}: {str(e)}")
            print(f"원본 데이터: {item}")
            error_count += 1
            continue
    
    # 저장 후 통계 조회
    stats = hook.get_first("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT index_id) as unique_assets,
            COUNT(DISTINCT market) as unique_markets,
            AVG(CASE WHEN funding_rate IS NOT NULL THEN funding_rate END) as avg_funding_rate,
            SUM(CASE WHEN funding_rate > 0 THEN 1 ELSE 0 END) as positive_funding_count,
            SUM(CASE WHEN funding_rate < 0 THEN 1 ELSE 0 END) as negative_funding_count
        FROM coingecko_derivatives
        WHERE batch_id = %s;
    """, parameters=(batch_id,))
    
    print(f"배치 {batch_id} 저장 완료:")
    print(f"  성공: {success_count}개, 실패: {error_count}개")
    print(f"  DB 통계: 총 {stats[0]}개 레코드, {stats[1]}개 자산, {stats[2]}개 거래소")
    print(f"  펀딩 비율: 평균 {float(stats[3] or 0):.4f}%, 양수 {stats[4]}개, 음수 {stats[5]}개")
    
    return {
        'batch_id': batch_id,
        'success_count': success_count,
        'error_count': error_count,
        'total_records': stats[0],
        'unique_assets': stats[1],
        'unique_markets': stats[2],
        'execution_time': context['execution_date'].isoformat()
    }

# DAG 정의
with DAG(
    dag_id='ingest_coingecko_derivatives_k8s',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    description='CoinGecko 파생상품 데이터 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'crypto', 'derivatives', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_coingecko_derivatives_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_derivatives.sql',
    )
    
    # 2. API 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_derivatives_data',
        python_callable=fetch_derivatives_data,
    )
    
    # 3. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_derivatives',
        python_callable=process_and_store_derivatives,
    )
    
    # Task 의존성
    create_table >> fetch_data >> process_data