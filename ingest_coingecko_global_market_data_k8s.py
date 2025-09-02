from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
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
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_global.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

def fetch_global_crypto_data(**context):
    """
    CoinGecko Global API에서 전체 암호화폐 시장 데이터 수집
    """
    # API 키 가져오기
    api_key = Variable.get('COINGECKO_API_KEY_1')
    if not api_key:
        raise ValueError("COINGECKO_API_KEY_1이 설정되지 않았습니다")
    
    API_URL = "https://api.coingecko.com/api/v3/global"
    
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key
    }
    
    print(f"CoinGecko Global API 요청 시작: {API_URL}")
    
    # 재시도 로직
    for attempt in range(3):
        try:
            response = requests.get(API_URL, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # 응답 구조 검증
                if 'data' not in data:
                    raise ValueError("응답에 'data' 키가 없습니다")
                
                global_data = data['data']
                print("전체 암호화폐 시장 데이터 수집 완료")
                
                # 주요 지표 로그 출력
                total_market_cap_usd = global_data.get('total_market_cap', {}).get('usd', 0)
                total_volume_usd = global_data.get('total_volume', {}).get('usd', 0)
                active_cryptos = global_data.get('active_cryptocurrencies', 0)
                markets = global_data.get('markets', 0)
                
                print(f"  활성 암호화폐: {active_cryptos:,}개")
                print(f"  거래소 수: {markets:,}개")
                print(f"  총 시가총액: ${total_market_cap_usd:,.0f}")
                print(f"  총 거래량: ${total_volume_usd:,.0f}")
                
                # 비트코인 도미넌스 확인
                btc_dominance = global_data.get('market_cap_percentage', {}).get('btc', 0)
                eth_dominance = global_data.get('market_cap_percentage', {}).get('eth', 0)
                print(f"  BTC 도미넌스: {btc_dominance:.1f}%")
                print(f"  ETH 도미넌스: {eth_dominance:.1f}%")
                
                # 24시간 시장 변동률
                market_change_24h = global_data.get('market_cap_change_percentage_24h_usd', 0)
                print(f"  24시간 시장 변동: {market_change_24h:+.2f}%")
                
                # XCom에 저장
                context['ti'].xcom_push(key='global_data', value=global_data)
                
                return {
                    'active_cryptocurrencies': active_cryptos,
                    'markets': markets,
                    'total_market_cap_usd': total_market_cap_usd,
                    'total_volume_usd': total_volume_usd,
                    'btc_dominance': btc_dominance,
                    'market_change_24h': market_change_24h,
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

def process_and_store_global_data(**context):
    """
    수집된 Global 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    global_data = context['ti'].xcom_pull(task_ids='fetch_global_crypto_data', key='global_data')
    
    if not global_data:
        raise ValueError("이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # execution_date는 deprecated, logical_date 사용
    execution_date = context['logical_date']
    
    print("Global 암호화폐 데이터 저장 시작")
    
    try:
        # 타임스탬프 변환
        def parse_timestamp(timestamp):
            if timestamp:
                try:
                    return datetime.fromtimestamp(int(timestamp))
                except:
                    return None
            return None
        
        # JSON 객체를 안전하게 추출하는 함수
        def extract_currency_data(data_dict, currency='usd'):
            if isinstance(data_dict, dict):
                return Decimal(str(data_dict.get(currency, 0))) if data_dict.get(currency) else None
            return None
        
        # 파라미터 준비
        params = {
            # 기본 통계
            'active_cryptocurrencies': global_data.get('active_cryptocurrencies'),
            'upcoming_icos': global_data.get('upcoming_icos', 0),
            'ongoing_icos': global_data.get('ongoing_icos', 0),
            'ended_icos': global_data.get('ended_icos', 0),
            'markets': global_data.get('markets'),
            
            # 시가총액 (주요 통화)
            'total_market_cap_usd': extract_currency_data(global_data.get('total_market_cap', {}), 'usd'),
            'total_market_cap_krw': extract_currency_data(global_data.get('total_market_cap', {}), 'krw'),
            'total_market_cap_btc': extract_currency_data(global_data.get('total_market_cap', {}), 'btc'),
            'total_market_cap_eth': extract_currency_data(global_data.get('total_market_cap', {}), 'eth'),
            
            # 거래량 (주요 통화)
            'total_volume_usd': extract_currency_data(global_data.get('total_volume', {}), 'usd'),
            'total_volume_krw': extract_currency_data(global_data.get('total_volume', {}), 'krw'),
            'total_volume_btc': extract_currency_data(global_data.get('total_volume', {}), 'btc'),
            'total_volume_eth': extract_currency_data(global_data.get('total_volume', {}), 'eth'),
            
            # 도미넌스 (주요 코인)
            'btc_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('btc', 0))),
            'eth_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('eth', 0))),
            'bnb_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('bnb', 0))),
            'xrp_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('xrp', 0))),
            'ada_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('ada', 0))),
            'sol_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('sol', 0))),
            'doge_dominance': Decimal(str(global_data.get('market_cap_percentage', {}).get('doge', 0))),
            
            # 시장 변동률
            'market_cap_change_percentage_24h_usd': Decimal(str(global_data.get('market_cap_change_percentage_24h_usd', 0))),
            
            # 원본 JSON 데이터 (전체 보존)
            'market_cap_percentage_json': json.dumps(global_data.get('market_cap_percentage', {})),
            'total_market_cap_json': json.dumps(global_data.get('total_market_cap', {})),
            'total_volume_json': json.dumps(global_data.get('total_volume', {})),
            
            # 시간 정보
            'coingecko_updated_at': parse_timestamp(global_data.get('updated_at')),
            'collected_at': execution_date
        }
        
        # SQL 실행
        hook.run(UPSERT_SQL, parameters=params)
        
        print("Global 데이터 저장 완료")
        
        # 저장 후 최신 데이터 확인
        latest_data = hook.get_first("""
            SELECT 
                active_cryptocurrencies,
                markets,
                total_market_cap_usd,
                btc_dominance,
                market_cap_change_percentage_24h_usd,
                collected_at
            FROM coingecko_global
            ORDER BY collected_at DESC
            LIMIT 1;
        """)
        
        if latest_data:
            print("저장된 최신 데이터 확인:")
            print(f"  활성 암호화폐: {latest_data[0]:,}개")
            print(f"  거래소: {latest_data[1]:,}개") 
            print(f"  시가총액: ${latest_data[2]:,.0f}")
            print(f"  BTC 도미넌스: {latest_data[3]:.1f}%")
            print(f"  24시간 변동: {latest_data[4]:+.2f}%")
            print(f"  수집 시간: {latest_data[5]}")
        
        return {
            'success': True,
            'execution_time': context['logical_date'].isoformat(),
            'records_inserted': 1
        }
        
    except Exception as e:
        print(f"데이터 저장 실패: {str(e)}")
        raise e

# DAG 정의
with DAG(
    dag_id='ingest_coingecko_global_k8s',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # 6시간마다 실행 (API 업데이트 주기 고려)
    catchup=False,
    description='CoinGecko 전체 암호화폐 시장 데이터 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'crypto', 'global', '6hourly'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_coingecko_global_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_global.sql',
    )
    
    # 2. Global 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_global_crypto_data',
        python_callable=fetch_global_crypto_data,
    )
    
    # 3. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_global_data',
        python_callable=process_and_store_global_data,
    )
    
    # Task 의존성
    create_table >> fetch_data >> process_data