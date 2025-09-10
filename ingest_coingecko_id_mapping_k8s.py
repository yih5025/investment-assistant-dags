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
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_id_mapping.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

def fetch_coingecko_markets(**context):
    """
    CoinGecko Markets API에서 전체 코인 매핑 데이터 수집 (페이지네이션)
    """
    API_URL = "https://api.coingecko.com/api/v3/coins/markets"
    
    # Airflow Variable에서 API 키 가져오기
    api_key = Variable.get("COINGECKO_API_KEY_1", default_var=None)
    
    # 헤더 설정 (API 키 포함)
    headers = {
        'User-Agent': 'Investment-Assistant/1.0',
        'Accept': 'application/json'
    }
    
    if api_key:
        headers['x-cg-demo-api-key'] = api_key
        print(f"🔑 API 키가 설정되었습니다")
    else:
        print(f"⚠️ API 키가 설정되지 않았습니다. Rate limit이 적용될 수 있습니다")
    
    all_coins = []
    page = 1
    max_pages = 20  # 최대 20페이지 (5000개 코인) 제한
    
    print(f"🚀 CoinGecko Markets API 전체 데이터 수집 시작")
    
    while page <= max_pages:
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 250,  # API 최대치
            'page': page,
            'sparkline': 'false',
            'price_change_percentage': '24h'
        }
        
        print(f"📄 페이지 {page} 처리 중...")
        
        # 재시도 로직
        success = False
        for attempt in range(3):
            try:
                response = requests.get(
                    API_URL,
                    params=params,
                    timeout=30,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # 데이터가 없으면 종료
                    if not data or len(data) == 0:
                        print(f"📄 페이지 {page}: 데이터 없음, 수집 완료")
                        break
                    
                    all_coins.extend(data)
                    print(f"📄 페이지 {page}: {len(data)}개 코인 수집 (누적: {len(all_coins)}개)")
                    success = True
                    break
                    
                elif response.status_code == 429:  # Rate limit
                    wait_time = 60 * (attempt + 1)
                    print(f"⚠️ Rate limit 도달. {wait_time}초 대기 후 재시도")
                    if attempt < 2:
                        import time
                        time.sleep(wait_time)
                        continue
                    
                else:
                    print(f"❌ API 요청 실패: {response.status_code} - {response.text}")
                    if attempt < 2:
                        import time
                        time.sleep(5)
                        continue
                    else:
                        raise ValueError(f"API 요청 실패: {response.status_code}")
                    
            except requests.RequestException as e:
                print(f"❌ 요청 중 오류 발생 (시도 {attempt + 1}/3): {str(e)}")
                if attempt < 2:
                    import time
                    time.sleep(5)
                    continue
                raise e
        
        if not success:
            print(f"❌ 페이지 {page} 처리 실패, 수집 중단")
            break
            
        # 다음 페이지로
        page += 1
        
        # API Rate Limit 방지를 위한 딜레이
        import time
        time.sleep(2)
    
    print(f"✅ 전체 데이터 수집 완료: {len(all_coins)}개 코인")
    
    # XCom에 저장
    context['ti'].xcom_push(key='market_data', value=all_coins)
    
    return {
        'total_coins': len(all_coins),
        'pages_processed': page - 1,
        'status': 'success'
    }

def process_and_store_mapping_data(**context):
    """
    수집된 매핑 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    data = context['ti'].xcom_pull(task_ids='fetch_coingecko_markets', key='market_data')
    
    if not data:
        raise ValueError("❌ 이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print(f"🚀 {len(data)}개 코인 데이터 처리 시작")
    
    success_count = 0
    error_count = 0
    
    for coin in data:
        try:
            # 데이터 검증
            if not coin.get('id') or not coin.get('symbol'):
                print(f"⚠️ 필수 데이터 누락: {coin}")
                error_count += 1
                continue
            
            # 날짜 변환 함수
            def parse_date(date_str):
                if date_str:
                    try:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    except:
                        return None
                return None
            
            # 파라미터 준비
            params = {
                'coingecko_id': coin['id'],
                'symbol': coin['symbol'].upper(),
                'name': coin['name'][:200],  # VARCHAR 제한
                'image_url': coin.get('image', '')[:500],  # VARCHAR 제한
                'current_price_usd': Decimal(str(coin.get('current_price', 0))),
                'market_cap_usd': int(coin.get('market_cap', 0)) if coin.get('market_cap') else None,
                'market_cap_rank': coin.get('market_cap_rank'),
                'total_volume_usd': int(coin.get('total_volume', 0)) if coin.get('total_volume') else None,
                'ath_usd': Decimal(str(coin.get('ath', 0))) if coin.get('ath') else None,
                'ath_date': parse_date(coin.get('ath_date')),
                'atl_usd': Decimal(str(coin.get('atl', 0))) if coin.get('atl') else None,
                'atl_date': parse_date(coin.get('atl_date')),
                'circulating_supply': Decimal(str(coin.get('circulating_supply', 0))) if coin.get('circulating_supply') else None,
                'total_supply': Decimal(str(coin.get('total_supply', 0))) if coin.get('total_supply') else None,
                'max_supply': Decimal(str(coin.get('max_supply', 0))) if coin.get('max_supply') else None,
                'last_updated': parse_date(coin.get('last_updated'))
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=params)
            success_count += 1
            
        except Exception as e:
            print(f"❌ 코인 저장 실패: {coin.get('id', 'Unknown')} - {str(e)}")
            error_count += 1
            continue
    
    
    print(f"✅ 데이터 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    return {
        'success_count': success_count,
        'error_count': error_count,

        'execution_time': context['execution_date'].isoformat()
    }

# DAG 정의
with DAG(
    dag_id='ingest_coingecko_id_mapping_k8s',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    description='CoinGecko 코인 ID 매핑 정보 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'crypto', 'id-mapping', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_coingecko_id_mapping_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_id_mapping.sql',
    )
    
    # 2. API 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_coingecko_markets',
        python_callable=fetch_coingecko_markets,
    )
    
    # 3. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_mapping_data',
        python_callable=process_and_store_mapping_data,
    )
    
    # Task 의존성
    create_table >> fetch_data >> process_data