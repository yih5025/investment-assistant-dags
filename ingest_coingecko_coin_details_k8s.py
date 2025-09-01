from airflow.models import Variable
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

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_coin_details.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_target_coin_ids(**context):
    """
    처리할 코인 ID 목록 조회 (점진적 확장 전략)
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 설정
    DAILY_TARGET_COINS = 200  # 하루 처리할 코인 수
    MAX_TARGET_COINS = 1000   # 최대 목표 코인 수
    
    # 이미 coin details 데이터를 수집한 코인들 확인
    processed_coins = hook.get_first("""
        SELECT COUNT(DISTINCT coingecko_id) 
        FROM coingecko_coin_details 
        WHERE updated_at >= CURRENT_DATE - INTERVAL '30 days';
    """)
    
    processed_count = processed_coins[0] if processed_coins else 0
    
    print(f"최근 30일 내 처리된 코인: {processed_count}개")
    
    # 목표 달성 여부 확인
    if processed_count >= MAX_TARGET_COINS:
        print(f"목표 달성 완료 ({processed_count}/{MAX_TARGET_COINS}개). DAG 종료.")
        context['ti'].xcom_push(key='coin_list', value=[])
        return {'status': 'completed', 'processed_count': processed_count}
    
    # 미처리 코인 목록 조회 (시총 순)
    query = """
    WITH unprocessed_coins AS (
        SELECT 
            cg.coingecko_id, 
            cg.symbol, 
            cg.name, 
            cg.market_cap_rank
        FROM coingecko_id_mapping cg
        LEFT JOIN (
            SELECT DISTINCT coingecko_id 
            FROM coingecko_coin_details 
            WHERE updated_at >= CURRENT_DATE - INTERVAL '30 days'
        ) processed ON cg.coingecko_id = processed.coingecko_id
        WHERE processed.coingecko_id IS NULL  -- 미처리만
        AND cg.market_cap_rank IS NOT NULL   -- 순위가 있는 것만
        AND cg.market_cap_rank <= %s         -- 상위 N개만
    )
    SELECT coingecko_id, symbol, name, market_cap_rank
    FROM unprocessed_coins
    ORDER BY market_cap_rank ASC
    LIMIT %s;
    """
    
    # 남은 목표량에 따라 처리량 결정
    remaining_target = min(MAX_TARGET_COINS - processed_count, DAILY_TARGET_COINS)
    
    results = hook.get_records(query, parameters=(MAX_TARGET_COINS, remaining_target))
    coin_list = [{'id': row[0], 'symbol': row[1], 'name': row[2], 'rank': row[3]} for row in results]
    
    print(f"오늘 처리할 코인: {len(coin_list)}개")
    print(f"진행률: {processed_count + len(coin_list)}/{MAX_TARGET_COINS}개 ({((processed_count + len(coin_list))/MAX_TARGET_COINS*100):.1f}%)")
    
    # 처리할 코인이 있는 경우 상위 5개 로그 출력
    if coin_list:
        print("오늘 처리할 상위 5개 코인:")
        for coin in coin_list[:5]:
            print(f"  순위 {coin['rank']}: {coin['symbol']} ({coin['name']})")
    
    # XCom에 저장
    context['ti'].xcom_push(key='coin_list', value=coin_list)
    return {
        'status': 'processing',
        'target_coins': len(coin_list),
        'processed_count': processed_count,
        'progress_percentage': round((processed_count + len(coin_list))/MAX_TARGET_COINS*100, 1)
    }

def fetch_coin_details_batch(**context):
    """
    코인 상세 정보 배치 수집 (Rate Limit 고려, API 키 적용)
    """
    # XCom에서 코인 목록 가져오기
    coin_list = context['ti'].xcom_pull(task_ids='get_target_coin_ids', key='coin_list')
    
    if not coin_list:
        print("처리할 코인이 없습니다.")
        context['ti'].xcom_push(key='batch_results', value=[])
        return {'total_processed': 0, 'success_count': 0, 'failed_count': 0}
    
    # API 키 가져오기
    api_key = Variable.get('COINGECKO_API_KEY_1')
    if not api_key:
        raise ValueError("COINGECKO_API_KEY_1이 설정되지 않았습니다")
    
    API_BASE_URL = "https://api.coingecko.com/api/v3/coins"
    BATCH_SIZE = 20  # Tickers보다 큰 배치 (단일 API 호출이므로)
    
    all_results = []
    total_batches = (len(coin_list) + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"전체 {len(coin_list)}개 코인을 {total_batches}개 배치로 처리")
    
    # 배치별 처리
    for i in range(0, len(coin_list), BATCH_SIZE):
        batch = coin_list[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        print(f"배치 {batch_num}/{total_batches} 처리 중 ({len(batch)}개 코인)")
        
        batch_success = 0
        batch_failed = 0
        
        for coin in batch:
            coin_id = coin['id']
            
            # 재시도 로직
            for attempt in range(3):
                try:
                    url = f"{API_BASE_URL}/{coin_id}"
                    params = {
                        'localization': 'false',
                        'tickers': 'false', 
                        'market_data': 'true',
                        'community_data': 'true',
                        'developer_data': 'true',
                        'sparkline': 'false'
                    }
                    
                    headers = {
                        "accept": "application/json",
                        "x-cg-demo-api-key": api_key
                    }
                    
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        coin_data = response.json()
                        all_results.append({
                            'coin_id': coin_id,
                            'status': 'success',
                            'data': coin_data
                        })
                        batch_success += 1
                        break
                        
                    elif response.status_code == 429:  # Rate limit
                        wait_time = 60 * (attempt + 1)
                        print(f"Rate limit 도달. {wait_time}초 대기")
                        if attempt < 2:
                            time.sleep(wait_time)
                            continue
                            
                    elif response.status_code == 404:
                        all_results.append({
                            'coin_id': coin_id,
                            'status': 'not_found',
                            'error': f'Coin {coin_id} not found'
                        })
                        batch_failed += 1
                        break
                        
                    else:
                        raise ValueError(f"API 오류: {response.status_code}")
                        
                except requests.RequestException as e:
                    print(f"{coin_id} 요청 실패 (시도 {attempt + 1}/3): {str(e)}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
            else:
                # 모든 재시도 실패
                all_results.append({
                    'coin_id': coin_id,
                    'status': 'failed',
                    'error': 'All retries failed'
                })
                batch_failed += 1
            
            # Rate Limit 방지
            time.sleep(2)  # Tickers보다 긴 대기 시간
        
        print(f"배치 {batch_num} 완료: 성공 {batch_success}개, 실패 {batch_failed}개")
        
        # 배치 간 대기 (Rate Limit 방지)
        if i + BATCH_SIZE < len(coin_list):
            print(f"배치 간 대기 (60초)")
            time.sleep(60)
    
    # 결과 통계
    success_count = len([r for r in all_results if r['status'] == 'success'])
    failed_count = len([r for r in all_results if r['status'] in ['failed', 'not_found']])
    
    print(f"전체 수집 완료: 성공 {success_count}개, 실패 {failed_count}개")
    
    # XCom에 저장
    context['ti'].xcom_push(key='batch_results', value=all_results)
    
    return {
        'total_processed': len(all_results),
        'success_count': success_count,
        'failed_count': failed_count
    }

def process_and_store_details(**context):
    """
    수집된 코인 상세 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    batch_results = context['ti'].xcom_pull(task_ids='fetch_coin_details_batch', key='batch_results')
    
    if not batch_results:
        raise ValueError("❌ 이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"🚀 {len(batch_results)}개 코인 데이터 저장 시작")
    
    for result in batch_results:
        coin_id = result['coin_id']
        
        if result['status'] != 'success':
            error_count += 1
            continue
        
        try:
            coin_data = result['data']
            
            # 복잡한 중첩 데이터 추출 함수들
            def safe_get(data, *keys, default=None):
                """중첩 딕셔너리에서 안전하게 값 추출"""
                current = data
                for key in keys:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        return default
                return current
            
            def parse_date(date_str):
                """날짜 문자열 파싱"""
                if date_str:
                    try:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    except:
                        return None
                return None
            
            def extract_urls(links_dict, key):
                """링크 딕셔너리에서 첫 번째 URL 추출"""
                urls = safe_get(links_dict, key, [])
                return urls[0] if urls and len(urls) > 0 else None
            
            # 데이터 추출
            links = safe_get(coin_data, 'links', {})
            market_data = safe_get(coin_data, 'market_data', {})
            community_data = safe_get(coin_data, 'community_data', {})
            developer_data = safe_get(coin_data, 'developer_data', {})
            image = safe_get(coin_data, 'image', {})
            
            # 파라미터 준비
            params = {
                'coingecko_id': coin_data.get('id'),
                'symbol': coin_data.get('symbol', '').upper(),
                'name': coin_data.get('name', '')[:200],
                'web_slug': coin_data.get('web_slug', '')[:200],
                
                # Tab 1 데이터
                'description_en': safe_get(coin_data, 'description', 'en', '')[:5000],  # TEXT 제한
                'genesis_date': parse_date(coin_data.get('genesis_date')),
                'country_origin': coin_data.get('country_origin', '')[:100],
                
                # Links
                'homepage_url': extract_urls(links, 'homepage'),
                'blockchain_site': extract_urls(links, 'blockchain_site'),
                'twitter_screen_name': links.get('twitter_screen_name', '')[:100],
                'facebook_username': links.get('facebook_username', '')[:100],
                'telegram_channel_identifier': links.get('telegram_channel_identifier', '')[:100],
                'subreddit_url': links.get('subreddit_url', '')[:500],
                'github_repos': json.dumps(links.get('repos_url', {})) if links.get('repos_url') else None,
                
                # Images
                'image_thumb': image.get('thumb', '')[:500],
                'image_small': image.get('small', '')[:500],
                'image_large': image.get('large', '')[:500],
                
                # Categories
                'categories': json.dumps(coin_data.get('categories', [])),
                
                # Market Data (Tab 3)
                'current_price_usd': Decimal(str(safe_get(market_data, 'current_price', 'usd', 0))),
                'current_price_krw': Decimal(str(safe_get(market_data, 'current_price', 'krw', 0))) if safe_get(market_data, 'current_price', 'krw') else None,
                'market_cap_usd': int(safe_get(market_data, 'market_cap', 'usd', 0)) if safe_get(market_data, 'market_cap', 'usd') else None,
                'market_cap_rank': market_data.get('market_cap_rank'),
                'total_volume_usd': int(safe_get(market_data, 'total_volume', 'usd', 0)) if safe_get(market_data, 'total_volume', 'usd') else None,
                
                # ATH/ATL
                'ath_usd': Decimal(str(safe_get(market_data, 'ath', 'usd', 0))) if safe_get(market_data, 'ath', 'usd') else None,
                'ath_change_percentage': Decimal(str(safe_get(market_data, 'ath_change_percentage', 'usd', 0))) if safe_get(market_data, 'ath_change_percentage', 'usd') else None,
                'ath_date': parse_date(safe_get(market_data, 'ath_date', 'usd')),
                'atl_usd': Decimal(str(safe_get(market_data, 'atl', 'usd', 0))) if safe_get(market_data, 'atl', 'usd') else None,
                'atl_change_percentage': Decimal(str(safe_get(market_data, 'atl_change_percentage', 'usd', 0))) if safe_get(market_data, 'atl_change_percentage', 'usd') else None,
                'atl_date': parse_date(safe_get(market_data, 'atl_date', 'usd')),
                
                # Supply Data
                'total_supply': Decimal(str(market_data.get('total_supply', 0))) if market_data.get('total_supply') else None,
                'circulating_supply': Decimal(str(market_data.get('circulating_supply', 0))) if market_data.get('circulating_supply') else None,
                'max_supply': Decimal(str(market_data.get('max_supply', 0))) if market_data.get('max_supply') else None,
                
                # Price Changes
                'price_change_24h_usd': Decimal(str(market_data.get('price_change_24h', 0))),
                'price_change_percentage_24h': Decimal(str(market_data.get('price_change_percentage_24h', 0))),
                'price_change_percentage_7d': Decimal(str(market_data.get('price_change_percentage_7d', 0))),
                'price_change_percentage_30d': Decimal(str(market_data.get('price_change_percentage_30d', 0))),
                
                # Community Data (Tab 2)
                'community_score': Decimal(str(coin_data.get('community_score', 0))) if coin_data.get('community_score') else None,
                'twitter_followers': community_data.get('twitter_followers'),
                'reddit_subscribers': community_data.get('reddit_subscribers'),
                'telegram_channel_user_count': community_data.get('telegram_channel_user_count'),
                
                # Developer Data (Tab 2)
                'developer_score': Decimal(str(coin_data.get('developer_score', 0))) if coin_data.get('developer_score') else None,
                'forks': developer_data.get('forks'),
                'stars': developer_data.get('stars'),
                'total_issues': developer_data.get('total_issues'),
                'closed_issues': developer_data.get('closed_issues'),
                'commit_count_4_weeks': developer_data.get('commit_count_4_weeks'),
                
                # Other Scores
                'public_interest_score': Decimal(str(coin_data.get('public_interest_score', 0))) if coin_data.get('public_interest_score') else None,
                'liquidity_score': Decimal(str(coin_data.get('liquidity_score', 0))) if coin_data.get('liquidity_score') else None,
                
                # Timestamps
                'coingecko_last_updated': parse_date(coin_data.get('last_updated'))
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=params)
            success_count += 1
            
        except Exception as e:
            print(f"❌ 코인 {coin_id} 저장 실패: {str(e)}")
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
    dag_id='ingest_coingecko_coin_details_k8s',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    description='CoinGecko 코인 상세 정보 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'crypto', 'details', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_coingecko_coin_details_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_coin_details.sql',
    )
    
    # 2. 처리할 코인 목록 조회
    get_coins = PythonOperator(
        task_id='get_target_coin_ids',
        python_callable=get_target_coin_ids,
    )
    
    # 3. 코인 상세 데이터 수집
    fetch_details = PythonOperator(
        task_id='fetch_coin_details_batch',
        python_callable=fetch_coin_details_batch,
    )
    
    # 4. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_details',
        python_callable=process_and_store_details,
    )
    
    # Task 의존성
    create_table >> get_coins >> fetch_details >> process_data