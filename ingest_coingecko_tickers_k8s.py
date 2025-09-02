from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
import decimal
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
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_tickers.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# 설정
DAILY_TARGET_COINS = 200  # 하루 처리할 코인 수
MAX_TARGET_COINS = 1000   # 최대 목표 코인 수
BATCH_SIZE = 15           # 배치당 처리 코인 수 (Rate Limit 고려)

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

def get_target_coin_ids(**context):
    """
    처리할 코인 ID 목록 조회 (점진적 확장 전략)
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 이미 tickers 데이터를 수집한 코인들 확인
    processed_coins = hook.get_first("""
        SELECT COUNT(DISTINCT coingecko_id) 
        FROM coingecko_tickers 
        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';
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
            FROM coingecko_tickers 
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
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

def fetch_tickers_batch(**context):
    """
    코인별 Tickers 데이터 배치 수집
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
                    # 김치프리미엄 분석을 위한 주요 거래소 필터링
                    url = f"{API_BASE_URL}/{coin_id}/tickers"
                    params = {
                        'exchange_ids': 'binance,coinbase-exchange,kraken,upbit,bithumb,bybit,okx,huobi,gate',  
                        'include_exchange_logo': 'false',
                        'page': 1,
                        'order': 'trust_score_desc',  # 신뢰도 높은 순
                        'depth': 'false',
                        'dex_pair_format': 'symbol'  # DEX 페어를 심볼로 표시
                    }
                    
                    headers = {
                        "accept": "application/json",
                        "x-cg-demo-api-key": api_key
                    }
                    
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        ticker_data = response.json()
                        
                        # 응답 검증
                        if 'name' not in ticker_data or 'tickers' not in ticker_data:
                            raise ValueError("응답 형식이 올바르지 않습니다")
                        
                        tickers = ticker_data.get('tickers', [])
                        print(f"  {coin_id}: {len(tickers)}개 티커 수집")
                        
                        all_results.append({
                            'coin_id': coin_id,
                            'coin_info': coin,
                            'status': 'success',
                            'data': ticker_data,
                            'ticker_count': len(tickers)
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
                            'coin_info': coin,
                            'status': 'not_found',
                            'error': f'Coin {coin_id} tickers not found',
                            'ticker_count': 0
                        })
                        batch_failed += 1
                        break
                        
                    else:
                        raise ValueError(f"API 오류: {response.status_code} - {response.text[:200]}")
                        
                except requests.RequestException as e:
                    print(f"{coin_id} 요청 실패 (시도 {attempt + 1}/3): {str(e)}")
                    if attempt < 2:
                        time.sleep(3)
                        continue
            else:
                # 모든 재시도 실패
                all_results.append({
                    'coin_id': coin_id,
                    'coin_info': coin,
                    'status': 'failed',
                    'error': 'All retries failed',
                    'ticker_count': 0
                })
                batch_failed += 1
            
            # Rate Limit 방지 (요청 간 간격)
            time.sleep(3)
        
        print(f"배치 {batch_num} 완료: 성공 {batch_success}개, 실패 {batch_failed}개")
        
        # 배치 간 대기 (Rate Limit 방지)
        if i + BATCH_SIZE < len(coin_list):
            print(f"배치 간 대기 (90초)")
            time.sleep(90)
    
    # 결과 통계
    success_count = len([r for r in all_results if r['status'] == 'success'])
    failed_count = len([r for r in all_results if r['status'] in ['failed', 'not_found']])
    total_tickers = sum([r.get('ticker_count', 0) for r in all_results])
    
    print(f"전체 수집 완료: 성공 {success_count}개, 실패 {failed_count}개")
    print(f"총 {total_tickers}개 티커 수집")
    
    # XCom에 저장
    context['ti'].xcom_push(key='batch_results', value=all_results)
    
    return {
        'total_processed': len(all_results),
        'success_count': success_count,
        'failed_count': failed_count,
        'total_tickers': total_tickers
    }

def process_and_store_tickers(**context):
    """
    수집된 Tickers 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    batch_results = context['ti'].xcom_pull(task_ids='fetch_tickers_batch', key='batch_results')
    
    if not batch_results:
        print("이전 태스크에서 데이터를 받지 못했습니다")
        return {'success_count': 0, 'error_count': 0}
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    execution_date = context['execution_date']
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
    
    print(f"배치 {batch_id} Tickers 데이터 저장 시작")
    
    success_count = 0
    error_count = 0
    korean_exchange_count = 0
    global_exchange_count = 0
    
    # 안전한 데이터 변환 함수들
    def safe_decimal(value, default=None):
        """안전하게 Decimal로 변환"""
        if value is None:
            return default
        try:
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
    
    def parse_timestamp(timestamp_str):
        """타임스탬프 문자열을 datetime으로 변환"""
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except (TypeError, ValueError) as e:
                print(f"타임스탬프 변환 실패: {str(e)}, 값: {timestamp_str}")
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
    
    # 안전한 execution_date 변환
    safe_collected_at = safe_execution_date(execution_date)
    
    for result in batch_results:
        coin_id = result['coin_id']
        coin_info = result['coin_info']
        
        if result['status'] != 'success':
            error_count += 1
            continue
        
        try:
            ticker_data = result['data']
            tickers = ticker_data.get('tickers', [])
            
            # 각 거래소별 티커 데이터 저장
            for ticker in tickers:
                try:
                    # 데이터 검증
                    market_info = ticker.get('market', {})
                    if not market_info or not ticker.get('base'):
                        continue
                    
                    market_name = market_info.get('name', '')
                    market_identifier = market_info.get('identifier', '')
                    
                    # 한국/글로벌 거래소 구분
                    is_korean_exchange = market_identifier.lower() in ['upbit', 'bithumb', 'korbit', 'coinone']
                    if is_korean_exchange:
                        korean_exchange_count += 1
                    else:
                        global_exchange_count += 1
                    
                    # 파라미터 준비
                    params = {
                        'batch_id': batch_id,
                        'coingecko_id': coin_id,
                        'coin_symbol': coin_info.get('symbol', '').upper(),
                        'coin_name': coin_info.get('name', ''),
                        'market_cap_rank': safe_int(coin_info.get('rank')),
                        
                        # 거래 정보
                        'base_symbol': ticker.get('base', '')[:20],
                        'target_symbol': ticker.get('target', '')[:20],
                        'market_name': market_name[:100],
                        'market_identifier': market_identifier[:50],
                        'is_korean_exchange': is_korean_exchange,
                        
                        # 가격 정보
                        'last_price': safe_decimal(ticker.get('last')),
                        'volume': safe_decimal(ticker.get('volume')),
                        
                        # 변환된 가격 (USD 기준 - 김치프리미엄 계산용)
                        'converted_last_usd': safe_decimal(ticker.get('converted_last', {}).get('usd')),
                        'converted_last_btc': safe_decimal(ticker.get('converted_last', {}).get('btc')),
                        'converted_volume_usd': safe_decimal(ticker.get('converted_volume', {}).get('usd')),
                        
                        # 거래소 품질 지표
                        'trust_score': ticker.get('trust_score', '')[:20],
                        'bid_ask_spread_percentage': safe_decimal(ticker.get('bid_ask_spread_percentage'), 0),
                        'has_trading_incentive': market_info.get('has_trading_incentive', False),
                        
                        # 유동성 지표 (depth 데이터가 있는 경우)
                        'cost_to_move_up_usd': safe_decimal(ticker.get('cost_to_move_up_usd')),
                        'cost_to_move_down_usd': safe_decimal(ticker.get('cost_to_move_down_usd')),
                        
                        # 상태 정보
                        'is_anomaly': ticker.get('is_anomaly', False),
                        'is_stale': ticker.get('is_stale', False),
                        
                        # URL 정보
                        'trade_url': ticker.get('trade_url', '')[:500],
                        
                        # 시간 정보
                        'timestamp': parse_timestamp(ticker.get('timestamp')),
                        'last_traded_at': parse_timestamp(ticker.get('last_traded_at')),
                        'last_fetch_at': parse_timestamp(ticker.get('last_fetch_at')),
                        'collected_at': safe_collected_at
                    }
                    
                    # SQL 실행
                    try:
                        hook.run(UPSERT_SQL, parameters=params)
                        success_count += 1
                        if success_count % 100 == 0:  # 100개마다 진행 상황 로그
                            print(f"진행 상황: {success_count}개 티커 성공 처리됨")
                    except Exception as sql_error:
                        print(f"❌ SQL 실행 실패 - {coin_id}/{market_name}: {str(sql_error)}")
                        print(f"문제가 된 파라미터: {params}")
                        error_count += 1
                        continue
                    
                except Exception as e:
                    print(f"❌ 티커 데이터 처리 실패 - {coin_id}/{market_name}: {str(e)}")
                    print(f"원본 티커 데이터: {ticker}")
                    error_count += 1
                    continue
                    
        except Exception as e:
            print(f"코인 {coin_id} 처리 실패: {str(e)}")
            error_count += 1
            continue
    
    # 저장 후 통계 조회
    stats = hook.get_first("""
        SELECT 
            COUNT(*) as total_tickers,
            COUNT(DISTINCT coingecko_id) as unique_coins,
            COUNT(DISTINCT market_identifier) as unique_markets,
            COUNT(CASE WHEN is_korean_exchange = true THEN 1 END) as korean_tickers,
            COUNT(CASE WHEN is_korean_exchange = false THEN 1 END) as global_tickers,
            AVG(CASE WHEN converted_volume_usd > 0 THEN converted_volume_usd END) as avg_volume_usd
        FROM coingecko_tickers
        WHERE batch_id = %s;
    """, parameters=(batch_id,))
    
    print(f"배치 {batch_id} 저장 완료:")
    print(f"  성공: {success_count}개, 실패: {error_count}개")
    print(f"  DB 통계: 총 {stats[0]}개 티커, {stats[1]}개 코인, {stats[2]}개 거래소")
    print(f"  한국 거래소: {stats[3]}개, 글로벌 거래소: {stats[4]}개")
    print(f"  평균 거래량: ${stats[5]:,.0f}" if stats[5] else "  평균 거래량: N/A")
    
    return {
        'batch_id': batch_id,
        'success_count': success_count,
        'error_count': error_count,
        'total_tickers': stats[0],
        'unique_coins': stats[1],
        'unique_markets': stats[2],
        'korean_tickers': stats[3],
        'global_tickers': stats[4],
        'execution_time': context['execution_date'].isoformat()
    }

# DAG 정의
with DAG(
    dag_id='ingest_coingecko_tickers_k8s',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    description='CoinGecko 거래소별 Tickers 데이터 수집 (김치프리미엄 분석용)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'crypto', 'tickers', 'kimchi-premium', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_coingecko_tickers_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_tickers.sql',
    )
    
    # 2. 처리할 코인 목록 조회
    get_coins = PythonOperator(
        task_id='get_target_coin_ids',
        python_callable=get_target_coin_ids,
    )
    
    # 3. Tickers 데이터 수집
    fetch_tickers = PythonOperator(
        task_id='fetch_tickers_batch',
        python_callable=fetch_tickers_batch,
    )
    
    # 4. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_tickers',
        python_callable=process_and_store_tickers,
    )
    
    # Task 의존성
    create_table >> get_coins >> fetch_tickers >> process_data