"""
빗썸 매칭 기반 CoinGecko Tickers 수집 DAG
- 빗썸 414개 코인과 매칭된 CoinGecko ID로 API 호출
- API 키 3개 로테이션으로 제한 해결
- 12시간마다 김치프리미엄 계산용 데이터 수집
- 확장된 거래소 목록 (19개) 지원
- UPSERT SQL 파일 사용으로 안정적인 데이터 저장
"""

from datetime import datetime, timedelta
import os
import json
import requests
import logging
import time
from typing import List, Dict, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ========================================================================================
# 설정 및 상수
# ========================================================================================

# SQL 파일 경로
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_coingecko_tickers_bithumb.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# API 키 설정 (Airflow Variable에서 가져오기)
def get_api_keys() -> List[str]:
    """CoinGecko API 키 3개 가져오기"""
    try:
        return [
            Variable.get('COINGECKO_API_KEY_2'),
            Variable.get('COINGECKO_API_KEY_3'), 
            Variable.get('COINGECKO_API_KEY_4')
        ]
    except Exception as e:
        logging.warning(f"API 키 Variable 가져오기 실패: {e}")
        # 테스트용 기본값 (실제 운영시 삭제)
        return ['demo-key-1', 'demo-key-2', 'demo-key-3']

# 지정된 거래소 목록 (19개)
PRIORITY_EXCHANGES = [
    'upbit',                    # 한국 #1 (김치프리미엄 계산 필수)
    'bithumb',                  # 한국 #2 (김치프리미엄 계산 필수)  
    'binance',                  # 글로벌 #1 (기준가격)
    'coinbase',                 # 미국 대표
    'kraken',                   # 유럽 대표
    'okx',                      # 아시아 메이저
    'bybit',                    # 파생상품 강세
    'kucoin',                   # 알트코인 강세
    'gate',                     # 광범위한 커버리지
    'huobi',                    # 아시아 메이저
    # 추가 거래소들
    'azbit',                    # Azbit
    'mexc',                     # MEXC
    'hotcoin',                  # Hotcoin
    'bitget',                   # Bitget
    'coinbase-exchange',        # Coinbase Exchange (Pro)
    'digifinex',               # DigiFinex
    'crypto_com',              # Crypto.com Exchange
    'cryptocom',               # Crypto.com (alternative ID)
    'coinbase_exchange'        # Coinbase Exchange (alternative ID)
]

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 9, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# ========================================================================================
# DAG 정의
# ========================================================================================

with DAG(
    dag_id='ingest_coingecko_tickers_bithumb_matched_k8s',
    default_args=default_args,
    description='빗썸 매칭 기반 CoinGecko Tickers 수집 (김치프리미엄 분석용)',
    schedule_interval='0 */12 * * *',  # 12시간마다 실행 (00:00, 12:00)
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR],
    tags=['coingecko', 'tickers', 'bithumb', 'kimchi-premium', 'crypto'],
) as dag:

    # ====================================================================================
    # 핵심 함수들
    # ====================================================================================

    def get_bithumb_matched_coins(**context) -> List[Dict]:
        """빗썸 매칭 테이블에서 CoinGecko ID 목록 조회"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 매칭 성공한 코인들 조회 (우선순위: 시가총액 순위 → 매칭 점수)
        query = """
        SELECT 
            market_code,
            symbol,
            coingecko_id,
            coingecko_name,
            market_cap_rank,
            match_method,
            match_score
        FROM bithumb_coingecko_mapping
        WHERE coingecko_id IS NOT NULL 
          AND coingecko_id != ''
        ORDER BY 
            CASE 
                WHEN market_cap_rank IS NULL THEN 999999 
                ELSE market_cap_rank 
            END ASC,
            match_score DESC,
            symbol ASC
        """
        
        records = hook.get_records(query)
        
        coins = []
        for record in records:
            coins.append({
                'market_code': record[0],      # KRW-BTC
                'symbol': record[1],           # BTC
                'coingecko_id': record[2],     # bitcoin
                'coingecko_name': record[3],   # Bitcoin
                'market_cap_rank': record[4],  # 1
                'match_method': record[5],     # MANUAL_MAPPING
                'match_score': record[6]       # 95.00
            })
        
        logging.info(f"✅ 빗썸 매칭된 코인 수: {len(coins)}개")
        
        # 상위 10개 코인 로깅 (디버깅용)
        logging.info("📋 상위 10개 매칭 결과:")
        for i, coin in enumerate(coins[:10]):
            logging.info(f"  {i+1:2d}. {coin['market_code']:12} → {coin['coingecko_id']:25} "
                        f"(Rank: {coin['market_cap_rank'] or 'N/A':>4}, Score: {coin['match_score']})")
        
        return coins

    def collect_tickers_from_coingecko(**context) -> Dict:
        """CoinGecko Tickers API 호출 및 데이터 수집"""

        coins = context['ti'].xcom_pull(task_ids='get_bithumb_matched_coins')
        if not coins:
            raise ValueError("이전 태스크에서 코인 데이터를 받지 못했습니다")
        
        api_keys = get_api_keys()
        logging.info(f"🔑 API 키 개수: {len(api_keys)}개")
        
        results = {
            'success': [],
            'failed': [],
            'api_calls': 0,
            'total_tickers': 0,
            'api_key_usage': {key[:8] + '...' if len(key) > 8 else key: 0 for key in api_keys}
        }
        
        api_key_index = 0
        
        for i, coin in enumerate(coins):
            coingecko_id = coin['coingecko_id']
            market_code = coin['market_code']
            symbol = coin['symbol']
            
            # API 키 로테이션
            current_api_key = api_keys[api_key_index]
            api_key_display = current_api_key[:8] + '...' if len(current_api_key) > 8 else current_api_key
            api_key_index = (api_key_index + 1) % len(api_keys)
            
            try:
                # CoinGecko Tickers API 호출 (거래소 제한 없음)
                url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}/tickers'
                
                params = {
                    'include_exchange_logo': 'false',
                    'page': 1,
                    'order': 'volume_desc',  # 거래량 높은 순으로 정렬
                    'depth': 'false'  # 김치프리미엄 계산에는 불필요
                }
                
                headers = {
                    'accept': 'application/json',
                    'x-cg-demo-api-key': current_api_key
                }
                
                logging.info(f"🔄 API 호출 {i+1:3d}/{len(coins)} | "
                           f"Key: {api_key_display} | {market_code} ({coingecko_id})")
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                results['api_calls'] += 1
                results['api_key_usage'][api_key_display] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    tickers = data.get('tickers', [])
                    coin_name = data.get('name', coin['coingecko_name'])
                    
                    # 스마트 티커 필터링
                    priority_tickers = []
                    other_tickers = []
                    
                    for ticker in tickers:
                        converted_last = ticker.get('converted_last', {})
                        if converted_last and converted_last.get('usd') is not None:
                            market = ticker.get('market', {})
                            exchange_id = market.get('identifier', '').lower()
                            
                            # 우선순위 거래소 여부 확인
                            if exchange_id in PRIORITY_EXCHANGES:
                                priority_tickers.append(ticker)
                            else:
                                other_tickers.append(ticker)
                    
                    # 최종 티커 선택
                    valid_tickers = priority_tickers.copy()
                    
                    # 기타 거래소들을 거래량으로 정렬하여 상위 10개 추가
                    other_tickers_sorted = sorted(
                        other_tickers, 
                        key=lambda x: x.get('converted_volume', {}).get('usd', 0) or 0, 
                        reverse=True
                    )
                    valid_tickers.extend(other_tickers_sorted[:10])
                    
                    # 결과 통계
                    priority_count = len(priority_tickers)
                    other_count = min(len(other_tickers_sorted), 10)
                    total_exchanges = set(ticker.get('market', {}).get('identifier', '') for ticker in valid_tickers)
                    
                    if valid_tickers:
                        results['success'].append({
                            'market_code': market_code,
                            'coingecko_id': coingecko_id,
                            'symbol': symbol,
                            'coin_name': coin_name,
                            'match_method': coin['match_method'],
                            'market_cap_rank': coin['market_cap_rank'],
                            'tickers': valid_tickers,
                            'ticker_count': len(valid_tickers),
                            'priority_exchanges': priority_count,
                            'other_exchanges': other_count,
                            'total_exchanges': len(total_exchanges)
                        })
                        results['total_tickers'] += len(valid_tickers)
                        
                        logging.info(f"✅ 성공: {symbol} - {len(valid_tickers)}개 티커 "
                                   f"(우선순위: {priority_count}, 기타: {other_count}, "
                                   f"총 거래소: {len(total_exchanges)}개)")
                    else:
                        results['failed'].append({
                            'market_code': market_code,
                            'coingecko_id': coingecko_id,
                            'symbol': symbol,
                            'reason': 'NO_VALID_USD_TICKERS'
                        })
                        logging.warning(f"❌ USD 티커 없음: {symbol}")
                
                elif response.status_code == 429:
                    # Rate Limit 처리
                    retry_after = int(response.headers.get('retry-after', '60'))
                    logging.warning(f"⏳ Rate limit 도달: {symbol}, {retry_after}초 대기")
                    
                    results['failed'].append({
                        'market_code': market_code,
                        'coingecko_id': coingecko_id,
                        'symbol': symbol,
                        'reason': f'RATE_LIMITED_RETRY_AFTER_{retry_after}'
                    })
                    
                    time.sleep(min(retry_after, 120))  # 최대 2분 대기
                
                elif response.status_code == 404:
                    results['failed'].append({
                        'market_code': market_code,
                        'coingecko_id': coingecko_id,
                        'symbol': symbol,
                        'reason': 'COIN_NOT_FOUND_404'
                    })
                    logging.error(f"❌ 코인 없음: {coingecko_id} (404)")
                
                else:
                    results['failed'].append({
                        'market_code': market_code,
                        'coingecko_id': coingecko_id,
                        'symbol': symbol,
                        'reason': f'HTTP_ERROR_{response.status_code}'
                    })
                    logging.error(f"❌ HTTP 에러: {symbol} - {response.status_code}")
                
                # API 제한 방지를 위한 지연
                if results['api_calls'] % 50 == 0:  # 50회마다 1분 휴식
                    logging.info(f"⏳ API 제한 방지: {results['api_calls']}회 호출 후 60초 휴식")
                    time.sleep(60)
                else:
                    time.sleep(2)  # 기본 2초 간격
                    
            except requests.exceptions.Timeout:
                results['failed'].append({
                    'market_code': market_code,
                    'coingecko_id': coingecko_id,
                    'symbol': symbol,
                    'reason': 'TIMEOUT_30s'
                })
                logging.error(f"❌ 타임아웃: {symbol}")
                
            except Exception as e:
                results['failed'].append({
                    'market_code': market_code,
                    'coingecko_id': coingecko_id,
                    'symbol': symbol,
                    'reason': f'EXCEPTION: {str(e)[:100]}'
                })
                logging.error(f"❌ 예외 발생: {symbol} - {e}")
        
        # 수집 결과 요약 로깅
        logging.info("=" * 80)
        logging.info("📊 CoinGecko Tickers 데이터 수집 완료")
        logging.info("-" * 80)
        logging.info(f"✅ 성공한 코인:  {len(results['success']):4d}개")
        logging.info(f"❌ 실패한 코인:  {len(results['failed']):4d}개") 
        logging.info(f"📞 총 API 호출:  {results['api_calls']:4d}회")
        logging.info(f"📈 총 티커 수:   {results['total_tickers']:4d}개")
        
        # API 키별 사용량
        logging.info(f"🔑 API 키 사용량:")
        for key_display, usage in results['api_key_usage'].items():
            logging.info(f"    {key_display}: {usage:3d}회")
            
        success_rate = len(results['success']) / len(coins) * 100 if coins else 0
        logging.info(f"📈 성공률:       {success_rate:5.1f}%")
        
        # 거래소별 수집 통계
        logging.info("📊 주요 거래소별 수집 통계:")
        exchange_stats = {}
        
        for success in results['success']:
            for ticker in success['tickers']:
                exchange_id = ticker.get('market', {}).get('identifier', 'unknown')
                if exchange_id not in exchange_stats:
                    exchange_stats[exchange_id] = {'coins': set(), 'tickers': 0}
                exchange_stats[exchange_id]['coins'].add(success['symbol'])
                exchange_stats[exchange_id]['tickers'] += 1
        
        # 거래소별 통계 출력 (많은 순으로)
        sorted_exchanges = sorted(
            exchange_stats.items(), 
            key=lambda x: x[1]['tickers'], 
            reverse=True
        )
        
        for exchange_id, stats in sorted_exchanges[:15]:  # 상위 15개만 출력
            coins_count = len(stats['coins'])
            tickers_count = stats['tickers']
            logging.info(f"    {exchange_id:20}: {coins_count:3d}개 코인, {tickers_count:4d}개 티커")
        
        if len(sorted_exchanges) > 15:
            logging.info(f"    ... 외 {len(sorted_exchanges)-15}개 거래소 더")
        
        logging.info("=" * 80)
        
        # 주요 실패 사유 분석
        if results['failed']:
            failure_reasons = {}
            for failed in results['failed']:
                reason = failed['reason']
                if reason not in failure_reasons:
                    failure_reasons[reason] = 0
                failure_reasons[reason] += 1
            
            logging.info("❌ 주요 실패 사유:")
            for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
                logging.info(f"    {reason}: {count}개")
        
        return results

    def store_tickers_to_database(**context):
        """수집된 티커 데이터를 PostgreSQL에 UPSERT 방식으로 저장"""
        
        results = context['ti'].xcom_pull(task_ids='collect_coingecko_tickers_data')
        if not results:
            raise ValueError("이전 태스크에서 수집된 티커 데이터를 받지 못했습니다")
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logging.info(f"💾 UPSERT SQL 파일을 사용하여 데이터 저장 시작")
        
        success_count = 0
        error_count = 0
        
        for success in results['success']:
            market_code = success['market_code']
            coingecko_id = success['coingecko_id']
            symbol = success['symbol']
            coin_name = success['coin_name']
            match_method = success['match_method']
            market_cap_rank = success['market_cap_rank']
            
            for ticker in success['tickers']:
                try:
                    # 티커 데이터 파싱
                    base = ticker.get('base', '')
                    target = ticker.get('target', '')
                    
                    market = ticker.get('market', {})
                    exchange_name = market.get('name', '')
                    exchange_id = market.get('identifier', '')
                    
                    # 숫자 데이터 안전 처리
                    last_price = ticker.get('last')
                    volume_24h = ticker.get('volume')
                    
                    # 거래량이 너무 클 경우 None으로 처리
                    if volume_24h and volume_24h > 999999999999:  # 10^12 제한
                        volume_24h = None
                    
                    converted_last = ticker.get('converted_last', {})
                    converted_last_usd = converted_last.get('usd') if converted_last else None
                    
                    converted_volume = ticker.get('converted_volume', {})
                    converted_volume_usd = converted_volume.get('usd') if converted_volume else None
                    
                    # 거래량이 너무 클 경우 None으로 처리  
                    if converted_volume_usd and converted_volume_usd > 999999999999:  # 10^12 제한
                        converted_volume_usd = None
                    
                    trust_score = ticker.get('trust_score', '')
                    bid_ask_spread = ticker.get('bid_ask_spread_percentage')
                    
                    # 시간 정보 파싱
                    def parse_timestamp(ts_str):
                        if ts_str:
                            # 'Z' 시간대를 '+00:00'으로 변환
                            return ts_str.replace('Z', '+00:00') if ts_str.endswith('Z') else ts_str
                        return None
                    
                    parsed_timestamp = parse_timestamp(ticker.get('timestamp'))
                    parsed_last_traded = parse_timestamp(ticker.get('last_traded_at'))
                    parsed_last_fetch = parse_timestamp(ticker.get('last_fetch_at'))
                    
                    # 기타 플래그들
                    is_anomaly = ticker.get('is_anomaly', False)
                    is_stale = ticker.get('is_stale', False)
                    trade_url = ticker.get('trade_url', '')
                    coin_mcap_usd = ticker.get('coin_mcap_usd')
                    
                    # 파라미터 준비 (named parameters 방식)
                    params = {
                        'market_code': market_code,
                        'coingecko_id': coingecko_id,
                        'symbol': symbol,
                        'coin_name': coin_name,
                        'base': base,
                        'target': target,
                        'exchange_name': exchange_name,
                        'exchange_id': exchange_id,
                        'last_price': last_price,
                        'volume_24h': volume_24h,
                        'converted_last_usd': converted_last_usd,
                        'converted_volume_usd': converted_volume_usd,
                        'trust_score': trust_score,
                        'bid_ask_spread_percentage': bid_ask_spread,
                        'timestamp': parsed_timestamp,
                        'last_traded_at': parsed_last_traded,
                        'last_fetch_at': parsed_last_fetch,
                        'is_anomaly': is_anomaly,
                        'is_stale': is_stale,
                        'trade_url': trade_url,
                        'coin_mcap_usd': coin_mcap_usd,
                        'match_method': match_method,
                        'market_cap_rank': market_cap_rank
                    }
                    
                    # UPSERT SQL 실행
                    hook.run(UPSERT_SQL, parameters=params)
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    logging.error(f"❌ 레코드 저장 실패 ({symbol}-{exchange_id}): {str(e)[:100]}")
                    continue
        
        # 저장 결과 요약
        logging.info("=" * 80)
        logging.info("💾 데이터베이스 UPSERT 저장 완료")
        logging.info("-" * 80)
        logging.info(f"✅ 성공적으로 저장: {success_count:,}개")
        logging.info(f"❌ 저장 실패:       {error_count:,}개")
        
        success_rate = (success_count / (success_count + error_count) * 100) if (success_count + error_count) > 0 else 0
        logging.info(f"📈 저장 성공률:     {success_rate:5.1f}%")
        
        # 저장된 데이터 검증
        verification_query = """
        SELECT 
            COUNT(*) as total_tickers,
            COUNT(DISTINCT coingecko_id) as unique_coins,
            COUNT(DISTINCT exchange_id) as unique_exchanges,
            COUNT(*) FILTER (WHERE DATE(created_at) = CURRENT_DATE OR DATE(updated_at) = CURRENT_DATE) as today_records,
            MIN(created_at) as first_record,
            MAX(GREATEST(created_at, COALESCE(updated_at, created_at))) as last_record
        FROM coingecko_tickers_bithumb
        """
        
        verification = hook.get_first(verification_query)
        if verification:
            logging.info(f"📊 저장 검증 결과:")
            logging.info(f"    전체 티커 수:   {verification[0]:,}개")
            logging.info(f"    고유 코인 수:   {verification[1]:,}개") 
            logging.info(f"    고유 거래소:    {verification[2]:,}개")
            logging.info(f"    오늘 처리 수:   {verification[3]:,}개")
            logging.info(f"    데이터 기간:    {verification[4]} ~ {verification[5]}")
        
        logging.info("=" * 80)
        
        return {
            'success_count': success_count,
            'error_count': error_count,
            'total_processed': success_count + error_count,
            'execution_time': context['execution_date'].isoformat()
        }

    # ====================================================================================
    # Task 정의 
    # ====================================================================================

    # Task 1: 테이블 생성
    create_tickers_table = PostgresOperator(
        task_id='create_coingecko_tickers_bithumb_table',
        postgres_conn_id='postgres_default',
        sql='create_coingecko_tickers_bithumb.sql',
    )

    # Task 2: 빗썸 매칭 코인 조회
    get_coins_task = PythonOperator(
        task_id='get_bithumb_matched_coins',
        python_callable=get_bithumb_matched_coins,
    )

    # Task 3: CoinGecko API 데이터 수집
    collect_tickers_task = PythonOperator(
        task_id='collect_coingecko_tickers_data',
        python_callable=collect_tickers_from_coingecko,
    )

    # Task 4: 데이터베이스 저장
    store_tickers_task = PythonOperator(
        task_id='store_tickers_to_database',
        python_callable=store_tickers_to_database,
    )

    # ====================================================================================
    # Task 의존성 설정
    # ====================================================================================

    create_tickers_table >> get_coins_task >> collect_tickers_task >> store_tickers_task