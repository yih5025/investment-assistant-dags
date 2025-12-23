# cache_sp500_to_redis.py - SP500 데이터를 DB에서 조회하여 Redis에 캐싱
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta, time
import json
import logging
import os
import redis
import pytz

logger = logging.getLogger(__name__)

# 시장 개장 여부 확인 함수
def get_market_open_close_kst():
    """미국 시장 개장/마감 시간을 한국 시간으로 변환"""
    et_tz = pytz.timezone('US/Eastern')
    kst_tz = pytz.timezone('Asia/Seoul')
    now_et = datetime.now(et_tz)
    
    # 오늘 날짜의 개장 시간 (9:30 AM ET)
    market_open_et = et_tz.localize(datetime.combine(now_et.date(), time(9, 30)))
    market_open_kst = market_open_et.astimezone(kst_tz)
    
    # 오늘 날짜의 마감 시간 (4:00 PM ET) - KST로 변환하면 다음날이 됨
    market_close_et = et_tz.localize(datetime.combine(now_et.date(), time(16, 0)))
    market_close_kst = market_close_et.astimezone(kst_tz)
    
    # 마감 시간이 다음날이 되도록 명시적으로 설정
    # (개장 시간이 마감 시간보다 크면 마감 시간을 다음날로 설정)
    if market_open_kst.date() == market_close_kst.date():
        # 마감 시간이 같은 날이면 다음날로 조정
        market_close_kst = market_close_kst + timedelta(days=1)
    
    return market_open_kst, market_close_kst

def is_us_market_open():
    """
    현재 시간이 미국 주식 시장 개장 시간인지 확인합니다. (한국 시간 기준)
    정규 장: 9:30 AM - 4:00 PM ET (23:30 - 06:00 KST, 여름 / 22:30 - 05:00 KST, 겨울)
    """
    try:
        now_kst = datetime.now(pytz.timezone('Asia/Seoul'))
        market_open_kst, market_close_kst = get_market_open_close_kst()

        # 주말 제외 (토, 일)
        if now_kst.weekday() >= 5:
            return False

        # 자정 넘어가는 경우 처리 (개장: 오늘 23:30, 마감: 내일 06:00)
        # 현재 시간이 개장 시간 이후이거나 마감 시간 이전이면 시장 개장
        if market_open_kst.date() < market_close_kst.date():
            # 자정을 넘기는 경우
            return now_kst >= market_open_kst or now_kst < market_close_kst
        else:
            # 같은 날인 경우 (일반적으로 발생하지 않지만 안전장치)
            return market_open_kst <= now_kst < market_close_kst
    except Exception as e:
        logger.warning(f"시장 개장 여부 확인 실패: {e}")
        return False

default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 16),
    'retries': None,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='cache_sp500_to_redis',
    default_args=default_args,
    description='SP500 데이터 Redis 캐싱 (변화율 계산 포함)',
    schedule_interval='*/10 * * * *',  # 매 10분마다 실행
    catchup=False,
    max_active_runs=1,
    tags=['sp500', 'redis', 'caching']
)
def sp500_caching_dag():
    
    @task
    def fetch_sp500_current_data():
        """DB에서 SP500 현재가 + 회사명 + 거래량 조회"""
        market_open = is_us_market_open()
        
        # 시장 마감 중: 빈 리스트 반환 → calculate_and_cache에서 기존 데이터 유지
        if not market_open:
            logger.info("🔒 시장 마감 중 - 기존 Redis 데이터 유지 모드")
            return []  # Skip 대신 빈 리스트 반환하여 calculate_and_cache로 전달
        
        logger.info("📊 SP500 현재 데이터 조회 시작 (시장 개장 중 - 10분마다)")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 각 종목별 최신 거래 데이터 조회
        query = """
        WITH latest_trades AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                price,
                volume,
                created_at
            FROM sp500_websocket_trades
            ORDER BY symbol, created_at DESC
        )
        SELECT 
            c.symbol,
            c.company_name,
            lt.price as current_price,
            lt.volume,
            lt.created_at as last_updated
        FROM sp500_companies c
        INNER JOIN latest_trades lt ON c.symbol = lt.symbol
        ORDER BY c.symbol
        """
        
        records = pg_hook.get_records(query)
        
        result = []
        for r in records:
            result.append({
                'symbol': r[0],
                'company_name': r[1],
                'current_price': float(r[2]) if r[2] else 0,
                'volume': int(r[3]) if r[3] else 0,
                'last_updated': r[4].isoformat() if r[4] else None
            })
        
        logger.info(f"✅ {len(result)}개 종목 현재 데이터 조회 완료")
        return result
    
    @task
    def fetch_previous_close(current_data):
        """전일 종가 조회"""
        # 시장이 닫혀있으면 skip
        if not current_data:
            logger.info("🔒 시장이 닫혀있어 전일 종가 조회를 건너뜁니다.")
            return {}
        
        logger.info("📈 전일 종가 조회 시작")
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 전일 종가 조회 (미국 시간 기준)
        query = """
        WITH ranked_prices AS (
            SELECT 
                symbol, 
                price,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY created_at DESC) as rn
            FROM sp500_websocket_trades
            WHERE created_at >= NOW() - INTERVAL '5 days'
                AND (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date < 
                    (NOW() AT TIME ZONE 'America/New_York')::date
        )
        SELECT symbol, price
        FROM ranked_prices
        WHERE rn = 1
        """
        
        records = pg_hook.get_records(query)
        
        # {symbol: previous_close_price} 형태로 변환
        result = {}
        for r in records:
            result[r[0]] = float(r[1]) if r[1] else 0
        
        logger.info(f"✅ {len(result)}개 종목 전일 종가 조회 완료")
        return result
    
    @task
    def fetch_24h_volume(current_data):
        """24시간 누적 거래량 조회"""
        # 시장이 닫혀있으면 skip
        if not current_data:
            logger.info("🔒 시장이 닫혀있어 24시간 거래량 조회를 건너뜁니다.")
            return {}
        
        logger.info("📊 24시간 거래량 조회 시작")
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 과거 24시간 동안의 거래량 합산
        query = """
        SELECT 
            symbol,
            SUM(volume) as volume_24h
        FROM sp500_websocket_trades
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY symbol
        """
        
        records = pg_hook.get_records(query)
        
        # {symbol: volume_24h} 형태로 변환
        result = {}
        for r in records:
            result[r[0]] = int(r[1]) if r[1] else 0
        
        logger.info(f"✅ {len(result)}개 종목 24시간 거래량 조회 완료")
        return result
    
    @task
    def calculate_and_cache(current_data, previous_close_map, volume_24h_map):
        """변화율 계산 + 24h 거래량 추가 후 Redis에 캐싱"""
        # Redis 연결
        try:
            # ⭐ Kubernetes Service 환경 변수 사용
            redis_host = os.getenv('REDIS_SERVICE_HOST', os.getenv('REDIS_HOST', 'redis-master'))
            redis_port = int(os.getenv('REDIS_SERVICE_PORT', os.getenv('REDIS_PORT_6379_TCP_PORT', 6379)))
            redis_password = os.getenv('REDIS_PASSWORD', None)
            redis_db = int(os.getenv('REDIS_DB', 0))
            
            redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            redis_client.ping()
            logger.info(f"✅ Redis 연결 성공: {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"❌ Redis 연결 실패: {e}")
            raise
        
        # Redis Hash Key
        redis_key = "sp500_market_data"
        
        # 시장이 닫혀있으면: 기존 Redis 데이터에서 변화율/변화액 유지
        if not current_data:
            logger.info("🔒 시장 마감 중 - 기존 Redis 데이터의 변화율/변화액 유지")
            
            # 기존 Redis 데이터 조회
            existing_data = redis_client.hgetall(redis_key)
            
            if not existing_data:
                logger.info("⚠️ Redis에 기존 데이터가 없습니다.")
                redis_client.close()
                return []
            
            # 기존 데이터를 파싱하여 변화율/변화액 유지
            pipeline = redis_client.pipeline()
            preserved_count = 0
            enriched_data = []
            
            for symbol, data_str in existing_data.items():
                try:
                    existing_json = json.loads(data_str)
                    
                    # 기존 변화율/변화액 유지 (현재가만 업데이트 가능하면 업데이트, 아니면 그대로 유지)
                    redis_data = {
                        'symbol': existing_json.get('symbol', symbol),
                        'company_name': existing_json.get('company_name', ''),
                        'current_price': existing_json.get('current_price', 0),
                        'change_amount': existing_json.get('change_amount', 0),  # 기존 값 유지
                        'change_percentage': existing_json.get('change_percentage', 0),  # 기존 값 유지
                        'volume': existing_json.get('volume', 0),
                        'volume_24h': existing_json.get('volume_24h', 0),
                        'last_updated': existing_json.get('last_updated', '')
                    }
                    
                    # Hash에 저장 (변화율/변화액 유지)
                    pipeline.hset(redis_key, symbol, json.dumps(redis_data))
                    preserved_count += 1
                    enriched_data.append(redis_data)
                    
                except Exception as e:
                    logger.warning(f"⚠️ {symbol} 기존 데이터 파싱 실패: {e}")
                    continue
            
            # TTL 설정 (7일)
            pipeline.expire(redis_key, 604800)
            
            try:
                pipeline.execute()
                logger.info(f"✅ {preserved_count}개 종목 변화율/변화액 유지 완료")
            except Exception as e:
                logger.error(f"❌ Redis 저장 실패: {e}")
                redis_client.close()
                raise
            
            redis_client.close()
            return enriched_data
        
        # 시장 개장 중: 새로운 변화율/변화액 계산
        logger.info(f"💾 Redis 캐싱 시작 ({len(current_data)}개 종목)")
        
        # Pipeline 사용하여 일괄 저장
        pipeline = redis_client.pipeline()
        cached_count = 0
        enriched_data = []  # DB 저장용 데이터
        
        for stock in current_data:
            symbol = stock['symbol']
            current_price = stock['current_price']
            previous_close = previous_close_map.get(symbol, 0)
            volume_24h = volume_24h_map.get(symbol, 0)
            
            # 변화율 계산
            if previous_close and previous_close > 0:
                change_amount = current_price - previous_close
                change_percentage = (change_amount / previous_close) * 100
            else:
                # 전일 종가를 찾지 못한 경우, 기존 Redis 데이터에서 변화율/변화액 가져오기 시도
                try:
                    existing_data_str = redis_client.hget(redis_key, symbol)
                    if existing_data_str:
                        existing_json = json.loads(existing_data_str)
                        change_amount = existing_json.get('change_amount', 0)
                        change_percentage = existing_json.get('change_percentage', 0)
                        logger.debug(f"📌 {symbol}: 전일 종가 없음, 기존 변화율 유지 ({change_percentage}%)")
                    else:
                        change_amount = 0
                        change_percentage = 0
                except Exception as e:
                    logger.debug(f"⚠️ {symbol}: 기존 데이터 조회 실패, 변화율 0으로 설정: {e}")
                    change_amount = 0
                    change_percentage = 0
            
            # Redis 저장용 데이터 (WebSocket 응답 포맷 + 24h volume)
            redis_data = {
                'symbol': symbol,
                'company_name': stock['company_name'],
                'current_price': current_price,
                'change_amount': round(change_amount, 2),
                'change_percentage': round(change_percentage, 2),
                'volume': stock['volume'],
                'volume_24h': volume_24h,  # 🆕 24시간 거래량
                'last_updated': stock['last_updated']
            }
            
            # Hash에 저장
            pipeline.hset(redis_key, symbol, json.dumps(redis_data))
            cached_count += 1
            
            # DB 저장용으로도 추가
            enriched_data.append(redis_data)
        
        # TTL 설정 (7일)
        pipeline.expire(redis_key, 604800)
        
        # 일괄 실행
        try:
            pipeline.execute()
            logger.info(f"✅ {cached_count}개 종목 Redis 캐싱 완료")
        except Exception as e:
            logger.error(f"❌ Redis 저장 실패: {e}")
            redis_client.close()
            raise
        
        # Pub/Sub 신호 발행 (WebSocket에 업데이트 알림)
        try:
            message = json.dumps({
                'message': 'SP500 market data updated',
                'count': cached_count,
                'timestamp': datetime.utcnow().isoformat()
            })
            redis_client.publish('sp500_market_updates', message)
            logger.info("📢 SP500 market data updated signal published (sp500_market_updates)")
        except Exception as e:
            logger.warning(f"⚠️ Pub/Sub 발행 실패: {e}")
        
        redis_client.close()
        
        logger.info(f"✅ Redis 캐싱 완료 - {len(enriched_data)}개 데이터 준비됨")
        return enriched_data  # DB 저장용으로 반환
    
    @task
    def save_to_database(enriched_data):
        """스냅샷 데이터를 DB에 저장 (히스토리 분석용)"""
        if not enriched_data:
            logger.info("🔒 저장할 데이터가 없습니다.")
            return
        
        logger.info(f"💾 DB 스냅샷 저장 시작 ({len(enriched_data)}개 종목)")
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        snapshot_time = datetime.now(pytz.timezone('Asia/Seoul'))
        saved_count = 0
        
        for data in enriched_data:
            try:
                # INSERT 쿼리 (중복 시 무시)
                insert_query = """
                INSERT INTO sp500_market_snapshots 
                (symbol, company_name, current_price, change_amount, change_percentage, 
                 volume, volume_24h, snapshot_time)
                VALUES (%(symbol)s, %(company_name)s, %(current_price)s, %(change_amount)s, 
                        %(change_percentage)s, %(volume)s, %(volume_24h)s, %(snapshot_time)s)
                ON CONFLICT (symbol, snapshot_time) DO NOTHING
                """
                
                params = {
                    'symbol': data['symbol'],
                    'company_name': data['company_name'],
                    'current_price': data['current_price'],
                    'change_amount': data['change_amount'],
                    'change_percentage': data['change_percentage'],
                    'volume': data['volume'],
                    'volume_24h': data['volume_24h'],
                    'snapshot_time': snapshot_time
                }
                
                pg_hook.run(insert_query, parameters=params)
                saved_count += 1
                
            except Exception as e:
                logger.warning(f"⚠️ {data['symbol']} 저장 실패: {e}")
                continue
        
        logger.info(f"✅ DB 스냅샷 저장 완료: {saved_count}개 종목")
    
    # Task 실행 흐름
    current_data = fetch_sp500_current_data()
    previous_close = fetch_previous_close(current_data)
    volume_24h = fetch_24h_volume(current_data)
    enriched_data = calculate_and_cache(current_data, previous_close, volume_24h)
    save_to_database(enriched_data)

# DAG 인스턴스 생성
dag_instance = sp500_caching_dag()