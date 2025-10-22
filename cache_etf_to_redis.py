# cache_etf_to_redis.py - ETF 데이터를 DB에서 조회하여 Redis에 캐싱
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
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
    market_open_et = et_tz.localize(datetime.combine(now_et.date(), time(9, 30)))
    market_close_et = et_tz.localize(datetime.combine(now_et.date(), time(16, 0)))
    return market_open_et.astimezone(kst_tz), market_close_et.astimezone(kst_tz)

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

        # 자정 넘어가는 경우 처리
        if market_open_kst.time() > market_close_kst.time():
            return now_kst >= market_open_kst or now_kst < market_close_kst
        else:
            return market_open_kst <= now_kst < market_close_kst
    except Exception as e:
        logger.warning(f"시장 개장 여부 확인 실패: {e}")
        return False

default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 22),
    'retries': None,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='cache_etf_to_redis',
    default_args=default_args,
    description='ETF 데이터 Redis 캐싱 (변화율 계산 포함)',
    schedule_interval='*/5 * * * *',  # 매 10분마다 실행
    catchup=False,
    max_active_runs=1,
    tags=['etf', 'redis', 'caching']
)
def etf_caching_dag():
    
    @task
    def fetch_etf_current_data():
        """DB에서 ETF 현재가 + ETF명 + 거래량 조회"""
        market_open = is_us_market_open()
        
        # 시장 마감 중: 전체 작업 건너뜀
        if not market_open:
            logger.info("🔒 시장 마감 중 - 작업 건너뜀")
            return []
        
        logger.info("📊 ETF 현재 데이터 조회 시작 (시장 개장 중 - 10분마다)")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 각 ETF별 최신 거래 데이터 조회
        query = """
        WITH latest_trades AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                price,
                volume,
                created_at
            FROM etf_realtime_prices
            ORDER BY symbol, created_at DESC
        )
        SELECT 
            b.symbol,
            b.name as etf_name,
            lt.price as current_price,
            lt.volume,
            lt.created_at as last_updated
        FROM etf_basic_info b
        INNER JOIN latest_trades lt ON b.symbol = lt.symbol
        ORDER BY b.symbol
        """
        
        records = pg_hook.get_records(query)
        
        result = []
        for r in records:
            result.append({
                'symbol': r[0],
                'etf_name': r[1],
                'current_price': float(r[2]) if r[2] else 0,
                'volume': int(r[3]) if r[3] else 0,
                'last_updated': r[4].isoformat() if r[4] else None
            })
        
        logger.info(f"✅ {len(result)}개 ETF 현재 데이터 조회 완료")
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
            FROM etf_realtime_prices
            WHERE created_at >= NOW() - INTERVAL '3 days'
                AND DATE(created_at AT TIME ZONE 'America/New_York') < 
                    CURRENT_DATE AT TIME ZONE 'America/New_York'
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
        
        logger.info(f"✅ {len(result)}개 ETF 전일 종가 조회 완료")
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
        FROM etf_realtime_prices
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY symbol
        """
        
        records = pg_hook.get_records(query)
        
        # {symbol: volume_24h} 형태로 변환
        result = {}
        for r in records:
            result[r[0]] = int(r[1]) if r[1] else 0
        
        logger.info(f"✅ {len(result)}개 ETF 24시간 거래량 조회 완료")
        return result
    
    @task
    def calculate_and_cache(current_data, previous_close_map, volume_24h_map):
        """변화율 계산 + 24h 거래량 추가 후 Redis에 캐싱"""
        # 시장이 닫혀있으면 skip
        if not current_data:
            logger.info("🔒 시장이 닫혀있어 Redis 캐싱을 건너뜁니다.")
            return []
        
        logger.info(f"💾 Redis 캐싱 시작 ({len(current_data)}개 ETF)")
        
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
        redis_key = "etf_market_data"
        
        # Pipeline 사용하여 일괄 저장
        pipeline = redis_client.pipeline()
        cached_count = 0
        enriched_data = []  # DB 저장용 데이터
        
        for etf in current_data:
            symbol = etf['symbol']
            current_price = etf['current_price']
            previous_close = previous_close_map.get(symbol, 0)
            volume_24h = volume_24h_map.get(symbol, 0)
            
            # 변화율 계산
            if previous_close and previous_close > 0:
                change_amount = current_price - previous_close
                change_percentage = (change_amount / previous_close) * 100
            else:
                change_amount = 0
                change_percentage = 0
            
            # Redis 저장용 데이터 (WebSocket 응답 포맷 + 24h volume)
            redis_data = {
                'symbol': symbol,
                'etf_name': etf['etf_name'],
                'current_price': current_price,
                'change_amount': round(change_amount, 2),
                'change_percentage': round(change_percentage, 2),
                'volume': etf['volume'],
                'volume_24h': volume_24h,  # 🆕 24시간 거래량
                'last_updated': etf['last_updated']
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
            logger.info(f"✅ {cached_count}개 ETF Redis 캐싱 완료")
        except Exception as e:
            logger.error(f"❌ Redis 저장 실패: {e}")
            redis_client.close()
            raise
        
        # Pub/Sub 신호 발행 (WebSocket에 업데이트 알림)
        try:
            message = json.dumps({
                'message': 'ETF market data updated',
                'count': cached_count,
                'timestamp': datetime.utcnow().isoformat()
            })
            redis_client.publish('etf_market_updates', message)
            logger.info("📢 ETF market data updated signal published (etf_market_updates)")
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
        
        logger.info(f"💾 DB 스냅샷 저장 시작 ({len(enriched_data)}개 ETF)")
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        snapshot_time = datetime.now(pytz.timezone('Asia/Seoul'))
        saved_count = 0
        
        for data in enriched_data:
            try:
                # INSERT 쿼리 (중복 시 무시)
                insert_query = """
                INSERT INTO etf_market_snapshots 
                (symbol, etf_name, current_price, change_amount, change_percentage, 
                 volume, volume_24h, snapshot_time)
                VALUES (%(symbol)s, %(etf_name)s, %(current_price)s, %(change_amount)s, 
                        %(change_percentage)s, %(volume)s, %(volume_24h)s, %(snapshot_time)s)
                ON CONFLICT (symbol, snapshot_time) DO NOTHING
                """
                
                params = {
                    'symbol': data['symbol'],
                    'etf_name': data['etf_name'],
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
        
        logger.info(f"✅ DB 스냅샷 저장 완료: {saved_count}개 ETF")
    
    # Task 실행 흐름
    current_data = fetch_etf_current_data()
    previous_close = fetch_previous_close(current_data)
    volume_24h = fetch_24h_volume(current_data)
    enriched_data = calculate_and_cache(current_data, previous_close, volume_24h)
    save_to_database(enriched_data)

# DAG 인스턴스 생성
dag_instance = etf_caching_dag()