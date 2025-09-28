# utils/market_data_collector.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging
import time
import statistics

logger = logging.getLogger(__name__)

class MarketAnalyzer:
    def __init__(self, pg_hook, market_collector=None):
        self.pg_hook = pg_hook
        self.market_collector = market_collector or MarketDataCollector()
    
    def calculate_price_changes(self, symbol, post_timestamp, market_data):
        """게시글 전후 가격 변화 분석 (OHLCV 데이터용으로 수정)"""
        start_time = time.time()

        try:
            price_timeline = market_data.get('price_timeline', [])
            if not price_timeline:
                return {}

            post_price = self._get_price_near_time(price_timeline, post_timestamp)
            if not post_price:
                return {}

            changes = {}
            hour_1_time = post_timestamp + timedelta(hours=1)
            hour_1_price = self._get_price_near_time(price_timeline, hour_1_time)
            if hour_1_price:
                changes['1h_change'] = round(((hour_1_price - post_price) / post_price) * 100, 2)

            hour_12_time = post_timestamp + timedelta(hours=12)
            hour_12_price = self._get_price_near_time(price_timeline, hour_12_time)
            if hour_12_price:
                changes['12h_change'] = round(((hour_12_price - post_price) / post_price) * 100, 2)

            if self.market_collector.is_crypto_symbol(symbol):
                hour_24_time = post_timestamp + timedelta(hours=24)
                hour_24_price = self._get_price_near_time(price_timeline, hour_24_time)
                if hour_24_price:
                    changes['24h_change'] = round(((hour_24_price - post_price) / post_price) * 100, 2)
            else:
                next_trading_day = self._get_next_trading_day(post_timestamp)
                next_day_price = self._get_average_price_around_time(price_timeline, next_trading_day, hours=1)
                if next_day_price:
                    changes['next_day_change'] = round(((next_day_price - post_price) / post_price) * 100, 2)

            changes['base_price'] = post_price
            elapsed = time.time() - start_time
            logger.info(f"💰 Price analysis for {symbol}: {elapsed:.2f}s")
            return changes

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ Price analysis failed for {symbol} in {elapsed:.2f}s: {e}")
            return {}
    
    def calculate_volume_changes(self, symbol, post_timestamp, market_data):
        """거래량 변화 분석 (OHLCV 데이터용으로 수정)"""
        start_time = time.time()
        try:
            timeline = market_data.get('price_timeline', [])
            if not timeline:
                return {}

            before_start_time = post_timestamp - timedelta(hours=1)
            total_volume_before = self._get_total_volume_in_range(timeline, before_start_time, post_timestamp)

            after_end_time = post_timestamp + timedelta(hours=1)
            total_volume_after = self._get_total_volume_in_range(timeline, post_timestamp, after_end_time)

            volume_changes = {}
            if total_volume_before is not None and total_volume_after is not None:
                volume_changes['total_volume_before_1h'] = round(total_volume_before, 2)
                volume_changes['total_volume_after_1h'] = round(total_volume_after, 2)
                if total_volume_before > 0:
                    change_pct = ((total_volume_after - total_volume_before) / total_volume_before) * 100
                    volume_changes['volume_change_pct_1h'] = round(change_pct, 2)

            elapsed = time.time() - start_time
            logger.info(f"📊 Volume analysis for {symbol}: {elapsed:.2f}s")
            return volume_changes
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ Volume analysis failed for {symbol} in {elapsed:.2f}s: {e}")
            return {}

    def _get_price_near_time(self, timeline, target_time, tolerance_hours=2):
        """특정 시간 근처 1분봉의 종가('close') 찾기"""
        target_timestamp = target_time.timestamp()
        closest_price = None
        min_diff = float('inf')

        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                time_diff = abs((point_time.timestamp() - target_timestamp))

                if time_diff <= tolerance_hours * 3600 and time_diff < min_diff:
                    min_diff = time_diff
                    if point.get('close') is not None:
                        closest_price = float(point['close'])
            except:
                continue
        return closest_price
    
    def _get_average_price_around_time(self, timeline, target_time, hours=1):
        """특정 시간 전후 일정 시간의 평균 종가"""
        start_time = target_time - timedelta(hours=hours/2)
        end_time = target_time + timedelta(hours=hours/2)

        prices = []
        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                if start_time <= point_time <= end_time:
                    if point.get('close') is not None:
                        prices.append(float(point['close']))
            except:
                continue
        return statistics.mean(prices) if prices else None

    def _get_total_volume_in_range(self, timeline, start_time, end_time):
        """특정 시간 범위 내의 모든 1분봉 거래량('volume') 합산"""
        total_volume = 0
        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                if start_time <= point_time < end_time:
                    volume_value = point.get('volume')
                    if volume_value is not None:
                        total_volume += float(volume_value)
            except:
                continue
        return total_volume

    def _get_next_trading_day(self, post_timestamp):
        """다음 거래일 구하기 (주말 제외)"""
        next_day = post_timestamp + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return next_day.replace(hour=10, minute=0, second=0, microsecond=0)

class MarketDataCollector:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self._sp500_symbols_cache = None
        self._crypto_symbols_cache = None
        self._etf_symbols_cache = None
    
    def collect_market_data(self, affected_assets, post_timestamp):
        """영향받은 자산들의 시장 데이터 수집"""
        market_data = {}
        start_time = time.time()
        for asset_info in affected_assets:
            symbol = asset_info['symbol']
            try:
                price_timeline = self._get_price_timeline(symbol, post_timestamp)
                if price_timeline:
                    market_data[symbol] = {
                        'price_timeline': price_timeline,
                        'data_source': self._get_data_source(symbol),
                        'asset_info': asset_info
                    }
            except Exception as e:
                logger.error(f"Failed to collect market data for {symbol}: {e}")
                continue
        end_time = time.time()
        logger.info(f"Market data collection time: {end_time - start_time} seconds")
        return market_data
    
    def _get_price_timeline(self, symbol, post_timestamp):
        """자산별 가격 타임라인 수집 - 확장된 시간 범위"""
        if self._is_sp500_symbol(symbol):
            start_time = post_timestamp - timedelta(days=2)
            end_time = post_timestamp + timedelta(days=2)
            return self._get_sp500_timeline(symbol, start_time, end_time)
        
        elif self.is_etf_symbol(symbol):
            start_time = post_timestamp - timedelta(days=2)
            end_time = post_timestamp + timedelta(days=2)
            return self._get_etf_timeline(symbol, start_time, end_time)

        elif self.is_crypto_symbol(symbol):
            start_time = post_timestamp - timedelta(days=1)
            end_time = post_timestamp + timedelta(days=1)
            return self._get_crypto_timeline(symbol, start_time, end_time)
        
        return []
    
    def _is_sp500_symbol(self, symbol):
        """SP500 심볼인지 확인 - DB에서 동적 조회"""
        if self._sp500_symbols_cache is None:
            try:
                query = "SELECT DISTINCT symbol FROM sp500_websocket_trades LIMIT 1000"
                results = self.pg_hook.get_records(query)
                self._sp500_symbols_cache = {row[0] for row in results}
                logger.info(f"Loaded {len(self._sp500_symbols_cache)} SP500 symbols from DB")
            except Exception as e:
                logger.error(f"Failed to load SP500 symbols: {e}")
                self._sp500_symbols_cache = set()
        return symbol in self._sp500_symbols_cache

    def is_etf_symbol(self, symbol):
        """ETF 심볼인지 확인 - DB에서 동적 조회"""
        if self._etf_symbols_cache is None:
            try:
                query = "SELECT DISTINCT symbol FROM etf_realtime_prices LIMIT 1000"
                results = self.pg_hook.get_records(query)
                self._etf_symbols_cache = {row[0] for row in results}
                logger.info(f"Loaded {len(self._etf_symbols_cache)} ETF symbols from DB")
            except Exception as e:
                logger.error(f"Failed to load ETF symbols: {e}")
                self._etf_symbols_cache = set()
        return symbol in self._etf_symbols_cache
    
    def is_crypto_symbol(self, symbol):
        """암호화폐 심볼인지 확인 - DB에서 동적 조회"""
        if self._crypto_symbols_cache is None:
            try:
                query = """
                SELECT DISTINCT REPLACE(market, 'KRW-', '') as symbol 
                FROM bithumb_ticker 
                WHERE market LIKE 'KRW-%'
                LIMIT 1000
                """
                results = self.pg_hook.get_records(query)
                self._crypto_symbols_cache = {row[0] for row in results}
                logger.info(f"Loaded {len(self._crypto_symbols_cache)} crypto symbols from DB")
            except Exception as e:
                logger.error(f"Failed to load crypto symbols: {e}")
                self._crypto_symbols_cache = set()
        return symbol in self._crypto_symbols_cache
    
    def _get_sp500_timeline(self, symbol, start_time, end_time):
        """SP500 타임라인을 1분 단위 OHLCV로 집계하여 수집합니다."""
        try:
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            WITH minute_data AS (
                SELECT
                    CAST(floor(timestamp_ms / 60000) * 60000 AS BIGINT) as minute_timestamp,
                    price,
                    volume,
                    ROW_NUMBER() OVER(PARTITION BY floor(timestamp_ms / 60000) ORDER BY timestamp_ms ASC) as rn_asc,
                    ROW_NUMBER() OVER(PARTITION BY floor(timestamp_ms / 60000) ORDER BY timestamp_ms DESC) as rn_desc
                FROM sp500_websocket_trades
                WHERE symbol = %s
                    AND timestamp_ms BETWEEN %s AND %s
            )
            SELECT
                minute_timestamp,
                MAX(CASE WHEN rn_asc = 1 THEN price ELSE NULL END) as open,
                MAX(price) as high,
                MIN(price) as low,
                MAX(CASE WHEN rn_desc = 1 THEN price ELSE NULL END) as close,
                SUM(volume) as volume
            FROM minute_data
            GROUP BY minute_timestamp
            ORDER BY minute_timestamp
            """
            
            results = self.pg_hook.get_records(query, parameters=[symbol, start_ms, end_ms])
            
            return [{
                'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
                'open': float(row[1]) if row[1] is not None else None,
                'high': float(row[2]) if row[2] is not None else None,
                'low': float(row[3]) if row[3] is not None else None,
                'close': float(row[4]) if row[4] is not None else None,
                'volume': int(row[5]) if row[5] else 0
            } for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get aggregated SP500 timeline for {symbol}: {e}")
            return []
        
    def _get_etf_timeline(self, symbol, start_time, end_time):
        """ETF 타임라인을 1분 단위 OHLCV로 집계하여 수집합니다."""
        try:
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            WITH minute_data AS (
                SELECT
                    CAST(floor(timestamp_ms / 60000) * 60000 AS BIGINT) as minute_timestamp,
                    price,
                    volume,
                    ROW_NUMBER() OVER(PARTITION BY floor(timestamp_ms / 60000) ORDER BY timestamp_ms ASC) as rn_asc,
                    ROW_NUMBER() OVER(PARTITION BY floor(timestamp_ms / 60000) ORDER BY timestamp_ms DESC) as rn_desc
                FROM etf_realtime_prices
                WHERE symbol = %s
                    AND timestamp_ms BETWEEN %s AND %s
            )
            SELECT
                minute_timestamp,
                MAX(CASE WHEN rn_asc = 1 THEN price ELSE NULL END) as open,
                MAX(price) as high,
                MIN(price) as low,
                MAX(CASE WHEN rn_desc = 1 THEN price ELSE NULL END) as close,
                SUM(volume) as volume
            FROM minute_data
            GROUP BY minute_timestamp
            ORDER BY minute_timestamp
            """
            results = self.pg_hook.get_records(query, parameters=[symbol, start_ms, end_ms])
            
            return [{
                'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
                'open': float(row[1]) if row[1] is not None else None,
                'high': float(row[2]) if row[2] is not None else None,
                'low': float(row[3]) if row[3] is not None else None,
                'close': float(row[4]) if row[4] is not None else None,
                'volume': int(row[5]) if row[5] else 0
            } for row in results]
        except Exception as e:
            logger.error(f"Failed to get aggregated ETF timeline for {symbol}: {e}")
            return []
    
    def _get_crypto_timeline(self, symbol, start_time, end_time):
        """암호화폐 타임라인을 1분 단위 OHLCV로 집계하여 수집합니다."""
        try:
            bithumb_market = f"KRW-{symbol}"
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)

            query = """
            WITH minute_data AS (
                SELECT
                    CAST(floor(trade_timestamp / 60000) * 60000 AS BIGINT) as minute_timestamp,
                    CAST(trade_price AS DECIMAL) as price,
                    CAST(trade_volume AS DECIMAL) as volume,
                    ROW_NUMBER() OVER(PARTITION BY floor(trade_timestamp / 60000) ORDER BY trade_timestamp ASC) as rn_asc,
                    ROW_NUMBER() OVER(PARTITION BY floor(trade_timestamp / 60000) ORDER BY trade_timestamp DESC) as rn_desc
                FROM bithumb_ticker
                WHERE market = %s
                    AND trade_timestamp BETWEEN %s AND %s
                    AND trade_price::text ~ '^[0-9]+\.?[0-9]*$'
            )
            SELECT
                minute_timestamp,
                MAX(CASE WHEN rn_asc = 1 THEN price ELSE NULL END) as open,
                MAX(price) as high,
                MIN(price) as low,
                MAX(CASE WHEN rn_desc = 1 THEN price ELSE NULL END) as close,
                SUM(volume) as volume
            FROM minute_data
            GROUP BY minute_timestamp
            ORDER BY minute_timestamp
            """

            results = self.pg_hook.get_records(query, parameters=[bithumb_market, start_ms, end_ms])

            return [{
                'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
                'open': float(row[1]) if row[1] is not None else None,
                'high': float(row[2]) if row[2] is not None else None,
                'low': float(row[3]) if row[3] is not None else None,
                'close': float(row[4]) if row[4] is not None else None,
                'volume': float(row[5]) if row[5] else 0
            } for row in results]

        except Exception as e:
            logger.error(f"Failed to get aggregated crypto timeline for {symbol}: {e}")
            return []
    
    def _get_data_source(self, symbol):
        """데이터 소스 식별"""
        if self._is_sp500_symbol(symbol):
            return 'sp500_realtime'
        elif self.is_etf_symbol(symbol):
            return 'etf_realtime'
        elif self.is_crypto_symbol(symbol):
            return 'bithumb_realtime'
        return 'unknown'