# utils/market_data_collector.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging
import time
import statistics

logger = logging.getLogger(__name__)

class MarketDataCollector:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self._sp500_symbols_cache = None
        self._crypto_symbols_cache = None
    
    def calculate_price_changes(self, symbol, post_timestamp, market_data):
        """게시글 전후 가격 변화 분석"""
        try:
            price_timeline = market_data.get('price_timeline', [])
            if not price_timeline:
                return {}
            
            # 게시글 시점 기준 가격 찾기
            post_price = self._get_price_near_time(price_timeline, post_timestamp)
            if not post_price:
                return {}
            
            # 시간별 가격 변화 계산
            changes = {}
            
            # 1시간 후 (실제로는 가장 가까운 데이터포인트)
            hour_1_time = post_timestamp + timedelta(hours=1)
            hour_1_price = self._get_price_near_time(price_timeline, hour_1_time)
            if hour_1_price:
                changes['1h_change'] = round(((hour_1_price - post_price) / post_price) * 100, 2)
            
            # 12시간 후
            hour_12_time = post_timestamp + timedelta(hours=12)
            hour_12_price = self._get_price_near_time(price_timeline, hour_12_time)
            if hour_12_price:
                changes['12h_change'] = round(((hour_12_price - post_price) / post_price) * 100, 2)
            
            # 24시간 후 (또는 다음 거래일)
            if self._is_crypto_symbol(symbol):
                # 암호화폐는 24시간 거래
                hour_24_time = post_timestamp + timedelta(hours=24)
                hour_24_price = self._get_price_near_time(price_timeline, hour_24_time)
                if hour_24_price:
                    changes['24h_change'] = round(((hour_24_price - post_price) / post_price) * 100, 2)
            else:
                # 주식은 다음 거래일 기준
                next_trading_day = self._get_next_trading_day(post_timestamp)
                next_day_price = self._get_average_price_around_time(price_timeline, next_trading_day, hours=1)
                if next_day_price:
                    changes['next_day_change'] = round(((next_day_price - post_price) / post_price) * 100, 2)
            
            changes['base_price'] = post_price
            return changes
            
        except Exception as e:
            logger.error(f"Price change calculation failed for {symbol}: {e}")
            return {}
    
    def calculate_volume_changes(self, symbol, post_timestamp, market_data):
        """게시글 전후 거래량 변화 분석"""
        try:
            price_timeline = market_data.get('price_timeline', [])
            if not price_timeline:
                return {}
            
            # 게시글 전 1시간 평균 거래량
            before_time = post_timestamp - timedelta(hours=1)
            before_volume = self._get_average_volume_around_time(price_timeline, before_time, hours=1)
            
            # 게시글 후 1시간 평균 거래량
            after_time = post_timestamp + timedelta(hours=1)  
            after_volume = self._get_average_volume_around_time(price_timeline, after_time, hours=1)
            
            volume_changes = {}
            if before_volume and after_volume and before_volume > 0:
                volume_changes['volume_change_1h'] = round(((after_volume - before_volume) / before_volume) * 100, 2)
                volume_changes['volume_spike_ratio'] = round(after_volume / before_volume, 2)
            
            # 24시간 전 대비 (기준선 설정)
            day_before_time = post_timestamp - timedelta(hours=24)
            baseline_volume = self._get_average_volume_around_time(price_timeline, day_before_time, hours=2)
            
            if baseline_volume and after_volume and baseline_volume > 0:
                volume_changes['volume_vs_baseline'] = round(((after_volume - baseline_volume) / baseline_volume) * 100, 2)
            
            volume_changes['baseline_volume'] = baseline_volume
            volume_changes['post_volume'] = after_volume
            
            return volume_changes
            
        except Exception as e:
            logger.error(f"Volume change calculation failed for {symbol}: {e}")
            return {}
    
    def _get_price_near_time(self, timeline, target_time, tolerance_hours=2):
        """특정 시간 근처의 가격 찾기"""
        target_timestamp = target_time.timestamp()
        closest_price = None
        min_diff = float('inf')
        
        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                time_diff = abs((point_time.timestamp() - target_timestamp))
                
                # tolerance_hours 시간 내의 데이터만 사용
                if time_diff <= tolerance_hours * 3600 and time_diff < min_diff:
                    min_diff = time_diff
                    closest_price = float(point['price'])
            except:
                continue
        
        return closest_price
    
    def _get_average_price_around_time(self, timeline, target_time, hours=1):
        """특정 시간 전후 일정 시간의 평균 가격"""
        start_time = target_time - timedelta(hours=hours/2)
        end_time = target_time + timedelta(hours=hours/2)
        
        prices = []
        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                if start_time <= point_time <= end_time:
                    prices.append(float(point['price']))
            except:
                continue
        
        return statistics.mean(prices) if prices else None
    
    def _get_average_volume_around_time(self, timeline, target_time, hours=1):
        """특정 시간 전후 일정 시간의 평균 거래량"""
        start_time = target_time - timedelta(hours=hours/2)
        end_time = target_time + timedelta(hours=hours/2)
        
        volumes = []
        for point in timeline:
            try:
                point_time = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                if start_time <= point_time <= end_time and point.get('volume'):
                    volumes.append(float(point['volume']))
            except:
                continue
        
        return statistics.mean(volumes) if volumes else None
    
    def _get_next_trading_day(self, post_timestamp):
        """다음 거래일 구하기 (주말 제외)"""
        next_day = post_timestamp + timedelta(days=1)
        
        # 주말이면 다음 월요일로
        while next_day.weekday() >= 5:  # 5=토요일, 6=일요일
            next_day += timedelta(days=1)
        
        # 거래 시간대로 조정 (오전 10시)
        next_day = next_day.replace(hour=10, minute=0, second=0, microsecond=0)
        return next_day
    
    def collect_market_data(self, affected_assets, post_timestamp):
        """영향받은 자산들의 시장 데이터 수집"""
        market_data = {}

        start_time = time.time()
        for asset_info in affected_assets:
            symbol = asset_info['symbol']
            
            try:
                # 가격 타임라인 데이터
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
        
        # SP500 주식인지 확인 (5일 범위)
        if self._is_sp500_symbol(symbol):
            start_time = post_timestamp - timedelta(days=2)
            end_time = post_timestamp + timedelta(days=2)
            return self._get_sp500_timeline(symbol, start_time, end_time)
        
        # 암호화폐인지 확인 (3일 범위)
        elif self._is_crypto_symbol(symbol):
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
    
    def _is_crypto_symbol(self, symbol):
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
        """SP500 가격 타임라인 - 5일 범위"""
        try:
            # 타임스탬프를 밀리초로 변환
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            SELECT timestamp_ms, price, volume
            FROM sp500_websocket_trades 
            WHERE symbol = %s 
                AND timestamp_ms BETWEEN %s AND %s
            ORDER BY timestamp_ms
            LIMIT 5000
            """
            
            results = self.pg_hook.get_records(query, parameters=[symbol, start_ms, end_ms])
            
            return [{
                'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
                'price': float(row[1]),
                'volume': int(row[2]) if row[2] else 0
            } for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get SP500 timeline for {symbol}: {e}")
            return []
    
    def _get_crypto_timeline(self, symbol, start_time, end_time):
        """암호화폐 가격 타임라인 - 3일 범위"""
        try:
            # KRW- 접두사 추가해서 빗썸 형식으로 변환
            bithumb_market = f"KRW-{symbol}"
            
            # 타임스탬프를 밀리초로 변환
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            SELECT trade_timestamp, 
                CAST(trade_price AS DECIMAL) as price,
                CAST(trade_volume AS DECIMAL) as volume
            FROM bithumb_ticker 
            WHERE market = %s 
                AND trade_timestamp BETWEEN %s AND %s
                -- 타입을 명확히 하여 안전하게 필터링
                AND trade_price::text ~ '^[0-9]+\.?[0-9]*$'
            ORDER BY trade_timestamp
            LIMIT 3000
            """
            
            results = self.pg_hook.get_records(query, parameters=[bithumb_market, start_ms, end_ms])
            
            return [{
                'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
                'price': float(row[1]) if row[1] else 0,
                'volume': float(row[2]) if row[2] else 0
            } for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get crypto timeline for {symbol}: {e}")
            return []
    
    def _get_data_source(self, symbol):
        """데이터 소스 식별"""
        if self._is_sp500_symbol(symbol):
            return 'sp500_realtime'
        elif self._is_crypto_symbol(symbol):
            return 'bithumb_realtime'
        return 'unknown'