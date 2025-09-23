# utils/market_data_collector.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class MarketDataCollector:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self._sp500_symbols_cache = None
        self._crypto_symbols_cache = None
    
    def collect_market_data(self, affected_assets, post_timestamp):
        """영향받은 자산들의 시장 데이터 수집"""
        market_data = {}
        
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