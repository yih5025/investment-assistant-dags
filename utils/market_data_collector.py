# utils/market_data_collector.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class MarketDataCollector:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='social_media_db')
    
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
        """자산별 가격 타임라인 수집"""
        start_time = post_timestamp - timedelta(hours=6)
        end_time = post_timestamp + timedelta(hours=6)
        
        # SP500 주식인지 확인
        if self._is_sp500_symbol(symbol):
            return self._get_sp500_timeline(symbol, start_time, end_time)
        
        # 암호화폐인지 확인
        elif self._is_crypto_symbol(symbol):
            return self._get_crypto_timeline(symbol, start_time, end_time)
        
        return []
    
    def _is_sp500_symbol(self, symbol):
        """SP500 심볼인지 확인"""
        sp500_symbols = ['TSLA', 'AAPL', 'MSFT', 'NVDA', 'META', 'GOOGL', 'AMZN', 'ORCL']
        return symbol in sp500_symbols
    
    def _is_crypto_symbol(self, symbol):
        """암호화폐 심볼인지 확인"""
        crypto_symbols = ['BTC', 'ETH', 'DOGE', 'SOL']
        return symbol in crypto_symbols
    
    def _get_sp500_timeline(self, symbol, start_time, end_time):
        """SP500 가격 타임라인"""
        # 타임스탬프를 밀리초로 변환
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        
        query = """
        SELECT timestamp_ms, price, volume
        FROM sp500_websocket_trades 
        WHERE symbol = %s 
            AND timestamp_ms BETWEEN %s AND %s
        ORDER BY timestamp_ms
        LIMIT 1000
        """
        
        results = self.pg_hook.get_records(query, parameters=[symbol, start_ms, end_ms])
        
        return [{
            'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
            'price': float(row[1]),
            'volume': int(row[2]) if row[2] else 0
        } for row in results]
    
    def _get_crypto_timeline(self, symbol, start_time, end_time):
        """암호화폐 가격 타임라인"""
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
            AND trade_price ~ '^[0-9]+\.?[0-9]*$'
        ORDER BY trade_timestamp
        LIMIT 1000
        """
        
        results = self.pg_hook.get_records(query, parameters=[bithumb_market, start_ms, end_ms])
        
        return [{
            'timestamp': datetime.fromtimestamp(row[0]/1000).isoformat(),
            'price': float(row[1]) if row[1] else 0,
            'volume': float(row[2]) if row[2] else 0
        } for row in results]
    
    def _get_data_source(self, symbol):
        """데이터 소스 식별"""
        if self._is_sp500_symbol(symbol):
            return 'sp500_realtime'
        elif self._is_crypto_symbol(symbol):
            return 'bithumb_realtime'
        return 'unknown'