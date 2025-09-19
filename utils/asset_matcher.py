# utils/asset_matcher.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SocialMediaAnalyzer:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self.account_mapping_cache = {}
        self.keyword_mapping_cache = {}
        self.market_data_range = None
    
    def determine_affected_assets(self, username, content, timestamp):
        """3단계 자산 매칭 로직"""
        affected_assets = []
        
        # 1단계: 계정별 하드코딩 매핑
        account_assets = self.get_account_assets(username)
        affected_assets.extend(account_assets)
        
        # 2단계: 키워드 매칭 (최대 5개까지만)
        if len(affected_assets) < 5:
            keyword_assets = self.extract_keyword_assets(content)
            for asset in keyword_assets:
                if len(affected_assets) >= 5:
                    break
                affected_assets.append(asset)
        
        # 3단계: 통계적 변동성 (최대 5개까지만)
        if len(affected_assets) < 5:
            volatile_asset = self.find_statistical_outlier(timestamp)
            if volatile_asset:
                affected_assets.append(volatile_asset)
        
        # 중복 제거 및 우선순위 정렬
        return self.dedupe_and_rank(affected_assets)
    
    def get_account_assets(self, username):
        """account_asset_mapping 테이블에서 계정별 자산 조회"""
        if username not in self.account_mapping_cache:
            query = """
            SELECT asset_symbol, priority 
            FROM account_asset_mapping 
            WHERE username = %s 
            ORDER BY priority ASC
            """
            results = self.pg_hook.get_records(query, parameters=[username])
            self.account_mapping_cache[username] = results or []
        
        return [{
            'symbol': row[0],
            'source': 'verified_account',
            'priority': row[1]
        } for row in self.account_mapping_cache[username]]
    
    def extract_keyword_assets(self, content):
        """keyword_asset_mapping 테이블에서 키워드 매칭"""
        if not content:
            return []
        
        content_lower = content.lower()
        
        # 모든 키워드 조회 (캐싱)
        if not self.keyword_mapping_cache:
            query = "SELECT keyword, asset_symbol FROM keyword_asset_mapping"
            results = self.pg_hook.get_records(query)
            self.keyword_mapping_cache = {row[0]: row[1] for row in results}
        
        matched_assets = []
        for keyword, asset_symbol in self.keyword_mapping_cache.items():
            if keyword in content_lower:
                # 금융 맥락인지 간단 체크
                if self._is_financial_context(content_lower, keyword):
                    matched_assets.append({
                        'symbol': asset_symbol,
                        'source': 'direct_mention',
                        'priority': 2
                    })
        
        return matched_assets
    
    def _is_financial_context(self, text, keyword):
        """키워드가 금융/비즈니스 맥락에서 언급되었는지 확인"""
        financial_terms = [
            'stock', 'price', 'market', 'trading', 'investment',
            'buy', 'sell', 'bullish', 'bearish', 'earnings',
            'revenue', 'profit', 'loss', 'valuation', 'ipo'
        ]
        
        # 키워드 주변 50자 내에서 금융 용어 확인
        keyword_pos = text.find(keyword)
        if keyword_pos == -1:
            return True  # 키워드가 있다면 일단 허용
        
        context = text[max(0, keyword_pos-50):keyword_pos+50]
        return any(term in context for term in financial_terms) or True  # 일단 관대하게
    
    def find_statistical_outlier(self, timestamp):
        """시장 데이터에서 통계적 변동성 탐지"""
        try:
            # 시장 데이터 범위 체크
            if not self._has_market_data_for_time(timestamp):
                return None
            
            start_time = timestamp - timedelta(hours=1)
            end_time = timestamp + timedelta(hours=2)
            
            # SP500 변동성 분석
            sp500_outlier = self._analyze_sp500_volatility(start_time, end_time)
            
            # 빗썸 변동성 분석  
            bithumb_outlier = self._analyze_bithumb_volatility(start_time, end_time)
            
            # 가장 높은 변동성 반환
            candidates = [x for x in [sp500_outlier, bithumb_outlier] if x]
            if candidates:
                return max(candidates, key=lambda x: x.get('volatility_score', 0))
            
            return None
            
        except Exception as e:
            logger.error(f"Statistical outlier analysis failed: {e}")
            return None
    
    def _has_market_data_for_time(self, timestamp):
        """해당 시간에 시장 데이터가 존재하는지 확인"""
        if not self.market_data_range:
            self.market_data_range = self._get_market_data_range()
        
        sp500_available = (
            self.market_data_range.get('sp500', {}).get('start', timestamp) <= timestamp <=
            self.market_data_range.get('sp500', {}).get('end', timestamp)
        )
        
        bithumb_available = (
            self.market_data_range.get('bithumb', {}).get('start', timestamp) <= timestamp <=
            self.market_data_range.get('bithumb', {}).get('end', timestamp)
        )
        
        return sp500_available or bithumb_available
    
    def _get_market_data_range(self):
        """시장 데이터 존재 범위 조회"""
        try:
            # SP500 데이터 범위
            sp500_query = """
            SELECT MIN(timestamp_ms) as start, MAX(timestamp_ms) as end
            FROM sp500_websocket_trades
            """
            sp500_result = self.pg_hook.get_first(sp500_query)
            
            # 빗썸 데이터 범위
            bithumb_query = """
            SELECT MIN(trade_timestamp) as start, MAX(trade_timestamp) as end  
            FROM bithumb_ticker
            """
            bithumb_result = self.pg_hook.get_first(bithumb_query)
            
            return {
                'sp500': {
                    'start': datetime.fromtimestamp(sp500_result[0]/1000) if sp500_result and sp500_result[0] else None,
                    'end': datetime.fromtimestamp(sp500_result[1]/1000) if sp500_result and sp500_result[1] else None
                },
                'bithumb': {
                    'start': datetime.fromtimestamp(bithumb_result[0]/1000) if bithumb_result and bithumb_result[0] else None,
                    'end': datetime.fromtimestamp(bithumb_result[1]/1000) if bithumb_result and bithumb_result[1] else None
                }
            }
        except Exception as e:
            logger.error(f"Failed to get market data range: {e}")
            return {}
    
    def _analyze_sp500_volatility(self, start_time, end_time):
        """SP500 데이터에서 변동성 분석"""
        try:
            # 타임스탬프를 밀리초로 변환
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            WITH price_changes AS (
                SELECT symbol,
                       MIN(price) as min_price,
                       MAX(price) as max_price,
                       COUNT(*) as trade_count
                FROM sp500_websocket_trades 
                WHERE timestamp_ms BETWEEN %s AND %s
                GROUP BY symbol
                HAVING COUNT(*) >= 5
            )
            SELECT symbol, 
                   ((max_price - min_price) / NULLIF(min_price, 0) * 100) as volatility_pct
            FROM price_changes
            WHERE min_price > 0
            ORDER BY volatility_pct DESC
            LIMIT 1
            """
            
            result = self.pg_hook.get_first(query, parameters=[start_ms, end_ms])
            
            if result and result[1] and result[1] > 2.0:  # 2% 이상 변동시만
                return {
                    'symbol': result[0],
                    'volatility_score': float(result[1]),
                    'source': 'statistical_correlation',
                    'priority': 3
                }
            
            return None
            
        except Exception as e:
            logger.error(f"SP500 volatility analysis failed: {e}")
            return None
    
    def _analyze_bithumb_volatility(self, start_time, end_time):
        """빗썸 데이터에서 변동성 분석"""
        try:
            # 타임스탬프를 밀리초로 변환
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            WITH crypto_changes AS (
                SELECT market,
                       MIN(CAST(low_price AS DECIMAL)) as min_price,
                       MAX(CAST(high_price AS DECIMAL)) as max_price
                FROM bithumb_ticker 
                WHERE trade_timestamp BETWEEN %s AND %s
                    AND low_price ~ '^[0-9]+\.?[0-9]*$'
                    AND high_price ~ '^[0-9]+\.?[0-9]*$'
                    AND market LIKE 'KRW-%'
                GROUP BY market
            )
            SELECT market,
                   ((max_price - min_price) / NULLIF(min_price, 0) * 100) as volatility_pct
            FROM crypto_changes
            WHERE min_price > 0
            ORDER BY volatility_pct DESC
            LIMIT 1
            """
            
            result = self.pg_hook.get_first(query, parameters=[start_ms, end_ms])
            
            if result and result[1] and result[1] > 3.0:  # 3% 이상 변동시만
                # KRW- 접두사 제거
                symbol = self._convert_bithumb_symbol(result[0])
                return {
                    'symbol': symbol,
                    'volatility_score': float(result[1]),
                    'source': 'statistical_correlation',
                    'priority': 3
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Bithumb volatility analysis failed: {e}")
            return None
    
    def _convert_bithumb_symbol(self, bithumb_market):
        """빗썸 마켓 형식을 표준 심볼로 변환 (KRW- 접두사 제거)"""
        if bithumb_market.startswith('KRW-'):
            return bithumb_market[4:]  # "KRW-ETH" → "ETH"
        elif bithumb_market.startswith('BTC-'):
            # BTC 페어는 아직 처리하지 않음 (나중에 필요시 확장)
            return None
        else:
            return bithumb_market
    
    def dedupe_and_rank(self, affected_assets):
        """중복 제거 및 우선순위 정렬"""
        seen_symbols = {}
        
        for asset in affected_assets:
            symbol = asset['symbol']
            
            # 같은 심볼이 있으면 우선순위가 높은 것 유지
            if symbol not in seen_symbols or asset['priority'] < seen_symbols[symbol]['priority']:
                seen_symbols[symbol] = asset
        
        # 우선순위 순으로 정렬하여 반환
        return sorted(seen_symbols.values(), key=lambda x: x['priority'])