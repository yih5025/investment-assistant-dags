# utils/asset_matcher.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, timezone
import logging
import re
from .symbol_mapping import COMPREHENSIVE_SYMBOL_MAPPING

logger = logging.getLogger(__name__)

class SocialMediaAnalyzer:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self.market_data_range = None
    
    def determine_affected_assets(self, username, content, timestamp):
        """단순화된 2단계 자산 매칭 로직"""
        affected_assets = []
        
        # 1단계: 범용 키워드 매핑 (최대 5개까지)
        keyword_assets = self.extract_keyword_assets_v2(content)
        affected_assets.extend(keyword_assets[:5])
        
        # 2단계: 통계적 변동성 (키워드 매칭이 5개 미만일 때만)
        if len(affected_assets) < 5:
            volatile_asset = self.find_statistical_outlier(timestamp)
            if volatile_asset:
                affected_assets.append(volatile_asset)
        
        return self.dedupe_and_rank(affected_assets)
    
    def extract_keyword_assets_v2(self, content):
        """새로운 관대한 키워드 매칭"""
        if not content:
            return []
        
        # 모든 단어 추출 (영문, 숫자, 하이픈 포함)
        words = re.findall(r'\b[A-Za-z0-9\-]+\b', content.lower())
        
        matched_assets = []
        
        for i, word in enumerate(words):
            # 심볼 매핑 확인
            if word in COMPREHENSIVE_SYMBOL_MAPPING:
                symbol = COMPREHENSIVE_SYMBOL_MAPPING[word]
                matched_assets.append({
                    'symbol': symbol,
                    'source': 'keyword_mention',
                    'priority': 1,  # 모든 키워드 매칭을 최고 우선순위로
                    'matched_keyword': word,
                    'position': i
                })
        
        return self.dedupe_assets(matched_assets)
    
    def dedupe_assets(self, matched_assets):
        """자산 중복 제거 (같은 심볼은 한 번만)"""
        seen_symbols = {}
        for asset in matched_assets:
            symbol = asset['symbol']
            if symbol not in seen_symbols:
                seen_symbols[symbol] = asset
        
        return list(seen_symbols.values())
    
    def dedupe_and_rank(self, affected_assets):
        """최종 중복 제거 및 우선순위 정렬"""
        seen_symbols = {}
        
        for asset in affected_assets:
            symbol = asset['symbol']
            if symbol not in seen_symbols or asset['priority'] < seen_symbols[symbol]['priority']:
                seen_symbols[symbol] = asset
        
        return sorted(seen_symbols.values(), key=lambda x: x['priority'])
    
    def find_statistical_outlier(self, timestamp):
        """시장 데이터에서 통계적 변동성 탐지"""
        try:
            # 모든 timestamp를 UTC timezone-aware로 통일
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            # 시장 데이터 범위 체크도 timezone-aware로 통일
            if not self._has_market_data_for_time(timestamp):
                return None
                
            start_time = timestamp - timedelta(hours=1)
            end_time = timestamp + timedelta(hours=2)
            
            # 이제 안전하게 밀리초 변환 가능
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # SP500 변동성 분석
            sp500_outlier = self._analyze_sp500_volatility(start_ms, end_ms)
            
            # 빗썸 변동성 분석  
            bithumb_outlier = self._analyze_bithumb_volatility(start_ms, end_ms)
            
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
                            'start': datetime.fromtimestamp(sp500_result[0]/1000, tz=timezone.utc) if sp500_result and sp500_result[0] else None,
                            'end': datetime.fromtimestamp(sp500_result[1]/1000, tz=timezone.utc) if sp500_result and sp500_result[1] else None
                        },
                        'bithumb': {
                            'start': datetime.fromtimestamp(bithumb_result[0]/1000, tz=timezone.utc) if bithumb_result and bithumb_result[0] else None,
                            'end': datetime.fromtimestamp(bithumb_result[1]/1000, tz=timezone.utc) if bithumb_result and bithumb_result[1] else None
                        }
                    }
        except Exception as e:
            logger.error(f"Failed to get market data range: {e}")
            return {}
    
    def _analyze_sp500_volatility(self, start_ms, end_ms):
        """SP500 데이터에서 변동성 분석"""
        try:
            # 타임스탬프를 밀리초로 이미 변환 됨
            
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
    
    def _analyze_bithumb_volatility(self, start_ms, end_ms):
        """빗썸 데이터에서 변동성 분석"""
        try:
            
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
            print(result)
            
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