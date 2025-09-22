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
    
    def determine_affected_assets(self, username, content, timestamp, post_id=None, post_source=None):
        """단순화된 2단계 자산 매칭 로직"""
        affected_assets = []
        
        # 1단계: 범용 키워드 매핑 (키워드 저장 포함)
        keyword_assets = self.extract_keyword_assets_v2(content, post_id, post_source)
        affected_assets.extend(keyword_assets[:5])
        
        # 2단계: 통계적 변동성
        if len(affected_assets) < 5:
            volatile_asset = self.find_statistical_outlier(timestamp)
            if volatile_asset:
                affected_assets.append(volatile_asset)
        
        return self.dedupe_and_rank(affected_assets)
    
    def extract_keyword_assets_v2(self, content, post_id=None, post_source=None):
        """새로운 관대한 키워드 매칭 + DB 저장"""
        if not content:
            return []
        
        words = re.findall(r'\b[A-Za-z0-9\-]+\b', content.lower())
        
        extracted_keywords = []
        matched_assets = []
        
        for i, word in enumerate(words):
            # 키워드 저장 준비
            extracted_keywords.append({
                'post_id': post_id,
                'post_source': post_source,
                'keyword': word,
                'keyword_position': i
            })
            
            # 심볼 매칭
            if word in COMPREHENSIVE_SYMBOL_MAPPING:
                symbol = COMPREHENSIVE_SYMBOL_MAPPING[word]
                matched_assets.append({
                    'symbol': symbol,
                    'source': 'keyword_mention',
                    'priority': 1,
                    'matched_keyword': word,
                    'position': i
                })
        
        # 키워드 DB 저장
        if post_id and post_source:
            self._save_keywords_to_db(extracted_keywords)
        
        return self.dedupe_assets(matched_assets)

    def _save_keywords_to_db(self, keywords):
        """추출된 키워드를 DB에 실제 저장"""
        if not keywords:
            return
        
        try:
            insert_query = """
            INSERT INTO post_keywords (post_id, post_source, keyword, keyword_position)
            VALUES %s
            ON CONFLICT DO NOTHING
            """
            
            values = [(k['post_id'], k['post_source'], k['keyword'], k['keyword_position']) 
                    for k in keywords]
            
            # PostgresHook으로 bulk insert
            conn = self.pg_hook.get_conn()
            cursor = conn.cursor()
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, values, template=None, page_size=1000)
            conn.commit()
            
            logger.info(f"Saved {len(keywords)} keywords to DB")
            
        except Exception as e:
            logger.error(f"Failed to save keywords: {e}")
    
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
        try:
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            if not self._has_market_data_for_time(timestamp):
                return None
                
            # SP500과 빗썸 다른 범위 적용
            sp500_outlier = self._analyze_sp500_volatility_extended(timestamp)
            bithumb_outlier = self._analyze_bithumb_volatility_standard(timestamp)
            
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
    
    def _analyze_sp500_volatility_extended(self, timestamp):
        """SP500 - 5일 범위 (주말/공휴일 고려)"""
        try:
            start_time = timestamp - timedelta(days=2)
            end_time = timestamp + timedelta(days=2)
            
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
                HAVING COUNT(*) >= 5  -- 5일 중 최소 5개 거래
            )
            SELECT symbol, 
                ((max_price - min_price) / NULLIF(min_price, 0) * 100) as volatility_pct
            FROM price_changes
            WHERE min_price > 0
            ORDER BY volatility_pct DESC
            LIMIT 1
            """
            
            result = self.pg_hook.get_first(query, parameters=[start_ms, end_ms])
            
            # 디버깅 로그 추가
            logger.info(f"SP500 query result: {result}, type: {type(result)}")
            
            if result and len(result) >= 2 and result[0] and result[1] and result[1] > 2.0:
                return {
                    'symbol': result[0],
                    'volatility_score': float(result[1]),
                    'source': 'statistical_correlation',
                    'priority': 3
                }
            
            logger.info("SP500 analysis: no qualifying volatility found")
            return None
            
        except Exception as e:
            logger.error(f"SP500 volatility analysis failed: {e}")
            return None

    def _analyze_bithumb_volatility_standard(self, timestamp):
        """빗썸 - 3일 범위 (24시간 거래)"""
        try:
            start_time = timestamp - timedelta(days=1)
            end_time = timestamp + timedelta(days=1)
            
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            query = """
            WITH crypto_changes AS (
                SELECT market,
                    MIN(CAST(trade_price AS DECIMAL)) as min_price,
                    MAX(CAST(trade_price AS DECIMAL)) as max_price,
                    COUNT(*) as trade_count
                FROM bithumb_ticker 
                WHERE trade_timestamp BETWEEN %s AND %s
                    AND trade_price ~ '^[0-9]+\.?[0-9]*$'
                    AND market LIKE 'KRW-%'
                GROUP BY market
                HAVING COUNT(*) >= 10  -- 3일이므로 더 많은 데이터 요구
            )
            SELECT market,
                ((max_price - min_price) / NULLIF(min_price, 0) * 100) as volatility_pct
            FROM crypto_changes
            WHERE min_price > 0
            ORDER BY volatility_pct DESC
            LIMIT 1
            """
            
            result = self.pg_hook.get_first(query, parameters=[start_ms, end_ms])
            
            # 디버깅 로그 추가
            logger.info(f"Bithumb query result: {result}, type: {type(result)}")
            
            if result and len(result) >= 2 and result[0] and result[1] and result[1] > 5.0:
                symbol = self._convert_bithumb_symbol(result[0])
                if symbol:
                    return {
                        'symbol': symbol,
                        'volatility_score': float(result[1]),
                        'source': 'statistical_correlation',
                        'priority': 3
                    }
            
            logger.info("Bithumb analysis: no qualifying volatility found")
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