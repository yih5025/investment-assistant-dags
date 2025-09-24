# utils/asset_matcher.py
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, timezone
import logging
import re
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# 방대한 키워드-심볼 매핑 딕셔너리
COMPREHENSIVE_SYMBOL_MAPPING = {
    # ===== 주요 기술 기업 =====
    # Apple 관련
    'apple': 'AAPL', 'aapl': 'AAPL', 'iphone': 'AAPL', 'ipad': 'AAPL', 
    'macbook': 'AAPL', 'mac': 'AAPL', 'ios': 'AAPL', 'siri': 'AAPL',
    'appstore': 'AAPL', 'itunes': 'AAPL', 'airpods': 'AAPL', 'imac': 'AAPL',
    'safari': 'AAPL', 'facetime': 'AAPL', 'airtags': 'AAPL',
    
    # Tesla 관련  
    'tesla': 'TSLA', 'tsla': 'TSLA', 'cybertruck': 'TSLA', 'model': 'TSLA',
    'supercharger': 'TSLA', 'autopilot': 'TSLA', 'fsd': 'TSLA', 
    'roadster': 'TSLA', 'semi': 'TSLA', 'gigafactory': 'TSLA',
    'powerwall': 'TSLA', 'solarglass': 'TSLA', 'spacex': 'TSLA',
    
    # Microsoft 관련
    'microsoft': 'MSFT', 'msft': 'MSFT', 'windows': 'MSFT', 'xbox': 'MSFT',
    'azure': 'MSFT', 'office': 'MSFT', 'teams': 'MSFT', 'outlook': 'MSFT',
    'copilot': 'MSFT', 'chatgpt': 'MSFT', 'openai': 'MSFT', 'bing': 'MSFT',
    'github': 'MSFT', 'linkedin': 'MSFT', 'surface': 'MSFT',
    
    # NVIDIA 관련
    'nvidia': 'NVDA', 'nvda': 'NVDA', 'gpu': 'NVDA', 'rtx': 'NVDA',
    'geforce': 'NVDA', 'cuda': 'NVDA', 'ai': 'NVDA', 'deeplearning': 'NVDA',
    'machinelearning': 'NVDA', 'gaming': 'NVDA', 'chip': 'NVDA',
    'semiconductor': 'NVDA', 'datacenter': 'NVDA',
    
    # Meta 관련
    'meta': 'META', 'facebook': 'META', 'instagram': 'META', 'whatsapp': 'META',
    'metaverse': 'META', 'oculus': 'META', 'vr': 'META', 'threads': 'META',
    'zuckerberg': 'META', 'reels': 'META',
    
    # Google/Alphabet 관련
    'google': 'GOOGL', 'googl': 'GOOGL', 'alphabet': 'GOOGL', 'youtube': 'GOOGL',
    'android': 'GOOGL', 'chrome': 'GOOGL', 'gmail': 'GOOGL', 'maps': 'GOOGL',
    'pixel': 'GOOGL', 'gemini': 'GOOGL', 'bard': 'GOOGL', 'waymo': 'GOOGL',
    
    # Amazon 관련
    'amazon': 'AMZN', 'amzn': 'AMZN', 'aws': 'AMZN', 'alexa': 'AMZN',
    'prime': 'AMZN', 'kindle': 'AMZN', 'echo': 'AMZN', 'bezos': 'AMZN',
    
    # ===== 암호화폐 =====
    # Bitcoin 관련
    'bitcoin': 'BTC', 'btc': 'BTC', 'satoshi': 'BTC', 'mining': 'BTC',
    'blockchain': 'BTC', 'cryptocurrency': 'BTC', 'crypto': 'BTC',
    'hodl': 'BTC', 'halving': 'BTC',
    
    # Ethereum 관련  
    'ethereum': 'ETH', 'eth': 'ETH', 'ether': 'ETH', 'vitalik': 'ETH',
    'smartcontract': 'ETH', 'defi': 'ETH', 'nft': 'ETH', 'web3': 'ETH',
    'dapp': 'ETH', 'gas': 'ETH', 'gwei': 'ETH',
    
    # 기타 암호화폐
    'dogecoin': 'DOGE', 'doge': 'DOGE', 'shib': 'DOGE', 'meme': 'DOGE',
    'solana': 'SOL', 'sol': 'SOL', 'cardano': 'ADA', 'ada': 'ADA',
    'polkadot': 'DOT', 'dot': 'DOT', 'chainlink': 'LINK', 'link': 'LINK',
    
    # ===== 기타 주요 기업 =====
    # Oracle
    'oracle': 'ORCL', 'orcl': 'ORCL', 'database': 'ORCL', 'java': 'ORCL',
    
    # 미디어/소셜
    'netflix': 'NFLX', 'nflx': 'NFLX', 'streaming': 'NFLX',
    'disney': 'DIS', 'dis': 'DIS', 'marvel': 'DIS', 'pixar': 'DIS',
    'twitter': 'TWTR', 'x': 'TWTR', 'twtr': 'TWTR',
    
    # 금융
    'paypal': 'PYPL', 'pypl': 'PYPL', 'venmo': 'PYPL',
    'visa': 'V', 'mastercard': 'MA', 'jpmorgan': 'JPM', 'jpm': 'JPM',
    'berkshire': 'BRK.B', 'buffett': 'BRK.B',
    
    # 자동차
    'ford': 'F', 'gm': 'GM', 'toyota': 'TM', 'volkswagen': 'VWAGY',
    'bmw': 'BMW', 'mercedes': 'DDAIF', 'rivian': 'RIVN', 'lucid': 'LCID',
    
    # 헬스케어/바이오
    'pfizer': 'PFE', 'pfe': 'PFE', 'moderna': 'MRNA', 'mrna': 'MRNA',
    'johnson': 'JNJ', 'jnj': 'JNJ', 'vaccine': 'PFE',
    
    # 에너지
    'exxon': 'XOM', 'xom': 'XOM', 'chevron': 'CVX', 'cvx': 'CVX',
    'oil': 'XOM', 'gas': 'XOM', 'energy': 'XOM',
    
    # 반도체 추가
    'intel': 'INTC', 'intc': 'INTC', 'amd': 'AMD', 'qualcomm': 'QCOM',
    'qcom': 'QCOM', 'broadcom': 'AVGO', 'avgo': 'AVGO',
    
    # 소매/이커머스
    'walmart': 'WMT', 'wmt': 'WMT', 'target': 'TGT', 'tgt': 'TGT',
    'costco': 'COST', 'homedepot': 'HD', 'hd': 'HD',
    
    # 통신
    'verizon': 'VZ', 'vz': 'VZ', 'att': 'T', 'tmobile': 'TMUS',
    'comcast': 'CMCSA', 'cmcsa': 'CMCSA',
    
    # 항공우주
    'boeing': 'BA', 'ba': 'BA', 'lockheed': 'LMT', 'lmt': 'LMT',
    'raytheon': 'RTX', 'rtx': 'RTX',
    
    # ===== 정치/정부 관련 =====
    'trump': 'DWAC', 'truthsocial': 'DWAC', 'dwac': 'DWAC',
    'maga': 'DWAC', 'republican': 'DWAC', 'conservative': 'DWAC',
    'election': 'SPY', 'politics': 'SPY', 'congress': 'SPY',
    'senate': 'SPY', 'whitehouse': 'SPY', 'biden': 'SPY',
    
    # ===== 경제 지표 관련 =====
    'inflation': 'SPY', 'fed': 'SPY', 'powell': 'SPY', 'rates': 'SPY',
    'gdp': 'SPY', 'unemployment': 'SPY', 'economy': 'SPY',
    'recession': 'SPY', 'bull': 'SPY', 'bear': 'SPY', 'market': 'SPY',
    'nasdaq': 'QQQ', 'qqq': 'QQQ', 'spy': 'SPY', 'dow': 'DIA',
    
    # ===== 일반 기술 용어 =====
    'technology': 'QQQ', 'tech': 'QQQ', 'software': 'QQQ',
    'cloud': 'MSFT', 'saas': 'CRM', 'cybersecurity': 'CRWD',
    'data': 'NVDA', 'analytics': 'NVDA', 'internet': 'QQQ',
    
    # ===== 산업 섹터 =====
    'banking': 'XLF', 'finance': 'XLF', 'healthcare': 'XLV',
    'pharmaceutical': 'XLV', 'retail': 'XLY', 'consumer': 'XLY',
    'industrial': 'XLI', 'materials': 'XLB', 'utilities': 'XLU',
    'realestate': 'XLRE', 'reit': 'XLRE'
}

def get_symbol_for_keyword(keyword):
    """키워드를 소문자로 변환하여 심볼 매핑"""
    return COMPREHENSIVE_SYMBOL_MAPPING.get(keyword.lower())

def get_all_keywords_for_symbol(symbol):
    """특정 심볼과 연관된 모든 키워드 반환"""
    return [k for k, v in COMPREHENSIVE_SYMBOL_MAPPING.items() if v == symbol]

class KeywordBuffer:
    """메모리 효율적인 키워드 배치 저장"""
    def __init__(self, pg_hook, buffer_size=50):
        self.pg_hook = pg_hook
        self.buffer = []
        self.buffer_size = buffer_size
    
    def add_keywords(self, keywords):
        self.buffer.extend(keywords)
        if len(self.buffer) >= self.buffer_size:
            self.flush_to_db()
    
    def flush_to_db(self):
        if not self.buffer:
            return
            
        try:
            insert_query = """
            INSERT INTO post_keywords (post_id, post_source, keyword, keyword_position)
            VALUES %s
            ON CONFLICT DO NOTHING
            """
            
            values = [(k['post_id'], k['post_source'], k['keyword'], k['keyword_position']) 
                     for k in self.buffer]
            
            conn = self.pg_hook.get_conn()
            cursor = conn.cursor()
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, values, template=None, page_size=100)
            conn.commit()
            cursor.close()
            
            logger.info(f"Saved {len(self.buffer)} keywords to DB")
            self.buffer.clear()
            
        except Exception as e:
            logger.error(f"Failed to save keywords: {e}")
            self.buffer.clear()  # 에러시에도 메모리 정리
    
    def __del__(self):
        # 객체 소멸시 남은 데이터 저장
        self.flush_to_db()

class SocialMediaAnalyzer:
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        self.market_data_range = None
        self.keyword_buffer = KeywordBuffer(self.pg_hook)
    
    def determine_affected_assets(self, username, content, timestamp, post_id=None, post_source=None):
        """단순화된 2단계 자산 매칭 로직"""
        affected_assets = []

        start_account_assets_time = time.time()
        account_assets = self.get_account_default_assets(username)
        affected_assets.extend(account_assets)
        end_account_assets_time = time.time()
        logger.info(f"Account assets time: {end_account_assets_time - start_account_assets_time} seconds")

        start_keyword_assets_time = time.time()
        # 1단계: 범용 키워드 매핑 (키워드 저장 포함)
        keyword_assets = self.extract_keyword_assets_v2(content, post_id, post_source)
        affected_assets.extend(keyword_assets[:5])
        end_keyword_assets_time = time.time()
        logger.info(f"Keyword assets time: {end_keyword_assets_time - start_keyword_assets_time} seconds")

        start_volatile_asset_time = time.time()
        # 2단계: 통계적 변동성 (키워드 매칭이 5개 미만일 때만)
        if len(affected_assets) < 5:
            volatile_asset = self.find_statistical_outlier(timestamp)
            if volatile_asset:
                affected_assets.append(volatile_asset)
        end_volatile_asset_time = time.time()
        logger.info(f"Volatile asset time: {end_volatile_asset_time - start_volatile_asset_time} seconds")
        return self.dedupe_and_rank(affected_assets)
    

    def get_account_default_assets(self, username):
        """계정별 기본 자산 매핑 (ETF 포함)"""
        account_mapping = {
            # === 개인 CEO/임원들 ===
            'elonmusk': [
                {'symbol': 'TSLA', 'source': 'account_default', 'priority': 2},
                {'symbol': 'DOGE', 'source': 'account_default', 'priority': 2}
            ],
            'tim_cook': [
                {'symbol': 'AAPL', 'source': 'account_default', 'priority': 2}
            ],
            'satyanadella': [
                {'symbol': 'MSFT', 'source': 'account_default', 'priority': 2}
            ],
            'sundarpichai': [
                {'symbol': 'GOOGL', 'source': 'account_default', 'priority': 2}
            ],
            'jeffbezos': [
                {'symbol': 'AMZN', 'source': 'account_default', 'priority': 2}
            ],
            
            # === 암호화폐 관련 인물들 ===
            'VitalikButerin': [
                {'symbol': 'ETH', 'source': 'account_default', 'priority': 2}
            ],
            'saylor': [
                {'symbol': 'BTC', 'source': 'account_default', 'priority': 2}
            ],
            'brian_armstrong': [
                {'symbol': 'BTC', 'source': 'account_default', 'priority': 2},
                {'symbol': 'ETH', 'source': 'account_default', 'priority': 2}
            ],
            
            # === 금융/투자 전문가들 (ETF 위주) ===
            'RayDalio': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'TLT', 'source': 'account_default', 'priority': 2},  # 채권 ETF
                {'symbol': 'LQD', 'source': 'account_default', 'priority': 2}   # 회사채 ETF
            ],
            'jimcramer': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'QQQ', 'source': 'account_default', 'priority': 2}   # 기술주 ETF
            ],
            'CathieDWood': [
                {'symbol': 'ARKK', 'source': 'account_default', 'priority': 2}, # 자신의 ETF
                {'symbol': 'TSLA', 'source': 'account_default', 'priority': 2}
            ],
            'mcuban': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'QQQ', 'source': 'account_default', 'priority': 2}
            ],
            'chamath': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'XLK', 'source': 'account_default', 'priority': 2}   # 기술 섹터 ETF
            ],
            
            # === 정부 관련 (거시경제 ETF) ===
            'SecYellen': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'TLT', 'source': 'account_default', 'priority': 2},  # 국채 ETF
                {'symbol': 'XLF', 'source': 'account_default', 'priority': 2}   # 금융 섹터 ETF
            ],
            
            # === 미디어/뉴스 계정들 (광범위한 시장 ETF) ===
            'CNBC': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'QQQ', 'source': 'account_default', 'priority': 2}
            ],
            'business': [  # Bloomberg
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'VNQ', 'source': 'account_default', 'priority': 2},  # 부동산 ETF
                {'symbol': 'XLE', 'source': 'account_default', 'priority': 2}   # 에너지 섹터 ETF
            ],
            'WSJ': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'IWM', 'source': 'account_default', 'priority': 2}   # 소형주 ETF
            ],
            
            # === 기업 공식 계정들 (기존과 동일) ===
            'Tesla': [
                {'symbol': 'TSLA', 'source': 'account_default', 'priority': 2}
            ],
            'nvidia': [
                {'symbol': 'NVDA', 'source': 'account_default', 'priority': 2}
            ],
            'Meta': [
                {'symbol': 'META', 'source': 'account_default', 'priority': 2}
            ],
            'Oracle': [
                {'symbol': 'ORCL', 'source': 'account_default', 'priority': 2}
            ],
            'IBM': [
                {'symbol': 'IBM', 'source': 'account_default', 'priority': 2}
            ],
            'Palantir': [
                {'symbol': 'PLTR', 'source': 'account_default', 'priority': 2}
            ],
            'IonQ': [
                {'symbol': 'IONQ', 'source': 'account_default', 'priority': 2}
            ],
            
            # === 암호화폐 공식 계정들 ===
            'BitCoin': [
                {'symbol': 'BTC', 'source': 'account_default', 'priority': 2}
            ],
            'Ethereum': [
                {'symbol': 'ETH', 'source': 'account_default', 'priority': 2}
            ],
            'Solana': [
                {'symbol': 'SOL', 'source': 'account_default', 'priority': 2}
            ],
            'Dogecoin': [
                {'symbol': 'DOGE', 'source': 'account_default', 'priority': 2}
            ],
            'CoinbaseAssets': [
                {'symbol': 'BTC', 'source': 'account_default', 'priority': 2},
                {'symbol': 'ETH', 'source': 'account_default', 'priority': 2}
            ],
            
            # === 미디어/뉴스 계정들 ===
            'CNBC': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'XLF', 'source': 'account_default', 'priority': 2}  # 금융 섹터 (CNBC 특성상)
            ],
            'business': [  # Bloomberg
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'TLT', 'source': 'account_default', 'priority': 2},  # 채권 (거시경제 전문)
                {'symbol': 'VNQ', 'source': 'account_default', 'priority': 2}   # 부동산 (Bloomberg 특성)
            ],
            'WSJ': [
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2},
                {'symbol': 'QQQ', 'source': 'account_default', 'priority': 2}   # 기술주 (WSJ 비즈니스 포커스)
            ],
            'realDonaldTrump': [
                {'symbol': 'DWAC', 'source': 'account_default', 'priority': 2},
                {'symbol': 'SPY', 'source': 'account_default', 'priority': 2}   # 경제 정책 영향
            ]
        }
        
        return account_mapping.get(username, [])

    def extract_keyword_assets_v2(self, content, post_id=None, post_source=None):
        if not content or len(content) > 1000:  # 너무 긴 글 제외
            return []
        
        # 더 제한적인 패턴으로 메모리 절약
        # 알파벳만, 3-12자, 최대 50개 단어만
        words = re.findall(r'\b[a-zA-Z]{3,12}\b', content.lower())[:30]
        
        matched_assets = []
        seen_symbols = set()
        
        # 조기 종료로 불필요한 반복 방지
        for word in words:
            if len(seen_symbols) >= 5:  # 최대 5개 심볼만
                break
                
            if word in COMPREHENSIVE_SYMBOL_MAPPING:
                symbol = COMPREHENSIVE_SYMBOL_MAPPING[word]
                if symbol not in seen_symbols:
                    seen_symbols.add(symbol)
                    matched_assets.append({
                        'symbol': symbol,
                        'source': 'keyword_mention',
                        'priority': 1,
                        'matched_keyword': word
                    })
        
        # 즉시 메모리 해제
        del words, seen_symbols
        
        # 키워드 저장 비활성화
        # self.keyword_buffer.add_keywords(matched_keywords)
        
        return matched_assets
    
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
            
            # 시장 데이터 범위 체크
            if not self._has_market_data_for_time(timestamp):
                return None
            sp500_start_time = time.time()
            # SP500과 빗썸 분석
            sp500_outlier = self._analyze_sp500_volatility(timestamp)
            sp500_end_time = time.time()
            logger.info(f"SP500 volatility analysis time: {sp500_end_time - sp500_start_time} seconds")

            bithumb_start_time = time.time()
            bithumb_outlier = self._analyze_bithumb_volatility(timestamp)
            bithumb_end_time = time.time()
            logger.info(f"Bithumb volatility analysis time: {bithumb_end_time - bithumb_start_time} seconds")

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
    
    def _analyze_sp500_volatility(self, timestamp):
        """SP500 - 5일 범위 (주말/공휴일 고려)"""
        try:
            # 5일 범위
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
            
            logger.info(f"SP500 query result: {result}")
            
            if result and len(result) >= 2 and result[0] and result[1] and float(result[1]) > 2.0:
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
    
    def _analyze_bithumb_volatility(self, timestamp):
        try:
            start_time = timestamp - timedelta(days=1)
            end_time = timestamp + timedelta(days=1)
            
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # 값을 직접 삽입 (매개변수 사용 안함)
            query = f"""
            WITH crypto_changes AS (
                SELECT market,
                    MIN(CAST(trade_price AS DECIMAL)) as min_price,
                    MAX(CAST(trade_price AS DECIMAL)) as max_price,
                    COUNT(*) as trade_count
                FROM bithumb_ticker 
                WHERE trade_timestamp BETWEEN {start_ms} AND {end_ms}
                    AND market LIKE 'KRW-%'
                    AND trade_price IS NOT NULL 
                    AND LENGTH(trade_price) > 0
                GROUP BY market
                HAVING COUNT(*) >= 3
            )
            SELECT market, ((max_price - min_price) / min_price * 100) as volatility_pct
            FROM crypto_changes
            WHERE min_price > 0
            ORDER BY volatility_pct DESC
            LIMIT 1
            """
            
            result = self.pg_hook.get_first(query)  # 매개변수 없음
            
            logger.info(f"Bithumb result: {result}")
            
            if result and len(result) >= 2:
                market, volatility_pct = result
                if volatility_pct and float(volatility_pct) > 3.0:
                    symbol = self._convert_bithumb_symbol(market)
                    if symbol:
                        return {
                            'symbol': symbol,
                            'volatility_score': float(volatility_pct),
                            'source': 'statistical_correlation',
                            'priority': 3
                        }
            
            return None
            
        except Exception as e:
            logger.error(f"Bithumb volatility analysis failed: {e}")
            return None
    
    def _convert_bithumb_symbol(self, bithumb_market):
        """빗썸 마켓 형식을 표준 심볼로 변환 (KRW- 접두사 제거)"""
        if bithumb_market and bithumb_market.startswith('KRW-'):
            return bithumb_market[4:]  # "KRW-ETH" → "ETH"
        elif bithumb_market and bithumb_market.startswith('BTC-'):
            # BTC 페어는 아직 처리하지 않음
            return None
        else:
            return bithumb_market
    
    def finalize_keywords(self):
        """DAG 종료시 남은 키워드 저장"""
        self.keyword_buffer.flush_to_db()