# utils/symbol_mapping.py

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