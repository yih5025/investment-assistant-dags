-- CoinGecko 거래소별 Tickers 데이터 테이블 생성 (김치프리미엄 분석용)
CREATE TABLE IF NOT EXISTS coingecko_tickers (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,  -- 배치 식별자
    
    -- 코인 기본 정보
    coingecko_id VARCHAR(100) NOT NULL,        -- CoinGecko 코인 ID
    coin_symbol VARCHAR(20) NOT NULL,          -- 코인 심볼 (BTC, ETH 등)
    coin_name VARCHAR(200) NOT NULL,           -- 코인 이름
    market_cap_rank INTEGER,                   -- 시가총액 순위
    
    -- 거래 쌍 정보
    base_symbol VARCHAR(20) NOT NULL,          -- 기축 통화 (BTC, ETH 등)
    target_symbol VARCHAR(20) NOT NULL,        -- 상대 통화 (USDT, USD, KRW 등)
    
    -- 거래소 정보
    market_name VARCHAR(100) NOT NULL,         -- 거래소 이름 (Binance, Upbit 등)
    market_identifier VARCHAR(50) NOT NULL,    -- 거래소 식별자 (binance, upbit 등)
    is_korean_exchange BOOLEAN DEFAULT FALSE,  -- 한국 거래소 여부 (김치프리미엄 분석용)
    has_trading_incentive BOOLEAN DEFAULT FALSE, -- 거래 인센티브 여부
    
    -- 가격 및 거래량
    last_price DECIMAL(25,10),                 -- 마지막 거래 가격 (원화 또는 달러)
    volume DECIMAL(25,6),                      -- 거래량 (기축통화 기준)
    
    -- USD 변환 가격 (김치프리미엄 계산 핵심)
    converted_last_usd DECIMAL(20,8),          -- USD 기준 마지막 거래가
    converted_last_btc DECIMAL(15,8),          -- BTC 기준 마지막 거래가
    converted_volume_usd DECIMAL(20,2),        -- USD 기준 거래량
    
    -- 거래소 품질 지표
    trust_score VARCHAR(20),                   -- 신뢰도 (green, yellow, red)
    bid_ask_spread_percentage DECIMAL(10,6),   -- 호가 스프레드 (%)
    
    -- 유동성 지표 (Market Depth)
    cost_to_move_up_usd DECIMAL(15,2),         -- 2% 상승시키는 비용 (USD)
    cost_to_move_down_usd DECIMAL(15,2),       -- 2% 하락시키는 비용 (USD)
    
    -- 데이터 품질 지표
    is_anomaly BOOLEAN DEFAULT FALSE,          -- 이상 데이터 여부
    is_stale BOOLEAN DEFAULT FALSE,            -- 오래된 데이터 여부
    
    -- URL 정보
    trade_url VARCHAR(500),                    -- 거래 URL
    
    -- 시간 정보
    timestamp TIMESTAMP,                       -- 티커 타임스탬프
    last_traded_at TIMESTAMP,                  -- 마지막 거래 시간
    last_fetch_at TIMESTAMP,                   -- 마지막 페치 시간
    collected_at TIMESTAMP NOT NULL,           -- 데이터 수집 시간
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성 (김치프리미엄 분석 최적화)
CREATE INDEX IF NOT EXISTS idx_tickers_batch_id ON coingecko_tickers(batch_id);
CREATE INDEX IF NOT EXISTS idx_tickers_coingecko_id ON coingecko_tickers(coingecko_id);
CREATE INDEX IF NOT EXISTS idx_tickers_coin_symbol ON coingecko_tickers(coin_symbol);
CREATE INDEX IF NOT EXISTS idx_tickers_market_identifier ON coingecko_tickers(market_identifier);
CREATE INDEX IF NOT EXISTS idx_tickers_korean_exchange ON coingecko_tickers(is_korean_exchange);
CREATE INDEX IF NOT EXISTS idx_tickers_collected_at ON coingecko_tickers(collected_at DESC);

-- 복합 인덱스 (김치프리미엄 계산 최적화)
CREATE INDEX IF NOT EXISTS idx_tickers_symbol_exchange_type ON coingecko_tickers(coin_symbol, is_korean_exchange, market_identifier);
CREATE INDEX IF NOT EXISTS idx_tickers_usd_price_volume ON coingecko_tickers(converted_last_usd, converted_volume_usd) WHERE converted_last_usd IS NOT NULL;

-- 김치프리미엄 실시간 계산 뷰 
CREATE OR REPLACE VIEW kimchi_premium_realtime AS
WITH korean_prices AS (
    -- 한국 거래소 가격 (Upbit, Bithumb 등)
    SELECT 
        coingecko_id,
        coin_symbol,
        market_identifier as korean_exchange,
        converted_last_usd as korean_price_usd,
        converted_volume_usd as korean_volume_usd,
        bid_ask_spread_percentage as korean_spread,
        trust_score as korean_trust_score,
        last_traded_at,
        ROW_NUMBER() OVER (PARTITION BY coingecko_id ORDER BY converted_volume_usd DESC) as rn
    FROM coingecko_tickers 
    WHERE is_korean_exchange = true
    AND batch_id = (SELECT batch_id FROM coingecko_tickers ORDER BY collected_at DESC LIMIT 1)
    AND converted_last_usd IS NOT NULL
    AND trust_score IN ('green', 'yellow')  -- 최소 신뢰도 조건
),
global_prices AS (
    -- 글로벌 주요 거래소 평균 가격 (Binance, Coinbase 등)
    SELECT 
        coingecko_id,
        coin_symbol,
        AVG(converted_last_usd) as global_avg_price_usd,
        SUM(converted_volume_usd) as total_global_volume_usd,
        AVG(bid_ask_spread_percentage) as avg_global_spread,
        COUNT(*) as global_exchange_count,
        STRING_AGG(market_identifier, ', ' ORDER BY converted_volume_usd DESC) as global_exchanges
    FROM coingecko_tickers 
    WHERE is_korean_exchange = false
    AND batch_id = (SELECT batch_id FROM coingecko_tickers ORDER BY collected_at DESC LIMIT 1)
    AND market_identifier IN ('binance', 'coinbase-exchange', 'kraken', 'bybit', 'okx')  -- 주요 거래소만
    AND converted_last_usd IS NOT NULL
    AND trust_score = 'green'  -- 높은 신뢰도만
    GROUP BY coingecko_id, coin_symbol
    HAVING COUNT(*) >= 2  -- 최소 2개 거래소 이상
)
SELECT 
    k.coingecko_id,
    k.coin_symbol,
    k.korean_exchange,
    k.korean_price_usd,
    g.global_avg_price_usd,
    
    -- 김치프리미엄 계산 (%)
    ((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) as kimchi_premium_percent,
    
    -- 절대 가격 차이 (USD)
    (k.korean_price_usd - g.global_avg_price_usd) as price_diff_usd,
    
    -- 거래량 및 유동성 정보
    k.korean_volume_usd,
    g.total_global_volume_usd,
    g.global_exchange_count,
    g.global_exchanges,
    
    -- 스프레드 및 품질 지표
    k.korean_spread,
    g.avg_global_spread,
    k.korean_trust_score,
    
    -- 차익거래 가능성 분석
    CASE 
        WHEN ABS((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) > 3 
             AND k.korean_volume_usd > 100000 
             AND g.total_global_volume_usd > 500000 
        THEN 'HIGH_OPPORTUNITY'
        WHEN ABS((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) > 1.5 
             AND k.korean_volume_usd > 50000 
        THEN 'MEDIUM_OPPORTUNITY'
        WHEN ABS((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) > 0.5 
        THEN 'LOW_OPPORTUNITY'
        ELSE 'NO_OPPORTUNITY'
    END as arbitrage_opportunity,
    
    k.last_traded_at as latest_korean_trade,
    CURRENT_TIMESTAMP as calculated_at
FROM korean_prices k
INNER JOIN global_prices g ON k.coingecko_id = g.coingecko_id
WHERE k.rn = 1  -- 거래량이 가장 큰 한국 거래소만
AND ABS((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) >= 0.3  -- 0.3% 이상 차이
ORDER BY ABS((k.korean_price_usd - g.global_avg_price_usd) / g.global_avg_price_usd * 100) DESC;

-- 거래소별 유동성 분석 뷰
CREATE OR REPLACE VIEW exchange_liquidity_analysis AS
SELECT 
    market_identifier,
    market_name,
    is_korean_exchange,
    COUNT(*) as ticker_count,
    COUNT(DISTINCT coingecko_id) as unique_coins,
    
    -- 거래량 통계
    SUM(converted_volume_usd) as total_volume_usd,
    AVG(converted_volume_usd) as avg_volume_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY converted_volume_usd) as median_volume_usd,
    
    -- 스프레드 통계 (유동성 지표)
    AVG(bid_ask_spread_percentage) as avg_spread_percent,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bid_ask_spread_percentage) as median_spread_percent,
    
    -- 신뢰도 분포
    COUNT(CASE WHEN trust_score = 'green' THEN 1 END) as green_trust_count,
    COUNT(CASE WHEN trust_score = 'yellow' THEN 1 END) as yellow_trust_count,
    COUNT(CASE WHEN trust_score = 'red' THEN 1 END) as red_trust_count,
    
    -- 데이터 품질
    COUNT(CASE WHEN is_anomaly = false AND is_stale = false THEN 1 END) as clean_data_count,
    
    -- 유동성 점수 계산 (거래량 * 신뢰도 / 스프레드)
    (SUM(converted_volume_usd) * 
     (COUNT(CASE WHEN trust_score = 'green' THEN 1 END) * 1.0 + 
      COUNT(CASE WHEN trust_score = 'yellow' THEN 0.7 END) + 
      COUNT(CASE WHEN trust_score = 'red' THEN 0.3 END)) / COUNT(*)) / 
    GREATEST(AVG(bid_ask_spread_percentage), 0.01) as liquidity_score,
    
    MAX(collected_at) as latest_update
FROM coingecko_tickers 
WHERE batch_id = (SELECT batch_id FROM coingecko_tickers ORDER BY collected_at DESC LIMIT 1)
AND converted_volume_usd IS NOT NULL
GROUP BY market_identifier, market_name, is_korean_exchange
ORDER BY liquidity_score DESC;

-- 코인별 가격 일관성 분석 뷰 (차익거래 기회 발견용)
CREATE OR REPLACE VIEW coin_price_consistency AS
WITH coin_stats AS (
    SELECT 
        coingecko_id,
        coin_symbol,
        COUNT(*) as exchange_count,
        COUNT(CASE WHEN is_korean_exchange = true THEN 1 END) as korean_exchanges,
        COUNT(CASE WHEN is_korean_exchange = false THEN 1 END) as global_exchanges,
        
        AVG(converted_last_usd) as avg_price_usd,
        STDDEV(converted_last_usd) as price_stddev_usd,
        MIN(converted_last_usd) as min_price_usd,
        MAX(converted_last_usd) as max_price_usd,
        
        SUM(converted_volume_usd) as total_volume_usd,
        AVG(bid_ask_spread_percentage) as avg_spread
    FROM coingecko_tickers 
    WHERE batch_id = (SELECT batch_id FROM coingecko_tickers ORDER BY collected_at DESC LIMIT 1)
    AND converted_last_usd IS NOT NULL
    AND trust_score IN ('green', 'yellow')
    GROUP BY coingecko_id, coin_symbol
    HAVING COUNT(*) >= 3  -- 최소 3개 거래소 이상
)
SELECT 
    coingecko_id,
    coin_symbol,
    exchange_count,
    korean_exchanges,
    global_exchanges,
    
    avg_price_usd,
    price_stddev_usd,
    min_price_usd,
    max_price_usd,
    
    -- 가격 편차 분석
    (price_stddev_usd / avg_price_usd * 100) as price_cv_percent,  -- 변동계수
    ((max_price_usd - min_price_usd) / avg_price_usd * 100) as price_range_percent,
    
    -- 차익거래 기회 평가
    CASE 
        WHEN ((max_price_usd - min_price_usd) / avg_price_usd * 100) > 5 
             AND total_volume_usd > 1000000 
        THEN 'STRONG_ARBITRAGE'
        WHEN ((max_price_usd - min_price_usd) / avg_price_usd * 100) > 2 
             AND total_volume_usd > 200000 
        THEN 'MODERATE_ARBITRAGE'
        WHEN ((max_price_usd - min_price_usd) / avg_price_usd * 100) > 1 
        THEN 'WEAK_ARBITRAGE'
        ELSE 'CONSISTENT_PRICING'
    END as arbitrage_potential,
    
    total_volume_usd,
    avg_spread,
    
    CURRENT_TIMESTAMP as analyzed_at
FROM coin_stats
ORDER BY price_range_percent DESC;