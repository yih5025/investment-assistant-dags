-- CoinGecko 전체 암호화폐 시장 데이터 테이블 생성
CREATE TABLE IF NOT EXISTS coingecko_global (
    id SERIAL PRIMARY KEY,
    
    -- 기본 시장 통계
    active_cryptocurrencies INTEGER,           -- 활성 암호화폐 개수
    upcoming_icos INTEGER DEFAULT 0,          -- 예정된 ICO 개수
    ongoing_icos INTEGER DEFAULT 0,           -- 진행 중인 ICO 개수
    ended_icos INTEGER DEFAULT 0,             -- 종료된 ICO 개수
    markets INTEGER,                          -- 거래소 개수
    
    -- 총 시가총액 (주요 통화별)
    total_market_cap_usd DECIMAL(25,2),       -- USD 기준 총 시가총액
    total_market_cap_krw DECIMAL(25,2),       -- KRW 기준 총 시가총액
    total_market_cap_btc DECIMAL(15,8),       -- BTC 기준 총 시가총액
    total_market_cap_eth DECIMAL(15,8),       -- ETH 기준 총 시가총액
    
    -- 총 거래량 (주요 통화별)
    total_volume_usd DECIMAL(25,2),           -- USD 기준 총 거래량
    total_volume_krw DECIMAL(25,2),           -- KRW 기준 총 거래량
    total_volume_btc DECIMAL(15,8),           -- BTC 기준 총 거래량
    total_volume_eth DECIMAL(15,8),           -- ETH 기준 총 거래량
    
    -- 시장 점유율 (도미넌스) - 주요 코인들
    btc_dominance DECIMAL(8,4) DEFAULT 0,     -- 비트코인 도미넌스 (%)
    eth_dominance DECIMAL(8,4) DEFAULT 0,     -- 이더리움 도미넌스 (%)
    bnb_dominance DECIMAL(8,4) DEFAULT 0,     -- 바이낸스코인 도미넌스 (%)
    xrp_dominance DECIMAL(8,4) DEFAULT 0,     -- 리플 도미넌스 (%)
    ada_dominance DECIMAL(8,4) DEFAULT 0,     -- 카르다노 도미넌스 (%)
    sol_dominance DECIMAL(8,4) DEFAULT 0,     -- 솔라나 도미넌스 (%)
    doge_dominance DECIMAL(8,4) DEFAULT 0,    -- 도지코인 도미넌스 (%)
    
    -- 시장 변동률
    market_cap_change_percentage_24h_usd DECIMAL(10,6) DEFAULT 0,  -- 24시간 시총 변동률 (%)
    
    -- 원본 JSON 데이터 (전체 보존용)
    market_cap_percentage_json JSONB,         -- 모든 코인의 도미넌스 데이터
    total_market_cap_json JSONB,              -- 모든 통화별 시총 데이터
    total_volume_json JSONB,                  -- 모든 통화별 거래량 데이터
    
    -- 시간 정보
    coingecko_updated_at TIMESTAMP,           -- CoinGecko 업데이트 시간
    collected_at TIMESTAMP NOT NULL,          -- 데이터 수집 시간
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성 (시계열 분석용)
CREATE INDEX IF NOT EXISTS idx_global_collected_at ON coingecko_global(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_global_updated_at ON coingecko_global(updated_at DESC);

-- 최신 시장 현황 조회용 뷰
CREATE OR REPLACE VIEW latest_crypto_market_overview AS
SELECT 
    active_cryptocurrencies,
    markets,
    total_market_cap_usd,
    total_volume_usd,
    btc_dominance,
    eth_dominance,
    (btc_dominance + eth_dominance) as btc_eth_combined_dominance,
    (100 - btc_dominance - eth_dominance) as altcoin_dominance,
    market_cap_change_percentage_24h_usd,
    
    -- 시장 규모 분석
    CASE 
        WHEN total_market_cap_usd > 3000000000000 THEN 'Bull Market'     -- 3조 이상
        WHEN total_market_cap_usd > 1500000000000 THEN 'Stable Market'   -- 1.5조 이상
        ELSE 'Bear Market'                                               -- 1.5조 미만
    END as market_sentiment,
    
    -- 도미넌스 분석
    CASE 
        WHEN btc_dominance > 60 THEN 'BTC Dominance High'
        WHEN btc_dominance > 45 THEN 'BTC Dominance Normal'
        ELSE 'Altseason'
    END as dominance_analysis,
    
    collected_at,
    coingecko_updated_at
FROM coingecko_global
WHERE collected_at = (SELECT MAX(collected_at) FROM coingecko_global);

-- 시장 변화 추이 분석 뷰 (최근 7일)
CREATE OR REPLACE VIEW crypto_market_trends AS
WITH daily_stats AS (
    SELECT 
        DATE(collected_at) as date,
        AVG(total_market_cap_usd) as avg_market_cap_usd,
        AVG(total_volume_usd) as avg_volume_usd,
        AVG(btc_dominance) as avg_btc_dominance,
        AVG(eth_dominance) as avg_eth_dominance,
        AVG(market_cap_change_percentage_24h_usd) as avg_change_24h
    FROM coingecko_global
    WHERE collected_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(collected_at)
),
trend_calculation AS (
    SELECT 
        *,
        LAG(avg_market_cap_usd) OVER (ORDER BY date) as prev_market_cap,
        LAG(avg_btc_dominance) OVER (ORDER BY date) as prev_btc_dominance
    FROM daily_stats
)
SELECT 
    date,
    avg_market_cap_usd,
    avg_volume_usd,
    avg_btc_dominance,
    avg_eth_dominance,
    avg_change_24h,
    
    -- 일별 변화율 계산
    CASE 
        WHEN prev_market_cap IS NOT NULL THEN 
            ((avg_market_cap_usd - prev_market_cap) / prev_market_cap * 100)
        ELSE NULL 
    END as daily_market_cap_change_percent,
    
    CASE 
        WHEN prev_btc_dominance IS NOT NULL THEN 
            (avg_btc_dominance - prev_btc_dominance)
        ELSE NULL 
    END as daily_btc_dominance_change,
    
    -- 추세 분석
    CASE 
        WHEN avg_change_24h > 5 THEN 'Strong Bullish'
        WHEN avg_change_24h > 2 THEN 'Bullish'
        WHEN avg_change_24h > -2 THEN 'Sideways'
        WHEN avg_change_24h > -5 THEN 'Bearish'
        ELSE 'Strong Bearish'
    END as market_trend
FROM trend_calculation
ORDER BY date DESC;

-- 도미넌스 변화 히스토리 뷰 (월별)
CREATE OR REPLACE VIEW monthly_dominance_history AS
SELECT 
    DATE_TRUNC('month', collected_at) as month,
    AVG(btc_dominance) as avg_btc_dominance,
    AVG(eth_dominance) as avg_eth_dominance,
    AVG(btc_dominance + eth_dominance) as avg_btc_eth_combined,
    MAX(btc_dominance) as max_btc_dominance,
    MIN(btc_dominance) as min_btc_dominance,
    STDDEV(btc_dominance) as btc_dominance_volatility,
    COUNT(*) as data_points
FROM coingecko_global
WHERE collected_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', collected_at)
ORDER BY month DESC;