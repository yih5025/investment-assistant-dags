-- CoinGecko 파생상품 데이터 테이블 생성
CREATE TABLE IF NOT EXISTS coingecko_derivatives (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,  -- 배치 식별자 (YYYYMMDD_HHMMSS)
    
    -- 기본 정보
    market VARCHAR(100) NOT NULL,              -- 거래소명 (예: "Binance (Futures)")
    symbol VARCHAR(50) NOT NULL,               -- 거래 심볼 (예: "BTCUSDT")
    index_id VARCHAR(20) NOT NULL,             -- 기초자산 (예: "BTC")
    
    -- 가격 데이터
    price DECIMAL(20,8),                       -- 파생상품 가격
    price_percentage_change_24h DECIMAL(10,4), -- 24시간 가격 변동률
    index_price DECIMAL(20,8),                 -- 기초자산(현물) 가격
    
    -- 파생상품 특성
    contract_type VARCHAR(20),                 -- 계약 타입 ("perpetual", "futures")
    basis DECIMAL(15,10),                      -- 베이시스 (선물가 - 현물가 차이율)
    spread DECIMAL(10,6),                      -- 스프레드
    funding_rate DECIMAL(10,6),                -- 펀딩 비율
    
    -- 시장 규모
    open_interest_usd DECIMAL(20,2),           -- 미체결약정 (USD)
    volume_24h_usd DECIMAL(20,2),              -- 24시간 거래량 (USD)
    
    -- 시간 데이터
    last_traded_at TIMESTAMP,                  -- 마지막 거래 시간
    expired_at TIMESTAMP,                      -- 만료 시간 (perpetual의 경우 NULL)
    collected_at TIMESTAMP NOT NULL,           -- 데이터 수집 시간
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_derivatives_batch_id ON coingecko_derivatives(batch_id);
CREATE INDEX IF NOT EXISTS idx_derivatives_index_id ON coingecko_derivatives(index_id);
CREATE INDEX IF NOT EXISTS idx_derivatives_market ON coingecko_derivatives(market);
CREATE INDEX IF NOT EXISTS idx_derivatives_collected_at ON coingecko_derivatives(collected_at);

-- 복합 인덱스 (조회 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_derivatives_symbol_market ON coingecko_derivatives(index_id, market);
CREATE INDEX IF NOT EXISTS idx_derivatives_funding_rate ON coingecko_derivatives(funding_rate) WHERE funding_rate IS NOT NULL;

-- 최신 파생상품 데이터 조회용 뷰
CREATE OR REPLACE VIEW latest_derivatives_summary AS
SELECT 
    index_id,
    COUNT(*) as market_count,
    AVG(funding_rate) as avg_funding_rate,
    SUM(CASE WHEN funding_rate > 0 THEN 1 ELSE 0 END) as positive_funding_count,
    SUM(CASE WHEN funding_rate < 0 THEN 1 ELSE 0 END) as negative_funding_count,
    SUM(open_interest_usd) as total_open_interest,
    SUM(volume_24h_usd) as total_volume_24h,
    AVG(basis) as avg_basis,
    MAX(collected_at) as latest_update
FROM coingecko_derivatives 
WHERE batch_id = (
    SELECT batch_id 
    FROM coingecko_derivatives 
    ORDER BY collected_at DESC 
    LIMIT 1
)
GROUP BY index_id
ORDER BY total_open_interest DESC;

-- 거래소별 요약 뷰
CREATE OR REPLACE VIEW derivatives_by_market AS
SELECT 
    market,
    COUNT(*) as product_count,
    COUNT(DISTINCT index_id) as unique_assets,
    AVG(funding_rate) as avg_funding_rate,
    SUM(open_interest_usd) as total_open_interest,
    SUM(volume_24h_usd) as total_volume_24h,
    MAX(collected_at) as latest_update
FROM coingecko_derivatives 
WHERE batch_id = (
    SELECT batch_id 
    FROM coingecko_derivatives 
    ORDER BY collected_at DESC 
    LIMIT 1
)
GROUP BY market
ORDER BY total_open_interest DESC;