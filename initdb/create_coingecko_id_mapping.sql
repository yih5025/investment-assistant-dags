-- CoinGecko ID 매핑 테이블 생성
CREATE TABLE IF NOT EXISTS coingecko_id_mapping (
    coingecko_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    image_url VARCHAR(500),
    current_price_usd DECIMAL(20,8),
    market_cap_usd BIGINT,
    market_cap_rank INTEGER,
    total_volume_usd BIGINT,
    ath_usd DECIMAL(20,8),
    ath_date TIMESTAMP,
    atl_usd DECIMAL(20,8), 
    atl_date TIMESTAMP,
    circulating_supply DECIMAL(20,2),
    total_supply DECIMAL(20,2),
    max_supply DECIMAL(20,2),
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_coingecko_mapping_symbol ON coingecko_id_mapping(symbol);
CREATE INDEX IF NOT EXISTS idx_coingecko_mapping_rank ON coingecko_id_mapping(market_cap_rank);
CREATE INDEX IF NOT EXISTS idx_coingecko_mapping_updated ON coingecko_id_mapping(updated_at);

-- 빗썸 심볼과 매핑을 위한 뷰 생성
CREATE OR REPLACE VIEW coingecko_bithumb_mapping AS
SELECT 
    cg.coingecko_id,
    cg.symbol as coingecko_symbol,
    cg.name,
    mb.market_code as bithumb_market,
    REPLACE(mb.market_code, 'KRW-', '') as bithumb_symbol,
    cg.market_cap_rank,
    cg.current_price_usd,
    cg.market_cap_usd
FROM coingecko_id_mapping cg
LEFT JOIN market_code_bithumb mb ON UPPER(cg.symbol) = UPPER(REPLACE(mb.market_code, 'KRW-', ''))
WHERE cg.market_cap_rank <= 500  -- 상위 500개만
ORDER BY cg.market_cap_rank;