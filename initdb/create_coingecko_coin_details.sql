-- CoinGecko 코인 상세 정보 테이블 생성
CREATE TABLE IF NOT EXISTS coingecko_coin_details (
    coingecko_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    web_slug VARCHAR(200),
    
    -- Tab 1: 개념 설명용 데이터
    description_en TEXT,
    genesis_date DATE,
    country_origin VARCHAR(100),
    
    -- Links (Tab 1, Tab 2)
    homepage_url TEXT,
    blockchain_site TEXT,
    twitter_screen_name VARCHAR(100),
    facebook_username VARCHAR(100),
    telegram_channel_identifier VARCHAR(100),
    subreddit_url TEXT,
    github_repos JSONB,
    
    -- 이미지
    image_thumb VARCHAR(500),
    image_small VARCHAR(500),
    image_large VARCHAR(500),
    
    -- Categories (Tab 1, Tab 2)
    categories JSONB,
    
    -- Market Data (Tab 3)
    current_price_usd DECIMAL(20,8),
    current_price_krw DECIMAL(20,2),
    market_cap_usd BIGINT,
    market_cap_rank INTEGER,
    total_volume_usd BIGINT,
    
    -- ATH/ATL (Tab 3)
    ath_usd DECIMAL(20,8),
    ath_change_percentage DECIMAL(20,4),
    ath_date TIMESTAMP,
    atl_usd DECIMAL(20,8),
    atl_change_percentage DECIMAL(20,4),
    atl_date TIMESTAMP,
    
    -- Supply Data (Tab 3)
    total_supply DECIMAL(30,2),
    circulating_supply DECIMAL(30,2),
    max_supply DECIMAL(30,2),
    
    -- Price Changes (Tab 3)
    price_change_24h_usd DECIMAL(20,8),
    price_change_percentage_24h DECIMAL(10,4),
    price_change_percentage_7d DECIMAL(10,4),
    price_change_percentage_30d DECIMAL(10,4),
    
    -- Community Data (Tab 2)
    community_score DECIMAL(5,2),
    twitter_followers INTEGER,
    reddit_subscribers INTEGER,
    telegram_channel_user_count INTEGER,
    
    -- Developer Data (Tab 2)
    developer_score DECIMAL(5,2),
    forks INTEGER,
    stars INTEGER,
    total_issues INTEGER,
    closed_issues INTEGER,
    commit_count_4_weeks INTEGER,
    
    -- Public Interest & Liquidity (Tab 2, Tab 3)
    public_interest_score DECIMAL(5,2),
    liquidity_score DECIMAL(5,2),
    
    -- Timestamps
    coingecko_last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_coin_details_symbol ON coingecko_coin_details(symbol);
CREATE INDEX IF NOT EXISTS idx_coin_details_rank ON coingecko_coin_details(market_cap_rank);
CREATE INDEX IF NOT EXISTS idx_coin_details_updated ON coingecko_coin_details(updated_at);
CREATE INDEX IF NOT EXISTS idx_coin_details_categories ON coingecko_coin_details USING GIN(categories);

-- 빗썸 코인과 상세 정보 매핑 뷰
CREATE OR REPLACE VIEW crypto_detail_with_bithumb AS
SELECT 
    cd.*,
    mb.market_code as bithumb_market,
    bt.trade_price as bithumb_krw_price,
    bt.change_rate as bithumb_change_rate,
    bt.acc_trade_volume_24h as bithumb_volume_24h
FROM coingecko_coin_details cd
LEFT JOIN market_code_bithumb mb ON UPPER(cd.symbol) = UPPER(REPLACE(mb.market_code, 'KRW-', ''))
LEFT JOIN bithumb_ticker bt ON mb.market_code = bt.market
WHERE cd.market_cap_rank <= 200
ORDER BY cd.market_cap_rank;