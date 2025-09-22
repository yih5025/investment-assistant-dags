-- dags/initdb/create_etf_basic_info.sql
CREATE TABLE IF NOT EXISTS etf_basic_info (
    symbol VARCHAR(10) PRIMARY KEY,
    name TEXT NOT NULL,
    fund_family TEXT,                   -- Vanguard, iShares, SPDR
    category TEXT,                      -- large_cap, technology, bonds
    popularity_rank INTEGER,            -- 1-1000 (Finnhub 선택 기준)
    
    -- ETF 핵심 정보 (Yahoo Finance)
    expense_ratio DECIMAL(6,4),         -- 0.0009 = 0.09%
    aum BIGINT,                        -- Assets Under Management
    inception_date DATE,
    dividend_yield DECIMAL(6,4),
    nav DECIMAL(10,4),                 -- Net Asset Value
    beta DECIMAL(8,4),
    week_52_high DECIMAL(10,4),
    week_52_low DECIMAL(10,4),
    ytd_return DECIMAL(8,4),
    
    -- 수집 상태 추적
    yahoo_collected BOOLEAN DEFAULT FALSE,
    finnhub_enabled BOOLEAN DEFAULT FALSE,
    alphavantage_collected BOOLEAN DEFAULT FALSE,
    
    -- 메타데이터
    description TEXT,
    website_url TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_etf_popularity ON etf_basic_info(popularity_rank);
CREATE INDEX IF NOT EXISTS idx_etf_category ON etf_basic_info(category);
CREATE INDEX IF NOT EXISTS idx_etf_finnhub_enabled ON etf_basic_info(finnhub_enabled);