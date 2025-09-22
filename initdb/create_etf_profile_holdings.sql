-- dags/initdb/create_etf_profile_holdings.sql
-- Alpha Vantage API 응답 전체를 그대로 저장하는 테이블

CREATE TABLE IF NOT EXISTS etf_profile_holdings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    
    -- ETF 기본 정보 (API root level)
    net_assets BIGINT,                    -- "365600000000"
    net_expense_ratio DECIMAL(6,4),       -- "0.002"
    portfolio_turnover DECIMAL(6,4),      -- "0.08"
    dividend_yield DECIMAL(6,4),          -- "0.0048"
    inception_date DATE,                  -- "1999-03-10"
    leveraged VARCHAR(10),                -- "NO"
    
    -- 섹터 분배 (JSON 컬럼)
    sectors JSONB,                        -- [{"sector": "INFORMATION TECHNOLOGY", "weight": "0.517"}]
    
    -- 구성종목 (JSON 컬럼)
    holdings JSONB,                       -- [{"symbol": "NVDA", "description": "NVIDIA CORP", "weight": "0.0943"}]
    
    -- 메타데이터
    collected_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (symbol) REFERENCES etf_basic_info(symbol)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_etf_profile_holdings_symbol ON etf_profile_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_etf_profile_holdings_collected ON etf_profile_holdings(collected_at DESC);