CREATE TABLE IF NOT EXISTS institutional_holders (
    symbol VARCHAR(20) NOT NULL,
    holder_name VARCHAR(255) NOT NULL,  -- 보유 기관명 (예: Vanguard Group)
    shares_held BIGINT,                 -- 보유 주식 수
    date_reported DATE,                 -- 보고일
    change_pct NUMERIC,                 -- 지분 변동률
    pct_out NUMERIC,                    -- 전체 주식 대비 비율 (%)
    value_usd NUMERIC,                  -- 평가 가치
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, holder_name)   -- 한 종목당 기관별 최신 데이터만 유지
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_institutional_holders_symbol ON institutional_holders(symbol);
CREATE INDEX IF NOT EXISTS idx_institutional_holders_date_reported ON institutional_holders(date_reported);