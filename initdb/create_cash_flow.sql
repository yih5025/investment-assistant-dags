CREATE TABLE IF NOT EXISTS cash_flow (
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,
    
    -- 핵심 지표
    operating_cashflow NUMERIC,     -- 영업활동 현금흐름
    investing_cashflow NUMERIC,     -- 투자활동 현금흐름
    financing_cashflow NUMERIC,     -- 재무활동 현금흐름
    capital_expenditures NUMERIC,   -- 자본지출 (CAPEX)
    free_cash_flow NUMERIC,         -- 잉여현금흐름 (FCF)
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, fiscal_date_ending)
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol ON cash_flow(symbol);
CREATE INDEX IF NOT EXISTS idx_cash_flow_fiscal_date ON cash_flow(fiscal_date_ending);