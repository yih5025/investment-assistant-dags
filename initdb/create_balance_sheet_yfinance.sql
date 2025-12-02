CREATE TABLE IF NOT EXISTS balance_sheet_yfinance (
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,
    
    -- 핵심 지표
    total_assets NUMERIC,           -- 총 자산
    total_liabilities NUMERIC,      -- 총 부채
    total_equity NUMERIC,           -- 총 자본 (주주지분)
    cash_and_equivalents NUMERIC,   -- 현금 및 현금성 자산
    total_debt NUMERIC,             -- 총 차입금
    net_debt NUMERIC,               -- 순차입금
    working_capital NUMERIC,        -- 운전자본
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, fiscal_date_ending)
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_balance_sheet_yfinance_symbol ON balance_sheet_yfinance(symbol);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_yfinance_fiscal_date ON balance_sheet_yfinance(fiscal_date_ending);