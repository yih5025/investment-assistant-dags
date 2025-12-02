CREATE TABLE IF NOT EXISTS income_stmt (
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,
    
    -- 핵심 지표
    total_revenue NUMERIC,          -- 총 매출
    gross_profit NUMERIC,           -- 매출총이익
    operating_income NUMERIC,       -- 영업이익
    net_income NUMERIC,             -- 당기순이익
    ebitda NUMERIC,                 -- EBITDA (이자,세금,감가상각비 차감전 이익)
    basic_eps NUMERIC,              -- 기본 주당순이익
    diluted_eps NUMERIC,            -- 희석 주당순이익
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, fiscal_date_ending)
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_income_stmt_symbol ON income_stmt(symbol);
CREATE INDEX IF NOT EXISTS idx_income_stmt_fiscal_date ON income_stmt(fiscal_date_ending);