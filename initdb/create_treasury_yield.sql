-- create_treasury_yield.sql
-- DDL: 미국 국채 수익률 테이블 생성

CREATE TABLE IF NOT EXISTS treasury_yield (
    date DATE NOT NULL,
    maturity VARCHAR(10) NOT NULL,
    interval_type VARCHAR(10) NOT NULL,  -- 'daily', 'weekly', 'monthly'
    yield_rate NUMERIC(6,4),             -- 수익률 (%) 예: 4.1234
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (date, maturity, interval_type)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_treasury_yield_date ON treasury_yield(date);
CREATE INDEX IF NOT EXISTS idx_treasury_yield_maturity ON treasury_yield(maturity);
CREATE INDEX IF NOT EXISTS idx_treasury_yield_interval ON treasury_yield(interval_type);

-- 복합 인덱스 (쿼리 최적화용)
CREATE INDEX IF NOT EXISTS idx_treasury_yield_date_maturity ON treasury_yield(date, maturity);