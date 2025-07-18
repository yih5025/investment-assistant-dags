CREATE TABLE IF NOT EXISTS sp500_top50 (
    symbol          VARCHAR(10) PRIMARY KEY,
    company_name    TEXT NOT NULL,
    sector          TEXT,
    industry        TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_sp500_sector ON sp500_top50(sector);
CREATE INDEX IF NOT EXISTS idx_sp500_industry ON sp500_top50(industry);