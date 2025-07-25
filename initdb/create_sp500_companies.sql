CREATE TABLE IF NOT EXISTS sp500_companies (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,              -- 주식 심볼 (AAPL, MSFT 등)
    company_name TEXT NOT NULL,               -- 회사명
    gics_sector TEXT,                         -- GICS 섹터
    gics_sub_industry TEXT,                   -- GICS 하위 산업
    headquarters TEXT,                        -- 본사 위치
    date_added DATE,                          -- S&P 500 추가일
    cik TEXT,                                 -- SEC CIK 번호
    founded TEXT,                             -- 설립년도
    market_cap BIGINT,                        -- 시가총액 (별도 수집)
    website TEXT,                             -- 웹사이트 (향후 추가)
    employees INTEGER,                        -- 직원 수 (향후 추가)
    created_at TIMESTAMP DEFAULT NOW(),      -- 레코드 생성일
    updated_at TIMESTAMP DEFAULT NOW()       -- 레코드 수정일
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_sp500_companies_symbol ON sp500_companies(symbol);
CREATE INDEX IF NOT EXISTS idx_sp500_companies_sector ON sp500_companies(gics_sector);
CREATE INDEX IF NOT EXISTS idx_sp500_companies_updated ON sp500_companies(updated_at);

COMMENT ON TABLE sp500_companies IS 'S&P 500 구성 기업 정보 (Wikipedia 스크래핑)';
COMMENT ON COLUMN sp500_companies.symbol IS '주식 심볼 (예: AAPL, MSFT)';
COMMENT ON COLUMN sp500_companies.gics_sector IS 'Global Industry Classification Standard 섹터';
