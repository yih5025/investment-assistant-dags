-- initdb/create_federal_funds_rate.sql
-- 연방기금금리 테이블 생성

CREATE TABLE IF NOT EXISTS federal_funds_rate (
    date            DATE NOT NULL PRIMARY KEY,
    rate            NUMERIC(6,3),  -- 최대 999.999% 금리 (소수점 3자리)
    interval_type   TEXT DEFAULT 'monthly',
    unit            TEXT DEFAULT 'percent',
    name            TEXT DEFAULT 'Effective Federal Funds Rate'
);

-- 인덱스 생성 (날짜 역순으로 최신 데이터 우선 조회)
CREATE INDEX IF NOT EXISTS idx_federal_funds_rate_date_desc ON federal_funds_rate(date DESC);

-- 코멘트 추가
COMMENT ON TABLE federal_funds_rate IS '미국 연방기금금리 데이터 (Alpha Vantage API)';
COMMENT ON COLUMN federal_funds_rate.date IS '기준일자 (YYYY-MM-DD 형식)';
COMMENT ON COLUMN federal_funds_rate.rate IS '연방기금금리 (퍼센트)';
COMMENT ON COLUMN federal_funds_rate.interval_type IS '데이터 간격 (daily, weekly, monthly)';
COMMENT ON COLUMN federal_funds_rate.unit IS '단위 (percent)';
COMMENT ON COLUMN federal_funds_rate.name IS '지표명칭';