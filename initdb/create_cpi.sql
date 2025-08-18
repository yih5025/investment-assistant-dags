-- initdb/create_cpi.sql
-- 소비자물가지수 테이블 생성

CREATE TABLE IF NOT EXISTS cpi (
    date            DATE NOT NULL PRIMARY KEY,
    cpi_value       NUMERIC(8,3),  -- 최대 99999.999 지수값 (소수점 3자리)
    interval_type   TEXT DEFAULT 'monthly',
    unit            TEXT DEFAULT 'index 1982-1984=100',
    name            TEXT DEFAULT 'Consumer Price Index for all Urban Consumers'
);

-- 인덱스 생성 (날짜 역순으로 최신 데이터 우선 조회)
CREATE INDEX IF NOT EXISTS idx_cpi_date_desc ON cpi(date DESC);

-- 코멘트 추가
COMMENT ON TABLE cpi IS '미국 소비자물가지수 데이터 (Alpha Vantage API)';
COMMENT ON COLUMN cpi.date IS '기준일자 (YYYY-MM-DD 형식)';
COMMENT ON COLUMN cpi.cpi_value IS '소비자물가지수 (1982-1984=100 기준)';
COMMENT ON COLUMN cpi.interval_type IS '데이터 간격 (monthly, semiannual)';
COMMENT ON COLUMN cpi.unit IS '단위 (index 1982-1984=100)';
COMMENT ON COLUMN cpi.name IS '지표명칭';