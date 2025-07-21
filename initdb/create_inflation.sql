-- initdb/create_inflation.sql
-- 인플레이션 테이블 생성

CREATE TABLE IF NOT EXISTS inflation (
    date            DATE NOT NULL PRIMARY KEY,
    inflation_rate  NUMERIC(6,3),  -- 최대 999.999% 인플레이션율 (소수점 3자리)
    interval_type   TEXT DEFAULT 'annual',
    unit            TEXT DEFAULT 'percent',
    name            TEXT DEFAULT 'Inflation, consumer prices for the United States'
);

-- 인덱스 생성 (날짜 역순으로 최신 데이터 우선 조회)
CREATE INDEX IF NOT EXISTS idx_inflation_date_desc ON inflation(date DESC);

-- 코멘트 추가
COMMENT ON TABLE inflation IS '미국 인플레이션율 데이터 (Alpha Vantage API)';
COMMENT ON COLUMN inflation.date IS '기준일자 (YYYY-MM-DD 형식)';
COMMENT ON COLUMN inflation.inflation_rate IS '인플레이션율 (퍼센트)';
COMMENT ON COLUMN inflation.interval_type IS '데이터 간격 (annual)';
COMMENT ON COLUMN inflation.unit IS '단위 (percent)';
COMMENT ON COLUMN inflation.name IS '지표명칭';