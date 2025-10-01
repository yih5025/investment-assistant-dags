-- initdb/create_ipo_calendar.sql
-- IPO 캘린더 테이블 생성

CREATE TABLE IF NOT EXISTS ipo_calendar (
    -- Primary Key: 자동 증가 ID
    id SERIAL PRIMARY KEY,
    
    -- IPO 정보
    symbol VARCHAR(10) NOT NULL,
    company_name TEXT NOT NULL,
    ipo_date DATE NOT NULL,
    price_range_low NUMERIC(10,2),
    price_range_high NUMERIC(10,2),
    currency VARCHAR(3) DEFAULT 'USD',
    exchange VARCHAR(20),
    
    -- 메타데이터
    fetched_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Unique 제약 조건: symbol과 ipo_date 조합은 고유해야 함
    CONSTRAINT uq_symbol_ipo_date UNIQUE (symbol, ipo_date)
);

-- 인덱스 생성 (검색 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_date ON ipo_calendar(ipo_date);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_exchange ON ipo_calendar(exchange);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_symbol ON ipo_calendar(symbol);

-- 테이블 설명
COMMENT ON TABLE ipo_calendar IS 'Alpha Vantage API를 통해 수집된 3개월 IPO 일정 데이터';
COMMENT ON COLUMN ipo_calendar.symbol IS '기업 심볼 (티커)';
COMMENT ON COLUMN ipo_calendar.company_name IS '기업명';
COMMENT ON COLUMN ipo_calendar.ipo_date IS 'IPO 예정일';
COMMENT ON COLUMN ipo_calendar.price_range_low IS '공모가 하한선';
COMMENT ON COLUMN ipo_calendar.price_range_high IS '공모가 상한선';
COMMENT ON COLUMN ipo_calendar.exchange IS '상장 거래소 (NYSE, NASDAQ 등)';