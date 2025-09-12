-- initdb/create_sp500_earnings_calendar.sql

-- 메인 캘린더 테이블
CREATE TABLE IF NOT EXISTS sp500_earnings_calendar (
    id SERIAL PRIMARY KEY,
    
    -- 기본 실적 정보 (earnings_calendar에서)
    symbol VARCHAR(10) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    fiscal_date_ending DATE,
    estimate DECIMAL(10,4),
    currency VARCHAR(3) DEFAULT 'USD',
    
    -- 기업 정보 (sp500_companies에서)
    gics_sector VARCHAR(100),
    gics_sub_industry VARCHAR(200),
    headquarters VARCHAR(255),
    
    -- 생성된 이벤트 정보
    event_type VARCHAR(50) DEFAULT 'earnings_report',
    event_title VARCHAR(500),
    event_description TEXT,
    
    -- 뉴스 통계
    total_news_count INTEGER DEFAULT 0,
    forecast_news_count INTEGER DEFAULT 0,    -- 예상 구간 뉴스 (-14~-1일)
    reaction_news_count INTEGER DEFAULT 0,    -- 반응 구간 뉴스 (+1~+7일)

    -- 메타데이터
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- 고유 제약조건
    CONSTRAINT unique_symbol_report_date UNIQUE (symbol, report_date)
);

-- 뉴스 상세 테이블
CREATE TABLE IF NOT EXISTS sp500_earnings_news (
    id SERIAL PRIMARY KEY,
    calendar_id INTEGER REFERENCES sp500_earnings_calendar(id) ON DELETE CASCADE,
    
    -- 뉴스 기본 정보
    source_table VARCHAR(50) NOT NULL, -- 'earnings_news_finnhub', 'company_news', 'market_news', 'market_news_sentiment'
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    source VARCHAR(100),
    
    -- 날짜 및 섹션 정보
    published_at TIMESTAMP NOT NULL,
    news_section VARCHAR(20) NOT NULL, -- 'forecast', 'reaction'
    days_from_earnings INTEGER, -- 실적일 기준 상대일수 (-14 ~ +7)
    
    -- 메타데이터
    fetched_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 중복 방지 (같은 캘린더 내에서 URL 중복 불가)
    CONSTRAINT unique_calendar_url UNIQUE (calendar_id, url)
);

-- 인덱스 생성 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_sp500_earnings_symbol ON sp500_earnings_calendar(symbol);
CREATE INDEX IF NOT EXISTS idx_sp500_earnings_report_date ON sp500_earnings_calendar(report_date);
CREATE INDEX IF NOT EXISTS idx_sp500_earnings_sector ON sp500_earnings_calendar(gics_sector);
CREATE INDEX IF NOT EXISTS idx_sp500_earnings_updated ON sp500_earnings_calendar(updated_at);

-- 뉴스 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_sp500_news_calendar_id ON sp500_earnings_news(calendar_id);
CREATE INDEX IF NOT EXISTS idx_sp500_news_section ON sp500_earnings_news(news_section);
CREATE INDEX IF NOT EXISTS idx_sp500_news_published ON sp500_earnings_news(published_at);
CREATE INDEX IF NOT EXISTS idx_sp500_news_source_table ON sp500_earnings_news(source_table);

-- JSONB 인덱스 제거 (key_themes 컬럼 없음)

-- 테이블 설명 (문서화)
COMMENT ON TABLE sp500_earnings_calendar IS 'SP500 기업 실적 발표 일정과 관련 뉴스 통합 캘린더';