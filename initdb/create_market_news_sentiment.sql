CREATE TABLE IF NOT EXISTS market_news_sentiment (
    batch_id BIGINT NOT NULL,
    
    -- 기사 정보
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    time_published TIMESTAMP NOT NULL,
    authors TEXT,
    summary TEXT,
    source TEXT,
    
    -- 감성 분석
    overall_sentiment_score DECIMAL(5,4),
    overall_sentiment_label VARCHAR(20),
    
    -- 관련 종목들 (JSON 형태로 저장)
    ticker_sentiment JSONB,
    
    -- 주제들 (JSON 형태로 저장)  
    topics JSONB,
    
    -- 메타데이터
    query_type VARCHAR(50) NOT NULL,  -- 'daily_market', 'fed_news', 'crypto_trend' 등
    query_params TEXT,  -- 실제 API 쿼리 파라미터
    
    -- 시스템 필드
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 복합 Primary Key (배치 + URL로 중복 방지)
    PRIMARY KEY (batch_id, url)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_market_news_batch_id ON market_news_sentiment(batch_id);
CREATE INDEX IF NOT EXISTS idx_market_news_time_published ON market_news_sentiment(time_published);
CREATE INDEX IF NOT EXISTS idx_market_news_sentiment_score ON market_news_sentiment(overall_sentiment_score);
CREATE INDEX IF NOT EXISTS idx_market_news_query_type ON market_news_sentiment(query_type);
CREATE INDEX IF NOT EXISTS idx_market_news_source ON market_news_sentiment(source);

-- JSONB 인덱스 (종목 및 주제 검색용)
CREATE INDEX IF NOT EXISTS idx_market_news_ticker_sentiment ON market_news_sentiment USING GIN(ticker_sentiment);
CREATE INDEX IF NOT EXISTS idx_market_news_topics ON market_news_sentiment USING GIN(topics);