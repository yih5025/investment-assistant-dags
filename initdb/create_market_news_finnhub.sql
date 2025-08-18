-- create_market_news_finnhub.sql
CREATE TABLE IF NOT EXISTS market_news_finnhub (
    category        TEXT NOT NULL,
    news_id         BIGINT NOT NULL,
    datetime        TIMESTAMP NOT NULL,
    headline        TEXT,
    image           TEXT,
    related         TEXT,
    source          TEXT,
    summary         TEXT,
    url             TEXT,
    fetched_at      TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (category, news_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_market_news_category ON market_news_finnhub(category);
CREATE INDEX IF NOT EXISTS idx_market_news_datetime ON market_news_finnhub(datetime);
CREATE INDEX IF NOT EXISTS idx_market_news_source ON market_news_finnhub(source);
CREATE INDEX IF NOT EXISTS idx_market_news_fetched ON market_news_finnhub(fetched_at);