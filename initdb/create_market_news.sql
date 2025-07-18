-- create_market_news.sql
CREATE TABLE IF NOT EXISTS market_news (
    source          TEXT NOT NULL,
    url             TEXT NOT NULL,
    author          TEXT,
    title           TEXT NOT NULL,
    description     TEXT,
    content         TEXT,
    published_at    TIMESTAMP NOT NULL,
    fetched_at      TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (source, url)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_market_news_published ON market_news(published_at);
CREATE INDEX IF NOT EXISTS idx_market_news_source ON market_news(source);
CREATE INDEX IF NOT EXISTS idx_market_news_fetched ON market_news(fetched_at);
CREATE INDEX IF NOT EXISTS idx_market_news_title ON market_news USING gin(to_tsvector('english', title));