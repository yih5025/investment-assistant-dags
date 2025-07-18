CREATE TABLE IF NOT EXISTS public.company_news (
    symbol          TEXT NOT NULL,
    source          TEXT,
    url             TEXT NOT NULL,
    title           TEXT,
    description     TEXT,
    content         TEXT,
    published_at    TIMESTAMP NOT NULL,
    fetched_at      TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (symbol, url)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_company_news_symbol ON company_news(symbol);
CREATE INDEX IF NOT EXISTS idx_company_news_published ON company_news(published_at);
CREATE INDEX IF NOT EXISTS idx_company_news_fetched ON company_news(fetched_at);