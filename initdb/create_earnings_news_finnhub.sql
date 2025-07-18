CREATE TABLE IF NOT EXISTS public.earnings_news_finnhub (
    symbol          TEXT NOT NULL,
    report_date     DATE NOT NULL,
    category        TEXT,
    article_id      TEXT,
    headline        TEXT,
    image           TEXT,
    related         TEXT,
    source          TEXT,
    summary         TEXT,
    url             TEXT NOT NULL,
    published_at    TIMESTAMP NOT NULL,
    fetched_at      TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (symbol, report_date, url)
);

CREATE INDEX IF NOT EXISTS idx_earnings_news_symbol ON earnings_news_finnhub(symbol);
CREATE INDEX IF NOT EXISTS idx_earnings_news_report_date ON earnings_news_finnhub(report_date);
CREATE INDEX IF NOT EXISTS idx_earnings_news_published ON earnings_news_finnhub(published_at);
CREATE INDEX IF NOT EXISTS idx_earnings_news_source ON earnings_news_finnhub(source);