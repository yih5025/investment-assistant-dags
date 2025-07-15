CREATE TABLE IF NOT EXISTS market_news (
    source TEXT NOT NULL,
    url TEXT NOT NULL,
    author TEXT,
    title TEXT NOT NULL,
    description TEXT,
    content TEXT,
    published_at TIMESTAMP NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, url)
);