CREATE TABLE IF NOT EXISTS public.company_news (
  symbol        TEXT      NOT NULL,
  source        TEXT,
  url           TEXT      NOT NULL,
  title         TEXT,
  description   TEXT,
  content       TEXT,
  published_at  TIMESTAMP,
  fetched_at    TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY(symbol, url)
);
