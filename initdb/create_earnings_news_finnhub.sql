CREATE TABLE IF NOT EXISTS public.earnings_news_finnhub (
  symbol          TEXT      NOT NULL,
  report_date     DATE      NOT NULL,
  category        TEXT,
  article_id      INTEGER   NOT NULL,
  headline        TEXT,
  image           TEXT,
  related         TEXT,
  source          TEXT,
  summary         TEXT,
  url             TEXT,
  published_at    TIMESTAMP,
  fetched_at      TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY(symbol, report_date, article_id)
);
