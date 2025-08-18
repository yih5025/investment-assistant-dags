CREATE TABLE IF NOT EXISTS public.earnings_news (
  symbol        TEXT      NOT NULL,
  company_name  TEXT      NOT NULL,
  report_date   DATE      NOT NULL,
  source        TEXT      NOT NULL,
  title         TEXT,
  url           TEXT,
  published_at  TIMESTAMP,
  fetched_at    TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (symbol, report_date, source, url)
);
