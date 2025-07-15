CREATE TABLE IF NOT EXISTS public.market_news_finnhub (
  category     TEXT        NOT NULL,
  news_id      BIGINT      NOT NULL,
  datetime     TIMESTAMP   NOT NULL,
  headline     TEXT        NOT NULL,
  image        TEXT,
  related      TEXT,
  source       TEXT,
  summary      TEXT,
  url          TEXT,
  fetched_at   TIMESTAMP   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (category, news_id)
);