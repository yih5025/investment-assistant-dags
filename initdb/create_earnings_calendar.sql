CREATE TABLE IF NOT EXISTS public.earnings_calendar (
  symbol              TEXT      NOT NULL,
  company_name        TEXT,
  report_date         DATE      NOT NULL,
  fiscal_date_ending  DATE,
  estimate            NUMERIC,
  currency            TEXT,
  event_description   TEXT,
  fetched_at          TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY(symbol, report_date)
);