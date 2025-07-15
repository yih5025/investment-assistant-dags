INSERT INTO public.earnings_calendar
  (symbol, company_name, report_date, fiscal_date_ending, estimate, currency)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, report_date) DO UPDATE
  SET fiscal_date_ending = EXCLUDED.fiscal_date_ending,
      estimate           = EXCLUDED.estimate,
      currency           = EXCLUDED.currency,
      fetched_at         = NOW();
