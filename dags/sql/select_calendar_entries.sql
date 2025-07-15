SELECT symbol, company_name, report_date
FROM public.earnings_calendar
WHERE fetched_at >= date_trunc('month', current_date);