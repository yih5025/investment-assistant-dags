INSERT INTO earnings_calendar (
    symbol, 
    company_name, 
    report_date, 
    fiscal_date_ending, 
    estimate, 
    currency,
    created_at
) VALUES (
    %(symbol)s, 
    %(company_name)s, 
    %(report_date)s, 
    %(fiscal_date_ending)s, 
    %(estimate)s, 
    %(currency)s,
    NOW()
)
ON CONFLICT (symbol, report_date)
DO UPDATE SET
    company_name = EXCLUDED.company_name,
    fiscal_date_ending = EXCLUDED.fiscal_date_ending,
    estimate = EXCLUDED.estimate,
    currency = EXCLUDED.currency;