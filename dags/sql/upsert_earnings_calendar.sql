INSERT INTO earnings_calendar (
    symbol, 
    name, 
    report_date, 
    fiscal_date_ending, 
    estimate, 
    currency,
    created_at,
    updated_at
) VALUES (
    %(symbol)s, 
    %(name)s, 
    %(report_date)s, 
    %(fiscal_date_ending)s, 
    %(estimate)s, 
    %(currency)s,
    NOW(),
    NOW()
)
ON CONFLICT (symbol, report_date)
DO UPDATE SET
    name = EXCLUDED.name,
    fiscal_date_ending = EXCLUDED.fiscal_date_ending,
    estimate = EXCLUDED.estimate,
    currency = EXCLUDED.currency,
    updated_at = NOW();