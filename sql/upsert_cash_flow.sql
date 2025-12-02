INSERT INTO cash_flow (
    symbol, fiscal_date_ending, 
    operating_cashflow, investing_cashflow, financing_cashflow, 
    capital_expenditures, free_cash_flow, updated_at
) VALUES (
    %(symbol)s, %(fiscal_date_ending)s, 
    %(operating_cashflow)s, %(investing_cashflow)s, %(financing_cashflow)s, 
    %(capital_expenditures)s, %(free_cash_flow)s, NOW()
)
ON CONFLICT (symbol, fiscal_date_ending) 
DO UPDATE SET
    operating_cashflow = EXCLUDED.operating_cashflow,
    investing_cashflow = EXCLUDED.investing_cashflow,
    financing_cashflow = EXCLUDED.financing_cashflow,
    capital_expenditures = EXCLUDED.capital_expenditures,
    free_cash_flow = EXCLUDED.free_cash_flow,
    updated_at = NOW();