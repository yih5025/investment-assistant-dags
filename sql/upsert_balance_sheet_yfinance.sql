INSERT INTO balance_sheet_yfinance (
    symbol, fiscal_date_ending, 
    total_assets, total_liabilities, total_equity, 
    cash_and_equivalents, total_debt, net_debt, working_capital, updated_at
) VALUES (
    %(symbol)s, %(fiscal_date_ending)s, 
    %(total_assets)s, %(total_liabilities)s, %(total_equity)s, 
    %(cash_and_equivalents)s, %(total_debt)s, %(net_debt)s, %(working_capital)s, NOW()
)
ON CONFLICT (symbol, fiscal_date_ending) 
DO UPDATE SET
    total_assets = EXCLUDED.total_assets,
    total_liabilities = EXCLUDED.total_liabilities,
    total_equity = EXCLUDED.total_equity,
    cash_and_equivalents = EXCLUDED.cash_and_equivalents,
    total_debt = EXCLUDED.total_debt,
    net_debt = EXCLUDED.net_debt,
    working_capital = EXCLUDED.working_capital,
    updated_at = NOW();