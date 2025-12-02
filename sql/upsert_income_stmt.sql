INSERT INTO income_stmt (
    symbol, fiscal_date_ending, 
    total_revenue, gross_profit, operating_income, net_income, 
    ebitda, basic_eps, diluted_eps, updated_at
) VALUES (
    %(symbol)s, %(fiscal_date_ending)s, 
    %(total_revenue)s, %(gross_profit)s, %(operating_income)s, %(net_income)s, 
    %(ebitda)s, %(basic_eps)s, %(diluted_eps)s, NOW()
)
ON CONFLICT (symbol, fiscal_date_ending) 
DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    gross_profit = EXCLUDED.gross_profit,
    operating_income = EXCLUDED.operating_income,
    net_income = EXCLUDED.net_income,
    ebitda = EXCLUDED.ebitda,
    basic_eps = EXCLUDED.basic_eps,
    diluted_eps = EXCLUDED.diluted_eps,
    updated_at = NOW();