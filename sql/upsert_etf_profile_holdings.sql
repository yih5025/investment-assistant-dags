-- dags/sql/upsert_etf_profile_holdings.sql
-- Alpha Vantage API 응답 전체를 그대로 저장

INSERT INTO etf_profile_holdings (
    symbol,
    net_assets,
    net_expense_ratio,
    portfolio_turnover,
    dividend_yield,
    inception_date,
    leveraged,
    sectors,
    holdings,
    collected_at,
    updated_at
) VALUES (
    %(symbol)s,
    %(net_assets)s,
    %(net_expense_ratio)s,
    %(portfolio_turnover)s,
    %(dividend_yield)s,
    %(inception_date)s,
    %(leveraged)s,
    %(sectors)s,
    %(holdings)s,
    NOW(),
    NOW()
)
ON CONFLICT (symbol) 
DO UPDATE SET 
    net_assets = EXCLUDED.net_assets,
    net_expense_ratio = EXCLUDED.net_expense_ratio,
    portfolio_turnover = EXCLUDED.portfolio_turnover,
    dividend_yield = EXCLUDED.dividend_yield,
    inception_date = EXCLUDED.inception_date,
    leveraged = EXCLUDED.leveraged,
    sectors = EXCLUDED.sectors,
    holdings = EXCLUDED.holdings,
    updated_at = NOW();