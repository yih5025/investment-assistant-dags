INSERT INTO institutional_holders (
    symbol, holder_name, shares_held, date_reported, 
    pct_out, value_usd, updated_at
) VALUES (
    %(symbol)s, %(holder)s, %(shares)s, %(date_reported)s, 
    %(pct_held)s, %(value)s, NOW()
)
ON CONFLICT (symbol, holder_name)
DO UPDATE SET
    shares_held = EXCLUDED.shares_held,
    date_reported = EXCLUDED.date_reported,
    pct_out = EXCLUDED.pct_out,
    value_usd = EXCLUDED.value_usd,
    updated_at = NOW();