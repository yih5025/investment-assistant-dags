INSERT INTO earnings_history (
    symbol, report_date, eps_estimate, eps_actual, surprise_pct,
    price_before_close, price_open, price_close,
    gap_pct, move_pct, updated_at
) VALUES (
    %(symbol)s, %(report_date)s, %(eps_estimate)s, %(eps_actual)s, %(surprise_pct)s,
    %(price_before_close)s, %(price_open)s, %(price_close)s,
    %(gap_pct)s, %(move_pct)s, NOW()
)
ON CONFLICT (symbol, report_date) 
DO UPDATE SET
    eps_estimate = EXCLUDED.eps_estimate,
    eps_actual = EXCLUDED.eps_actual,
    surprise_pct = EXCLUDED.surprise_pct,
    price_before_close = EXCLUDED.price_before_close,
    price_open = EXCLUDED.price_open,
    price_close = EXCLUDED.price_close,
    gap_pct = EXCLUDED.gap_pct,
    move_pct = EXCLUDED.move_pct,
    updated_at = NOW();