-- dags/sql/upsert_ipo_calendar.sql
-- IPO 캘린더 데이터 Upsert (중복 시 업데이트)

INSERT INTO ipo_calendar (
    symbol,
    company_name,
    ipo_date,
    price_range_low,
    price_range_high,
    currency,
    exchange,
    fetched_at,
    created_at,
    updated_at
) VALUES (
    %(symbol)s,
    %(company_name)s,
    %(ipo_date)s,
    %(price_range_low)s,
    %(price_range_high)s,
    %(currency)s,
    %(exchange)s,
    NOW(),
    NOW(),
    NOW()
)
ON CONFLICT (symbol, ipo_date)
DO UPDATE SET
    company_name = EXCLUDED.company_name,
    price_range_low = EXCLUDED.price_range_low,
    price_range_high = EXCLUDED.price_range_high,
    currency = EXCLUDED.currency,
    exchange = EXCLUDED.exchange,
    fetched_at = NOW(),
    updated_at = NOW();