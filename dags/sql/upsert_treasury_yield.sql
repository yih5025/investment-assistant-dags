-- upsert_treasury_yield.sql
-- DML: 국채 수익률 데이터 upsert 처리

INSERT INTO treasury_yield (
    date, maturity, interval_type, yield_rate, created_at, updated_at
) VALUES (
    %(date)s, %(maturity)s, %(interval_type)s, %(yield_rate)s, NOW(), NOW()
)
ON CONFLICT (date, maturity, interval_type)
DO UPDATE SET
    yield_rate = EXCLUDED.yield_rate,
    updated_at = NOW();