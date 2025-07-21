-- dags/sql/upsert_inflation.sql
-- 인플레이션 데이터 Upsert

INSERT INTO inflation (
    date, 
    inflation_rate, 
    interval_type,
    unit,
    name
) VALUES (
    %(date)s, 
    %(inflation_rate)s, 
    %(interval_type)s,
    %(unit)s,
    %(name)s
)
ON CONFLICT (date)
DO UPDATE SET
    inflation_rate = EXCLUDED.inflation_rate,
    interval_type = EXCLUDED.interval_type,
    unit = EXCLUDED.unit,
    name = EXCLUDED.name;