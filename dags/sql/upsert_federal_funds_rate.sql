-- dags/sql/upsert_federal_funds_rate.sql
-- 연방기금금리 데이터 Upsert

INSERT INTO federal_funds_rate (
    date, 
    rate, 
    interval_type,
    unit,
    name
) VALUES (
    %(date)s, 
    %(rate)s, 
    %(interval_type)s,
    %(unit)s,
    %(name)s
)
ON CONFLICT (date)
DO UPDATE SET
    rate = EXCLUDED.rate,
    interval_type = EXCLUDED.interval_type,
    unit = EXCLUDED.unit,
    name = EXCLUDED.name;