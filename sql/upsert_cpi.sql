-- dags/sql/upsert_cpi.sql
-- 소비자물가지수 데이터 Upsert

INSERT INTO cpi (
    date, 
    cpi_value, 
    interval_type,
    unit,
    name
) VALUES (
    %(date)s, 
    %(cpi_value)s, 
    %(interval_type)s,
    %(unit)s,
    %(name)s
)
ON CONFLICT (date)
DO UPDATE SET
    cpi_value = EXCLUDED.cpi_value,
    interval_type = EXCLUDED.interval_type,
    unit = EXCLUDED.unit,
    name = EXCLUDED.name;