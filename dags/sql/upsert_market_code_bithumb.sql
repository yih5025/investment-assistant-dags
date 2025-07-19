-- upsert_market_code_bithumb.sql
INSERT INTO market_code_bithumb (
    market_code,
    korean_name,
    english_name,
    market_warning
) VALUES (
    %(market_code)s,
    %(korean_name)s,
    %(english_name)s,
    %(market_warning)s
)
ON CONFLICT (market_code)
DO UPDATE SET
    korean_name = EXCLUDED.korean_name,
    english_name = EXCLUDED.english_name,
    market_warning = EXCLUDED.market_warning;