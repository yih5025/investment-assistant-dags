-- dags/sql/upsert_top_gainers.sql
-- Top gainers, losers, actively traded 데이터 저장용 SQL (batch_id 버전)

INSERT INTO top_gainers (
    batch_id,
    last_updated,
    symbol,
    category,
    rank_position,
    price,
    change_amount,
    change_percentage,
    volume,
    created_at
) VALUES (
    %(batch_id)s,
    %(last_updated)s,
    %(symbol)s,
    %(category)s,
    %(rank_position)s,
    %(price)s,
    %(change_amount)s,
    %(change_percentage)s,
    %(volume)s,
    NOW()
)
ON CONFLICT (batch_id, symbol, category)
DO UPDATE SET
    last_updated = EXCLUDED.last_updated,
    rank_position = EXCLUDED.rank_position,
    price = EXCLUDED.price,
    change_amount = EXCLUDED.change_amount,
    change_percentage = EXCLUDED.change_percentage,
    volume = EXCLUDED.volume,
    created_at = NOW();