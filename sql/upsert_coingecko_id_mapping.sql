INSERT INTO coingecko_id_mapping (
    coingecko_id, symbol, name, image_url,
    current_price_usd, market_cap_usd, market_cap_rank, total_volume_usd,
    ath_usd, ath_date, atl_usd, atl_date,
    circulating_supply, total_supply, max_supply, last_updated, updated_at
) VALUES (
    %(coingecko_id)s, %(symbol)s, %(name)s, %(image_url)s,
    %(current_price_usd)s, %(market_cap_usd)s, %(market_cap_rank)s, %(total_volume_usd)s,
    %(ath_usd)s, %(ath_date)s, %(atl_usd)s, %(atl_date)s,
    %(circulating_supply)s, %(total_supply)s, %(max_supply)s, %(last_updated)s, CURRENT_TIMESTAMP
)
ON CONFLICT (coingecko_id) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    image_url = EXCLUDED.image_url,
    current_price_usd = EXCLUDED.current_price_usd,
    market_cap_usd = EXCLUDED.market_cap_usd,
    market_cap_rank = EXCLUDED.market_cap_rank,
    total_volume_usd = EXCLUDED.total_volume_usd,
    ath_usd = EXCLUDED.ath_usd,
    ath_date = EXCLUDED.ath_date,
    atl_usd = EXCLUDED.atl_usd,
    atl_date = EXCLUDED.atl_date,
    circulating_supply = EXCLUDED.circulating_supply,
    total_supply = EXCLUDED.total_supply,
    max_supply = EXCLUDED.max_supply,
    last_updated = EXCLUDED.last_updated,
    updated_at = CURRENT_TIMESTAMP;