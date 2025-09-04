INSERT INTO coingecko_tickers_bithumb (
    market_code, coingecko_id, symbol, coin_name,
    base, target, exchange_name, exchange_id,
    last_price, volume_24h, converted_last_usd, converted_volume_usd,
    trust_score, bid_ask_spread_percentage,
    timestamp, last_traded_at, last_fetch_at,
    is_anomaly, is_stale, trade_url, coin_mcap_usd,
    match_method, market_cap_rank
) VALUES (
    %(market_code)s, %(coingecko_id)s, %(symbol)s, %(coin_name)s,
    %(base)s, %(target)s, %(exchange_name)s, %(exchange_id)s,
    %(last_price)s, %(volume_24h)s, %(converted_last_usd)s, %(converted_volume_usd)s,
    %(trust_score)s, %(bid_ask_spread_percentage)s,
    %(timestamp)s, %(last_traded_at)s, %(last_fetch_at)s,
    %(is_anomaly)s, %(is_stale)s, %(trade_url)s, %(coin_mcap_usd)s,
    %(match_method)s, %(market_cap_rank)s
)
ON CONFLICT (coingecko_id, exchange_id, COALESCE(timestamp, '1970-01-01'::timestamp))
DO UPDATE SET
    market_code = EXCLUDED.market_code,
    symbol = EXCLUDED.symbol,
    coin_name = EXCLUDED.coin_name,
    base = EXCLUDED.base,
    target = EXCLUDED.target,
    exchange_name = EXCLUDED.exchange_name,
    last_price = EXCLUDED.last_price,
    volume_24h = EXCLUDED.volume_24h,
    converted_last_usd = EXCLUDED.converted_last_usd,
    converted_volume_usd = EXCLUDED.converted_volume_usd,
    trust_score = EXCLUDED.trust_score,
    bid_ask_spread_percentage = EXCLUDED.bid_ask_spread_percentage,
    last_traded_at = EXCLUDED.last_traded_at,
    last_fetch_at = EXCLUDED.last_fetch_at,
    is_anomaly = EXCLUDED.is_anomaly,
    is_stale = EXCLUDED.is_stale,
    trade_url = EXCLUDED.trade_url,
    coin_mcap_usd = EXCLUDED.coin_mcap_usd,
    match_method = EXCLUDED.match_method,
    market_cap_rank = EXCLUDED.market_cap_rank,
    updated_at = NOW();