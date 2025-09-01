INSERT INTO coingecko_tickers (
    batch_id, coingecko_id, coin_symbol, coin_name, market_cap_rank,
    base_symbol, target_symbol, market_name, market_identifier, is_korean_exchange, has_trading_incentive,
    last_price, volume, converted_last_usd, converted_last_btc, converted_volume_usd,
    trust_score, bid_ask_spread_percentage, cost_to_move_up_usd, cost_to_move_down_usd,
    is_anomaly, is_stale, trade_url,
    timestamp, last_traded_at, last_fetch_at, collected_at, updated_at
) VALUES (
    %(batch_id)s, %(coingecko_id)s, %(coin_symbol)s, %(coin_name)s, %(market_cap_rank)s,
    %(base_symbol)s, %(target_symbol)s, %(market_name)s, %(market_identifier)s, %(is_korean_exchange)s, %(has_trading_incentive)s,
    %(last_price)s, %(volume)s, %(converted_last_usd)s, %(converted_last_btc)s, %(converted_volume_usd)s,
    %(trust_score)s, %(bid_ask_spread_percentage)s, %(cost_to_move_up_usd)s, %(cost_to_move_down_usd)s,
    %(is_anomaly)s, %(is_stale)s, %(trade_url)s,
    %(timestamp)s, %(last_traded_at)s, %(last_fetch_at)s, %(collected_at)s, CURRENT_TIMESTAMP
);