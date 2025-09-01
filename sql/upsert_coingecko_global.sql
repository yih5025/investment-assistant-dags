INSERT INTO coingecko_global (
    active_cryptocurrencies, upcoming_icos, ongoing_icos, ended_icos, markets,
    total_market_cap_usd, total_market_cap_krw, total_market_cap_btc, total_market_cap_eth,
    total_volume_usd, total_volume_krw, total_volume_btc, total_volume_eth,
    btc_dominance, eth_dominance, bnb_dominance, xrp_dominance, ada_dominance, sol_dominance, doge_dominance,
    market_cap_change_percentage_24h_usd,
    market_cap_percentage_json, total_market_cap_json, total_volume_json,
    coingecko_updated_at, collected_at, updated_at
) VALUES (
    %(active_cryptocurrencies)s, %(upcoming_icos)s, %(ongoing_icos)s, %(ended_icos)s, %(markets)s,
    %(total_market_cap_usd)s, %(total_market_cap_krw)s, %(total_market_cap_btc)s, %(total_market_cap_eth)s,
    %(total_volume_usd)s, %(total_volume_krw)s, %(total_volume_btc)s, %(total_volume_eth)s,
    %(btc_dominance)s, %(eth_dominance)s, %(bnb_dominance)s, %(xrp_dominance)s, %(ada_dominance)s, %(sol_dominance)s, %(doge_dominance)s,
    %(market_cap_change_percentage_24h_usd)s,
    %(market_cap_percentage_json)s, %(total_market_cap_json)s, %(total_volume_json)s,
    %(coingecko_updated_at)s, %(collected_at)s, CURRENT_TIMESTAMP
);