INSERT INTO coingecko_derivatives (
    batch_id, market, symbol, index_id, 
    price, price_percentage_change_24h, index_price,
    contract_type, basis, spread, funding_rate,
    open_interest_usd, volume_24h_usd,
    last_traded_at, expired_at, collected_at, updated_at
) VALUES (
    %(batch_id)s, %(market)s, %(symbol)s, %(index_id)s,
    %(price)s, %(price_percentage_change_24h)s, %(index_price)s,
    %(contract_type)s, %(basis)s, %(spread)s, %(funding_rate)s,
    %(open_interest_usd)s, %(volume_24h_usd)s,
    %(last_traded_at)s, %(expired_at)s, %(collected_at)s, CURRENT_TIMESTAMP
);