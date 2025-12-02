INSERT INTO insider_trading (
    symbol, insider_name, position, transaction_date, 
    transaction_type, shares_transacted, price, shares_owned_after, updated_at
) VALUES (
    %(symbol)s, %(insider)s, %(position)s, %(date)s, 
    %(type)s, %(shares)s, %(price)s, %(shares_after)s, NOW()
)
ON CONFLICT (symbol, insider_name, transaction_date, transaction_type)
DO NOTHING; -- 과거 내역은 변하지 않으므로 중복이면 무시