-- Migration: Fix numeric overflow for percentage columns
-- Date: 2025-11-04
-- Issue: atl_change_percentage and ath_change_percentage causing numeric field overflow
-- Solution: Increase precision from DECIMAL(10,4) to DECIMAL(20,4)
-- Status: APPLIED ✓

-- 암호화폐는 최저가 대비 수조%의 상승률을 기록할 수 있으므로
-- DECIMAL(20,4)로 변경 (최대값: 9,999,999,999,999,999.9999)

-- Step 1: Drop dependent views
DROP VIEW IF EXISTS crypto_detail_with_bithumb CASCADE;

-- Step 2: Alter column types
ALTER TABLE coingecko_coin_details 
    ALTER COLUMN ath_change_percentage TYPE NUMERIC(20,4);

ALTER TABLE coingecko_coin_details 
    ALTER COLUMN atl_change_percentage TYPE NUMERIC(20,4);

-- Step 3: Recreate views
CREATE OR REPLACE VIEW crypto_detail_with_bithumb AS
SELECT 
    cd.*,
    mb.market_code as bithumb_market,
    bt.trade_price as bithumb_krw_price,
    bt.change_rate as bithumb_change_rate,
    bt.acc_trade_volume_24h as bithumb_volume_24h
FROM coingecko_coin_details cd
LEFT JOIN market_code_bithumb mb ON UPPER(cd.symbol) = UPPER(REPLACE(mb.market_code, 'KRW-', ''))
LEFT JOIN bithumb_ticker bt ON mb.market_code = bt.market
WHERE cd.market_cap_rank <= 200
ORDER BY cd.market_cap_rank;

-- 참고: 다른 percentage 컬럼들은 일반적으로 -100% ~ +수천% 범위이므로 DECIMAL(10,4)로 충분
-- price_change_percentage_24h
-- price_change_percentage_7d  
-- price_change_percentage_30d

