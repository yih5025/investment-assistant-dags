-- create_balance_sheet.sql
CREATE TABLE IF NOT EXISTS balance_sheet (
    symbol                              TEXT NOT NULL,
    fiscalDateEnding                   DATE NOT NULL,
    reportedCurrency                   TEXT,
    totalAssets                        NUMERIC,
    totalCurrentAssets                 NUMERIC,
    cashAndCashEquivalentsAtCarryingValue NUMERIC,
    cashAndShortTermInvestments        NUMERIC,
    inventory                          NUMERIC,
    currentNetReceivables              NUMERIC,
    totalNonCurrentAssets              NUMERIC,
    propertyPlantEquipment             NUMERIC,
    accumulatedDepreciationAmortizationPPE NUMERIC,
    intangibleAssets                   NUMERIC,
    intangibleAssetsExcludingGoodwill  NUMERIC,
    goodwill                           NUMERIC,
    investments                        NUMERIC,
    longTermInvestments                NUMERIC,
    shortTermInvestments               NUMERIC,
    otherCurrentAssets                 NUMERIC,
    otherNonCurrentAssets              NUMERIC,
    totalLiabilities                   NUMERIC,
    totalCurrentLiabilities            NUMERIC,
    currentAccountsPayable             NUMERIC,
    deferredRevenue                    NUMERIC,
    currentDebt                        NUMERIC,
    shortTermDebt                      NUMERIC,
    totalNonCurrentLiabilities         NUMERIC,
    capitalLeaseObligations            NUMERIC,
    longTermDebt                       NUMERIC,
    currentLongTermDebt                NUMERIC,
    longTermDebtNoncurrent             NUMERIC,
    shortLongTermDebtTotal             NUMERIC,
    otherCurrentLiabilities            NUMERIC,
    otherNonCurrentLiabilities         NUMERIC,
    totalShareholderEquity             NUMERIC,
    treasuryStock                      NUMERIC,
    retainedEarnings                   NUMERIC,
    commonStock                        NUMERIC,
    commonStockSharesOutstanding       NUMERIC,
    fetched_at                         TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (symbol, fiscalDateEnding)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol ON balance_sheet(symbol);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_fiscal_date ON balance_sheet(fiscalDateEnding);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_fetched ON balance_sheet(fetched_at);