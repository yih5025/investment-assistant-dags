CREATE TABLE IF NOT EXISTS insider_trading (
    symbol VARCHAR(20) NOT NULL,
    insider_name VARCHAR(255),          -- 내부자 이름
    position VARCHAR(255),              -- 직위 (Director, Officer 등)
    transaction_date DATE NOT NULL,     -- 거래일
    transaction_type VARCHAR(50),       -- 매수(Buy) / 매도(Sale) 등
    shares_transacted BIGINT,           -- 거래 주식 수
    price NUMERIC,                      -- 거래 가격
    shares_owned_after BIGINT,          -- 거래 후 보유량
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 복합키: 같은 날 같은 사람이 여러 번 거래할 수도 있으므로 인덱스 관리만 함
    CONSTRAINT pk_insider_trading UNIQUE (symbol, insider_name, transaction_date, transaction_type)
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_insider_trading_symbol ON insider_trading(symbol);
CREATE INDEX IF NOT EXISTS idx_insider_trading_transaction_date ON insider_trading(transaction_date);
CREATE INDEX IF NOT EXISTS idx_insider_trading_transaction_type ON insider_trading(transaction_type);