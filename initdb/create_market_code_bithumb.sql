-- create_market_code_bithumb.sql  
CREATE TABLE IF NOT EXISTS market_code_bithumb (
    market_code         TEXT PRIMARY KEY,
    korean_name         TEXT NOT NULL,
    english_name        TEXT NOT NULL,
    market_warning      TEXT
);