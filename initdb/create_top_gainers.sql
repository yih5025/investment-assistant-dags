-- 테이블 생성 SQL (initdb 폴더) - batch_id 버전
CREATE TABLE IF NOT EXISTS top_gainers (
    batch_id BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    category VARCHAR(20) NOT NULL,
    rank_position INTEGER,
    price DECIMAL(10,4),
    change_amount DECIMAL(10,4),
    change_percentage VARCHAR(20),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 복합 Primary Key: 한 배치 내에서 심볼+카테고리 유일성 보장
    PRIMARY KEY (batch_id, symbol, category)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_top_gainers_batch_id ON top_gainers(batch_id);
CREATE INDEX IF NOT EXISTS idx_top_gainers_symbol ON top_gainers(symbol);
CREATE INDEX IF NOT EXISTS idx_top_gainers_category ON top_gainers(category);
CREATE INDEX IF NOT EXISTS idx_top_gainers_updated ON top_gainers(last_updated);

-- 최신 배치 조회용 복합 인덱스 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_top_gainers_batch_category_rank 
ON top_gainers(batch_id, category, rank_position);