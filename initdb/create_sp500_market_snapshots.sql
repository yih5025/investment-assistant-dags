-- SP500 시장 스냅샷 테이블 생성
-- 변화율, 24시간 거래량 등을 주기적으로 저장하여 히스토리 분석에 활용

CREATE TABLE IF NOT EXISTS sp500_market_snapshots (
    symbol VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    current_price DECIMAL(10, 2),
    change_amount DECIMAL(10, 2),
    change_percentage DECIMAL(10, 4),
    volume BIGINT,                      -- 현재 거래량
    volume_24h BIGINT,                  -- 24시간 누적 거래량
    snapshot_time TIMESTAMP NOT NULL,   -- 스냅샷 시각
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (symbol, snapshot_time)
);

-- 인덱스 생성 (조회 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_sp500_snapshots_time ON sp500_market_snapshots(snapshot_time);
CREATE INDEX IF NOT EXISTS idx_sp500_snapshots_symbol ON sp500_market_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_sp500_snapshots_created ON sp500_market_snapshots(created_at);

-- 테이블 코멘트
COMMENT ON TABLE sp500_market_snapshots IS 'SP500 종목별 시장 스냅샷 (변화율, 24시간 거래량 등)';
COMMENT ON COLUMN sp500_market_snapshots.volume IS '현재 거래량';
COMMENT ON COLUMN sp500_market_snapshots.volume_24h IS '24시간 누적 거래량';
COMMENT ON COLUMN sp500_market_snapshots.snapshot_time IS '스냅샷 촬영 시각';

