-- 기존 데이터가 있다면 보존하되, 구조가 다르면 DROP 후 다시 생성하는 것이 깔끔함 (초기 개발 단계 가정)
CREATE TABLE IF NOT EXISTS earnings_history (
    symbol VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,          -- 실적 발표일
    
    -- 실적 수치 (컨센서스 vs 실제)
    eps_estimate NUMERIC,
    eps_actual NUMERIC,
    surprise_pct NUMERIC,               -- 서프라이즈 비율
    
    -- 주가 반응 (핵심 데이터)
    price_before_close NUMERIC,         -- 발표 전일 종가
    price_open NUMERIC,                 -- 반응 후 시가 (Gap 확인용)
    price_close NUMERIC,                -- 반응 후 종가
    
    -- 변동률 (미리 계산하여 저장)
    gap_pct NUMERIC,                    -- 갭 상승률 (%)
    move_pct NUMERIC,                   -- 종가 변동률 (%)
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (symbol, report_date)
);

-- 빠른 조회를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_earnings_history_symbol ON earnings_history(symbol);
CREATE INDEX IF NOT EXISTS idx_earnings_history_report_date ON earnings_history(report_date);