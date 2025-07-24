-- Truth Social 트렌딩 해시태그 테이블 생성

CREATE TABLE IF NOT EXISTS truth_social_tags (
    -- 🔑 기본 식별 정보  
    name TEXT NOT NULL,                           -- 해시태그 이름 (Obama, Trump 등)
    collected_date DATE NOT NULL,                 -- 수집 날짜
    
    -- 🔗 링크 정보
    url TEXT,                                     -- 해시태그 URL
    
    -- 📊 사용량 통계 (당일)
    total_uses INTEGER DEFAULT 0,                -- 총 사용 횟수
    total_accounts INTEGER DEFAULT 0,            -- 사용한 계정 수
    recent_statuses_count INTEGER DEFAULT 0,     -- 최근 포스트 수
    
    -- 📈 주간 히스토리 (7일간)
    history_data JSONB,                          -- 전체 히스토리 데이터
    day_0_uses INTEGER DEFAULT 0,               -- 오늘 사용량
    day_1_uses INTEGER DEFAULT 0,               -- 1일 전 사용량
    day_2_uses INTEGER DEFAULT 0,               -- 2일 전 사용량
    day_3_uses INTEGER DEFAULT 0,               -- 3일 전 사용량
    day_4_uses INTEGER DEFAULT 0,               -- 4일 전 사용량
    day_5_uses INTEGER DEFAULT 0,               -- 5일 전 사용량
    day_6_uses INTEGER DEFAULT 0,               -- 6일 전 사용량
    
    -- 📊 계산된 메트릭
    trend_score DECIMAL(10,2),                   -- 트렌드 점수 (상승세 계산)
    growth_rate DECIMAL(5,2),                    -- 성장률 (전일 대비 %)
    weekly_average DECIMAL(10,2),                -- 주간 평균 사용량
    
    -- 🏷️ 분류
    tag_category TEXT,                           -- 카테고리 (politics, economy, etc)
    market_relevance INTEGER DEFAULT 0,          -- 시장 관련도 (0-10)
    
    -- 🕒 메타데이터
    collected_at TIMESTAMP DEFAULT NOW(),        -- 수집 시간
    updated_at TIMESTAMP DEFAULT NOW(),          -- 업데이트 시간
    
    -- 복합 Primary Key (해시태그명 + 날짜)
    PRIMARY KEY (name, collected_date)
);

-- 📊 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_tags_collected_date ON truth_social_tags(collected_date DESC);
CREATE INDEX IF NOT EXISTS idx_tags_name ON truth_social_tags(name);
CREATE INDEX IF NOT EXISTS idx_tags_total_uses ON truth_social_tags(total_uses DESC);
CREATE INDEX IF NOT EXISTS idx_tags_trend_score ON truth_social_tags(trend_score DESC);
CREATE INDEX IF NOT EXISTS idx_tags_growth_rate ON truth_social_tags(growth_rate DESC);
CREATE INDEX IF NOT EXISTS idx_tags_market_relevance ON truth_social_tags(market_relevance DESC) 
WHERE market_relevance > 0;

-- 특정 날짜의 인기 태그 조회용
CREATE INDEX IF NOT EXISTS idx_tags_date_uses ON truth_social_tags(collected_date, total_uses DESC);

-- 📋 코멘트
COMMENT ON TABLE truth_social_tags IS 'Truth Social 트렌딩 해시태그 일별 통계';
COMMENT ON COLUMN truth_social_tags.name IS '해시태그 이름 (# 제외)';
COMMENT ON COLUMN truth_social_tags.collected_date IS '데이터 수집 날짜 (일별 스냅샷)';
COMMENT ON COLUMN truth_social_tags.trend_score IS '트렌드 점수 (상승세, 지속성 고려)';
COMMENT ON COLUMN truth_social_tags.market_relevance IS '투자/시장 관련도 점수 0-10';