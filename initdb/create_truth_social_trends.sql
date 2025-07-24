-- Truth Social 트렌딩 포스트 테이블 생성

CREATE TABLE IF NOT EXISTS truth_social_trends (
    -- 🔑 기본 식별 정보
    id TEXT PRIMARY KEY,                           -- 포스트 고유 ID
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,  -- 작성 시간
    
    -- 👤 계정 정보
    username TEXT NOT NULL,                        -- 작성자 계정명
    account_id TEXT NOT NULL,                      -- 계정 고유 ID
    display_name TEXT,                             -- 표시명
    
    -- 📝 포스트 내용
    content TEXT NOT NULL,                         -- 포스트 내용
    clean_content TEXT,                            -- HTML 태그 제거된 텍스트
    language TEXT,                                 -- 언어 코드
    
    -- 📊 참여 통계
    replies_count INTEGER DEFAULT 0,              -- 댓글 수
    reblogs_count INTEGER DEFAULT 0,              -- 리포스트 수
    favourites_count INTEGER DEFAULT 0,           -- 좋아요 수
    upvotes_count INTEGER DEFAULT 0,              -- 업보트 수
    downvotes_count INTEGER DEFAULT 0,            -- 다운보트 수
    
    -- 🔗 링크 정보
    url TEXT,                                      -- 포스트 URL
    uri TEXT,                                      -- 포스트 URI
    
    -- 🏷️ 태그 및 멘션
    tags JSONB DEFAULT '[]'::jsonb,               -- 해시태그 배열
    mentions JSONB DEFAULT '[]'::jsonb,           -- 멘션 배열
    
    -- 📋 게시글 속성
    visibility TEXT DEFAULT 'public',             -- 공개 범위
    sensitive BOOLEAN DEFAULT FALSE,               -- 민감 콘텐츠 여부
    in_reply_to_id TEXT,                          -- 답글 대상 ID
    
    -- 🔥 트렌딩 관련
    trend_rank INTEGER,                           -- 트렌딩 순위 (수집 시점 기준)
    trend_score DECIMAL(10,2),                    -- 트렌딩 점수 (계산된 값)
    
    -- 🕒 메타데이터
    collected_at TIMESTAMP DEFAULT NOW(),         -- 수집 시간 (트렌딩 시점)
    updated_at TIMESTAMP DEFAULT NOW()            -- 업데이트 시간
);

-- 📊 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_trends_collected_at ON truth_social_trends(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_trends_created_at ON truth_social_trends(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trends_rank ON truth_social_trends(trend_rank);
CREATE INDEX IF NOT EXISTS idx_trends_score ON truth_social_trends(trend_score DESC);
CREATE INDEX IF NOT EXISTS idx_trends_username ON truth_social_trends(username);

-- 전문 검색용 인덱스
CREATE INDEX IF NOT EXISTS idx_trends_content_search ON truth_social_trends 
USING gin(to_tsvector('english', clean_content));

-- 📋 코멘트
COMMENT ON TABLE truth_social_trends IS 'Truth Social 트렌딩 포스트 데이터';
COMMENT ON COLUMN truth_social_trends.trend_rank IS '수집 시점의 트렌딩 순위';
COMMENT ON COLUMN truth_social_trends.collected_at IS '트렌딩으로 수집된 시간';