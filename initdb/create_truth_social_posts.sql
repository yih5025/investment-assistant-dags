-- Truth Social 포스트 데이터 테이블 생성
-- 트럼프, 백악관, DonaldJTrumpJr 계정 데이터 저장용

CREATE TABLE IF NOT EXISTS truth_social_posts (
    -- 🔑 기본 식별 정보
    id TEXT PRIMARY KEY,                           -- 포스트 고유 ID
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,  -- 작성 시간 (UTC)
    
    -- 👤 계정 정보
    username TEXT NOT NULL,                        -- realDonaldTrump, WhiteHouse, DonaldJTrumpJr
    account_id TEXT NOT NULL,                      -- 계정 고유 ID
    display_name TEXT,                             -- Donald J. Trump, The White House
    verified BOOLEAN DEFAULT FALSE,                -- 인증 계정 여부
    
    -- 📝 포스트 내용
    content TEXT NOT NULL,                         -- HTML 포함 원본 내용
    clean_content TEXT,                            -- HTML 태그 제거된 텍스트
    language TEXT,                                 -- 언어 코드 (en, null 등)
    
    -- 📊 참여 통계
    replies_count INTEGER DEFAULT 0,              -- 댓글 수
    reblogs_count INTEGER DEFAULT 0,              -- 리포스트 수  
    favourites_count INTEGER DEFAULT 0,           -- 좋아요 수
    upvotes_count INTEGER DEFAULT 0,              -- 업보트 수
    downvotes_count INTEGER DEFAULT 0,            -- 다운보트 수
    
    -- 🔗 링크 및 미디어
    url TEXT,                                      -- 포스트 URL
    uri TEXT,                                      -- 포스트 URI
    has_media BOOLEAN DEFAULT FALSE,               -- 미디어 첨부 여부
    media_count INTEGER DEFAULT 0,                -- 미디어 개수
    media_attachments JSONB,                      -- 미디어 첨부 파일 정보
    
    -- 🏷️ 태그 및 멘션
    tags JSONB DEFAULT '[]'::jsonb,               -- 해시태그 배열
    mentions JSONB DEFAULT '[]'::jsonb,           -- 멘션 배열
    has_tags BOOLEAN DEFAULT FALSE,                -- 해시태그 존재 여부
    has_mentions BOOLEAN DEFAULT FALSE,            -- 멘션 존재 여부
    
    -- 🔗 카드 정보 (링크 미리보기)
    card_url TEXT,                                 -- 카드 링크 URL
    card_title TEXT,                              -- 카드 제목
    card_description TEXT,                        -- 카드 설명
    card_image TEXT,                              -- 카드 이미지 URL
    
    -- 📋 게시글 속성
    visibility TEXT DEFAULT 'public',             -- 공개 범위
    sensitive BOOLEAN DEFAULT FALSE,               -- 민감 콘텐츠 여부
    spoiler_text TEXT,                            -- 스포일러 텍스트
    in_reply_to_id TEXT,                          -- 답글 대상 ID
    quote_id TEXT,                                -- 인용 포스트 ID
    
    -- 🏛️ 계정 분류 (투자 분석용)
    account_type TEXT DEFAULT 'individual',       -- government, individual, business
    market_influence INTEGER DEFAULT 0,           -- 시장 영향력 점수 (0-10)
    
    -- 🕒 메타데이터
    collected_at TIMESTAMP DEFAULT NOW(),         -- 수집 시간
    updated_at TIMESTAMP DEFAULT NOW()            -- 업데이트 시간
);

-- 📊 인덱스 생성 (성능 최적화)

-- 기본 조회용 인덱스
CREATE INDEX IF NOT EXISTS idx_truth_posts_username ON truth_social_posts(username);
CREATE INDEX IF NOT EXISTS idx_truth_posts_created_at ON truth_social_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_truth_posts_collected_at ON truth_social_posts(collected_at DESC);

-- 계정별 시간순 조회 (가장 자주 사용)
CREATE INDEX IF NOT EXISTS idx_truth_posts_username_created ON truth_social_posts(username, created_at DESC);

-- 시장 분석용 인덱스
CREATE INDEX IF NOT EXISTS idx_truth_posts_market_influence ON truth_social_posts(market_influence DESC) 
WHERE market_influence > 0;

-- 트럼프 전용 고속 인덱스 (가장 중요한 계정)
CREATE INDEX IF NOT EXISTS idx_truth_posts_trump ON truth_social_posts(created_at DESC) 
WHERE username = 'realDonaldTrump';

-- 정부 계정 인덱스
CREATE INDEX IF NOT EXISTS idx_truth_posts_government ON truth_social_posts(created_at DESC) 
WHERE account_type = 'government';

-- 미디어 포함 포스트 인덱스
CREATE INDEX IF NOT EXISTS idx_truth_posts_with_media ON truth_social_posts(created_at DESC) 
WHERE has_media = TRUE;

-- 전문 검색용 인덱스 (내용 검색)
CREATE INDEX IF NOT EXISTS idx_truth_posts_content_search ON truth_social_posts 
USING gin(to_tsvector('english', clean_content));

-- 해시태그 검색용 인덱스
CREATE INDEX IF NOT EXISTS idx_truth_posts_tags_gin ON truth_social_posts USING gin(tags);

-- 📋 코멘트 추가
COMMENT ON TABLE truth_social_posts IS '트럼프, 백악관, DonaldJTrumpJr Truth Social 포스트 데이터';
COMMENT ON COLUMN truth_social_posts.id IS '포스트 고유 ID (Truth Social 내부 ID)';
COMMENT ON COLUMN truth_social_posts.created_at IS '포스트 작성 시간 (UTC 타임존)';
COMMENT ON COLUMN truth_social_posts.username IS '계정명 (realDonaldTrump, WhiteHouse, DonaldJTrumpJr)';
COMMENT ON COLUMN truth_social_posts.clean_content IS 'HTML 태그 제거된 순수 텍스트';
COMMENT ON COLUMN truth_social_posts.market_influence IS '시장 영향력 점수: 0(무관) ~ 10(최고)';
COMMENT ON COLUMN truth_social_posts.account_type IS '계정 유형: government(정부), individual(개인), business(기업)';

-- 🎯 계정별 기본 market_influence 설정
-- 이후 UPDATE 쿼리로 설정하거나 애플리케이션에서 처리
-- realDonaldTrump: 10 (최고 영향력)
-- WhiteHouse: 9 (정부 공식)  
-- DonaldJTrumpJr: 7 (높은 영향력)