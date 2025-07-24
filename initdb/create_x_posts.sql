-- create_x_posts.sql
-- X API 트윗 데이터 저장용 테이블 (실제 API 구조 기반)

CREATE TABLE IF NOT EXISTS x_posts (
    -- 기본 트윗 정보 (항상 존재)
    tweet_id TEXT PRIMARY KEY,
    author_id TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    lang TEXT DEFAULT 'en',
    
    -- 참여도 지표 (public_metrics - 항상 존재)
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    like_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    bookmark_count INTEGER DEFAULT 0,
    impression_count INTEGER DEFAULT 0,
    
    -- 엔티티 정보 (entities - 있을 때만)
    hashtags JSONB,              -- entities.hashtags
    mentions JSONB,              -- entities.mentions  
    urls JSONB,                  -- entities.urls
    cashtags JSONB,              -- entities.cashtags ($TSLA 등)
    annotations JSONB,           -- entities.annotations
    
    -- X 자동 분류 (context_annotations - 핵심!)
    context_annotations JSONB,   -- 전체 context_annotations 배열
    
    -- 사용자 정보 (includes.users에서 추출)
    username TEXT,
    display_name TEXT,
    user_verified BOOLEAN DEFAULT FALSE,
    user_followers_count INTEGER DEFAULT 0,
    user_following_count INTEGER DEFAULT 0,
    user_tweet_count INTEGER DEFAULT 0,
    
    -- 편집 정보
    edit_history_tweet_ids JSONB,
    
    -- 메타 정보
    source_account TEXT NOT NULL,       -- 수집한 계정명
    collected_at TIMESTAMP DEFAULT NOW(),
    
    -- 제약조건
    CONSTRAINT valid_tweet_id CHECK (LENGTH(tweet_id) > 0),
    CONSTRAINT valid_source_account CHECK (LENGTH(source_account) > 0)
);

-- 성능 최적화 인덱스들
CREATE INDEX IF NOT EXISTS idx_x_posts_author_id ON x_posts(author_id);
CREATE INDEX IF NOT EXISTS idx_x_posts_username ON x_posts(username);
CREATE INDEX IF NOT EXISTS idx_x_posts_created_at ON x_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_x_posts_source_account ON x_posts(source_account);
CREATE INDEX IF NOT EXISTS idx_x_posts_collected_at ON x_posts(collected_at DESC);

-- 투자 분석용 인덱스
CREATE INDEX IF NOT EXISTS idx_x_posts_financial ON x_posts(created_at DESC) 
    WHERE has_financial_context = TRUE;
CREATE INDEX IF NOT EXISTS idx_x_posts_high_impact ON x_posts(market_impact_score DESC, created_at DESC) 
    WHERE market_impact_score >= 5;
CREATE INDEX IF NOT EXISTS idx_x_posts_tesla ON x_posts(created_at DESC) 
    WHERE has_tesla_context = TRUE;

-- 참여도 기반 인덱스 (인기 트윗 조회용)
CREATE INDEX IF NOT EXISTS idx_x_posts_engagement ON x_posts(
    (like_count + retweet_count + reply_count + quote_count) DESC, 
    created_at DESC
);

-- 전문 검색용 인덱스 (트윗 내용 검색)
CREATE INDEX IF NOT EXISTS idx_x_posts_text_search ON x_posts 
    USING gin(to_tsvector('english', text));

-- 계정별 최신 트윗 조회 최적화
CREATE INDEX IF NOT EXISTS idx_x_posts_account_recent ON x_posts(source_account, created_at DESC);

-- 중복 방지 제약조건 (같은 트윗이 중복 저장되지 않도록)
CREATE UNIQUE INDEX IF NOT EXISTS idx_x_posts_unique_id ON x_posts(tweet_id);