-- create_x_posts.sql (업데이트된 버전)
-- Secondary Token 지원을 위한 새 필드들 추가

CREATE TABLE IF NOT EXISTS x_posts (
    tweet_id TEXT PRIMARY KEY,
    author_id TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    lang TEXT DEFAULT 'en',
    source_account TEXT NOT NULL,
    
    -- === 새로 추가된 필드들 ===
    account_category TEXT DEFAULT 'core_investors',  -- crypto, tech_ceo, institutional, media, corporate
    collection_source TEXT DEFAULT 'primary_token',  -- primary_token, secondary_token
    
    -- 참여도 지표
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    like_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    bookmark_count INTEGER DEFAULT 0,
    impression_count INTEGER DEFAULT 0,
    
    -- 엔티티 정보 (JSON 형태)
    hashtags JSONB,
    mentions JSONB,
    urls JSONB,
    cashtags JSONB,
    annotations JSONB,
    context_annotations JSONB,
    
    -- 사용자 정보
    username TEXT,
    display_name TEXT,
    user_verified BOOLEAN DEFAULT FALSE,
    user_followers_count INTEGER DEFAULT 0,
    user_following_count INTEGER DEFAULT 0,
    user_tweet_count INTEGER DEFAULT 0,
    
    -- 메타 정보
    edit_history_tweet_ids JSONB,
    collected_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_x_posts_source_account ON x_posts(source_account);
CREATE INDEX IF NOT EXISTS idx_x_posts_created_at ON x_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_x_posts_collected_at ON x_posts(collected_at DESC);

-- === 새로 추가된 인덱스들 ===
CREATE INDEX IF NOT EXISTS idx_x_posts_account_category ON x_posts(account_category);
CREATE INDEX IF NOT EXISTS idx_x_posts_collection_source ON x_posts(collection_source);
CREATE INDEX IF NOT EXISTS idx_x_posts_category_source ON x_posts(account_category, collection_source);

-- 복합 인덱스 (카테고리별 최신 트윗 조회용)
CREATE INDEX IF NOT EXISTS idx_x_posts_category_created_at ON x_posts(account_category, created_at DESC);

-- 전문 검색 인덱스 (트윗 내용 검색용)
CREATE INDEX IF NOT EXISTS idx_x_posts_text_search ON x_posts USING gin(to_tsvector('english', text));

-- 참여도 기반 인덱스 (인기 트윗 조회용)
CREATE INDEX IF NOT EXISTS idx_x_posts_engagement ON x_posts((like_count + retweet_count + reply_count) DESC);

-- 사용자별 최신 트윗 조회용
CREATE INDEX IF NOT EXISTS idx_x_posts_user_created_at ON x_posts(source_account, created_at DESC);