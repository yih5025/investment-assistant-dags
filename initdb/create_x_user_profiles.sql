-- create_user_profiles.sql (간단 버전)
-- X API 사용자 ID 매핑용 테이블

CREATE TABLE IF NOT EXISTS x_user_profiles (
    username TEXT PRIMARY KEY,              -- saylor, elonmusk 등
    user_id TEXT NOT NULL UNIQUE,          -- 실제 X API user_id
    display_name TEXT,                      -- Michael Saylor, Elon Musk 등
    category TEXT,                          -- crypto, tech_ceo, institutional
    created_at TIMESTAMP DEFAULT NOW()
);

-- 기본 인덱스
CREATE INDEX IF NOT EXISTS idx_user_profiles_category ON x_user_profiles(category);   