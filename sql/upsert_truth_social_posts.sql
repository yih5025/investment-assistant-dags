-- Truth Social 포스트 데이터 Upsert SQL
-- 중복 방지 및 업데이트 처리

INSERT INTO truth_social_posts (
    -- 기본 식별 정보
    id,
    created_at,
    
    -- 계정 정보  
    username,
    account_id,
    display_name,
    verified,
    
    -- 포스트 내용
    content,
    clean_content,
    language,
    
    -- 참여 통계
    replies_count,
    reblogs_count, 
    favourites_count,
    upvotes_count,
    downvotes_count,
    
    -- 링크 및 미디어
    url,
    uri,
    has_media,
    media_count,
    media_attachments,
    
    -- 태그 및 멘션
    tags,
    mentions,
    has_tags,
    has_mentions,
    
    -- 카드 정보
    card_url,
    card_title,
    card_description,
    card_image,
    
    -- 게시글 속성
    visibility,
    sensitive,
    spoiler_text,
    in_reply_to_id,
    quote_id,
    
    -- 계정 분류
    account_type,
    market_influence,
    
    -- 메타데이터
    collected_at,
    updated_at
) VALUES (
    -- 기본 식별 정보
    %(id)s,
    %(created_at)s,
    
    -- 계정 정보
    %(username)s,
    %(account_id)s,
    %(display_name)s,
    %(verified)s,
    
    -- 포스트 내용
    %(content)s,
    %(clean_content)s,
    %(language)s,
    
    -- 참여 통계
    %(replies_count)s,
    %(reblogs_count)s,
    %(favourites_count)s,
    %(upvotes_count)s,
    %(downvotes_count)s,
    
    -- 링크 및 미디어
    %(url)s,
    %(uri)s,
    %(has_media)s,
    %(media_count)s,
    %(media_attachments)s,
    
    -- 태그 및 멘션
    %(tags)s,
    %(mentions)s,
    %(has_tags)s,
    %(has_mentions)s,
    
    -- 카드 정보
    %(card_url)s,
    %(card_title)s,
    %(card_description)s,
    %(card_image)s,
    
    -- 게시글 속성
    %(visibility)s,
    %(sensitive)s,
    %(spoiler_text)s,
    %(in_reply_to_id)s,
    %(quote_id)s,
    
    -- 계정 분류
    %(account_type)s,
    %(market_influence)s,
    
    -- 메타데이터
    NOW(),  -- collected_at
    NOW()   -- updated_at
)
ON CONFLICT (id) 
DO UPDATE SET
    -- 📊 변동 가능한 통계 정보만 업데이트
    replies_count = EXCLUDED.replies_count,
    reblogs_count = EXCLUDED.reblogs_count,
    favourites_count = EXCLUDED.favourites_count,
    upvotes_count = EXCLUDED.upvotes_count,
    downvotes_count = EXCLUDED.downvotes_count,
    
    -- 🔗 미디어 정보 (추가될 수 있음)
    media_count = EXCLUDED.media_count,
    media_attachments = EXCLUDED.media_attachments,
    has_media = EXCLUDED.has_media,
    
    -- 🏷️ 태그/멘션 (추가/수정될 수 있음)
    tags = EXCLUDED.tags,
    mentions = EXCLUDED.mentions,
    has_tags = EXCLUDED.has_tags,
    has_mentions = EXCLUDED.has_mentions,
    
    -- 🔗 카드 정보 (나중에 추가될 수 있음)
    card_url = EXCLUDED.card_url,
    card_title = EXCLUDED.card_title,
    card_description = EXCLUDED.card_description,
    card_image = EXCLUDED.card_image,
    
    -- 🎯 분석 정보 업데이트
    market_influence = EXCLUDED.market_influence,
    
    -- 🕒 업데이트 시간 갱신
    updated_at = NOW()
    
-- ✅ 실제로 업데이트된 경우만 updated_at 변경
WHERE 
    truth_social_posts.replies_count != EXCLUDED.replies_count OR
    truth_social_posts.reblogs_count != EXCLUDED.reblogs_count OR
    truth_social_posts.favourites_count != EXCLUDED.favourites_count OR
    truth_social_posts.upvotes_count != EXCLUDED.upvotes_count OR
    truth_social_posts.downvotes_count != EXCLUDED.downvotes_count OR
    truth_social_posts.media_count != EXCLUDED.media_count OR
    truth_social_posts.tags::text != EXCLUDED.tags::text OR
    truth_social_posts.mentions::text != EXCLUDED.mentions::text;