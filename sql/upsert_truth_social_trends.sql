-- Truth Social 트렌딩 포스트 Upsert SQL

INSERT INTO truth_social_trends (
    -- 기본 식별 정보
    id,
    created_at,
    
    -- 계정 정보
    username,
    account_id,
    display_name,
    
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
    
    -- 링크 정보
    url,
    uri,
    
    -- 미디어 정보
    has_media,
    media_count,
    media_attachments,
    
    -- 태그 및 멘션
    tags,
    mentions,
    
    -- 게시글 속성
    visibility,
    sensitive,
    in_reply_to_id,
    
    -- 트렌딩 관련
    trend_rank,
    trend_score,
    
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
    
    -- 링크 정보
    %(url)s,
    %(uri)s,
    
    -- 미디어 정보
    %(has_media)s,
    %(media_count)s,
    %(media_attachments)s,
    
    -- 태그 및 멘션
    %(tags)s,
    %(mentions)s,
    
    -- 게시글 속성
    %(visibility)s,
    %(sensitive)s,
    %(in_reply_to_id)s,
    
    -- 트렌딩 관련
    %(trend_rank)s,
    %(trend_score)s,
    
    -- 메타데이터
    NOW(),  -- collected_at
    NOW()   -- updated_at
)
ON CONFLICT (id)
DO UPDATE SET
    -- 📊 변동 가능한 통계 정보 업데이트
    replies_count = EXCLUDED.replies_count,
    reblogs_count = EXCLUDED.reblogs_count,
    favourites_count = EXCLUDED.favourites_count,
    upvotes_count = EXCLUDED.upvotes_count,
    downvotes_count = EXCLUDED.downvotes_count,
    
    -- 🔥 트렌딩 정보 업데이트 (순위 변동 가능)
    trend_rank = EXCLUDED.trend_rank,
    trend_score = EXCLUDED.trend_score,
    
    -- 🎬 미디어 정보 업데이트 (NULL에서 값이 들어올 수 있음)
    has_media = EXCLUDED.has_media,
    media_count = EXCLUDED.media_count,
    media_attachments = COALESCE(EXCLUDED.media_attachments, truth_social_trends.media_attachments),
    
    -- 🕒 업데이트 시간 갱신
    updated_at = NOW()
    
WHERE 
    truth_social_trends.replies_count != EXCLUDED.replies_count OR
    truth_social_trends.reblogs_count != EXCLUDED.reblogs_count OR
    truth_social_trends.favourites_count != EXCLUDED.favourites_count OR
    truth_social_trends.trend_rank != EXCLUDED.trend_rank;