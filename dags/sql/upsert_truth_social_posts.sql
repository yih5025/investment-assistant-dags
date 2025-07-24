-- dags/sql/upsert_truth_social_posts.sql
INSERT INTO truth_social_posts (
    id, username, display_name, content, created_at, updated_at,
    reblogs_count, favourites_count, replies_count, url,
    media_attachments, hashtags, mentions, is_trending, is_sensitive,
    language, visibility, source_type, collected_at
) VALUES (
    %(id)s, %(username)s, %(display_name)s, %(content)s, %(created_at)s, %(updated_at)s,
    %(reblogs_count)s, %(favourites_count)s, %(replies_count)s, %(url)s,
    %(media_attachments)s, %(hashtags)s, %(mentions)s, %(is_trending)s, %(is_sensitive)s,
    %(language)s, %(visibility)s, %(source_type)s, NOW()
)
ON CONFLICT (id)
DO UPDATE SET
    -- 변동 가능한 메트릭 업데이트
    reblogs_count = EXCLUDED.reblogs_count,
    favourites_count = EXCLUDED.favourites_count,
    replies_count = EXCLUDED.replies_count,
    updated_at = EXCLUDED.updated_at,
    collected_at = NOW(),
    
    -- 트렌딩 상태 업데이트 (시간에 따라 변할 수 있음)
    is_trending = CASE 
        WHEN EXCLUDED.is_trending = true THEN true 
        ELSE truth_social_posts.is_trending 
    END;