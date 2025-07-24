-- upsert_x_posts.sql
-- X API 트윗 데이터 UPSERT (실제 API 구조 기반)

INSERT INTO x_posts (
    tweet_id,
    author_id,
    text,
    created_at,
    lang,
    retweet_count,
    reply_count,
    like_count,
    quote_count,
    bookmark_count,
    impression_count,
    hashtags,
    mentions,
    urls,
    cashtags,
    annotations,
    context_annotations,
    username,
    display_name,
    user_verified,
    user_followers_count,
    user_following_count,
    user_tweet_count,
    edit_history_tweet_ids,
    source_account
) VALUES (
    %(tweet_id)s,
    %(author_id)s,
    %(text)s,
    %(created_at)s,
    %(lang)s,
    %(retweet_count)s,
    %(reply_count)s,
    %(like_count)s,
    %(quote_count)s,
    %(bookmark_count)s,
    %(impression_count)s,
    %(hashtags)s,
    %(mentions)s,
    %(urls)s,
    %(cashtags)s,
    %(annotations)s,
    %(context_annotations)s,
    %(username)s,
    %(display_name)s,
    %(user_verified)s,
    %(user_followers_count)s,
    %(user_following_count)s,
    %(user_tweet_count)s,
    %(edit_history_tweet_ids)s,
    %(source_account)s
)
ON CONFLICT (tweet_id)
DO UPDATE SET
    -- 참여도 지표는 최신 값으로 업데이트 (계속 변함)
    retweet_count = EXCLUDED.retweet_count,
    reply_count = EXCLUDED.reply_count,
    like_count = EXCLUDED.like_count,
    quote_count = EXCLUDED.quote_count,
    bookmark_count = EXCLUDED.bookmark_count,
    impression_count = EXCLUDED.impression_count,
    
    -- 사용자 정보 업데이트 (팔로워 수 등 변동)
    user_followers_count = EXCLUDED.user_followers_count,
    user_following_count = EXCLUDED.user_following_count,
    user_tweet_count = EXCLUDED.user_tweet_count,
    
    -- 수집 시간 업데이트
    collected_at = NOW();