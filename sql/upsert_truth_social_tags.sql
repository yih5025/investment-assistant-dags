-- Truth Social 트렌딩 해시태그 Upsert SQL

INSERT INTO truth_social_tags (
    -- 기본 식별 정보
    name,
    collected_date,
    
    -- 링크 정보
    url,
    
    -- 사용량 통계
    total_uses,
    total_accounts,
    recent_statuses_count,
    
    -- 주간 히스토리
    history_data,
    day_0_uses,
    day_1_uses,
    day_2_uses,
    day_3_uses,
    day_4_uses,
    day_5_uses,
    day_6_uses,
    
    -- 계산된 메트릭
    trend_score,
    growth_rate,
    weekly_average,
    
    -- 분류
    tag_category,
    market_relevance,
    
    -- 메타데이터
    collected_at,
    updated_at
) VALUES (
    -- 기본 식별 정보
    %(name)s,
    %(collected_date)s,
    
    -- 링크 정보
    %(url)s,
    
    -- 사용량 통계
    %(total_uses)s,
    %(total_accounts)s,
    %(recent_statuses_count)s,
    
    -- 주간 히스토리
    %(history_data)s,
    %(day_0_uses)s,
    %(day_1_uses)s,
    %(day_2_uses)s,
    %(day_3_uses)s,
    %(day_4_uses)s,
    %(day_5_uses)s,
    %(day_6_uses)s,
    
    -- 계산된 메트릭
    %(trend_score)s,
    %(growth_rate)s,
    %(weekly_average)s,
    
    -- 분류
    %(tag_category)s,
    %(market_relevance)s,
    
    -- 메타데이터
    NOW(),  -- collected_at
    NOW()   -- updated_at
)
ON CONFLICT (name, collected_date)
DO UPDATE SET
    -- 📊 사용량 통계 업데이트 (하루 중 변동 가능)
    total_uses = EXCLUDED.total_uses,
    total_accounts = EXCLUDED.total_accounts,
    recent_statuses_count = EXCLUDED.recent_statuses_count,
    
    -- 📈 히스토리 데이터 업데이트
    history_data = EXCLUDED.history_data,
    day_0_uses = EXCLUDED.day_0_uses,
    day_1_uses = EXCLUDED.day_1_uses,
    day_2_uses = EXCLUDED.day_2_uses,
    day_3_uses = EXCLUDED.day_3_uses,
    day_4_uses = EXCLUDED.day_4_uses,
    day_5_uses = EXCLUDED.day_5_uses,
    day_6_uses = EXCLUDED.day_6_uses,
    
    -- 📊 계산된 메트릭 업데이트
    trend_score = EXCLUDED.trend_score,
    growth_rate = EXCLUDED.growth_rate,
    weekly_average = EXCLUDED.weekly_average,
    
    -- 🏷️ 분류 정보 업데이트 (수동 설정 가능)
    tag_category = EXCLUDED.tag_category,
    market_relevance = EXCLUDED.market_relevance,
    
    -- 🕒 업데이트 시간 갱신
    updated_at = NOW()
    
WHERE 
    truth_social_tags.total_uses != EXCLUDED.total_uses OR
    truth_social_tags.total_accounts != EXCLUDED.total_accounts OR
    truth_social_tags.recent_statuses_count != EXCLUDED.recent_statuses_count;