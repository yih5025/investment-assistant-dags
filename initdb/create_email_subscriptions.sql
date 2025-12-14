CREATE TABLE IF NOT EXISTS public.email_subscriptions (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    scope VARCHAR(20) NOT NULL, -- 'SP500' 또는 'NASDAQ'
    unsubscribe_token UUID DEFAULT gen_random_uuid(), -- 구독 취소용 고유 토큰
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 한 이메일로 같은 옵션을 중복 구독하는 것 방지
    CONSTRAINT unique_email_scope UNIQUE (email, scope)
);

CREATE INDEX idx_subs_active_scope ON email_subscriptions(is_active, scope);