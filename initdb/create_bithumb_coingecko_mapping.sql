-- 빗썸-CoinGecko 매칭 테이블 생성
-- 빗썸 414개 코인을 CoinGecko ID와 1:1 매칭하는 중간 테이블

DROP TABLE IF EXISTS bithumb_coingecko_mapping CASCADE;

CREATE TABLE bithumb_coingecko_mapping (
    -- Primary Key
    id SERIAL PRIMARY KEY,
    
    -- 빗썸 정보
    market_code TEXT UNIQUE NOT NULL,         -- KRW-BTC (빗썸 마켓 코드)
    symbol TEXT NOT NULL,                     -- BTC (심볼, KRW- 제거)
    bithumb_korean_name TEXT,                 -- 비트코인 (빗썸 한글명)
    bithumb_english_name TEXT,                -- Bitcoin (빗썸 영문명)
    
    -- CoinGecko 매칭 정보
    coingecko_id TEXT,                        -- bitcoin (CoinGecko API ID)
    coingecko_name TEXT,                      -- Bitcoin (CoinGecko 이름)
    market_cap_rank INTEGER,                  -- 1 (시가총액 순위)
    market_cap_usd BIGINT,                    -- 2214363943526 (시가총액 USD)
    current_price_usd DECIMAL(20,8),          -- 111207.00000000 (현재가 USD)
    
    -- 매칭 메타데이터
    match_method TEXT NOT NULL,               -- 매칭 방법 (MANUAL_MAPPING, AUTO_MATCHING 등)
    match_score DECIMAL(5,2),                 -- 95.50 (매칭 점수, 0~100)
    is_verified BOOLEAN DEFAULT FALSE,        -- 수동 검증 완료 여부
    
    -- 추가 플래그
    is_active BOOLEAN DEFAULT TRUE,           -- 활성 상태
    has_issues BOOLEAN DEFAULT FALSE,         -- 문제 있는 매칭 표시
    notes TEXT,                              -- 수동 메모
    
    -- 시간 정보
    created_at TIMESTAMP DEFAULT NOW(),       -- 매칭 생성 시간
    updated_at TIMESTAMP DEFAULT NOW(),       -- 마지막 수정 시간
    verified_at TIMESTAMP,                    -- 검증 완료 시간
    verified_by TEXT                          -- 검증자 (사용자명 등)
);

-- 인덱스 생성 (성능 최적화)
CREATE UNIQUE INDEX idx_bithumb_coingecko_market_code 
    ON bithumb_coingecko_mapping(market_code);

CREATE INDEX idx_bithumb_coingecko_symbol 
    ON bithumb_coingecko_mapping(symbol);

CREATE INDEX idx_bithumb_coingecko_id 
    ON bithumb_coingecko_mapping(coingecko_id);

-- 매칭 방법별 검색용
CREATE INDEX idx_bithumb_coingecko_match_method 
    ON bithumb_coingecko_mapping(match_method);

-- 점수 기반 검색용
CREATE INDEX idx_bithumb_coingecko_match_score 
    ON bithumb_coingecko_mapping(match_score DESC);

-- 검증 상태별 검색용
CREATE INDEX idx_bithumb_coingecko_verification 
    ON bithumb_coingecko_mapping(is_verified, match_score DESC);

-- 시가총액 순위별 검색용
CREATE INDEX idx_bithumb_coingecko_rank 
    ON bithumb_coingecko_mapping(market_cap_rank)
    WHERE market_cap_rank IS NOT NULL;

-- 테이블 및 컬럼 코멘트
COMMENT ON TABLE bithumb_coingecko_mapping IS '빗썸 코인과 CoinGecko ID 매칭 테이블 - 김치프리미엄 계산의 기초';

COMMENT ON COLUMN bithumb_coingecko_mapping.market_code IS '빗썸 마켓 코드 (예: KRW-BTC)';
COMMENT ON COLUMN bithumb_coingecko_mapping.symbol IS 'KRW- 제거한 순수 심볼 (예: BTC)';
COMMENT ON COLUMN bithumb_coingecko_mapping.coingecko_id IS 'CoinGecko API에서 사용하는 ID (예: bitcoin)';
COMMENT ON COLUMN bithumb_coingecko_mapping.match_method IS '매칭 방법: MANUAL_MAPPING, EXACT_NAME_MATCH, TOP_10_RANK, AUTO_MATCHING';
COMMENT ON COLUMN bithumb_coingecko_mapping.match_score IS '매칭 신뢰도 점수 (0~100), 높을수록 정확함';
COMMENT ON COLUMN bithumb_coingecko_mapping.is_verified IS '수동으로 검증 완료된 매칭 여부';

-- 매칭 품질 검증용 뷰 생성
CREATE OR REPLACE VIEW v_bithumb_coingecko_mapping_summary AS
SELECT 
    -- 전체 통계
    COUNT(*) as total_mappings,
    COUNT(CASE WHEN coingecko_id IS NOT NULL THEN 1 END) as successful_mappings,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_mappings,
    COUNT(CASE WHEN has_issues = true THEN 1 END) as problematic_mappings,
    
    -- 매칭 방법별 통계
    COUNT(CASE WHEN match_method = 'MANUAL_MAPPING' THEN 1 END) as manual_mappings,
    COUNT(CASE WHEN match_method = 'EXACT_NAME_MATCH' THEN 1 END) as exact_name_mappings,
    COUNT(CASE WHEN match_method = 'TOP_10_RANK' THEN 1 END) as top10_rank_mappings,
    COUNT(CASE WHEN match_method LIKE '%AUTO%' THEN 1 END) as auto_mappings,
    
    -- 품질 지표
    AVG(match_score) as average_match_score,
    COUNT(CASE WHEN match_score >= 90 THEN 1 END) as high_quality_mappings,
    COUNT(CASE WHEN match_score BETWEEN 70 AND 89 THEN 1 END) as medium_quality_mappings,
    COUNT(CASE WHEN match_score < 70 THEN 1 END) as low_quality_mappings,
    
    -- 시가총액 순위 분포
    COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 100 THEN 1 END) as top100_coins,
    COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 500 THEN 1 END) as top500_coins,
    
    -- 시간 정보
    MIN(created_at) as first_mapping_time,
    MAX(updated_at) as last_update_time
    
FROM bithumb_coingecko_mapping;

COMMENT ON VIEW v_bithumb_coingecko_mapping_summary IS '빗썸-CoinGecko 매칭 품질 요약 통계';

-- 문제가 있는 매칭 확인용 뷰
CREATE OR REPLACE VIEW v_bithumb_coingecko_issues AS
SELECT 
    market_code,
    symbol,
    bithumb_english_name,
    coingecko_id,
    coingecko_name,
    match_method,
    match_score,
    is_verified,
    
    -- 문제 유형 판단
    CASE 
        WHEN coingecko_id IS NULL THEN 'NO_COINGECKO_ID'
        WHEN match_score < 60 AND is_verified = false THEN 'LOW_SCORE_UNVERIFIED'
        WHEN match_method = 'AUTO_MATCHING' AND match_score < 80 THEN 'AUTO_LOW_QUALITY'
        WHEN has_issues = true THEN 'MANUALLY_FLAGGED'
        ELSE 'REVIEW_RECOMMENDED'
    END as issue_type,
    
    -- 검토 우선순위 (1=높음, 3=낮음)
    CASE 
        WHEN coingecko_id IS NULL THEN 1
        WHEN match_score < 60 THEN 1
        WHEN match_score < 80 AND is_verified = false THEN 2
        ELSE 3
    END as review_priority,
    
    notes,
    created_at,
    updated_at

FROM bithumb_coingecko_mapping
WHERE 
    coingecko_id IS NULL 
    OR match_score < 80 
    OR has_issues = true 
    OR (match_method = 'AUTO_MATCHING' AND is_verified = false)

ORDER BY 
    CASE 
        WHEN coingecko_id IS NULL THEN 1
        WHEN match_score < 60 THEN 2
        WHEN match_score < 80 THEN 3
        ELSE 4
    END,
    match_score ASC;

COMMENT ON VIEW v_bithumb_coingecko_issues IS '수동 검토가 필요한 문제가 있는 매칭들';

-- 매칭 결과 확인용 뷰 (성공한 매칭만)
CREATE OR REPLACE VIEW v_bithumb_coingecko_successful AS
SELECT 
    bcm.market_code,
    bcm.symbol,
    bcm.bithumb_korean_name,
    bcm.bithumb_english_name,
    bcm.coingecko_id,
    bcm.coingecko_name,
    bcm.market_cap_rank,
    bcm.current_price_usd,
    bcm.match_method,
    bcm.match_score,
    bcm.is_verified,
    
    -- CoinGecko ID 매핑 테이블과 조인하여 최신 데이터 가져오기
    cim.current_price_usd as latest_price_usd,
    cim.market_cap_usd as latest_market_cap_usd,
    cim.last_updated as price_last_updated

FROM bithumb_coingecko_mapping bcm
LEFT JOIN coingecko_id_mapping cim 
    ON bcm.coingecko_id = cim.coingecko_id

WHERE bcm.coingecko_id IS NOT NULL
  AND bcm.is_active = true

ORDER BY 
    COALESCE(bcm.market_cap_rank, 999999),
    bcm.match_score DESC,
    bcm.symbol;

COMMENT ON VIEW v_bithumb_coingecko_successful IS '성공적으로 매칭된 빗썸-CoinGecko 조합 (Tickers DAG에서 사용)';

-- 매칭 업데이트용 함수들
CREATE OR REPLACE FUNCTION update_mapping_verification(
    p_market_code TEXT,
    p_coingecko_id TEXT,
    p_verified_by TEXT DEFAULT 'system'
) RETURNS BOOLEAN AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE bithumb_coingecko_mapping 
    SET 
        coingecko_id = p_coingecko_id,
        is_verified = true,
        verified_at = NOW(),
        verified_by = p_verified_by,
        updated_at = NOW(),
        match_method = 'MANUAL_VERIFICATION',
        match_score = 100.0
    WHERE market_code = p_market_code;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    IF updated_count > 0 THEN
        RAISE NOTICE '✅ 매칭 업데이트 완료: % → %', p_market_code, p_coingecko_id;
        RETURN TRUE;
    ELSE
        RAISE NOTICE '❌ 매칭 업데이트 실패: % (존재하지 않는 market_code)', p_market_code;
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_mapping_verification IS '매칭 수동 검증 및 업데이트';

-- 문제 있는 매칭 플래그 설정 함수
CREATE OR REPLACE FUNCTION flag_mapping_issue(
    p_market_code TEXT,
    p_notes TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE bithumb_coingecko_mapping 
    SET 
        has_issues = true,
        notes = p_notes,
        updated_at = NOW()
    WHERE market_code = p_market_code;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    IF updated_count > 0 THEN
        RAISE NOTICE '⚠️  매칭 이슈 플래그 설정: % - %', p_market_code, p_notes;
        RETURN TRUE;
    ELSE
        RAISE NOTICE '❌ 이슈 플래그 설정 실패: % (존재하지 않는 market_code)', p_market_code;
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION flag_mapping_issue IS '문제가 있는 매칭에 이슈 플래그 설정';

-- 트리거 함수: updated_at 자동 업데이트
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 트리거 생성
CREATE TRIGGER trigger_bithumb_coingecko_mapping_updated_at
    BEFORE UPDATE ON bithumb_coingecko_mapping
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 샘플 사용법 안내 (주석)
/*
매칭 테이블 사용법:

1. 전체 매칭 현황 확인:
   SELECT * FROM v_bithumb_coingecko_mapping_summary;

2. 문제가 있는 매칭 확인:
   SELECT * FROM v_bithumb_coingecko_issues ORDER BY review_priority, match_score;

3. 성공한 매칭 목록 (Tickers DAG에서 사용):
   SELECT market_code, coingecko_id FROM v_bithumb_coingecko_successful;

4. 수동 매칭 수정 (예: KRW-KNC → kyber-network-crystal):
   SELECT update_mapping_verification('KRW-KNC', 'kyber-network-crystal', 'admin');

5. 문제 플래그 설정:
   SELECT flag_mapping_issue('KRW-XYZ', '코인이 상장폐지됨');

6. 특정 점수 미만 매칭 확인:
   SELECT * FROM bithumb_coingecko_mapping WHERE match_score < 70 ORDER BY match_score;
*/

-- 테이블 생성 완료 로그
DO $$ 
BEGIN 
    RAISE NOTICE '✅ bithumb_coingecko_mapping 테이블 생성 완료';
    RAISE NOTICE '📊 인덱스 7개, 뷰 3개, 함수 3개, 트리거 1개 생성됨';
    RAISE NOTICE '🔄 이제 매칭 DAG를 실행하여 414개 코인 매칭을 진행하세요';
    RAISE NOTICE '📋 매칭 완료 후 v_bithumb_coingecko_issues 뷰로 수동 검토 필요 항목 확인';
END $$;