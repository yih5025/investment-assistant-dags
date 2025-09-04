-- CoinGecko Tickers (빗썸 매칭) 테이블 생성
-- 김치프리미엄 계산을 위한 거래소별 가격 데이터 저장

DROP TABLE IF EXISTS coingecko_tickers_bithumb CASCADE;

CREATE TABLE coingecko_tickers_bithumb (
    id SERIAL PRIMARY KEY,
    
    -- 빗썸 매칭 정보
    market_code TEXT NOT NULL,                    -- KRW-BTC (빗썸 마켓 코드)
    coingecko_id TEXT NOT NULL,                   -- bitcoin (CoinGecko ID)
    symbol TEXT NOT NULL,                         -- BTC (심볼)
    coin_name TEXT,                               -- Bitcoin (코인명)
    
    -- 거래 쌍 정보
    base TEXT NOT NULL,                           -- BTC (기본 통화)
    target TEXT NOT NULL,                         -- USDT, USD, KRW 등 (대상 통화)
    
    -- 거래소 정보 
    exchange_name TEXT NOT NULL,                  -- Binance, Upbit, Coinbase 등
    exchange_id TEXT NOT NULL,                    -- binance, upbit, coinbase 등
    
    -- 가격 및 거래량 정보
    last_price DECIMAL(20,8),                     -- 최근 거래가
    volume_24h DECIMAL(20,8),                     -- 24시간 거래량 (원본 통화)
    converted_last_usd DECIMAL(20,8),             -- USD 환산 가격 (김치프리미엄 계산용)
    converted_volume_usd BIGINT,                  -- USD 환산 거래량
    
    -- 신뢰성 및 스프레드 정보
    trust_score TEXT,                             -- green, yellow, red (신뢰도)
    bid_ask_spread_percentage DECIMAL(10,4),      -- 매수/매도 호가 스프레드 (%)
    
    -- 시간 정보
    timestamp TIMESTAMP,                          -- 가격 업데이트 시간
    last_traded_at TIMESTAMP,                     -- 마지막 거래 시간  
    last_fetch_at TIMESTAMP,                      -- API에서 조회한 시간
    
    -- 상태 플래그
    is_anomaly BOOLEAN DEFAULT FALSE,             -- 이상 거래 감지 여부
    is_stale BOOLEAN DEFAULT FALSE,               -- 오래된 데이터 여부
    
    -- 추가 정보
    trade_url TEXT,                              -- 거래소 거래 페이지 URL
    coin_mcap_usd BIGINT,                        -- 코인 시가총액 (USD)
    
    -- 매칭 메타데이터
    match_method TEXT,                           -- 매칭 방법 (MANUAL_MAPPING, RANK_AND_NAME 등)
    market_cap_rank INTEGER,                     -- CoinGecko 시가총액 순위
    
    -- 시스템 정보
    created_at TIMESTAMP DEFAULT NOW(),          -- 레코드 생성 시간
    updated_at TIMESTAMP DEFAULT NOW()           -- 레코드 수정 시간
);

-- 성능 최적화 인덱스 생성
-- 1. 기본 검색용 인덱스
CREATE INDEX idx_coingecko_tickers_market_code 
    ON coingecko_tickers_bithumb(market_code);

CREATE INDEX idx_coingecko_tickers_coingecko_id 
    ON coingecko_tickers_bithumb(coingecko_id);

CREATE INDEX idx_coingecko_tickers_symbol 
    ON coingecko_tickers_bithumb(symbol);

-- 2. 거래소별 검색용 인덱스  
CREATE INDEX idx_coingecko_tickers_exchange_id 
    ON coingecko_tickers_bithumb(exchange_id);

CREATE INDEX idx_coingecko_tickers_exchange_symbol 
    ON coingecko_tickers_bithumb(exchange_id, symbol);

-- 3. 시간 기반 검색용 인덱스
CREATE INDEX idx_coingecko_tickers_created_at 
    ON coingecko_tickers_bithumb(created_at DESC);

CREATE INDEX idx_coingecko_tickers_date_created 
    ON coingecko_tickers_bithumb(DATE(created_at));

-- 4. 김치프리미엄 계산용 복합 인덱스
CREATE INDEX idx_coingecko_tickers_kimchi_premium 
    ON coingecko_tickers_bithumb(symbol, exchange_id, created_at DESC)
    WHERE converted_last_usd IS NOT NULL;

-- 5. 특정 거래소 조합 (김치프리미엄 핵심)
CREATE INDEX idx_coingecko_tickers_upbit_binance 
    ON coingecko_tickers_bithumb(symbol, converted_last_usd, created_at DESC)
    WHERE exchange_id IN ('upbit', 'binance');

-- 6. 시가총액 순위별 조회용
CREATE INDEX idx_coingecko_tickers_market_cap_rank 
    ON coingecko_tickers_bithumb(market_cap_rank, symbol)
    WHERE market_cap_rank IS NOT NULL;

-- 7. 신뢰도 기반 검색용
CREATE INDEX idx_coingecko_tickers_trust_score 
    ON coingecko_tickers_bithumb(trust_score, exchange_id)
    WHERE trust_score = 'green';

-- 8. 거래량 기반 정렬용  
CREATE INDEX idx_coingecko_tickers_volume_usd 
    ON coingecko_tickers_bithumb(converted_volume_usd DESC)
    WHERE converted_volume_usd IS NOT NULL;

-- 테이블 코멘트 추가
COMMENT ON TABLE coingecko_tickers_bithumb IS '빗썸 매칭 기반 CoinGecko 거래소별 티커 데이터 - 김치프리미엄 계산용';

-- 주요 컬럼 코멘트
COMMENT ON COLUMN coingecko_tickers_bithumb.market_code IS '빗썸 마켓 코드 (예: KRW-BTC)';
COMMENT ON COLUMN coingecko_tickers_bithumb.coingecko_id IS 'CoinGecko API ID (예: bitcoin)';
COMMENT ON COLUMN coingecko_tickers_bithumb.converted_last_usd IS 'USD 환산 가격 - 김치프리미엄 계산의 핵심';
COMMENT ON COLUMN coingecko_tickers_bithumb.exchange_id IS '거래소 식별자 (upbit, binance, coinbase 등)';
COMMENT ON COLUMN coingecko_tickers_bithumb.match_method IS '빗썸-CoinGecko 매칭 방법 추적';
COMMENT ON COLUMN coingecko_tickers_bithumb.trust_score IS 'CoinGecko 신뢰도 점수 (green > yellow > red)';

-- 김치프리미엄 계산용 뷰 생성
CREATE OR REPLACE VIEW v_kimchi_premium_realtime AS
SELECT 
    symbol,
    coin_name,
    market_code,
    
    -- 업비트 가격 (한국 거래소)
    upbit.converted_last_usd as upbit_usd_price,
    upbit.volume_24h as upbit_volume,
    upbit.last_traded_at as upbit_last_traded,
    
    -- 바이낸스 가격 (글로벌 거래소)  
    binance.converted_last_usd as binance_usd_price,
    binance.volume_24h as binance_volume,
    binance.last_traded_at as binance_last_traded,
    
    -- 김치프리미엄 계산
    CASE 
        WHEN binance.converted_last_usd > 0 THEN
            ((upbit.converted_last_usd - binance.converted_last_usd) / binance.converted_last_usd * 100)
        ELSE NULL 
    END as kimchi_premium_percentage,
    
    -- 가격 차이 (절대값)
    (upbit.converted_last_usd - binance.converted_last_usd) as price_difference_usd,
    
    -- 데이터 신선도
    GREATEST(upbit.created_at, binance.created_at) as data_updated_at,
    
    -- 거래량 합계 (유동성 참고용)
    (COALESCE(upbit.converted_volume_usd, 0) + COALESCE(binance.converted_volume_usd, 0)) as total_volume_usd

FROM (
    SELECT DISTINCT symbol, coin_name, market_code 
    FROM coingecko_tickers_bithumb 
    WHERE DATE(created_at) = CURRENT_DATE
) coins

LEFT JOIN (
    SELECT DISTINCT ON (symbol) 
        symbol, converted_last_usd, volume_24h, last_traded_at, 
        created_at, converted_volume_usd
    FROM coingecko_tickers_bithumb 
    WHERE exchange_id = 'upbit' 
      AND DATE(created_at) = CURRENT_DATE
      AND converted_last_usd IS NOT NULL
    ORDER BY symbol, created_at DESC
) upbit ON coins.symbol = upbit.symbol

LEFT JOIN (
    SELECT DISTINCT ON (symbol)
        symbol, converted_last_usd, volume_24h, last_traded_at,
        created_at, converted_volume_usd  
    FROM coingecko_tickers_bithumb
    WHERE exchange_id = 'binance'
      AND DATE(created_at) = CURRENT_DATE 
      AND converted_last_usd IS NOT NULL
    ORDER BY symbol, created_at DESC  
) binance ON coins.symbol = binance.symbol

WHERE upbit.converted_last_usd IS NOT NULL 
   OR binance.converted_last_usd IS NOT NULL

ORDER BY 
    CASE 
        WHEN upbit.converted_last_usd IS NOT NULL AND binance.converted_last_usd IS NOT NULL THEN
            ABS(((upbit.converted_last_usd - binance.converted_last_usd) / binance.converted_last_usd * 100))
        ELSE 0 
    END DESC;

COMMENT ON VIEW v_kimchi_premium_realtime IS '실시간 김치프리미엄 계산 뷰 - 업비트 vs 바이낸스 가격 비교';

-- 데이터 정리용 함수 (선택사항)
CREATE OR REPLACE FUNCTION cleanup_old_tickers_data() 
RETURNS INTEGER AS $
DECLARE
    deleted_count INTEGER;
BEGIN
    -- 7일 이전 데이터 삭제
    DELETE FROM coingecko_tickers_bithumb 
    WHERE created_at < CURRENT_DATE - INTERVAL '7 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- 통계 정보 업데이트
    ANALYZE coingecko_tickers_bithumb;
    
    RETURN deleted_count;
END;
$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_old_tickers_data() IS '7일 이전의 오래된 티커 데이터 자동 정리';

-- 테이블 생성 완료 로그
DO $ 
BEGIN 
    RAISE NOTICE '✅ coingecko_tickers_bithumb 테이블 생성 완료';
    RAISE NOTICE '📊 인덱스 8개, 김치프리미엄 뷰 1개, 정리 함수 1개 추가됨';
    RAISE NOTICE '🔄 12시간마다 실행되는 DAG로 빗썸 414개 코인 데이터 수집 예정';
END $;