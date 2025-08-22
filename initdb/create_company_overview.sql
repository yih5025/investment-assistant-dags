-- initdb/create_company_overview.sql

CREATE TABLE IF NOT EXISTS company_overview (
    -- 기본 식별 정보
    batch_id                        BIGINT NOT NULL,
    symbol                          TEXT NOT NULL,
    PRIMARY KEY (batch_id, symbol),
    asset_type                      VARCHAR(50),
    name                           VARCHAR(200) NOT NULL,
    description                    TEXT,
    cik                            VARCHAR(20),
    exchange                       VARCHAR(20),
    currency                       VARCHAR(10),
    country                        VARCHAR(50),
    
    -- 섹터 및 산업 정보
    sector                         VARCHAR(100),
    industry                       VARCHAR(200),
    address                        VARCHAR(300),
    official_site                  VARCHAR(200),
    
    -- 재무 연도 정보
    fiscal_year_end                VARCHAR(20),
    latest_quarter                 VARCHAR(20),
    
    -- 시장 및 재무 지표
    market_capitalization          BIGINT,
    ebitda                         BIGINT,
    pe_ratio                       NUMERIC(10,4),
    peg_ratio                      NUMERIC(10,4),
    book_value                     NUMERIC(10,4),
    dividend_per_share             NUMERIC(10,4),
    dividend_yield                 NUMERIC(8,6),
    eps                            NUMERIC(10,4),
    revenue_per_share_ttm          NUMERIC(10,4),
    profit_margin                  NUMERIC(8,6),
    operating_margin_ttm           NUMERIC(8,6),
    return_on_assets_ttm           NUMERIC(8,6),
    return_on_equity_ttm           NUMERIC(8,6),
    revenue_ttm                    BIGINT,
    gross_profit_ttm               BIGINT,
    diluted_eps_ttm                NUMERIC(10,4),
    
    -- 성장률 지표
    quarterly_earnings_growth_yoy   NUMERIC(8,6),
    quarterly_revenue_growth_yoy    NUMERIC(8,6),
    
    -- 분석가 평가
    analyst_target_price           NUMERIC(10,4),
    
    -- 밸류에이션 지표
    trailing_pe                    NUMERIC(10,4),
    forward_pe                     NUMERIC(10,4),
    price_to_sales_ratio_ttm       NUMERIC(10,4),
    price_to_book_ratio            NUMERIC(10,4),
    ev_to_revenue                  NUMERIC(10,4),
    ev_to_ebitda                   NUMERIC(10,4),
    beta                           NUMERIC(8,6),
    
    -- 주가 정보
    week_52_high                   NUMERIC(10,4),
    week_52_low                    NUMERIC(10,4),
    day_50_moving_average          NUMERIC(10,4),
    day_200_moving_average         NUMERIC(10,4),
    
    -- 주식 정보
    shares_outstanding             BIGINT,
    dividend_date                  VARCHAR(20),
    ex_dividend_date               VARCHAR(20),
    
    -- 메타데이터
    created_at                     TIMESTAMP DEFAULT NOW()
);

-- 인덱스 생성 (조회 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_company_overview_batch_id ON company_overview(batch_id);
CREATE INDEX IF NOT EXISTS idx_company_overview_batch_symbol ON company_overview(batch_id, symbol);
CREATE INDEX IF NOT EXISTS idx_company_overview_symbol ON company_overview(symbol);
CREATE INDEX IF NOT EXISTS idx_company_overview_sector ON company_overview(sector);
CREATE INDEX IF NOT EXISTS idx_company_overview_industry ON company_overview(industry);
CREATE INDEX IF NOT EXISTS idx_company_overview_country ON company_overview(country);
CREATE INDEX IF NOT EXISTS idx_company_overview_market_cap ON company_overview(market_capitalization);
CREATE INDEX IF NOT EXISTS idx_company_overview_pe_ratio ON company_overview(pe_ratio);

-- 코멘트 추가
COMMENT ON TABLE company_overview IS 'SP500 기업 상세 정보 (Alpha Vantage Company Overview API)';
COMMENT ON COLUMN company_overview.symbol IS '주식 심볼 (Primary Key)';
COMMENT ON COLUMN company_overview.name IS '회사명';
COMMENT ON COLUMN company_overview.sector IS '섹터 (Technology, Healthcare 등)';
COMMENT ON COLUMN company_overview.industry IS '산업군 (Software, Biotechnology 등)';
COMMENT ON COLUMN company_overview.market_capitalization IS '시가총액 (USD)';
COMMENT ON COLUMN company_overview.pe_ratio IS 'P/E 비율';
COMMENT ON COLUMN company_overview.dividend_yield IS '배당 수익률';
COMMENT ON COLUMN company_overview.beta IS '베타 (시장 대비 변동성)';
COMMENT ON COLUMN company_overview.created_at IS '레코드 생성 시간';