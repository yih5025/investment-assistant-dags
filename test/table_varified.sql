-- 데이터 검증용 SQL 쿼리들

-- =============================================================================
-- 1. 암호화폐 데이터
-- =============================================================================

-- 빗썸 마켓코드 확인
\d market_code_bithumb
SELECT COUNT(*) as total_markets FROM market_code_bithumb;
SELECT market_code, korean_name, english_name FROM market_code_bithumb LIMIT 5;
SELECT 
    CASE 
        WHEN market_code LIKE 'KRW-%' THEN 'KRW'
        ELSE 'OTHER'
    END as market_type,
    COUNT(*) 
FROM market_code_bithumb 
GROUP BY market_type;

-- 빗썸 실시간 데이터
\d bithumb_ticker
SELECT COUNT(*) as total_tickers FROM bithumb_ticker;
SELECT market, trade_date, trade_price, trade_volume FROM bithumb_ticker ORDER BY collected_at DESC LIMIT 3;
SELECT 
    DATE(collected_at) as date,
    COUNT(*) as records_count,
    COUNT(DISTINCT market) as unique_markets
FROM bithumb_ticker 
GROUP BY DATE(collected_at) 
ORDER BY date DESC 
LIMIT 7;

-- =============================================================================
-- 2. 주식 데이터
-- =============================================================================

-- Finnhub 실시간 거래 데이터
\d finnhub_trades
SELECT COUNT(*) as total_trades FROM finnhub_trades;
SELECT symbol, price, volume, timestamp_ms, category FROM finnhub_trades ORDER BY timestamp_ms DESC LIMIT 3;
SELECT 
    category,
    COUNT(*) as trade_count,
    COUNT(DISTINCT symbol) as unique_symbols
FROM finnhub_trades 
GROUP BY category 
ORDER BY trade_count DESC;

-- Top Gainers (트렌딩 주식)
\d top_gainers
SELECT COUNT(*) as total_gainers FROM top_gainers;
SELECT symbol, company_name, price, change_percentage, category FROM top_gainers ORDER BY batch_id DESC LIMIT 3;
SELECT 
    category,
    COUNT(*) as symbol_count,
    AVG(change_percentage) as avg_change
FROM top_gainers 
WHERE batch_id = (SELECT MAX(batch_id) FROM top_gainers)
GROUP BY category;

-- =============================================================================
-- 3. 뉴스 & 소셜 미디어 데이터
-- =============================================================================

-- X(Twitter) 포스트
\d x_posts
SELECT COUNT(*) as total_tweets FROM x_posts;
SELECT source_account, text, like_count, retweet_count, created_at FROM x_posts ORDER BY created_at DESC LIMIT 2;
SELECT 
    source_account,
    COUNT(*) as tweet_count,
    AVG(like_count + retweet_count) as avg_engagement
FROM x_posts 
GROUP BY source_account 
ORDER BY tweet_count DESC 
LIMIT 10;

-- Truth Social 포스트
\d truth_social_posts
SELECT COUNT(*) as total_truth_posts FROM truth_social_posts;
SELECT username, content, reblogs_count, favourites_count, created_at FROM truth_social_posts ORDER BY created_at DESC LIMIT 2;
SELECT 
    username,
    COUNT(*) as post_count,
    AVG(favourites_count) as avg_likes
FROM truth_social_posts 
GROUP BY username 
ORDER BY post_count DESC;

-- 시장 뉴스
\d market_news
SELECT COUNT(*) as total_market_news FROM market_news;
SELECT source, title, published_at FROM market_news ORDER BY published_at DESC LIMIT 2;
SELECT 
    source,
    COUNT(*) as article_count,
    DATE(MAX(published_at)) as latest_article
FROM market_news 
GROUP BY source 
ORDER BY article_count DESC;

-- Alpha Vantage 감성 뉴스
\d market_news_sentiment
SELECT COUNT(*) as total_sentiment_news FROM market_news_sentiment;
SELECT title, overall_sentiment_score, source, time_published FROM market_news_sentiment ORDER BY time_published DESC LIMIT 2;
SELECT 
    CASE 
        WHEN overall_sentiment_score > 0.1 THEN 'Positive'
        WHEN overall_sentiment_score < -0.1 THEN 'Negative'
        ELSE 'Neutral'
    END as sentiment,
    COUNT(*) as news_count
FROM market_news_sentiment 
GROUP BY sentiment;

-- =============================================================================
-- 4. 기업 기본 데이터
-- =============================================================================

-- S&P 500 기업 목록
\d sp500_top50
SELECT COUNT(*) as total_companies FROM sp500_top50;
SELECT symbol, name, sector, industry FROM sp500_top50 LIMIT 5;
SELECT 
    sector,
    COUNT(*) as company_count
FROM sp500_top50 
GROUP BY sector 
ORDER BY company_count DESC;

-- 기업 뉴스
\d company_news
SELECT COUNT(*) as total_company_news FROM company_news;
SELECT headline, summary, source, datetime FROM company_news ORDER BY datetime DESC LIMIT 2;
SELECT 
    DATE(datetime) as news_date,
    COUNT(*) as news_count
FROM company_news 
GROUP BY DATE(datetime) 
ORDER BY news_date DESC 
LIMIT 7;

-- 실적 캘린더
\d earnings_calendar
SELECT COUNT(*) as total_earnings FROM earnings_calendar;
SELECT symbol, report_date, estimate, actual FROM earnings_calendar ORDER BY report_date DESC LIMIT 3;
SELECT 
    DATE_TRUNC('month', report_date) as month,
    COUNT(*) as earnings_count
FROM earnings_calendar 
GROUP BY month 
ORDER BY month DESC 
LIMIT 6;

-- =============================================================================
-- 5. 경제 지표 데이터
-- =============================================================================

-- 연방기금금리
\d federal_funds_rate
SELECT COUNT(*) as total_fed_rates FROM federal_funds_rate;
SELECT date, value FROM federal_funds_rate ORDER BY date DESC LIMIT 3;
SELECT 
    DATE_TRUNC('year', date) as year,
    COUNT(*) as data_points,
    AVG(value) as avg_rate
FROM federal_funds_rate 
GROUP BY year 
ORDER BY year DESC;

-- 국채수익률
\d treasury_yield
SELECT COUNT(*) as total_treasury FROM treasury_yield;
SELECT date, value FROM treasury_yield ORDER BY date DESC LIMIT 3;
SELECT 
    DATE_TRUNC('month', date) as month,
    AVG(value) as avg_yield
FROM treasury_yield 
GROUP BY month 
ORDER BY month DESC 
LIMIT 6;

-- CPI (소비자물가지수)
\d cpi
SELECT COUNT(*) as total_cpi FROM cpi;
SELECT date, value FROM cpi ORDER BY date DESC LIMIT 3;
SELECT 
    DATE_TRUNC('year', date) as year,
    AVG(value) as avg_cpi,
    MAX(value) - MIN(value) as yearly_range
FROM cpi 
GROUP BY year 
ORDER BY year DESC;

-- =============================================================================
-- 6. 전체 데이터 현황 요약
-- =============================================================================

-- 테이블별 레코드 수 요약
SELECT 
    'bithumb_ticker' as table_name, COUNT(*) as record_count FROM bithumb_ticker
UNION ALL
SELECT 'finnhub_trades', COUNT(*) FROM finnhub_trades
UNION ALL  
SELECT 'x_posts', COUNT(*) FROM x_posts
UNION ALL
SELECT 'truth_social_posts', COUNT(*) FROM truth_social_posts
UNION ALL
SELECT 'market_news', COUNT(*) FROM market_news
UNION ALL
SELECT 'market_news_sentiment', COUNT(*) FROM market_news_sentiment
UNION ALL
SELECT 'top_gainers', COUNT(*) FROM top_gainers
UNION ALL
SELECT 'sp500_top50', COUNT(*) FROM sp500_top50
UNION ALL
SELECT 'earnings_calendar', COUNT(*) FROM earnings_calendar
UNION ALL
SELECT 'federal_funds_rate', COUNT(*) FROM federal_funds_rate
ORDER BY record_count DESC;