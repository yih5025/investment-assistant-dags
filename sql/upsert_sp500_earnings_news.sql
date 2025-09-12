-- dags/sql/upsert_sp500_earnings_news.sql

INSERT INTO sp500_earnings_news (
    calendar_id,
    source_table,
    title,
    url,
    summary,
    content,
    source,
    published_at,
    news_section,
    days_from_earnings,
    fetched_at,
    created_at
) VALUES (
    %(calendar_id)s,
    %(source_table)s,
    %(title)s,
    %(url)s,
    %(summary)s,
    %(content)s,
    %(source)s,
    %(published_at)s,
    %(news_section)s,
    %(days_from_earnings)s,
    NOW(),
    NOW()
)
ON CONFLICT (calendar_id, url) 
DO UPDATE SET
    source_table = EXCLUDED.source_table,
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    content = EXCLUDED.content,
    source = EXCLUDED.source,
    published_at = EXCLUDED.published_at,
    news_section = EXCLUDED.news_section,
    days_from_earnings = EXCLUDED.days_from_earnings,
    fetched_at = NOW();