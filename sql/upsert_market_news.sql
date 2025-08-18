-- upsert_market_news.sql
INSERT INTO market_news (
    source,
    url,
    author,
    title,
    description,
    content,
    published_at
) VALUES (
    %(source)s,
    %(url)s,
    %(author)s,
    %(title)s,
    %(description)s,
    %(content)s,
    %(published_at)s
)
ON CONFLICT (source, url)
DO UPDATE SET
    author = EXCLUDED.author,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    published_at = EXCLUDED.published_at,
    fetched_at = NOW();