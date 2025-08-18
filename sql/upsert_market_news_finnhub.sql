-- upsert_market_news_finnhub.sql
INSERT INTO market_news_finnhub (
    category,
    news_id,
    datetime,
    headline,
    image,
    related,
    source,
    summary,
    url
) VALUES (
    %(category)s,
    %(news_id)s,
    %(datetime)s,
    %(headline)s,
    %(image)s,
    %(related)s,
    %(source)s,
    %(summary)s,
    %(url)s
)
ON CONFLICT (category, news_id)
DO UPDATE SET
    datetime = EXCLUDED.datetime,
    headline = EXCLUDED.headline,
    image = EXCLUDED.image,
    related = EXCLUDED.related,
    source = EXCLUDED.source,
    summary = EXCLUDED.summary,
    url = EXCLUDED.url,
    fetched_at = NOW();