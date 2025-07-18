INSERT INTO public.earnings_news_finnhub (
    symbol,
    report_date,
    category,
    article_id,
    headline,
    image,
    related,
    source,
    summary,
    url,
    published_at
) VALUES (
    %(symbol)s,
    %(report_date)s,
    %(category)s,
    %(article_id)s,
    %(headline)s,
    %(image)s,
    %(related)s,
    %(source)s,
    %(summary)s,
    %(url)s,
    %(published_at)s
)
ON CONFLICT (symbol, report_date, url)
DO UPDATE SET
    category = EXCLUDED.category,
    article_id = EXCLUDED.article_id,
    headline = EXCLUDED.headline,
    image = EXCLUDED.image,
    related = EXCLUDED.related,
    source = EXCLUDED.source,
    summary = EXCLUDED.summary,
    published_at = EXCLUDED.published_at,
    fetched_at = NOW();