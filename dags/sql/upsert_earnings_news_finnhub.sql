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
)
VALUES (
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
ON CONFLICT (symbol, report_date, article_id) DO UPDATE
  SET
    category     = EXCLUDED.category,
    headline     = EXCLUDED.headline,
    image        = EXCLUDED.image,
    related      = EXCLUDED.related,
    source       = EXCLUDED.source,
    summary      = EXCLUDED.summary,
    url          = EXCLUDED.url,
    published_at = EXCLUDED.published_at,
    fetched_at   = NOW();
