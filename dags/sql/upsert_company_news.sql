INSERT INTO public.company_news
  (symbol, source, url, title, description, content, published_at)
VALUES
  (%(symbol)s, %(source)s, %(url)s, %(title)s, %(description)s, %(content)s, %(published_at)s)
ON CONFLICT (symbol, url) DO UPDATE
  SET
    title         = EXCLUDED.title,
    description   = EXCLUDED.description,
    content       = EXCLUDED.content,
    published_at  = EXCLUDED.published_at,
    fetched_at    = NOW();
