INSERT INTO public.market_news
  (source, url, author, title, description, content, published_at)
VALUES
  (%(source)s, %(url)s, %(author)s, %(title)s, %(description)s, %(content)s, %(published_at)s)
ON CONFLICT (source, url) DO NOTHING;