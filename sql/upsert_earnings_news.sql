INSERT INTO public.earnings_news
  (symbol, report_date, source, url, title, description, published_at)
VALUES (
  %(symbol)s,
  %(report_date)s,
  %(source)s,
  %(url)s,
  %(title)s,
  %(description)s,
  %(published_at)s
)
ON CONFLICT (symbol, report_date, source, url) DO NOTHING;