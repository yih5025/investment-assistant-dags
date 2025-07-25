INSERT INTO market_news_sentiment (
    batch_id,
    title,
    url,
    time_published,
    authors,
    summary,
    source,
    overall_sentiment_score,
    overall_sentiment_label,
    ticker_sentiment,
    topics,
    query_type,
    query_params,
    created_at
) VALUES (
    %(batch_id)s,
    %(title)s,
    %(url)s,
    %(time_published)s,
    %(authors)s,
    %(summary)s,
    %(source)s,
    %(overall_sentiment_score)s,
    %(overall_sentiment_label)s,
    %(ticker_sentiment)s,
    %(topics)s,
    %(query_type)s,
    %(query_params)s,
    NOW()
)
ON CONFLICT (batch_id, url)
DO UPDATE SET
    title = EXCLUDED.title,
    time_published = EXCLUDED.time_published,
    authors = EXCLUDED.authors,
    summary = EXCLUDED.summary,
    source = EXCLUDED.source,
    overall_sentiment_score = EXCLUDED.overall_sentiment_score,
    overall_sentiment_label = EXCLUDED.overall_sentiment_label,
    ticker_sentiment = EXCLUDED.ticker_sentiment,
    topics = EXCLUDED.topics,
    query_type = EXCLUDED.query_type,
    query_params = EXCLUDED.query_params,
    created_at = NOW();