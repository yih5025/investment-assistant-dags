# app/api/endpoints/market_news_sentiment_endpoint.py

from fastapi import APIRouter, Depends, Query, Path, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from app.dependencies import get_db
from app.schemas.market_news_sentiment_schema import (
    MarketSentimentListResponse,
    TopicListResponse,
    TickerListResponse,
    TopicRankingResponse,
    TickerRankingResponse,
    TopicNewsResponse,
    TickerNewsResponse,
    CrossAnalysisResponse,
    BatchInfo,
    SentimentQueryParams,
    RankingQueryParams,
    SentimentTrendsResponse
)
from app.services.market_news_sentiment_service import MarketNewsSentimentService

# Market News Sentiment 라우터 생성
router = APIRouter(
    tags=["Market News Sentiment"],
    responses={
        404: {"description": "요청한 뉴스 감성 분석 데이터를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)

# =============================================================================
# 기본 뉴스 조회 API
# =============================================================================

@router.get("/", response_model=MarketSentimentListResponse)
async def get_market_sentiment_news(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터 (1-30일)"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋"),
    min_sentiment: Optional[float] = Query(None, ge=-1.0, le=1.0, description="최소 감성 점수"),
    max_sentiment: Optional[float] = Query(None, ge=-1.0, le=1.0, description="최대 감성 점수"),
    sentiment_labels: Optional[str] = Query(None, description="감성 라벨 필터 (쉼표 구분)"),
    sort_by: str = Query("time_published", description="정렬 기준"),
    order: str = Query("desc", pattern="^(asc|desc)$", description="정렬 순서")
):
    """
    시장 뉴스 감성 분석 데이터를 조회합니다.
    
    전체 뉴스 목록을 감성 점수와 함께 제공하며, 다양한 필터링 옵션을 지원합니다.
    
    Args:
        days: 최근 N일 데이터 조회 (기본 7일)
        limit: 최대 결과 개수 (기본 20개)
        offset: 페이징 오프셋
        min_sentiment: 최소 감성 점수 (-1.0 ~ 1.0)
        max_sentiment: 최대 감성 점수 (-1.0 ~ 1.0)
        sentiment_labels: 감성 라벨 필터 (예: "Bullish,Bearish")
        sort_by: 정렬 기준 (time_published, sentiment_score)
        order: 정렬 순서 (asc, desc)
        
    Returns:
        MarketSentimentListResponse: 뉴스 목록 + 배치 정보
    """
    service = MarketNewsSentimentService(db)
    
    # 감성 라벨 파싱
    labels_list = None
    if sentiment_labels:
        labels_list = [label.strip() for label in sentiment_labels.split(",")]
    
    # 뉴스 목록 조회
    news_list, total_count = service.get_news_list(
        days=days, limit=limit, offset=offset,
        min_sentiment=min_sentiment, max_sentiment=max_sentiment,
        sentiment_labels=labels_list, sort_by=sort_by, order=order
    )
    
    # 배치 정보 조회
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=total_count,
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )

# =============================================================================
# Ticker 관련 API
# =============================================================================

@router.get("/tickers", response_model=TickerListResponse)
async def get_all_tickers(db: Session = Depends(get_db)):
    """
    언급된 모든 티커 목록을 조회합니다.
    
    Returns:
        TickerListResponse: 티커 목록과 상세 정보
    """
    service = MarketNewsSentimentService(db)
    
    tickers = service.get_all_tickers()
    
    # 티커별 간단한 통계 정보
    ticker_details = []
    for ticker in tickers[:10]:  # 상위 10개만 상세 정보 제공
        summary = service.calculate_ticker_sentiment_summary(ticker, days=7)
        ticker_details.append({
            "ticker": ticker,
            "mention_count": summary.get("mention_count", 0),
            "avg_sentiment": summary.get("avg_sentiment_score", 0.0),
            "sentiment_label": summary.get("sentiment_label", "알 수 없음")
        })
    
    return TickerListResponse(
        total_tickers=len(tickers),
        tickers=tickers,
        ticker_details=ticker_details
    )


@router.get("/ticker/{symbol}", response_model=TickerNewsResponse)
async def get_ticker_news(
    symbol: str = Path(..., description="주식 심볼", example="AAPL"),
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 티커의 뉴스를 조회합니다.
    
    Args:
        symbol: 주식 심볼 (예: "AAPL", "TSLA")
        days: 최근 N일 데이터
        limit: 최대 결과 개수
        offset: 페이징 오프셋
        
    Returns:
        TickerNewsResponse: 티커별 뉴스 + 감성 요약
    """
    service = MarketNewsSentimentService(db)
    
    # 심볼 대문자 변환
    symbol = symbol.upper()
    
    news_list, ticker_summary = service.get_news_by_ticker(symbol, days, limit, offset)
    
    if not news_list:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}' 티커의 뉴스를 찾을 수 없습니다."
        )
    
    return TickerNewsResponse(
        ticker=symbol,
        ticker_sentiment_summary=ticker_summary,
        news=news_list
    )


@router.get("/tickers/ranking", response_model=TickerRankingResponse)
async def get_ticker_ranking(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="랭킹 계산 기간"),
    top_count: int = Query(10, ge=1, le=50, description="상위 티커 개수"),
    bottom_count: int = Query(10, ge=1, le=50, description="하위 티커 개수"),
    min_mentions: int = Query(2, ge=1, description="최소 언급 횟수")
):
    """
    티커별 감성 랭킹을 조회합니다.
    
    가장 "핫한" 티커(긍정적 감성)와 "차가운" 티커(부정적 감성)를 제공합니다.
    
    Args:
        days: 랭킹 계산 기간 (기본 7일)
        top_count: 상위 티커 개수 (기본 10개)
        bottom_count: 하위 티커 개수 (기본 10개)
        min_mentions: 최소 언급 횟수 (기본 2회)
        
    Returns:
        TickerRankingResponse: 티커별 감성 랭킹
    """
    service = MarketNewsSentimentService(db)
    
    hot_tickers, cold_tickers = service.calculate_ticker_sentiment_ranking(
        days, top_count, bottom_count, min_mentions
    )
    
    return TickerRankingResponse(
        ranking_period=f"최근 {days}일",
        total_tickers=len(hot_tickers) + len(cold_tickers),
        hot_tickers=hot_tickers,
        cold_tickers=cold_tickers
    )

# =============================================================================
# 크로스 분석 API
# =============================================================================

@router.get("/topic/{topic}/tickers", response_model=CrossAnalysisResponse)
async def get_topic_related_tickers(
    topic: str = Path(..., description="주제명", example="Technology"),
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="분석 기간"),
    limit: int = Query(10, ge=1, le=50, description="관련 티커 개수")
):
    """
    특정 주제와 관련된 티커들을 조회합니다.
    
    Args:
        topic: 주제명
        days: 분석 기간
        limit: 관련 티커 개수
        
    Returns:
        CrossAnalysisResponse: 주제↔티커 크로스 분석 결과
    """
    service = MarketNewsSentimentService(db)
    
    related_tickers = service.get_tickers_by_topic(topic, days, limit)
    topic_summary = service.calculate_topic_sentiment_summary(topic, days)
    
    if not related_tickers:
        raise HTTPException(
            status_code=404,
            detail=f"'{topic}' 주제와 관련된 티커를 찾을 수 없습니다."
        )
    
    return CrossAnalysisResponse(
        analysis_type="topic_to_tickers",
        primary_key=topic,
        related_items=related_tickers,
        sentiment_summary=topic_summary
    )


@router.get("/ticker/{symbol}/topics", response_model=CrossAnalysisResponse)
async def get_ticker_related_topics(
    symbol: str = Path(..., description="주식 심볼", example="AAPL"),
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="분석 기간"),
    limit: int = Query(10, ge=1, le=50, description="관련 주제 개수")
):
    """
    특정 티커와 관련된 주제들을 조회합니다.
    
    Args:
        symbol: 주식 심볼
        days: 분석 기간
        limit: 관련 주제 개수
        
    Returns:
        CrossAnalysisResponse: 티커↔주제 크로스 분석 결과
    """
    service = MarketNewsSentimentService(db)
    
    # 심볼 대문자 변환
    symbol = symbol.upper()
    
    related_topics = service.get_topics_by_ticker(symbol, days, limit)
    ticker_summary = service.calculate_ticker_sentiment_summary(symbol, days)
    
    if not related_topics:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}' 티커와 관련된 주제를 찾을 수 없습니다."
        )
    
    return CrossAnalysisResponse(
        analysis_type="ticker_to_topics",
        primary_key=symbol,
        related_items=related_topics,
        sentiment_summary=ticker_summary
    )

# =============================================================================
# 정보 및 헬프 API
# =============================================================================

@router.get("/info", response_model=dict)
async def get_api_info(db: Session = Depends(get_db)):
    """
    Market Sentiment API 정보를 제공합니다.
    
    Returns:
        dict: API 정보 및 사용 가능한 엔드포인트
    """
    service = MarketNewsSentimentService(db)
    
    # 기본 통계 정보
    batch_info = service.get_batch_info()
    topics = service.get_all_topics()
    tickers = service.get_all_tickers()
    
    return {
        "api_name": "Market Sentiment Analysis API",
        "version": "1.0.0",
        "description": "시장 뉴스 감성 분석 데이터를 제공합니다",
        "features": [
            "JSONB 기반 복합 데이터 처리",
            "주제별/티커별 감성 분석",
            "실시간 감성 랭킹",
            "크로스 분석 (주제↔티커)",
            "다양한 필터링 옵션"
        ],
        "data_summary": {
            "latest_batch_id": batch_info.get("latest_batch_id"),
            "collection_date": batch_info.get("collection_date"),
            "total_topics": len(topics),
            "total_tickers": len(tickers)
        },
        "endpoints": {
            "news": {
                "list": "/api/v1/market-sentiment/",
                "latest": "/api/v1/market-sentiment/latest",
                "batch": "/api/v1/market-sentiment/batch/{batch_id}",
                "bullish": "/api/v1/market-sentiment/bullish",
                "bearish": "/api/v1/market-sentiment/bearish",
                "neutral": "/api/v1/market-sentiment/neutral"
            },
            "topics": {
                "list": "/api/v1/market-sentiment/topics",
                "detail": "/api/v1/market-sentiment/topic/{topic}",
                "ranking": "/api/v1/market-sentiment/topics/ranking"
            },
            "tickers": {
                "list": "/api/v1/market-sentiment/tickers",
                "detail": "/api/v1/market-sentiment/ticker/{symbol}",
                "ranking": "/api/v1/market-sentiment/tickers/ranking"
            },
            "cross_analysis": {
                "topic_to_tickers": "/api/v1/market-sentiment/topic/{topic}/tickers",
                "ticker_to_topics": "/api/v1/market-sentiment/ticker/{symbol}/topics"
            }
        },
        "sample_topics": topics[:5],
        "sample_tickers": tickers[:5]
    }


@router.get("/stats", response_model=dict)
async def get_sentiment_stats(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="통계 계산 기간")
):
    """
    감성 분석 통계를 제공합니다.
    
    Args:
        days: 통계 계산 기간
        
    Returns:
        dict: 감성 분석 통계 정보
    """
    service = MarketNewsSentimentService(db)
    
    # 전체 뉴스 통계
    all_news, total_count = service.get_news_list(days=days, limit=1000)
    
    # 감성별 분류
    bullish_news = service.get_sentiment_filtered_news("bullish", days, 1000)
    bearish_news = service.get_sentiment_filtered_news("bearish", days, 1000)
    neutral_news = service.get_sentiment_filtered_news("neutral", days, 1000)
    
    # 평균 감성 점수 계산
    sentiment_scores = [
        news.get("overall_sentiment_score", 0) 
        for news in all_news 
        if news.get("overall_sentiment_score") is not None
    ]
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
    
    return {
        "period": f"최근 {days}일",
        "total_news": total_count,
        "sentiment_distribution": {
            "bullish": len(bullish_news),
            "bearish": len(bearish_news),
            "neutral": len(neutral_news)
        },
        "sentiment_percentages": {
            "bullish": round(len(bullish_news) / total_count * 100, 1) if total_count > 0 else 0,
            "bearish": round(len(bearish_news) / total_count * 100, 1) if total_count > 0 else 0,
            "neutral": round(len(neutral_news) / total_count * 100, 1) if total_count > 0 else 0
        },
        "average_sentiment_score": round(avg_sentiment, 4),
        "market_mood": "긍정적" if avg_sentiment > 0.1 else "부정적" if avg_sentiment < -0.1 else "중립적",
        "market_mood_emoji": "📈" if avg_sentiment > 0.1 else "📉" if avg_sentiment < -0.1 else "➡️"
    }
    
    # 감성 라벨 파싱
    labels_list = None
    if sentiment_labels:
        labels_list = [label.strip() for label in sentiment_labels.split(",")]
    
    # 뉴스 목록 조회
    news_list, total_count = service.get_news_list(
        days=days, limit=limit, offset=offset,
        min_sentiment=min_sentiment, max_sentiment=max_sentiment,
        sentiment_labels=labels_list, sort_by=sort_by, order=order
    )
    
    # 배치 정보 조회
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=total_count,
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )


@router.get("/latest", response_model=MarketSentimentListResponse)
async def get_latest_news(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    최신 뉴스만 조회합니다 (최근 24시간).
    
    Returns:
        MarketSentimentListResponse: 최신 뉴스 목록
    """
    service = MarketNewsSentimentService(db)
    
    news_list, total_count = service.get_news_list(
        days=1, limit=limit, offset=offset, sort_by="time_published", order="desc"
    )
    
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=total_count,
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )


@router.get("/batch/{batch_id}", response_model=MarketSentimentListResponse)
async def get_batch_news(
    batch_id: int = Path(..., description="배치 ID", example=2),
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 배치 ID의 뉴스를 조회합니다.
    
    Args:
        batch_id: 조회할 배치 ID
        limit: 최대 결과 개수
        offset: 페이징 오프셋
        
    Returns:
        MarketSentimentListResponse: 해당 배치의 뉴스 목록
    """
    service = MarketNewsSentimentService(db)
    
    news_list = service.get_news_by_batch(batch_id, limit, offset)
    
    if not news_list:
        raise HTTPException(
            status_code=404,
            detail=f"배치 ID {batch_id}의 데이터를 찾을 수 없습니다."
        )
    
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=len(news_list),
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )

# =============================================================================
# 감성 필터 API
# =============================================================================

@router.get("/bullish", response_model=MarketSentimentListResponse)
async def get_bullish_news(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    긍정적인 뉴스만 조회합니다 (감성 점수 > 0.1).
    
    Returns:
        MarketSentimentListResponse: 긍정적인 뉴스 목록
    """
    service = MarketNewsSentimentService(db)
    
    news_list = service.get_sentiment_filtered_news("bullish", days, limit, offset)
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=len(news_list),
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )


@router.get("/bearish", response_model=MarketSentimentListResponse)
async def get_bearish_news(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    부정적인 뉴스만 조회합니다 (감성 점수 < -0.1).
    
    Returns:
        MarketSentimentListResponse: 부정적인 뉴스 목록
    """
    service = MarketNewsSentimentService(db)
    
    news_list = service.get_sentiment_filtered_news("bearish", days, limit, offset)
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=len(news_list),
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )


@router.get("/neutral", response_model=MarketSentimentListResponse)
async def get_neutral_news(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    중립적인 뉴스만 조회합니다 (감성 점수 -0.1 ~ 0.1).
    
    Returns:
        MarketSentimentListResponse: 중립적인 뉴스 목록
    """
    service = MarketNewsSentimentService(db)
    
    news_list = service.get_sentiment_filtered_news("neutral", days, limit, offset)
    batch_info = service.get_batch_info()
    
    return MarketSentimentListResponse(
        total_count=len(news_list),
        batch_info=BatchInfo(**batch_info),
        news=news_list
    )

# =============================================================================
# Topic 관련 API
# =============================================================================

@router.get("/topics", response_model=TopicListResponse)
async def get_all_topics(db: Session = Depends(get_db)):
    """
    사용 가능한 모든 주제 목록을 조회합니다.
    
    Returns:
        TopicListResponse: 주제 목록과 상세 정보
    """
    service = MarketNewsSentimentService(db)
    
    topics = service.get_all_topics()
    
    # 주제별 간단한 통계 정보
    topic_details = []
    for topic in topics[:10]:  # 상위 10개만 상세 정보 제공
        summary = service.calculate_topic_sentiment_summary(topic, days=7)
        topic_details.append({
            "topic": topic,
            "news_count": summary.get("total_news", 0),
            "avg_sentiment": summary.get("avg_sentiment_score", 0.0),
            "sentiment_label": summary.get("sentiment_label", "알 수 없음")
        })
    
    return TopicListResponse(
        total_topics=len(topics),
        topics=topics,
        topic_details=topic_details
    )


@router.get("/topic/{topic}", response_model=TopicNewsResponse)
async def get_topic_news(
    topic: str = Path(..., description="주제명", example="Technology"),
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="최근 N일 데이터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이징 오프셋")
):
    """
    특정 주제의 뉴스를 조회합니다.
    
    Args:
        topic: 주제명 (예: "Technology", "Energy & Transportation")
        days: 최근 N일 데이터
        limit: 최대 결과 개수
        offset: 페이징 오프셋
        
    Returns:
        TopicNewsResponse: 주제별 뉴스 + 감성 요약
    """
    service = MarketNewsSentimentService(db)
    
    news_list, topic_summary = service.get_news_by_topic(topic, days, limit, offset)
    
    if not news_list:
        raise HTTPException(
            status_code=404,
            detail=f"'{topic}' 주제의 뉴스를 찾을 수 없습니다."
        )
    
    return TopicNewsResponse(
        topic=topic,
        topic_sentiment_summary=topic_summary,
        news=news_list
    )


@router.get("/topics/ranking", response_model=TopicRankingResponse)
async def get_topic_ranking(
    db: Session = Depends(get_db),
    days: int = Query(7, ge=1, le=30, description="랭킹 계산 기간"),
    top_count: int = Query(10, ge=1, le=50, description="상위 주제 개수"),
    bottom_count: int = Query(10, ge=1, le=50, description="하위 주제 개수"),
    min_mentions: int = Query(2, ge=1, description="최소 언급 횟수")
):
    """
    주제별 감성 랭킹을 조회합니다.
    
    가장 "핫한" 주제(긍정적 감성)와 "차가운" 주제(부정적 감성)를 제공합니다.
    
    Args:
        days: 랭킹 계산 기간 (기본 7일)
        top_count: 상위 주제 개수 (기본 10개)
        bottom_count: 하위 주제 개수 (기본 10개)
        min_mentions: 최소 언급 횟수 (기본 2회)
        
    Returns:
        TopicRankingResponse: 주제별 감성 랭킹
    """
    service = MarketNewsSentimentService(db)
    
    hot_topics, cold_topics = service.calculate_topic_sentiment_ranking(
        days, top_count, bottom_count, min_mentions
    )
    
    return TopicRankingResponse(
        ranking_period=f"최근 {days}일",
        total_topics=len(hot_topics) + len(cold_topics),
        hot_topics=hot_topics,
        cold_topics=cold_topics
    )

# =============================================================================
# 감정 점수 추이 API (프론트엔드 차트용)
# =============================================================================

@router.get("/sentiment-trends", response_model=SentimentTrendsResponse)
async def get_sentiment_trends(
    db: Session = Depends(get_db),
    interval: str = Query("daily", pattern="^(hourly|daily)$", description="시간 간격 (hourly/daily)"),
    days: int = Query(7, ge=1, le=30, description="분석 기간"),
    tickers: Optional[str] = Query(None, description="분석할 티커들 (쉼표 구분, 예: AAPL,TSLA,NVDA)"),
    topics: Optional[str] = Query(None, description="분석할 주제들 (쉼표 구분, 예: Technology,Energy)")
):
    """
    프론트엔드 차트용 감정 점수 추이 데이터를 제공합니다.
    
    원시 감정 점수 수치를 시간대별로 제공하여 차트나 그래프를 그릴 수 있습니다.
    
    Args:
        interval: 시간 간격 ('hourly' 또는 'daily')
        days: 분석 기간 (1-30일)
        tickers: 특정 티커들의 추이 분석 (선택사항)
        topics: 특정 주제들의 추이 분석 (선택사항)
        
    Returns:
        SentimentTrendsResponse: 시간대별 감정 점수 추이 (원시 수치)
        
    Example:
        - GET /sentiment-trends?interval=daily&days=7
        - GET /sentiment-trends?interval=hourly&days=3&tickers=AAPL,TSLA
        - GET /sentiment-trends?days=14&topics=Technology,Energy
    """
    service = MarketNewsSentimentService(db)
    
    # 티커 목록 파싱
    ticker_list = None
    if tickers:
        ticker_list = [ticker.strip().upper() for ticker in tickers.split(",")]
    
    # 주제 목록 파싱
    topic_list = None
    if topics:
        topic_list = [topic.strip() for topic in topics.split(",")]
    
    # 감정 점수 추이 계산
    trends_data = service.get_sentiment_trends(
        interval=interval,
        days=days,
        tickers=ticker_list,
        topics=topic_list
    )
    
    return SentimentTrendsResponse(**trends_data)