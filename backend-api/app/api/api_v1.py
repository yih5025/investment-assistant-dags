from fastapi import APIRouter

# 각 도메인별 엔드포인트 라우터들을 import
from .endpoints import earnings_calendar_endpoint
from .endpoints import earnings_calendar_news_endpoint
from .endpoints import truth_social_endpoint
from .endpoints import market_news_endpoint
from .endpoints import financial_news_endpoint
from .endpoints import company_news_endpoint
from .endpoints import market_news_sentiment_endpoint

# API v1 메인 라우터 생성
api_router = APIRouter()

# === 실적 발표 캘린더 API ===
api_router.include_router(
    earnings_calendar_endpoint.router,
    prefix="/earnings-calendar", 
    tags=["earnings-calendar"]  
)

# === 실적 캘린더 뉴스 API ===
api_router.include_router(
    earnings_calendar_news_endpoint.router,
    prefix="/earnings-calendar-news",
    tags=["earnings-calendar-news"]
)

# === Truth Social API ===
api_router.include_router(
    truth_social_endpoint.router,
    prefix="/truth-social",
    tags=["truth-social"]
)

# === Market News API ===
api_router.include_router(
    market_news_endpoint.router,
    prefix="/market-news",
    tags=["market-news"]
)

# === Financial News API ===
api_router.include_router(
    financial_news_endpoint.router,
    prefix="/financial-news",
    tags=["financial-news"]
)

# === Company News API ===
api_router.include_router(
    company_news_endpoint.router,
    prefix="/company-news",
    tags=["company-news"]
)

# === Market News Sentiment API ===
api_router.include_router(
    market_news_sentiment_endpoint.router,
    prefix="/market-news-sentiment",
    tags=["market-news-sentiment"]
)

# API v1 정보 엔드포인트
@api_router.get("/", tags=["API Info"])
async def api_v1_info():
    """
    API v1 정보를 반환합니다.
    
    현재 사용 가능한 엔드포인트 목록과 기본 정보를 제공합니다.
    
    Returns:
        dict: API v1 정보 및 사용 가능한 엔드포인트 목록
    """
    return {
        "message": "api v1",
        "version": "1.0.0",
        "available_endpoints": {
            "earnings-calendar": {
                "description": "실적 발표 일정 캘린더 API",
                "endpoints": [
                    "GET /earnings-calendar/ - 실적 발표 일정 조회",
                    "GET /earnings-calendar/today - 오늘의 실적 발표",
                    "GET /earnings-calendar/upcoming - 다가오는 실적 발표",
                    "GET /earnings-calendar/symbol/{symbol} - 특정 심볼의 실적 일정"
                ]
            },
            "earnings-calendar-news": {
                "description": "실적 캘린더 뉴스 API",
                "endpoints": [
                    "GET /earnings-calendar-news/symbol/{symbol}/report-date/{date} - 특정 실적 뉴스 조회",
                    "GET /earnings-calendar-news/symbol/{symbol} - 기업별 실적 뉴스 조회",
                    "GET /earnings-calendar-news/recent-summary?symbol={symbol} - 특정 기업 최근 실적 뉴스"
                ]
            },
            "truth-social": {
                "description": "Truth Social API",
                "endpoints": [
                    "GET /truth-social/posts - Truth Social 게시물 조회",
                    "GET /truth-social/tags - Truth Social 태그 조회",
                    "GET /truth-social/trends - Truth Social 트렌드 조회"
                ]
            },
            "market-news": {
                "description": "시장 뉴스 API",
                "endpoints": [
                    "GET /market-news/ - 뉴스 목록 조회",
                    "GET /market-news/search - 뉴스 검색",
                    "GET /market-news/recent - 최근 뉴스",
                    "GET /market-news/{source}/{url} - 특정 뉴스 상세"
                ]
            },
            "financial-news": {
                "description": "금융 뉴스 API",
                "endpoints": [
                    "GET /financial-news/ - 뉴스 목록 조회",
                    "GET /financial-news/search - 뉴스 검색",
                    "GET /financial-news/recent - 최근 뉴스",
                    "GET /financial-news/{category}/{news_id} - 특정 뉴스 상세"
                ]
            },
            "company-news": {
                "description": "기업 뉴스 API",
                "endpoints": [
                    "GET /company-news/trending - 트렌딩 주식 뉴스 조회",
                    "GET /company-news/trending/category/{category} - 트렌딩 주식 뉴스 조회 (카테고리별)",
                    "GET /company-news/symbol/{symbol} - 특정 주식 뉴스 조회",
                ]
            },
            "market-news-sentiment": {
                "description": "시장 뉴스 감성 분석 API (JSONB 기반)",
                "endpoints": [
                    "GET /market-news-sentiment/ - 전체 뉴스 감성 분석 조회",
                    "GET /market-news-sentiment/latest - 최신 뉴스 (24시간)",
                    "GET /market-news-sentiment/batch/{batch_id} - 특정 배치 뉴스",
                    "GET /market-news-sentiment/bullish - 긍정적 뉴스",
                    "GET /market-news-sentiment/bearish - 부정적 뉴스", 
                    "GET /market-news-sentiment/neutral - 중립적 뉴스",
                    "GET /market-news-sentiment/topics - 모든 주제 목록",
                    "GET /market-news-sentiment/topic/{topic} - 주제별 뉴스",
                    "GET /market-news-sentiment/topics/ranking - 주제별 감성 랭킹",
                    "GET /market-news-sentiment/tickers - 모든 티커 목록",
                    "GET /market-news-sentiment/ticker/{symbol} - 티커별 뉴스",
                    "GET /market-news-sentiment/tickers/ranking - 티커별 감성 랭킹",
                    "GET /market-news-sentiment/topic/{topic}/tickers - 주제↔티커 크로스 분석",
                    "GET /market-news-sentiment/ticker/{symbol}/topics - 티커↔주제 크로스 분석",
                    "GET /market-news-sentiment/sentiment-trends - 감정 점수 추이 (차트용)",
                    "GET /market-news-sentiment/info - API 정보",
                    "GET /market-news-sentiment/stats - 감성 분석 통계"
                ]
            }
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    }

# API 통계 엔드포인트 (선택적)
@api_router.get("/stats", tags=["API Info"])
async def api_stats():
    """
    API 사용 통계를 반환합니다.
    
    현재는 기본 정보만 제공하며, 향후 실제 사용량 통계를 추가할 예정입니다.
    
    Returns:
        dict: API 통계 정보
    """
    return {
        "total_endpoints": 6,
        "implemented_domains": ["earnings-calendar", "earnings-calendar-news", "truth-social", "market-news", "financial-news", "company-news", "market-news-sentiment"],
        "planned_domains": ["crypto-prices", "crypto-markets", "stocks-trades", "stocks-gainers"],
        "database_tables": {
            "earnings_calendar": "실적 발표 일정",
            "earnings_calendar_news": "실적 관련 뉴스",
            "truth_social_posts": "Truth Social 게시물",
            "financial_news": "Finnhub 금융 뉴스",
            "market_news": "News API 뉴스",
            "company_news": "Finnhub 기업 뉴스",
            "market_news_sentiment": "시장 뉴스 감성 분석 (JSONB)"
        },
        "api_features": {
            "market-news-sentiment": {
                "total_endpoints": 17,
                "features": [
                    "JSONB 기반 복합 데이터 처리",
                    "주제별/티커별 감성 분석",
                    "실시간 감성 랭킹",
                    "크로스 분석 (주제↔티커)",
                    "프론트엔드 차트용 추이 데이터",
                    "다양한 필터링 옵션"
                ]
            }
        }
    }

@api_router.get("/test", tags=["API TEST"])
async def test():
    return {"message": "API v1 test"}