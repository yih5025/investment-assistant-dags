from fastapi import APIRouter

# 각 도메인별 엔드포인트 라우터들을 import
from .endpoints import earnings_calendar_endpoint
from .endpoints import earnings_calendar_news_endpoint
from .endpoints import truth_social_endpoint
from .endpoints import market_news_endpoint
from .endpoints import financial_news_endpoint

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
        "total_endpoints": 5,
        "implemented_domains": ["earnings-calendar", "earnings-calendar-news", "truth-social", "market-news", "financial-news"],
        "planned_domains": ["crypto-prices", "crypto-markets", "stocks-trades", "stocks-gainers", "news-market", "news-sentiment"],
        "database_tables": {
            "earnings_calendar": "실적 발표 일정",
            "earnings_calendar_news": "실적 관련 뉴스",
            "truth_social_posts": "Truth Social 게시물",
            "financial_news": "Finnhub 금융 뉴스",
            "market_news": "News API 뉴스",
        }
    }

@api_router.get("/test", tags=["API TEST"])
async def test():
    return {"message": "API v1 test"}