from fastapi import APIRouter

# 각 도메인별 엔드포인트 라우터들을 import
# 현재는 earnings_calendar만 구현되어 있고, 나머지는 점진적으로 추가 예정
from .endpoints import earnings_calendar
# from .endpoints import crypto    # 향후 구현
# from .endpoints import stocks    # 향후 구현  
# from .endpoints import news      # 향후 구현
# from .endpoints import market    # 향후 구현
# from .endpoints import health    # 향후 구현

# API v1 메인 라우터 생성
api_router = APIRouter()

# === 실적 발표 캘린더 API ===
api_router.include_router(
    earnings_calendar.router,
    prefix="/earnings-calendar",  # /api/v1/earnings-calendar로 시작하는 모든 엔드포인트
    tags=["earnings-calendar"]   # API 문서에서 "earnings-calendar" 그룹으로 분류
)

# === 향후 추가될 API들 ===
# 암호화폐 가격 API
"""
api_router.include_router(
    crypto_prices.router,
    prefix="/crypto-prices",
    tags=["crypto-prices"]
)
"""

# 암호화폐 마켓 API
"""
api_router.include_router(
    crypto_markets.router,
    prefix="/crypto-markets", 
    tags=["crypto-markets"]
)
"""

# 주식 거래 데이터 API  
"""
api_router.include_router(
    stocks_trades.router,
    prefix="/stocks-trades",
    tags=["stocks-trades"]
)
"""

# 주식 상승률 API
"""
api_router.include_router(
    stocks_gainers.router,
    prefix="/stocks-gainers",
    tags=["stocks-gainers"]
)
"""

# 시장 뉴스 API
"""
api_router.include_router(
    news_market.router,
    prefix="/news-market",
    tags=["news-market"]
)
"""

# 뉴스 감성분석 API
"""
api_router.include_router(
    news_sentiment.router,
    prefix="/news-sentiment",
    tags=["news-sentiment"]
)
"""

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
        "message": "Investment Assistant API v1",
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
            "crypto-prices": {
                "description": "암호화폐 가격 데이터 API (향후 구현)",
                "table": "bithumb_ticker",
                "status": "coming_soon"
            },
            "crypto-markets": {
                "description": "암호화폐 마켓 정보 API (향후 구현)",
                "table": "market_code_bithumb", 
                "status": "coming_soon"
            },
            "stocks-trades": {
                "description": "주식 거래 데이터 API (향후 구현)",
                "table": "finnhub_trades",
                "status": "coming_soon"
            },
            "stocks-gainers": {
                "description": "주식 상승률 API (향후 구현)",
                "table": "top_gainers",
                "status": "coming_soon"
            },
            "news-market": {
                "description": "시장 뉴스 API (향후 구현)",
                "table": "market_news",
                "status": "coming_soon"
            },
            "news-sentiment": {
                "description": "뉴스 감성분석 API (향후 구현)",
                "table": "market_news_sentiment",
                "status": "coming_soon"
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
        "total_endpoints": 1,  # 현재 구현된 도메인 수
        "implemented_domains": ["earnings-calendar"],
        "planned_domains": ["crypto-prices", "crypto-markets", "stocks-trades", "stocks-gainers", "news-market", "news-sentiment"],
        "database_tables": {
            "earnings_calendar": "실적 발표 일정",
            "bithumb_ticker": "빗썸 암호화폐 데이터",
            "finnhub_trades": "Finnhub 주식 거래 데이터",
            "market_news": "시장 뉴스",
            "truth_social_posts": "Truth Social 게시물",
            "x_posts": "X(Twitter) 게시물"
        }
    }