from fastapi import APIRouter
from app.config import settings

# 도메인별 엔드포인트 라우터 imports
from .endpoints import (
    earnings_calendar_endpoint,
    earnings_calendar_news_endpoint,
    truth_social_endpoint,
    market_news_endpoint,
    financial_news_endpoint,
    company_news_endpoint,
    market_news_sentiment_endpoint,
    inflation_endpoint,
    federal_funds_rate_endpoint,
    cpi_endpoint,
    x_posts_endpoint,
    balance_sheet_endpoint,
    treasury_yield_endpoint,
    websocket_endpoint,
)

# API v1 메인 라우터 생성
api_router = APIRouter()

# 라우터 설정 구성 (각 도메인의 prefix, 설명, 카테고리 지정)
ROUTER_CONFIGS = [
    # 뉴스 관련 API
    {
        "router": market_news_endpoint.router,
        "prefix": "/market-news",
        "tag": "Market News",
        "category": "뉴스",
        "description": "시장 뉴스 API"
    },
    {
        "router": financial_news_endpoint.router,
        "prefix": "/financial-news",
        "tag": "Financial News",
        "category": "뉴스",
        "description": "금융 뉴스 API"
    },
    {
        "router": company_news_endpoint.router,
        "prefix": "/company-news",
        "tag": "Company News",
        "category": "뉴스",
        "description": "기업 뉴스 API"
    },
    {
        "router": market_news_sentiment_endpoint.router,
        "prefix": "/market-news-sentiment",
        "tag": "Market News Sentiment",
        "category": "뉴스",
        "description": "시장 뉴스 감성 분석 API"
    },

    # 소셜 미디어 API
    {
        "router": truth_social_endpoint.router,
        "prefix": "/truth-social",
        "tag": "Truth Social",
        "category": "소셜미디어",
        "description": "Truth Social API"
    },
    {
        "router": x_posts_endpoint.router,
        "prefix": "/x-posts",
        "tag": "X Posts",
        "category": "소셜미디어",
        "description": "X Posts API"
    },

    # 실적 관련 API
    {
        "router": earnings_calendar_endpoint.router,
        "prefix": "/earnings-calendar",
        "tag": "Earnings Calendar",
        "category": "실적정보",
        "description": "실적 발표 캘린더 API"
    },
    {
        "router": earnings_calendar_news_endpoint.router,
        "prefix": "/earnings-calendar-news",
        "tag": "Earnings Calendar News",
        "category": "실적정보",
        "description": "실적 캘린더 뉴스 API"
    },

    # 경제 지표 API
    {
        "router": inflation_endpoint.router,
        "prefix": "/inflation",
        "tag": "Inflation",
        "category": "경제지표",
        "description": "인플레이션 데이터 API"
    },
    {
        "router": federal_funds_rate_endpoint.router,
        "prefix": "/federal-funds-rate",
        "tag": "Federal Funds Rate",
        "category": "경제지표",
        "description": "연방기금금리 API"
    },
    {
        "router": cpi_endpoint.router,
        "prefix": "/cpi",
        "tag": "CPI",
        "category": "경제지표",
        "description": "소비자물가지수 API"
    },

    # 재무/국채 API
    {
        "router": balance_sheet_endpoint.router,
        "prefix": "/balance-sheet",
        "tag": "Balance Sheet",
        "category": "재무제표",
        "description": "재무제표 API"
    },
    {
        "router": treasury_yield_endpoint.router,
        "prefix": "/treasury-yield",
        "tag": "Treasury Yield",
        "category": "국채수익률",
        "description": "국채 수익률 API"
    },

    # WebSocket 실시간 데이터 API (이미 router 내부에 prefix="/ws" 보유)
    {
        "router": websocket_endpoint.router,
        "prefix": "",
        "tag": "WebSocket",
        "category": "실시간",
        "description": "실시간 WebSocket 데이터 API"
    },
]

# 라우터 등록 (각 엔드포인트 모듈의 자체 태그 사용)
for config in ROUTER_CONFIGS:
    api_router.include_router(
        config["router"],
        prefix=config["prefix"],
    )


# ========== API 정보 엔드포인트 ==========

@api_router.get("/", tags=["API Info"], summary="API v1 정보")
async def api_v1_info():
    """
    API v1 기본 정보와 사용 가능한 엔드포인트 목록을 반환합니다.
    
    Returns:
        dict: API 버전, 설명, 사용 가능한 엔드포인트 목록
    """
    # 동적으로 엔드포인트 정보 생성
    available_endpoints = {}
    for config in ROUTER_CONFIGS:
        key = config["prefix"].lstrip("/") or "ws"
        available_endpoints[key] = {
            "description": config["description"],
            "prefix": f"{settings.api_v1_prefix}{config['prefix']}",
            "tag": config.get("tag"),
            "category": config.get("category"),
        }

    # 카테고리 매핑
    categories = {}
    for config in ROUTER_CONFIGS:
        categories.setdefault(config["category"], []).append(config["prefix"].lstrip("/"))

    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "description": "투자 도우미 서비스의 메인 API",
        "base_url": settings.api_v1_prefix,
        "total_endpoints": len(ROUTER_CONFIGS),
        "categories": categories,
        "available_endpoints": available_endpoints,
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_json": "/openapi.json"
        }
    }


@api_router.get("/health", tags=["API Info"], summary="API 상태 확인")
async def health_check():
    """
    API 서버의 상태를 확인합니다.
    
    Returns:
        dict: API 상태 정보
    """
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "uptime": "operational",
        "docs": "/docs",
    }


@api_router.get("/stats", tags=["API Info"], summary="API 통계 정보")
async def api_stats():
    """
    API 사용 통계 및 구성 정보를 반환합니다.
    
    Returns:
        dict: API 구성 통계 및 기능 정보
    """
    # 카테고리별 라우터 수 계산
    category_counts = {}
    for config in ROUTER_CONFIGS:
        category_counts[config["category"]] = category_counts.get(config["category"], 0) + 1

    return {
        "api_summary": {
            "total_routers": len(ROUTER_CONFIGS),
            "categories": category_counts,
        },
        "implemented_domains": [config["prefix"].lstrip("/") for config in ROUTER_CONFIGS],
        "base_url": settings.api_v1_prefix,
        "documentation": {"swagger_ui": "/docs", "redoc": "/redoc"},
        "features": [
            "pagination",
            "filtering",
            "sorting",
            "real_time",
            "sentiment_analysis",
        ],
    }


# 개발/테스트용 엔드포인트
@api_router.get("/test", tags=["Development"], summary="API 테스트")
async def api_test():
    """
    API 연결 테스트용 엔드포인트입니다.
    
    Returns:
        dict: 테스트 응답 메시지
    """
    return {
        "message": "API v1 연결 테스트 성공",
        "timestamp": "2025-01-27",
        "status": "ok"
    }