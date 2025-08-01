from fastapi import APIRouter

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
    x_posts_endpoint
)

# API v1 메인 라우터 생성
api_router = APIRouter()

# 라우터 설정 구성
ROUTER_CONFIGS = [
    # 뉴스 관련 API
    {
        "router": market_news_endpoint.router,
        "prefix": "/market-news",
        "tags": ["market-news"],
        "description": "시장 뉴스 API"
    },
    {
        "router": financial_news_endpoint.router,
        "prefix": "/financial-news", 
        "tags": ["financial-news"],
        "description": "금융 뉴스 API"
    },
    {
        "router": company_news_endpoint.router,
        "prefix": "/company-news",
        "tags": ["company-news"], 
        "description": "기업 뉴스 API"
    },
    {
        "router": market_news_sentiment_endpoint.router,
        "prefix": "/market-news-sentiment",
        "tags": ["market-news-sentiment"],
        "description": "시장 뉴스 감성 분석 API"
    },
    
    # 소셜 미디어 API
    {
        "router": truth_social_endpoint.router,
        "prefix": "/truth-social",
        "tags": ["truth-social"],
        "description": "Truth Social API"
    },
    {
        "router": x_posts_endpoint.router,
        "prefix": "/x-posts",
        "tags": ["x-posts"],
        "description": "X Posts API"
    },
    
    # 실적 관련 API
    {
        "router": earnings_calendar_endpoint.router,
        "prefix": "/earnings-calendar",
        "tags": ["earnings-calendar"],
        "description": "실적 발표 캘린더 API"
    },
    {
        "router": earnings_calendar_news_endpoint.router,
        "prefix": "/earnings-calendar-news",
        "tags": ["earnings-calendar-news"],
        "description": "실적 캘린더 뉴스 API"
    },
    
    # 경제 지표 API
    {
        "router": inflation_endpoint.router,
        "prefix": "/inflation",
        "tags": ["inflation"],
        "description": "인플레이션 데이터 API"
    },
    {
        "router": federal_funds_rate_endpoint.router,
        "prefix": "/federal-funds-rate",
        "tags": ["federal-funds-rate"],
        "description": "연방기금금리 API"
    },
    {
        "router": cpi_endpoint.router,
        "prefix": "/cpi",
        "tags": ["cpi"],
        "description": "소비자물가지수 API"
    }
]

# 라우터 등록
for config in ROUTER_CONFIGS:
    api_router.include_router(
        config["router"],
        prefix=config["prefix"],
        tags=config["tags"]
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
        key = config["prefix"].lstrip("/")
        available_endpoints[key] = {
            "description": config["description"],
            "prefix": config["prefix"],
            "tag": config["tags"][0] if config["tags"] else None
        }
    
    return {
        "service": "Investment Assistant API",
        "version": "1.0.0",
        "description": "투자 도우미 서비스의 메인 API",
        "total_endpoints": len(ROUTER_CONFIGS),
        "categories": {
            "뉴스": ["market-news", "financial-news", "company-news", "market-news-sentiment"],
            "소셜미디어": ["truth-social", "x-posts"], 
            "실적정보": ["earnings-calendar", "earnings-calendar-news"],
            "경제지표": ["inflation", "federal-funds-rate", "cpi"]
        },
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
        "service": "investment-assistant-api",
        "version": "1.0.0",
        "uptime": "operational"
    }


@api_router.get("/stats", tags=["API Info"], summary="API 통계 정보")
async def api_stats():
    """
    API 사용 통계 및 구성 정보를 반환합니다.
    
    Returns:
        dict: API 구성 통계 및 기능 정보
    """
    return {
        "api_summary": {
            "total_routers": len(ROUTER_CONFIGS),
            "categories": {
                "뉴스_관련": 4,
                "소셜미디어": 2, 
                "실적_정보": 2,
                "경제_지표": 3
            }
        },
        "implemented_domains": [config["prefix"].lstrip("/") for config in ROUTER_CONFIGS],
        "planned_features": [
            "crypto-prices",
            "stock-analysis", 
            "portfolio-management",
            "risk-assessment"
        ],
        "database_integration": {
            "postgresql": "메인 데이터베이스",
            "jsonb_support": "복합 데이터 처리",
            "time_series": "시계열 데이터 최적화"
        },
        "api_features": {
            "pagination": "모든 목록 API에서 지원",
            "filtering": "다양한 필터링 옵션",
            "sorting": "정렬 기능 지원",
            "real_time": "실시간 데이터 업데이트",
            "sentiment_analysis": "뉴스 감성 분석"
        }
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