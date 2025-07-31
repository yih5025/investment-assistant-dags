from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.financial_news_service import FinancialNewsService
from app.schemas.financial_news_schema import (
    FinancialNewsResponse,
    FinancialNewsListResponse,
    FinancialNewsSearchResponse,
    CategoriesStatsResponse,
    CategoryType
)

# 라우터 생성 (태그로 API 문서 그룹화)
router = APIRouter(
    prefix="/api/v1/financial-news",
    tags=["Financial News"],
    responses={
        404: {"description": "뉴스를 찾을 수 없습니다"},
        422: {"description": "잘못된 요청 파라미터"},
        500: {"description": "서버 내부 오류"}
    }
)


@router.get("/", response_model=FinancialNewsListResponse, summary="금융 뉴스 목록 조회")
async def get_financial_news_list(
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수 (최대 100)"),
    categories: Optional[List[CategoryType]] = Query(None, description="카테고리 필터 (crypto, forex, merger, general)"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    symbols: Optional[List[str]] = Query(None, description="관련 종목 필터 (BTCUSD, AAPL 등)"),
    sources: Optional[List[str]] = Query(None, description="뉴스 소스 필터"),
    db: Session = Depends(get_db)
):
    """
    **Finnhub 금융 뉴스 목록을 조회합니다.**
    
    ### 주요 기능:
    - **카테고리 필터링**: crypto(암호화폐), forex(외환), merger(인수합병), general(일반)
    - **종목 기반 필터링**: 특정 종목이 언급된 뉴스만 조회
    - **날짜 범위 필터링**: 특정 기간의 뉴스만 조회
    - **소스 필터링**: 특정 뉴스 소스만 포함
    - **페이징**: 대용량 데이터 효율적 처리
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/?categories=crypto&limit=10
    GET /api/v1/financial-news/?symbols=BTCUSD,ETHUSD&start_date=2025-07-01
    GET /api/v1/financial-news/?categories=merger&sources=Reuters
    ```
    
    ### 응답 구조:
    - **total**: 전체 뉴스 개수
    - **items**: 뉴스 목록 (요약 정보)
    - **category**: 현재 조회 카테고리 (단일 카테고리 필터링 시)
    - **page, limit**: 페이징 정보
    - **has_next**: 다음 페이지 존재 여부
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_financial_news_list(
            skip=skip,
            limit=limit,
            categories=categories,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            sources=sources
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"금융 뉴스 목록 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/category/{category}", response_model=FinancialNewsListResponse, summary="카테고리별 뉴스 조회")
async def get_category_financial_news(
    category: CategoryType = Path(..., description="뉴스 카테고리"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    **특정 카테고리의 금융 뉴스를 조회합니다.**
    
    ### 지원 카테고리:
    - **crypto**: 암호화폐 관련 뉴스 (Bitcoin, Ethereum 등)
    - **forex**: 외환 시장 뉴스 (USD, EUR, JPY 등)
    - **merger**: 인수합병 뉴스 (M&A, 기업 매각 등)
    - **general**: 일반 금융 뉴스 (경제 지표, 정책 등)
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/category/crypto?limit=15
    GET /api/v1/financial-news/category/forex?start_date=2025-07-01
    GET /api/v1/financial-news/category/merger?page=2&limit=25
    ```
    
    ### 특징:
    - 해당 카테고리 뉴스만 필터링
    - 최신순 정렬
    - 날짜 범위 추가 필터링 가능
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_category_news(
            category=category,
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"카테고리별 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/recent", response_model=List[FinancialNewsResponse], summary="최근 금융 뉴스 조회")
async def get_recent_financial_news(
    hours: int = Query(24, ge=1, le=168, description="몇 시간 이내 뉴스 (최대 7일)"),
    limit: int = Query(10, ge=1, le=50, description="최대 개수 (최대 50)"),
    categories: Optional[List[CategoryType]] = Query(None, description="대상 카테고리"),
    db: Session = Depends(get_db)
):
    """
    **최근 발행된 금융 뉴스를 조회합니다.**
    
    ### 특징:
    - 설정한 시간 이내에 발행된 뉴스만 조회
    - 전체 정보 포함 (summary, image, url 등)
    - 최신순으로 정렬
    - 카테고리별 필터링 가능
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/recent?hours=6&limit=5
    GET /api/v1/financial-news/recent?categories=crypto,forex
    GET /api/v1/financial-news/recent  # 24시간 이내 최신 10개 (기본값)
    ```
    
    ### 용도:
    - 대시보드 최신 뉴스 섹션
    - 실시간 뉴스 피드
    - 긴급 뉴스 알림 시스템
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_recent_financial_news(
            hours=hours,
            limit=limit,
            categories=categories
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/search", response_model=FinancialNewsSearchResponse, summary="금융 뉴스 검색")
async def search_financial_news(
    q: str = Query(..., min_length=2, description="검색어 (최소 2글자)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    categories: Optional[List[CategoryType]] = Query(None, description="검색 대상 카테고리"),
    start_date: Optional[datetime] = Query(None, description="검색 시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="검색 종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    **금융 뉴스에서 전문 검색을 수행합니다.**
    
    ### 검색 대상:
    - **headline**: 뉴스 헤드라인 (주요 검색 대상)
    - **summary**: 뉴스 요약문
    - **source**: 뉴스 소스명
    
    ### 검색 기능:
    - 대소문자 무시 검색
    - 부분 문자열 매칭
    - 카테고리별 필터링
    - 날짜 범위 제한
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/search?q=Bitcoin
    GET /api/v1/financial-news/search?q=merger&categories=merger
    GET /api/v1/financial-news/search?q=Federal&start_date=2025-07-01
    ```
    
    ### 고급 검색:
    - 여러 키워드: `q=Bitcoin Ethereum` (OR 검색)
    - 카테고리 조합: `categories=crypto,forex`
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.search_financial_news(
            query_text=q,
            skip=skip,
            limit=limit,
            categories=categories,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 검색 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/detail/{category}/{news_id}", response_model=FinancialNewsResponse, summary="뉴스 상세 조회")
async def get_financial_news_detail(
    category: CategoryType = Path(..., description="뉴스 카테고리"),
    news_id: int = Path(..., description="Finnhub 뉴스 ID", ge=1),
    db: Session = Depends(get_db)
):
    """
    **특정 금융 뉴스의 상세 정보를 조회합니다.**
    
    ### 특징:
    - category + news_id 복합키로 고유 뉴스 식별
    - 전체 정보 포함 (이미지, 요약, 원본 URL 등)
    - 관련 종목 리스트 파싱
    - 편의 필드 제공 (short_headline, has_image 등)
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/detail/crypto/12345678901
    GET /api/v1/financial-news/detail/forex/98765432109
    ```
    
    ### 응답 정보:
    - 전체 헤드라인 및 요약
    - 이미지 URL (있는 경우)
    - 관련 종목 목록
    - 원본 뉴스 링크
    - 데이터 수집 시간
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_news_by_id(category=category, news_id=news_id)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"뉴스를 찾을 수 없습니다. category: {category}, news_id: {news_id}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 상세 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/symbol/{symbol}", response_model=FinancialNewsListResponse, summary="종목별 뉴스 조회")
async def get_news_by_symbol(
    symbol: str = Path(..., description="종목 코드 (예: BTCUSD, AAPL)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    categories: Optional[List[CategoryType]] = Query(None, description="검색 대상 카테고리"),
    db: Session = Depends(get_db)
):
    """
    **특정 종목과 관련된 뉴스를 조회합니다.**
    
    ### 특징:
    - 종목 코드가 related 필드에 포함된 뉴스 검색
    - 부분 문자열 매칭 (BTCUSD 검색 시 BTC 포함 뉴스도 매칭)
    - 카테고리별 추가 필터링 가능
    - 최신순 정렬
    
    ### 지원 종목 예시:
    - **암호화폐**: BTCUSD, ETHUSD, ADAUSD
    - **주식**: AAPL, GOOGL, TSLA, NVDA
    - **외환**: EURUSD, GBPUSD, USDJPY
    - **기타**: 모든 관련 종목 코드
    
    ### 사용 예시:
    ```
    GET /api/v1/financial-news/symbol/BTCUSD?categories=crypto
    GET /api/v1/financial-news/symbol/AAPL?limit=15
    GET /api/v1/financial-news/symbol/EURUSD?categories=forex
    ```
    """
    try:
        skip = (page - 1) * limit
        service = FinancialNewsService(db)
        
        result = service.get_news_by_symbol(
            symbol=symbol.upper(),
            skip=skip,
            limit=limit,
            categories=categories
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"종목별 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/categories/stats", response_model=CategoriesStatsResponse, summary="카테고리별 통계")
async def get_categories_statistics(
    db: Session = Depends(get_db)
):
    """
    **금융 뉴스 카테고리별 통계 정보를 조회합니다.**
    
    ### 제공 통계:
    - 각 카테고리별 뉴스 개수
    - 카테고리별 최신 뉴스 날짜
    - 전체 뉴스 개수
    - 사용 가능한 카테고리 목록
    
    ### 응답 예시:
    ```json
    {
        "categories": [
            {
                "category": "crypto",
                "category_display_name": "암호화폐",
                "count": 1520,
                "latest_news_date": "2025-07-20T14:30:15"
            },
            {
                "category": "general",
                "category_display_name": "일반 금융",
                "count": 980,
                "latest_news_date": "2025-07-20T13:45:22"
            }
        ],
        "total_news": 3850,
        "available_categories": ["crypto", "forex", "merger", "general"]
    }
    ```
    
    ### 사용 용도:
    - 대시보드 통계 섹션
    - 카테고리 필터 UI 구성
    - 데이터 품질 모니터링
    - 사용자 관심도 분석
    """
    try:
        service = FinancialNewsService(db)
        result = service.get_categories_statistics()
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"카테고리 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/trending-symbols", summary="트렌딩 종목 조회")
async def get_trending_symbols(
    days: int = Query(7, ge=1, le=30, description="조회할 일수 (최대 30일)"),
    limit: int = Query(10, ge=1, le=50, description="반환할 종목 수"),
    db: Session = Depends(get_db)
):
    """
    **최근 가장 많이 언급된 종목을 조회합니다.**
    
    ### 특징:
    - 지정한 기간 동안 뉴스에서 가장 많이 언급된 종목 순위
    - related 필드 분석을 통한 종목 추출
    - 언급 횟수 기준 내림차순 정렬
    - 실시간 시장 관심도 파악 가능
    
    ### 응답 예시:
    ```json
    {
        "trending_symbols": [
            {"symbol": "BTCUSD", "mentions": 45},
            {"symbol": "ETHUSD", "mentions": 32},
            {"symbol": "AAPL", "mentions": 28},
            {"symbol": "TSLA", "mentions": 21}
        ],
        "period_days": 7,
        "total_symbols": 156
    }
    ```
    
    ### 사용 용도:
    - 트렌딩 섹션 구성
    - 투자 관심종목 추천
    - 시장 센티먼트 분석
    - 포트폴리오 구성 참고
    """
    try:
        service = FinancialNewsService(db)
        trending_data = service.get_trending_symbols(days=days, limit=limit)
        
        return {
            "trending_symbols": [
                {"symbol": symbol, "mentions": count}
                for symbol, count in trending_data
            ],
            "period_days": days,
            "total_symbols": len(trending_data)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"트렌딩 종목 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/health", summary="Financial News API 상태 확인")
async def health_check(db: Session = Depends(get_db)):
    """
    **Financial News API의 상태를 확인합니다.**
    
    ### 확인 항목:
    - 데이터베이스 연결 상태
    - 각 카테고리별 뉴스 개수
    - 최신 뉴스 날짜
    - API 버전 정보
    
    ### 응답 예시:
    ```json
    {
        "status": "healthy",
        "database": "connected",
        "categories_count": {
            "crypto": 1520,
            "forex": 890,
            "merger": 340,
            "general": 1100
        },
        "total_news": 3850,
        "latest_news_date": "2025-07-20T14:30:15",
        "api_version": "1.0.0",
        "timestamp": "2025-07-20T15:45:30"
    }
    ```
    """
    try:
        service = FinancialNewsService(db)
        
        # 카테고리별 통계로 DB 연결 및 데이터 상태 확인
        stats = service.get_categories_statistics()
        
        # 카테고리별 개수 딕셔너리 생성
        categories_count = {
            cat.category: cat.count for cat in stats.categories
        }
        
        # 최신 뉴스 날짜 찾기
        latest_date = None
        if stats.categories:
            latest_dates = [cat.latest_news_date for cat in stats.categories if cat.latest_news_date]
            if latest_dates:
                latest_date = max(latest_dates)
        
        return {
            "status": "healthy",
            "database": "connected",
            "categories_count": categories_count,
            "total_news": stats.total_news,
            "latest_news_date": latest_date,
            "available_categories": stats.available_categories,
            "api_version": "1.0.0",
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"서비스 상태 확인 실패: {str(e)}"
        )