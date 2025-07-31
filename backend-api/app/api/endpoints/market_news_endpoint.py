from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.market_news_service import MarketNewsService
from app.schemas.market_news_schema import (
    MarketNewsResponse,
    MarketNewsListResponse,
    MarketNewsSearchResponse
)

# 라우터 생성 (태그로 API 문서 그룹화)
router = APIRouter(
    prefix="/api/v1/market-news",
    tags=["Market News"],
    responses={
        404: {"description": "뉴스를 찾을 수 없습니다"},
        500: {"description": "서버 내부 오류"}
    }
)


@router.get("/", response_model=MarketNewsListResponse, summary="시장 뉴스 목록 조회")
async def get_market_news_list(
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수 (최대 100)"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜 (YYYY-MM-DD 또는 ISO 형식)"),
    sources: Optional[List[str]] = Query(None, description="포함할 소스 목록"),
    exclude_sources: Optional[List[str]] = Query(None, description="제외할 소스 목록"),
    db: Session = Depends(get_db)
):
    """
    **시장 뉴스 목록을 페이징하여 조회합니다.**
    
    ### 주요 기능:
    - **페이징**: page, limit 파라미터로 페이지 단위 조회
    - **날짜 필터링**: start_date, end_date로 특정 기간 뉴스만 조회
    - **소스 필터링**: 특정 뉴스 소스만 포함하거나 제외
    - **최신순 정렬**: published_at 기준 내림차순
    
    ### 사용 예시:
    ```
    GET /api/v1/market-news/?page=1&limit=10
    GET /api/v1/market-news/?start_date=2025-07-01&end_date=2025-07-31
    GET /api/v1/market-news/?sources=BBC News,Reuters
    ```
    
    ### 응답 구조:
    - **total**: 전체 뉴스 개수
    - **items**: 뉴스 목록 (content 제외한 요약 정보)
    - **page, limit**: 페이징 정보
    - **has_next**: 다음 페이지 존재 여부
    """
    try:
        # 페이지 번호를 offset으로 변환 (page 1 = skip 0)
        skip = (page - 1) * limit
        
        # 서비스 인스턴스 생성
        service = MarketNewsService(db)
        
        # 뉴스 목록 조회
        result = service.get_news_list(
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            sources=sources,
            exclude_sources=exclude_sources
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 목록 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/recent", response_model=List[MarketNewsResponse], summary="최근 뉴스 조회")
async def get_recent_market_news(
    hours: int = Query(24, ge=1, le=168, description="몇 시간 이내 뉴스 (최대 7일)"),
    limit: int = Query(10, ge=1, le=50, description="최대 개수 (최대 50)"),
    db: Session = Depends(get_db)
):
    """
    **최근 발행된 시장 뉴스를 조회합니다.**
    
    ### 특징:
    - 설정한 시간 이내에 발행된 뉴스만 조회
    - 최신순으로 정렬
    - 전체 정보 포함 (content 포함)
    
    ### 사용 예시:
    ```
    GET /api/v1/market-news/recent?hours=6&limit=5  # 6시간 이내 최신 5개
    GET /api/v1/market-news/recent                  # 24시간 이내 최신 10개 (기본값)
    ```
    """
    try:
        service = MarketNewsService(db)
        result = service.get_recent_news(hours=hours, limit=limit)
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/search", response_model=MarketNewsSearchResponse, summary="뉴스 검색")
async def search_market_news(
    q: str = Query(..., min_length=2, description="검색어 (최소 2글자)"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    start_date: Optional[datetime] = Query(None, description="검색 시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="검색 종료 날짜"),
    db: Session = Depends(get_db)
):
    """
    **뉴스 전문 검색을 수행합니다.**
    
    ### 검색 대상:
    - **제목 (title)**: 주요 검색 대상
    - **설명 (description)**: 뉴스 요약문
    - **본문 (content)**: 전체 기사 내용
    
    ### 검색 엔진:
    - PostgreSQL Full-Text Search 사용
    - 영어 텍스트 처리 최적화
    - 관련도 순으로 정렬
    
    ### 사용 예시:
    ```
    GET /api/v1/market-news/search?q=Trump
    GET /api/v1/market-news/search?q=economy&start_date=2025-07-01
    GET /api/v1/market-news/search?q="stock market"  # 구문 검색
    ```
    """
    try:
        skip = (page - 1) * limit
        service = MarketNewsService(db)
        
        result = service.search_news(
            query_text=q,
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 검색 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/detail", response_model=MarketNewsResponse, summary="뉴스 상세 조회")
async def get_market_news_detail(
    source: str = Query(..., description="뉴스 소스"),
    url: str = Query(..., description="뉴스 URL"),
    db: Session = Depends(get_db)
):
    """
    **특정 뉴스의 상세 정보를 조회합니다.**
    
    ### 특징:
    - source + url 복합키로 고유 뉴스 식별
    - 전체 본문 내용 포함
    - short_description, content_preview 등 편의 정보 제공
    
    ### 사용 예시:
    ```
    GET /api/v1/market-news/detail?source=BBC News&url=https://www.bbc.com/news/articles/c23g5xpggzmo
    ```
    """
    try:
        service = MarketNewsService(db)
        result = service.get_news_by_url(source=source, url=url)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"뉴스를 찾을 수 없습니다. source: {source}, url: {url}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"뉴스 상세 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/sources", summary="뉴스 소스별 통계")
async def get_news_sources_stats(
    db: Session = Depends(get_db)
):
    """
    **뉴스 소스별 통계 정보를 조회합니다.**
    
    ### 응답 정보:
    - 각 소스별 뉴스 개수
    - 뉴스 개수 기준 내림차순 정렬
    
    ### 사용 용도:
    - 어떤 소스가 많은 뉴스를 제공하는지 파악
    - 소스 필터링 UI 구성 시 참고 데이터
    - 데이터 품질 모니터링
    
    ### 응답 예시:
    ```json
    {
        "sources": [
            {"source": "BBC News", "count": 1250},
            {"source": "Reuters", "count": 980},
            {"source": "CNN", "count": 750}
        ]
    }
    ```
    """
    try:
        service = MarketNewsService(db)
        sources_data = service.get_news_sources()
        
        return {
            "sources": [
                {"source": source, "count": count}
                for source, count in sources_data
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"소스 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/daily-stats", summary="일별 뉴스 발행 통계")
async def get_daily_news_stats(
    days: int = Query(30, ge=1, le=365, description="조회할 일수 (최대 1년)"),
    db: Session = Depends(get_db)
):
    """
    **일별 뉴스 발행 통계를 조회합니다.**
    
    ### 특징:
    - 지정한 일수만큼 과거 데이터 조회
    - 날짜별 뉴스 발행 개수 집계
    - 최근 날짜부터 내림차순 정렬
    
    ### 사용 용도:
    - 뉴스 발행 트렌드 분석
    - 차트 및 대시보드 데이터
    - 데이터 수집 상태 모니터링
    
    ### 응답 예시:
    ```json
    {
        "daily_stats": [
            {"date": "2025-07-20", "count": 45},
            {"date": "2025-07-19", "count": 38},
            {"date": "2025-07-18", "count": 52}
        ],
        "period_days": 30,
        "total_news": 1234
    }
    ```
    """
    try:
        service = MarketNewsService(db)
        daily_data = service.get_daily_news_count(days=days)
        
        # 전체 뉴스 개수 계산
        total_news = sum(count for _, count in daily_data)
        
        return {
            "daily_stats": [
                {"date": date, "count": count}
                for date, count in daily_data
            ],
            "period_days": days,
            "total_news": total_news
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"일별 통계 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get("/health", summary="Market News API 상태 확인")
async def health_check(db: Session = Depends(get_db)):
    """
    **Market News API의 상태를 확인합니다.**
    
    ### 확인 항목:
    - 데이터베이스 연결 상태
    - 최근 뉴스 데이터 존재 여부
    - 전체 뉴스 개수
    
    ### 응답 예시:
    ```json
    {
        "status": "healthy",
        "database": "connected",
        "total_news": 15432,
        "latest_news_date": "2025-07-20T14:30:15",
        "api_version": "1.0.0",
        "timestamp": "2025-07-20T15:45:30"
    }
    ```
    """
    try:
        service = MarketNewsService(db)
        
        # 기본 통계 조회로 DB 연결 테스트
        total_count = service.db.query(service.db.query(MarketNews).count().label('count')).scalar()
        
        # 최신 뉴스 날짜 조회
        latest_news = service.db.query(MarketNews.published_at).order_by(
            MarketNews.published_at.desc()
        ).first()
        
        latest_date = latest_news[0] if latest_news else None
        
        return {
            "status": "healthy",
            "database": "connected",
            "total_news": total_count,
            "latest_news_date": latest_date,
            "api_version": "1.0.0",
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"서비스 상태 확인 실패: {str(e)}"
        )