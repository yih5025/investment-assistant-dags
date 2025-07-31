from datetime import datetime, date
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.services.earnings_calendar_news_service import EarningsCalendarNewsService
from app.schemas.earnings_calendar_news_schema import (
    EarningsCalendarNewsResponse,
    EarningsCalendarNewsListResponse,
    EarningsCalendarNewsSearchResponse
)

# 라우터 생성
router = APIRouter(
    tags=["Earnings Calendar News"],
    responses={
        404: {"description": "뉴스를 찾을 수 없습니다"},
        500: {"description": "서버 내부 오류"}
    }
)


@router.get(
    "/symbol/{symbol}/report-date/{report_date}", 
    response_model=EarningsCalendarNewsListResponse, 
    summary="특정 기업의 특정 실적 발표 관련 뉴스 조회"
)
async def get_news_by_symbol_and_date(
    symbol: str = Path(..., description="기업 심볼 (예: AAPL)", example="AAPL"),
    report_date: date = Path(..., description="실적 발표 예정일 (YYYY-MM-DD)", example="2025-07-25"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    db: Session = Depends(get_db)
):
    """
    **실적 캘린더 UI 연동의 핵심 API**
    
    ### 사용 시나리오:
    1. 실적 캘린더에서 "7월 25일 애플 실적 발표" 클릭
    2. 해당 실적 발표와 관련된 모든 뉴스들 조회
    3. 뉴스 목록을 시간순으로 표시
    
    ### 특징:
    - **정확한 매칭**: 특정 기업의 특정 실적 발표에 대한 뉴스만 조회
    - **시간순 정렬**: 최신 뉴스부터 표시
    - **페이징 지원**: 많은 뉴스가 있어도 효율적 처리
    
    ### 사용 예시:
    ```
    GET /api/v1/earnings-calendar-news/symbol/AAPL/report-date/2025-07-25
    → 애플의 2025-07-25 실적 발표 관련 뉴스들
    
    GET /api/v1/earnings-calendar-news/symbol/GOOGL/report-date/2025-07-26?limit=10
    → 구글의 2025-07-26 실적 발표 관련 뉴스 10개
    ```
    
    ### 응답 구조:
    - **symbol, report_date**: 현재 조회 중인 기업과 실적 발표일
    - **items**: 해당 실적 관련 뉴스 목록
    - **total**: 전체 뉴스 개수
    """
    try:
        skip = (page - 1) * limit
        service = EarningsCalendarNewsService(db)
        
        result = service.get_news_by_symbol_and_date(
            symbol=symbol.upper(),
            report_date=report_date,
            skip=skip,
            limit=limit
        )
        
        # 결과가 없으면 404 대신 빈 결과 반환 (실적 일정은 있지만 뉴스가 없을 수 있음)
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"실적 관련 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/symbol/{symbol}", 
    response_model=EarningsCalendarNewsListResponse, 
    summary="특정 기업의 모든 실적 관련 뉴스 조회"
)
async def get_news_by_symbol(
    symbol: str = Path(..., description="기업 심볼", example="AAPL"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    start_date: Optional[date] = Query(None, description="실적 발표일 시작 필터"),
    end_date: Optional[date] = Query(None, description="실적 발표일 종료 필터"),
    db: Session = Depends(get_db)
):
    """
    **특정 기업의 모든 실적 관련 뉴스를 조회합니다.**
    
    ### 사용 시나리오:
    - 기업 상세 페이지에서 해당 기업의 모든 실적 관련 뉴스 표시
    - 특정 기간 동안의 실적 뉴스만 필터링
    
    ### 특징:
    - **기업별 통합 조회**: 해당 기업의 모든 실적 발표에 대한 뉴스
    - **날짜 범위 필터링**: 특정 기간의 실적만 조회 가능
    - **시간순 정렬**: 최신 뉴스부터 표시
    
    ### 사용 예시:
    ```
    GET /api/v1/earnings-calendar-news/symbol/AAPL
    → 애플의 모든 실적 관련 뉴스
    
    GET /api/v1/earnings-calendar-news/symbol/AAPL?start_date=2025-07-01&end_date=2025-07-31
    → 애플의 7월 실적 관련 뉴스만
    ```
    """
    try:
        skip = (page - 1) * limit
        service = EarningsCalendarNewsService(db)
        
        result = service.get_news_by_symbol(
            symbol=symbol.upper(),
            skip=skip,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"기업별 실적 뉴스 조회 중 오류가 발생했습니다: {str(e)}"
        )



@router.get(
    "/recent-summary", 
    response_model=EarningsCalendarNewsListResponse, 
    summary="특정 기업의 최근 실적 뉴스 요약"
)
async def get_recent_summary(
    symbol: str = Query(..., description="기업 심볼", example="AAPL"),
    db: Session = Depends(get_db)
):
    """
    **캘린더 UI용 특정 기업의 최근 실적 뉴스 요약**
    
    ### 사용 시나리오:
    1. 캘린더에서 특정 기업 선택 시 최근 7일간의 실적 관련 뉴스 표시
    2. 해당 기업의 실적 관련 이슈를 빠르게 파악
    3. 최대 10개의 최신 뉴스 제공
    
    ### 특징:
    - **기간 고정**: 최근 7일간 고정
    - **개수 고정**: 최대 10개 고정  
    - **단순함**: symbol만 입력하면 됨
    - **캘린더 친화적**: UI에서 사용하기 쉬운 구조
    
    ### 사용 예시:
    ```
    GET /api/v1/earnings-calendar-news/recent-summary?symbol=AAPL
    
    응답:
    {
      "total": 8,
      "items": [
        {
          "symbol": "AAPL",
          "report_date": "2025-07-25", 
          "headline": "Apple Q3 Earnings Preview: iPhone Sales in Focus",
          "published_at": "2025-07-24T16:30:00"
        }
      ],
      "symbol": "AAPL",
      "page": 1,
      "limit": 10,
      "has_next": false
    }
    ```
    """
    try:
        service = EarningsCalendarNewsService(db)
        
        # 고정값: 최근 7일, 최대 10개
        result = service.get_recent_news_by_symbol(
            symbol=symbol.upper(),
            days=7,
            limit=10
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"최근 실적 뉴스 요약 조회 중 오류가 발생했습니다: {str(e)}"
        )