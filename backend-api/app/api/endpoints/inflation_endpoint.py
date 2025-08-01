from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...dependencies import get_db
from ...services.inflation_service import InflationService
from ...schemas.inflation_schema import (
    InflationResponse,
    InflationListResponse,
    InflationChartResponse,
    InflationStatsResponse
)

router = APIRouter()

@router.get("/", response_model=InflationListResponse)
async def get_inflation_list(
    order_by: str = Query("desc", description="정렬 순서 (desc/asc)"),
    db: Session = Depends(get_db)
):
    """
    인플레이션 전체 데이터 조회
    
    Args:
        order_by: 정렬 순서 ('desc': 최신순, 'asc': 과거순)
        
    Returns:
        전체 인플레이션 데이터 목록
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_all_inflation_data(order_by=order_by)
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")

@router.get("/chart", response_model=InflationChartResponse)
async def get_inflation_chart_data(db: Session = Depends(get_db)):
    """
    차트 시각화용 인플레이션 데이터
    
    프론트엔드에서 그래프 그리기 최적화된 형태:
    - 연도와 비율만 포함
    - 통계 정보 포함 (최신값, 평균, 최고/최저값)
    
    Returns:
        차트용 데이터 + 통계 정보
    """
    try:
        service = InflationService(db)
        return service.get_chart_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"차트 데이터 조회 실패: {str(e)}")

@router.get("/recent", response_model=InflationListResponse)
async def get_recent_inflation(
    years: int = Query(10, ge=1, le=20, description="조회할 연수 (1-20)"),
    db: Session = Depends(get_db)
):
    """
    최근 N년 인플레이션 데이터 조회
    
    Args:
        years: 조회할 연수 (1~20년, 기본값: 10년)
        
    Returns:
        최근 N년 인플레이션 데이터
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_recent_years(years)
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"최근 데이터 조회 실패: {str(e)}")

@router.get("/year/{year}", response_model=InflationResponse)
async def get_inflation_by_year(
    year: int,
    db: Session = Depends(get_db)
):
    """
    특정 연도 인플레이션 데이터 조회
    
    Args:
        year: 조회할 연도 (1900~2100)
        
    Returns:
        해당 연도 인플레이션 데이터
    """
    try:
        service = InflationService(db)
        inflation_data = service.get_year_data(year)
        
        if not inflation_data:
            raise HTTPException(
                status_code=404, 
                detail=f"{year}년 인플레이션 데이터를 찾을 수 없습니다"
            )
        
        return inflation_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"연도별 데이터 조회 실패: {str(e)}")

@router.get("/statistics", response_model=InflationStatsResponse)
async def get_inflation_statistics(db: Session = Depends(get_db)):
    """
    인플레이션 통계 정보
    
    Returns:
        최신값, 평균, 최고/최저값, 전체 연수 등 통계 정보
    """
    try:
        service = InflationService(db)
        return service.get_statistics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"통계 정보 조회 실패: {str(e)}")

@router.get("/range", response_model=InflationListResponse)
async def get_inflation_range(
    start_year: int = Query(..., ge=1900, description="시작 연도"),
    end_year: int = Query(..., le=2100, description="종료 연도"),
    db: Session = Depends(get_db)
):
    """
    연도 범위별 인플레이션 데이터 조회
    
    Args:
        start_year: 시작 연도
        end_year: 종료 연도
        
    Returns:
        지정 범위의 인플레이션 데이터
    """
    if start_year > end_year:
        raise HTTPException(
            status_code=400, 
            detail="시작 연도는 종료 연도보다 작거나 같아야 합니다"
        )
    
    try:
        service = InflationService(db)
        inflation_data = service.get_range_data(start_year, end_year)
        
        return InflationListResponse(
            total_count=len(inflation_data),
            items=inflation_data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"범위별 데이터 조회 실패: {str(e)}")