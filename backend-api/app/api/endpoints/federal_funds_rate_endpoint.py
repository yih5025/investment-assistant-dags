from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...dependencies import get_db
from ...services.federal_funds_rate_service import FederalFundsRateService
from ...schemas.federal_funds_rate_schema import (
    FederalFundsRateResponse,
    FederalFundsRateListResponse,
    FederalFundsRateChartResponse,
    FederalFundsRateStatsResponse,
    FederalFundsRateTrendResponse
)

router = APIRouter()

@router.get("/", response_model=FederalFundsRateListResponse)
async def get_federal_funds_rate_list(
    order_by: str = Query("desc", regex="^(desc|asc)$"),
    db: Session = Depends(get_db)
):
    """
    연방기금금리 전체 데이터 조회
    
    미국 연준(Fed)에서 설정하는 기준금리 데이터
    - 월별 데이터 제공
    - 경제정책 및 투자 분석의 핵심 지표
    
    Args:
        order_by: 정렬 순서 ('desc': 최신순, 'asc': 과거순)
    """
    try:
        service = FederalFundsRateService(db)
        data = service.get_all_rates(order_by=order_by)
        
        return FederalFundsRateListResponse(
            total_count=len(data),
            items=data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chart", response_model=FederalFundsRateChartResponse)
async def get_federal_funds_rate_chart_data(db: Session = Depends(get_db)):
    """
    연방기금금리 차트용 데이터
    
    프론트엔드 그래프 최적화 형태:
    - 월별 금리 데이터
    - 현재 금리 및 트렌드 정보
    - 전월 대비 변화량
    - 12개월 평균, 최고/최저값
    """
    try:
        service = FederalFundsRateService(db)
        return service.get_chart_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/recent", response_model=FederalFundsRateListResponse)
async def get_recent_federal_funds_rate(
    months: int = Query(12, ge=1, le=60, description="조회할 개월 수 (1-60)"),
    db: Session = Depends(get_db)
):
    """
    최근 N개월 연방기금금리 조회
    
    Args:
        months: 조회할 개월 수 (1~60개월, 기본값: 12개월)
    """
    try:
        service = FederalFundsRateService(db)
        data = service.get_recent_months(months)
        
        return FederalFundsRateListResponse(
            total_count=len(data),
            items=data
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/statistics", response_model=FederalFundsRateStatsResponse)
async def get_federal_funds_rate_statistics(db: Session = Depends(get_db)):
    """
    연방기금금리 통계 정보
    
    Returns:
        현재 금리, 전월 대비 변화, 트렌드, 12개월 통계 등
    """
    try:
        service = FederalFundsRateService(db)
        return service.get_statistics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trend", response_model=FederalFundsRateTrendResponse)
async def get_federal_funds_rate_trend_analysis(db: Session = Depends(get_db)):
    """
    연방기금금리 트렌드 분석
    
    상세한 금리 사이클 분석:
    - 최근 6개월/12개월 데이터
    - 트렌드 방향 (상승/하락/안정)
    - 변동성 점수 (0-10)
    - 금리 정책 단계 (긴축/완화/중립)
    """
    try:
        service = FederalFundsRateService(db)
        return service.get_trend_analysis()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/current", response_model=FederalFundsRateResponse)
async def get_current_federal_funds_rate(db: Session = Depends(get_db)):
    """
    현재 연방기금금리 조회
    
    Returns:
        가장 최신의 연방기금금리 데이터
    """
    try:
        service = FederalFundsRateService(db)
        recent_data = service.get_recent_months(1)
        
        if not recent_data:
            raise HTTPException(
                status_code=404, 
                detail="현재 연방기금금리 데이터를 찾을 수 없습니다"
            )
        
        return recent_data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))