from datetime import datetime, date
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.models.earnings_calendar_news_model import EarningsCalendarNews
from app.schemas.earnings_calendar_news_schema import (
    EarningsCalendarNewsResponse,
    EarningsCalendarNewsListItem,
    EarningsCalendarNewsListResponse,
    EarningsCalendarNewsSearchResponse
)


class EarningsCalendarNewsService:
    """
    실적 캘린더 연계 뉴스 서비스
    
    핵심 기능:
    1. 특정 기업의 특정 실적 발표에 대한 뉴스 조회 (달력 UI 클릭 시)
    2. 기업별 실적 관련 뉴스 조회
    3. 전체 실적 뉴스 목록 조회
    """

    def __init__(self, db: Session):
        self.db = db

    def get_news_by_symbol_and_date(
        self,
        symbol: str,
        report_date: date,
        skip: int = 0,
        limit: int = 20
    ) -> EarningsCalendarNewsListResponse:
        """
        특정 기업의 특정 실적 발표에 대한 뉴스 조회
        
        용도: 달력 UI에서 "7월 25일 애플 실적 발표" 클릭 시 해당 뉴스들 표시
        
        Args:
            symbol: 기업 심볼 (예: AAPL)
            report_date: 실적 발표일
            skip: 페이징 offset
            limit: 페이징 limit
            
        Returns:
            EarningsCalendarNewsListResponse: 해당 실적 관련 뉴스 목록
        """
        # 특정 symbol + report_date 조합의 뉴스들 조회
        query = self.db.query(EarningsCalendarNews).filter(
            and_(
                EarningsCalendarNews.symbol == symbol,
                EarningsCalendarNews.report_date == report_date
            )
        ).order_by(EarningsCalendarNews.published_at.desc())
        
        # 전체 개수 및 페이징
        total = query.count()
        items = query.offset(skip).limit(limit).all()
        
        # 응답 데이터 생성
        news_items = []
        for item in items:
            news_item = EarningsCalendarNewsListItem(
                symbol=item.symbol,
                report_date=item.report_date,
                url=item.url,
                headline=item.headline,
                source=item.source,
                published_at=item.published_at,
                short_headline=item.short_headline,
                has_image=item.has_image,
                fetched_at=item.fetched_at
            )
            news_items.append(news_item)
        
        # 페이징 정보
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=news_items,
            symbol=symbol,
            report_date=report_date,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_by_symbol(
        self,
        symbol: str,
        skip: int = 0,
        limit: int = 20,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> EarningsCalendarNewsListResponse:
        """
        특정 기업의 모든 실적 관련 뉴스 조회
        
        Args:
            symbol: 기업 심볼
            skip: 페이징 offset
            limit: 페이징 limit
            start_date: 실적 발표일 시작 필터
            end_date: 실적 발표일 종료 필터
            
        Returns:
            EarningsCalendarNewsListResponse: 해당 기업의 실적 관련 뉴스 목록
        """
        query = self.db.query(EarningsCalendarNews).filter(
            EarningsCalendarNews.symbol == symbol
        )
        
        # 실적 발표일 범위 필터링
        if start_date:
            query = query.filter(EarningsCalendarNews.report_date >= start_date)
        if end_date:
            query = query.filter(EarningsCalendarNews.report_date <= end_date)
            
        query = query.order_by(EarningsCalendarNews.published_at.desc())
        
        # 전체 개수 및 페이징
        total = query.count()
        items = query.offset(skip).limit(limit).all()
        
        # 응답 데이터 생성
        news_items = []
        for item in items:
            news_item = EarningsCalendarNewsListItem(
                symbol=item.symbol,
                report_date=item.report_date,
                url=item.url,
                headline=item.headline,
                source=item.source,
                published_at=item.published_at,
                short_headline=item.short_headline,
                has_image=item.has_image,
                fetched_at=item.fetched_at
            )
            news_items.append(news_item)
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=news_items,
            symbol=symbol,
            report_date=None,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_earnings_calendar_news_list(
        self,
        skip: int = 0,
        limit: int = 20,
        symbols: Optional[List[str]] = None,
        start_report_date: Optional[date] = None,
        end_report_date: Optional[date] = None,
        start_published_date: Optional[datetime] = None,
        end_published_date: Optional[datetime] = None,
        sources: Optional[List[str]] = None
    ) -> EarningsCalendarNewsListResponse:
        """
        전체 실적 캘린더 뉴스 목록 조회
        
        Args:
            skip: 페이징 offset
            limit: 페이징 limit
            symbols: 기업 심볼 필터
            start_report_date: 실적 발표일 시작
            end_report_date: 실적 발표일 종료
            start_published_date: 뉴스 발행일 시작
            end_published_date: 뉴스 발행일 종료
            sources: 뉴스 소스 필터
            
        Returns:
            EarningsCalendarNewsListResponse: 필터링된 뉴스 목록
        """
        # 기본 쿼리
        query = self.db.query(EarningsCalendarNews).order_by(EarningsCalendarNews.published_at.desc())
        
        # 필터링
        if symbols:
            query = query.filter(EarningsCalendarNews.symbol.in_(symbols))
        if start_report_date:
            query = query.filter(EarningsCalendarNews.report_date >= start_report_date)
        if end_report_date:
            query = query.filter(EarningsCalendarNews.report_date <= end_report_date)
        if start_published_date:
            query = query.filter(EarningsCalendarNews.published_at >= start_published_date)
        if end_published_date:
            query = query.filter(EarningsCalendarNews.published_at <= end_published_date)
        if sources:
            query = query.filter(EarningsCalendarNews.source.in_(sources))
        
        # 페이징
        total = query.count()
        items = query.offset(skip).limit(limit).all()
        
        # 응답 생성
        news_items = []
        for item in items:
            news_item = EarningsCalendarNewsListItem(
                symbol=item.symbol,
                report_date=item.report_date,
                url=item.url,
                headline=item.headline,
                source=item.source,
                published_at=item.published_at,
                short_headline=item.short_headline,
                has_image=item.has_image,
                fetched_at=item.fetched_at
            )
            news_items.append(news_item)
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsListResponse(
            total=total,
            items=news_items,
            symbol=None,
            report_date=None,
            page=page,
            limit=limit,
            has_next=has_next
        )

    def get_news_detail(
        self,
        symbol: str,
        report_date: date,
        url: str
    ) -> Optional[EarningsCalendarNewsResponse]:
        """
        특정 뉴스 상세 조회 (3중 복합 키)
        
        Args:
            symbol: 기업 심볼
            report_date: 실적 발표일
            url: 뉴스 URL
            
        Returns:
            EarningsCalendarNewsResponse: 뉴스 상세 정보
        """
        news = self.db.query(EarningsCalendarNews).filter(
            and_(
                EarningsCalendarNews.symbol == symbol,
                EarningsCalendarNews.report_date == report_date,
                EarningsCalendarNews.url == url
            )
        ).first()
        
        if not news:
            return None
        
        return EarningsCalendarNewsResponse(
            symbol=news.symbol,
            report_date=news.report_date,
            url=news.url,
            headline=news.headline,
            source=news.source,
            published_at=news.published_at,
            category=news.category,
            article_id=news.article_id,
            image=news.image,
            related=news.related,
            summary=news.summary,
            fetched_at=news.fetched_at,
            short_headline=news.short_headline,
            short_summary=news.short_summary,
            related_symbols=news.related_symbols,
            has_image=news.has_image
        )

    def search_earnings_calendar_news(
        self,
        query_text: str,
        skip: int = 0,
        limit: int = 20,
        symbols: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> EarningsCalendarNewsSearchResponse:
        """
        실적 관련 뉴스 검색
        
        Args:
            query_text: 검색어
            skip: 페이징 offset
            limit: 페이징 limit
            symbols: 검색 대상 기업
            start_date: 실적 발표일 범위 시작
            end_date: 실적 발표일 범위 종료
            
        Returns:
            EarningsCalendarNewsSearchResponse: 검색 결과
        """
        # 검색 쿼리
        search_query = self.db.query(EarningsCalendarNews).filter(
            or_(
                EarningsCalendarNews.headline.ilike(f'%{query_text}%'),
                EarningsCalendarNews.summary.ilike(f'%{query_text}%'),
                EarningsCalendarNews.source.ilike(f'%{query_text}%')
            )
        ).order_by(EarningsCalendarNews.published_at.desc())
        
        # 추가 필터링
        if symbols:
            search_query = search_query.filter(EarningsCalendarNews.symbol.in_(symbols))
        if start_date:
            search_query = search_query.filter(EarningsCalendarNews.report_date >= start_date)
        if end_date:
            search_query = search_query.filter(EarningsCalendarNews.report_date <= end_date)
        
        # 페이징
        total = search_query.count()
        items = search_query.offset(skip).limit(limit).all()
        
        # 응답 생성
        news_items = []
        for item in items:
            news_items.append(EarningsCalendarNewsResponse(
                symbol=item.symbol,
                report_date=item.report_date,
                url=item.url,
                headline=item.headline,
                source=item.source,
                published_at=item.published_at,
                category=item.category,
                article_id=item.article_id,
                image=item.image,
                related=item.related,
                summary=item.summary,
                fetched_at=item.fetched_at,
                short_headline=item.short_headline,
                short_summary=item.short_summary,
                related_symbols=item.related_symbols,
                has_image=item.has_image
            ))
        
        page = (skip // limit) + 1
        has_next = (skip + limit) < total
        
        return EarningsCalendarNewsSearchResponse(
            total=total,
            items=news_items,
            search_query=query_text,
            page=page,
            limit=limit,
            has_next=has_next
        )