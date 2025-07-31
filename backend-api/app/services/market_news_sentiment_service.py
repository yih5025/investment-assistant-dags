from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc, asc, text
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import json

from app.models.market_news_sentiment_model import MarketNewsSentiment

class MarketNewsSentimentService:
    """
    시장 뉴스 감성 분석 비즈니스 로직을 처리하는 서비스 클래스
    
    주요 기능:
    1. JSONB 데이터 파싱 (ticker_sentiment, topics)
    2. 감성 점수 기반 분석 및 랭킹
    3. 주제별/티커별 필터링
    4. 크로스 분석 (주제↔티커 관계)
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    # =========================================================================
    # JSONB 데이터 파싱 유틸리티 메서드
    # =========================================================================
    
    def parse_ticker_sentiment(self, ticker_sentiment_json: Any) -> List[Dict]:
        """
        ticker_sentiment JSONB 데이터를 파싱합니다.
        
        Args:
            ticker_sentiment_json: JSONB 데이터 (리스트 형태)
            
        Returns:
            List[Dict]: 파싱된 티커 감성 데이터
        """
        if not ticker_sentiment_json:
            return []
            
        try:
            if isinstance(ticker_sentiment_json, str):
                data = json.loads(ticker_sentiment_json)
            else:
                data = ticker_sentiment_json
                
            result = []
            for item in data:
                result.append({
                    "ticker": item.get("ticker", ""),
                    "relevance_score": float(item.get("relevance_score", 0)),
                    "ticker_sentiment_label": item.get("ticker_sentiment_label", "Neutral"),
                    "ticker_sentiment_score": float(item.get("ticker_sentiment_score", 0))
                })
            return result
        except (json.JSONDecodeError, TypeError, KeyError):
            return []
    
    def parse_topics(self, topics_json: Any) -> List[Dict]:
        """
        topics JSONB 데이터를 파싱합니다.
        
        Args:
            topics_json: JSONB 데이터 (리스트 형태)
            
        Returns:
            List[Dict]: 파싱된 주제 관련도 데이터
        """
        if not topics_json:
            return []
            
        try:
            if isinstance(topics_json, str):
                data = json.loads(topics_json)
            else:
                data = topics_json
                
            result = []
            for item in data:
                result.append({
                    "topic": item.get("topic", ""),
                    "relevance_score": float(item.get("relevance_score", 0))
                })
            return result
        except (json.JSONDecodeError, TypeError, KeyError):
            return []
    
    def enrich_news_with_jsonb_data(self, news_items: List[MarketNewsSentiment]) -> List[Dict]:
        """
        뉴스 아이템에 JSONB 데이터를 파싱하여 추가합니다.
        
        Args:
            news_items: 원본 뉴스 데이터
            
        Returns:
            List[Dict]: JSONB 데이터가 파싱된 뉴스 데이터
        """
        enriched_news = []
        
        for news in news_items:
            news_dict = {
                "batch_id": news.batch_id,
                "url": news.url,
                "title": news.title,
                "time_published": news.time_published,
                "authors": news.authors,
                "summary": news.summary,
                "source": news.source,
                "overall_sentiment_score": float(news.overall_sentiment_score) if news.overall_sentiment_score else None,
                "overall_sentiment_label": news.overall_sentiment_label,
                "query_type": news.query_type,
                "query_params": news.query_params,
                "created_at": news.created_at,
                
                # 감성 해석 추가
                "sentiment_interpretation": news.sentiment_interpretation,
                "sentiment_emoji": news.sentiment_emoji,
                
                # JSONB 데이터 파싱
                "related_tickers": self.parse_ticker_sentiment(news.ticker_sentiment),
                "related_topics": self.parse_topics(news.topics)
            }
            enriched_news.append(news_dict)
            
        return enriched_news
    
    # =========================================================================
    # 기본 뉴스 조회 메서드
    # =========================================================================
    
    def get_latest_batch_id(self) -> Optional[int]:
        """최신 배치 ID를 조회합니다."""
        result = self.db.query(func.max(MarketNewsSentiment.batch_id)).scalar()
        return result if result else None
    
    def get_batch_info(self) -> Dict[str, Any]:
        """배치 정보를 조회합니다."""
        latest_batch_id = self.get_latest_batch_id()
        
        # 총 배치 수 조회
        total_batches = self.db.query(func.count(func.distinct(MarketNewsSentiment.batch_id))).scalar()
        
        # 최신 배치의 수집 날짜 조회
        collection_date = "알 수 없음"
        if latest_batch_id:
            latest_news = (self.db.query(MarketNewsSentiment)
                          .filter(MarketNewsSentiment.batch_id == latest_batch_id)
                          .first())
            if latest_news and latest_news.created_at:
                collection_date = latest_news.created_at.strftime("%Y-%m-%d")
        
        return {
            "latest_batch_id": latest_batch_id or 0,
            "collection_date": collection_date,
            "total_batches": total_batches or 0
        }
    
    def get_news_list(self, days: int = 7, limit: int = 20, offset: int = 0,
                     min_sentiment: Optional[float] = None, max_sentiment: Optional[float] = None,
                     sentiment_labels: Optional[List[str]] = None,
                     sort_by: str = "time_published", order: str = "desc") -> Tuple[List[Dict], int]:
        """
        뉴스 목록을 조회합니다.
        
        Args:
            days: 최근 N일 데이터
            limit: 결과 개수 제한
            offset: 페이징 오프셋
            min_sentiment: 최소 감성 점수
            max_sentiment: 최대 감성 점수
            sentiment_labels: 감성 라벨 필터
            sort_by: 정렬 기준
            order: 정렬 순서
            
        Returns:
            Tuple[news_list, total_count]: 뉴스 목록과 총 개수
        """
        # 기본 쿼리
        query = self.db.query(MarketNewsSentiment)
        
        # 날짜 필터
        cutoff_date = datetime.now() - timedelta(days=days)
        query = query.filter(MarketNewsSentiment.time_published >= cutoff_date)
        
        # 감성 점수 필터
        if min_sentiment is not None:
            query = query.filter(MarketNewsSentiment.overall_sentiment_score >= min_sentiment)
        if max_sentiment is not None:
            query = query.filter(MarketNewsSentiment.overall_sentiment_score <= max_sentiment)
        
        # 감성 라벨 필터
        if sentiment_labels:
            query = query.filter(MarketNewsSentiment.overall_sentiment_label.in_(sentiment_labels))
        
        # 총 개수 계산
        total_count = query.count()
        
        # 정렬
        if sort_by == "sentiment_score":
            sort_column = MarketNewsSentiment.overall_sentiment_score
        elif sort_by == "time_published":
            sort_column = MarketNewsSentiment.time_published
        else:
            sort_column = MarketNewsSentiment.time_published
        
        if order == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))
        
        # 페이징
        news_items = query.limit(limit).offset(offset).all()
        
        # JSONB 데이터 파싱하여 반환
        enriched_news = self.enrich_news_with_jsonb_data(news_items)
        
        return enriched_news, total_count
    
    def get_news_by_batch(self, batch_id: int, limit: int = 20, offset: int = 0) -> List[Dict]:
        """특정 배치의 뉴스를 조회합니다."""
        news_items = (self.db.query(MarketNewsSentiment)
                     .filter(MarketNewsSentiment.batch_id == batch_id)
                     .order_by(desc(MarketNewsSentiment.time_published))
                     .limit(limit)
                     .offset(offset)
                     .all())
        
        return self.enrich_news_with_jsonb_data(news_items)
    
    # =========================================================================
    # Topic & Ticker 관련 메서드
    # =========================================================================
    
    def get_all_topics(self) -> List[str]:
        """모든 주제 목록을 조회합니다."""
        # PostgreSQL JSONB 쿼리로 모든 topic 추출
        query = text("""
            SELECT DISTINCT jsonb_array_elements(topics)->>'topic' as topic
            FROM market_news_sentiment 
            WHERE topics IS NOT NULL
            ORDER BY topic
        """)
        
        result = self.db.execute(query).fetchall()
        return [row[0] for row in result if row[0]]
    
    def get_all_tickers(self) -> List[str]:
        """모든 티커 목록을 조회합니다."""
        # PostgreSQL JSONB 쿼리로 모든 ticker 추출
        query = text("""
            SELECT DISTINCT jsonb_array_elements(ticker_sentiment)->>'ticker' as ticker
            FROM market_news_sentiment 
            WHERE ticker_sentiment IS NOT NULL
            ORDER BY ticker
        """)
        
        result = self.db.execute(query).fetchall()
        return [row[0] for row in result if row[0]]
    
    def get_news_by_topic(self, topic: str, days: int = 7, limit: int = 20, offset: int = 0) -> Tuple[List[Dict], Dict]:
        """특정 주제의 뉴스를 조회합니다."""
        # PostgreSQL JSONB 연산자 사용하여 특정 주제 필터링
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            SELECT * FROM market_news_sentiment 
            WHERE topics @> :topic_filter
            AND time_published >= :cutoff_date
            ORDER BY time_published DESC
            LIMIT :limit OFFSET :offset
        """)
        
        topic_filter = json.dumps([{"topic": topic}])
        result = self.db.execute(query, {
            "topic_filter": topic_filter,
            "cutoff_date": cutoff_date,
            "limit": limit,
            "offset": offset
        }).fetchall()
        
        # 결과를 MarketNewsSentiment 객체로 변환
        news_items = []
        for row in result:
            news_item = MarketNewsSentiment()
            for i, column in enumerate(MarketNewsSentiment.__table__.columns):
                setattr(news_item, column.name, row[i])
            news_items.append(news_item)
        
        enriched_news = self.enrich_news_with_jsonb_data(news_items)
        
        # 주제별 감성 요약 계산
        topic_summary = self.calculate_topic_sentiment_summary(topic, days)
        
        return enriched_news, topic_summary
    
    def get_news_by_ticker(self, ticker: str, days: int = 7, limit: int = 20, offset: int = 0) -> Tuple[List[Dict], Dict]:
        """특정 티커의 뉴스를 조회합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            SELECT * FROM market_news_sentiment 
            WHERE ticker_sentiment @> :ticker_filter
            AND time_published >= :cutoff_date
            ORDER BY time_published DESC
            LIMIT :limit OFFSET :offset
        """)
        
        ticker_filter = json.dumps([{"ticker": ticker}])
        result = self.db.execute(query, {
            "ticker_filter": ticker_filter,
            "cutoff_date": cutoff_date,
            "limit": limit,
            "offset": offset
        }).fetchall()
        
        # 결과를 MarketNewsSentiment 객체로 변환
        news_items = []
        for row in result:
            news_item = MarketNewsSentiment()
            for i, column in enumerate(MarketNewsSentiment.__table__.columns):
                setattr(news_item, column.name, row[i])
            news_items.append(news_item)
        
        enriched_news = self.enrich_news_with_jsonb_data(news_items)
        
        # 티커별 감성 요약 계산
        ticker_summary = self.calculate_ticker_sentiment_summary(ticker, days)
        
        return enriched_news, ticker_summary
    
    # =========================================================================
    # 감성 분석 및 랭킹 메서드
    # =========================================================================
    
    def get_sentiment_filtered_news(self, sentiment_type: str, days: int = 7, 
                                   limit: int = 20, offset: int = 0) -> List[Dict]:
        """감성별로 필터링된 뉴스를 조회합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = self.db.query(MarketNewsSentiment).filter(
            MarketNewsSentiment.time_published >= cutoff_date
        )
        
        # 감성 타입별 필터링
        if sentiment_type == "bullish":
            query = query.filter(MarketNewsSentiment.overall_sentiment_score > 0.1)
        elif sentiment_type == "bearish":
            query = query.filter(MarketNewsSentiment.overall_sentiment_score < -0.1)
        elif sentiment_type == "neutral":
            query = query.filter(
                and_(
                    MarketNewsSentiment.overall_sentiment_score >= -0.1,
                    MarketNewsSentiment.overall_sentiment_score <= 0.1
                )
            )
        
        news_items = (query.order_by(desc(MarketNewsSentiment.time_published))
                     .limit(limit)
                     .offset(offset)
                     .all())
        
        return self.enrich_news_with_jsonb_data(news_items)
    
    def calculate_topic_sentiment_ranking(self, days: int = 7, top_count: int = 10,
                                        bottom_count: int = 10, min_mentions: int = 2) -> Tuple[List[Dict], List[Dict]]:
        """주제별 감성 랭킹을 계산합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # PostgreSQL JSONB 집계 쿼리
        query = text("""
            WITH topic_stats AS (
                SELECT 
                    jsonb_array_elements(topics)->>'topic' as topic,
                    AVG(overall_sentiment_score) as avg_sentiment,
                    COUNT(*) as news_count,
                    AVG((jsonb_array_elements(topics)->>'relevance_score')::float) as avg_relevance
                FROM market_news_sentiment 
                WHERE time_published >= :cutoff_date
                AND topics IS NOT NULL
                AND overall_sentiment_score IS NOT NULL
                GROUP BY jsonb_array_elements(topics)->>'topic'
                HAVING COUNT(*) >= :min_mentions
            )
            SELECT topic, avg_sentiment, news_count, avg_relevance
            FROM topic_stats
            ORDER BY avg_sentiment DESC
        """)
        
        result = self.db.execute(query, {
            "cutoff_date": cutoff_date,
            "min_mentions": min_mentions
        }).fetchall()
        
        # 결과 처리
        all_topics = []
        for row in result:
            topic, avg_sentiment, news_count, avg_relevance = row
            
            # 감성 라벨 결정
            sentiment_label, sentiment_emoji = self._get_sentiment_label_and_emoji(avg_sentiment)
            
            # 관련 티커 조회
            related_tickers = self._get_tickers_by_topic(topic, days)
            
            topic_data = {
                "topic": topic,
                "avg_sentiment_score": float(avg_sentiment),
                "news_count": news_count,
                "sentiment_label": sentiment_label,
                "sentiment_emoji": sentiment_emoji,
                "trend": "상승" if avg_sentiment > 0.05 else "하락" if avg_sentiment < -0.05 else "안정",
                "related_tickers": related_tickers[:5]  # 상위 5개만
            }
            all_topics.append(topic_data)
        
        # Hot & Cold 분리
        hot_topics = all_topics[:top_count]
        cold_topics = all_topics[-bottom_count:] if len(all_topics) > bottom_count else []
        cold_topics.reverse()  # 가장 부정적인 것부터
        
        return hot_topics, cold_topics
    
    def calculate_ticker_sentiment_ranking(self, days: int = 7, top_count: int = 10,
                                         bottom_count: int = 10, min_mentions: int = 2) -> Tuple[List[Dict], List[Dict]]:
        """티커별 감성 랭킹을 계산합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # PostgreSQL JSONB 집계 쿼리
        query = text("""
            WITH ticker_stats AS (
                SELECT 
                    jsonb_array_elements(ticker_sentiment)->>'ticker' as ticker,
                    AVG((jsonb_array_elements(ticker_sentiment)->>'ticker_sentiment_score')::float) as avg_sentiment,
                    COUNT(*) as mention_count,
                    AVG((jsonb_array_elements(ticker_sentiment)->>'relevance_score')::float) as avg_relevance
                FROM market_news_sentiment 
                WHERE time_published >= :cutoff_date
                AND ticker_sentiment IS NOT NULL
                GROUP BY jsonb_array_elements(ticker_sentiment)->>'ticker'
                HAVING COUNT(*) >= :min_mentions
            )
            SELECT ticker, avg_sentiment, mention_count, avg_relevance
            FROM ticker_stats
            ORDER BY avg_sentiment DESC
        """)
        
        result = self.db.execute(query, {
            "cutoff_date": cutoff_date,
            "min_mentions": min_mentions
        }).fetchall()
        
        # 결과 처리
        all_tickers = []
        for row in result:
            ticker, avg_sentiment, mention_count, avg_relevance = row
            
            # 감성 라벨 결정
            sentiment_label, sentiment_emoji = self._get_sentiment_label_and_emoji(avg_sentiment)
            
            # 관련 주제 조회
            related_topics = self._get_topics_by_ticker(ticker, days)
            
            ticker_data = {
                "ticker": ticker,
                "avg_sentiment_score": float(avg_sentiment),
                "sentiment_label": sentiment_label,
                "sentiment_emoji": sentiment_emoji,
                "mention_count": mention_count,
                "avg_relevance_score": float(avg_relevance),
                "related_topics": related_topics[:3]  # 상위 3개만
            }
            all_tickers.append(ticker_data)
        
        # Hot & Cold 분리
        hot_tickers = all_tickers[:top_count]
        cold_tickers = all_tickers[-bottom_count:] if len(all_tickers) > bottom_count else []
        cold_tickers.reverse()  # 가장 부정적인 것부터
        
        return hot_tickers, cold_tickers
    
    def calculate_topic_sentiment_summary(self, topic: str, days: int = 7) -> Dict[str, Any]:
        """특정 주제의 감성 요약을 계산합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            SELECT 
                AVG(overall_sentiment_score) as avg_sentiment,
                COUNT(*) as total_news,
                COUNT(CASE WHEN overall_sentiment_score > 0.1 THEN 1 END) as bullish_count,
                COUNT(CASE WHEN overall_sentiment_score < -0.1 THEN 1 END) as bearish_count,
                COUNT(CASE WHEN overall_sentiment_score BETWEEN -0.1 AND 0.1 THEN 1 END) as neutral_count
            FROM market_news_sentiment 
            WHERE topics @> :topic_filter
            AND time_published >= :cutoff_date
            AND overall_sentiment_score IS NOT NULL
        """)
        
        topic_filter = json.dumps([{"topic": topic}])
        result = self.db.execute(query, {
            "topic_filter": topic_filter,
            "cutoff_date": cutoff_date
        }).fetchone()
        
        if not result or result[0] is None:
            return {
                "avg_sentiment_score": 0.0,
                "sentiment_label": "데이터 없음",
                "sentiment_emoji": "❓",
                "total_news": 0,
                "sentiment_distribution": {"bullish": 0, "bearish": 0, "neutral": 0}
            }
        
        avg_sentiment, total_news, bullish_count, bearish_count, neutral_count = result
        sentiment_label, sentiment_emoji = self._get_sentiment_label_and_emoji(avg_sentiment)
        
        return {
            "avg_sentiment_score": float(avg_sentiment),
            "sentiment_label": sentiment_label,
            "sentiment_emoji": sentiment_emoji,
            "total_news": total_news,
            "sentiment_distribution": {
                "bullish": bullish_count,
                "bearish": bearish_count,
                "neutral": neutral_count
            }
        }
    
    def calculate_ticker_sentiment_summary(self, ticker: str, days: int = 7) -> Dict[str, Any]:
        """특정 티커의 감성 요약을 계산합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            SELECT 
                AVG((jsonb_array_elements(ticker_sentiment)->>'ticker_sentiment_score')::float) as avg_sentiment,
                COUNT(*) as mention_count,
                AVG((jsonb_array_elements(ticker_sentiment)->>'relevance_score')::float) as avg_relevance
            FROM market_news_sentiment 
            WHERE ticker_sentiment @> :ticker_filter
            AND time_published >= :cutoff_date
        """)
        
        ticker_filter = json.dumps([{"ticker": ticker}])
        result = self.db.execute(query, {
            "ticker_filter": ticker_filter,
            "cutoff_date": cutoff_date
        }).fetchone()
        
        if not result or result[0] is None:
            return {
                "avg_sentiment_score": 0.0,
                "sentiment_label": "데이터 없음",
                "sentiment_emoji": "❓",
                "mention_count": 0,
                "avg_relevance_score": 0.0
            }
        
        avg_sentiment, mention_count, avg_relevance = result
        sentiment_label, sentiment_emoji = self._get_sentiment_label_and_emoji(avg_sentiment)
        
        return {
            "avg_sentiment_score": float(avg_sentiment),
            "sentiment_label": sentiment_label,
            "sentiment_emoji": sentiment_emoji,
            "mention_count": mention_count,
            "avg_relevance_score": float(avg_relevance)
        }
    
    # =========================================================================
    # 크로스 분석 메서드
    # =========================================================================
    
    def get_tickers_by_topic(self, topic: str, days: int = 7, limit: int = 10) -> List[Dict]:
        """특정 주제와 관련된 티커들을 조회합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            WITH topic_tickers AS (
                SELECT 
                    jsonb_array_elements(ticker_sentiment)->>'ticker' as ticker,
                    AVG((jsonb_array_elements(ticker_sentiment)->>'ticker_sentiment_score')::float) as avg_sentiment,
                    COUNT(*) as mention_count
                FROM market_news_sentiment 
                WHERE topics @> :topic_filter
                AND time_published >= :cutoff_date
                AND ticker_sentiment IS NOT NULL
                GROUP BY jsonb_array_elements(ticker_sentiment)->>'ticker'
            )
            SELECT ticker, avg_sentiment, mention_count
            FROM topic_tickers
            ORDER BY mention_count DESC, avg_sentiment DESC
            LIMIT :limit
        """)
        
        topic_filter = json.dumps([{"topic": topic}])
        result = self.db.execute(query, {
            "topic_filter": topic_filter,
            "cutoff_date": cutoff_date,
            "limit": limit
        }).fetchall()
        
        return [
            {
                "ticker": row[0],
                "avg_sentiment_score": float(row[1]),
                "mention_count": row[2],
                "sentiment_label": self._get_sentiment_label_and_emoji(row[1])[0]
            }
            for row in result
        ]
    
    def get_topics_by_ticker(self, ticker: str, days: int = 7, limit: int = 10) -> List[Dict]:
        """특정 티커와 관련된 주제들을 조회합니다."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = text("""
            WITH ticker_topics AS (
                SELECT 
                    jsonb_array_elements(topics)->>'topic' as topic,
                    AVG((jsonb_array_elements(topics)->>'relevance_score')::float) as avg_relevance,
                    COUNT(*) as mention_count
                FROM market_news_sentiment 
                WHERE ticker_sentiment @> :ticker_filter
                AND time_published >= :cutoff_date
                AND topics IS NOT NULL
                GROUP BY jsonb_array_elements(topics)->>'topic'
            )
            SELECT topic, avg_relevance, mention_count
            FROM ticker_topics
            ORDER BY avg_relevance DESC, mention_count DESC
            LIMIT :limit
        """)
        
        ticker_filter = json.dumps([{"ticker": ticker}])
        result = self.db.execute(query, {
            "ticker_filter": ticker_filter,
            "cutoff_date": cutoff_date,
            "limit": limit
        }).fetchall()
        
        return [
            {
                "topic": row[0],
                "avg_relevance_score": float(row[1]),
                "mention_count": row[2]
            }
            for row in result
        ]
    
    # =========================================================================
    # 내부 유틸리티 메서드
    # =========================================================================
    
    def _get_sentiment_label_and_emoji(self, score: Optional[float]) -> Tuple[str, str]:
        """감성 점수를 라벨과 이모지로 변환합니다."""
        if score is None:
            return "알 수 없음", "❓"
        
        score = float(score)
        if score >= 0.3:
            return "매우긍정적", "🚀"
        elif score >= 0.1:
            return "긍정적", "📈"
        elif score >= -0.1:
            return "중립적", "➡️"
        elif score >= -0.3:
            return "부정적", "📉"
        else:
            return "매우부정적", "🔻"
    
    def _get_tickers_by_topic(self, topic: str, days: int = 7) -> List[str]:
        """특정 주제의 관련 티커 목록을 조회합니다."""
        tickers_data = self.get_tickers_by_topic(topic, days, 10)
        return [item["ticker"] for item in tickers_data]
    
    def _get_topics_by_ticker(self, ticker: str, days: int = 7) -> List[str]:
        """특정 티커의 관련 주제 목록을 조회합니다."""
        topics_data = self.get_topics_by_ticker(ticker, days, 10)
        return [item["topic"] for item in topics_data]