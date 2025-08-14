import React from 'react';
import { ArrowLeft, ExternalLink, TrendingUp, TrendingDown, Target, Calendar, Building, Clock, Zap, BarChart, Newspaper, User, Globe } from 'lucide-react';

// ============================================================================
// 타입 정의 (메인 뉴스 페이지와 동일)
// ============================================================================

interface MarketNewsItem {
  type: "market";
  source: string;
  url: string;
  author: string;
  title: string;
  description: string;
  content: string;
  published_at: string;
  short_description?: string;
  fetched_at?: string;
}

interface FinancialNewsItem {
  type: "financial";
  category: "crypto" | "forex" | "merger" | "general";
  news_id: number;
  datetime: string;
  headline: string;
  image: string;
  related: string;
  source: string;
  summary: string;
  url: string;
  published_at?: string;
  short_headline?: string;
  has_image?: boolean;
  related_symbols?: string[];
  category_display_name?: string;
  fetched_at?: string;
}

interface CompanyNewsItem {
  type: "company";
  symbol: string;
  category: string;
  rank_position?: number;
  price?: number;
  change_percentage?: string;
  volume?: number;
  news_count: number;
  news: Array<{
    article_id: number;
    headline: string;
    image: string;
    related: string;
    source: string;
    summary: string;
    url: string;
    published_at: string;
  }>;
}

interface SentimentNewsItem {
  type: "sentiment";
  title: string;
  url: string;
  time_published: string;
  authors: string;
  summary: string;
  source: string;
  overall_sentiment_score: number;
  overall_sentiment_label: string;
  ticker_sentiment: Array<{
    ticker: string;
    relevance_score: string;
    ticker_sentiment_label: string;
    ticker_sentiment_score: string;
  }>;
  topics: Array<{
    topic: string;
    relevance_score: string;
  }>;
  query_type: string;
  query_params: string;
  batch_id?: number;
  created_at?: string;
  sentiment_interpretation?: string;
  sentiment_emoji?: string;
}

export type NewsItem = MarketNewsItem | FinancialNewsItem | CompanyNewsItem | SentimentNewsItem;

interface NewsDetailPageProps {
  newsItem: NewsItem;
  onBack: () => void;
}

// ============================================================================
// 뉴스 상세 페이지 컴포넌트
// ============================================================================

export default function ImprovedNewsDetailPage({ newsItem, onBack }: NewsDetailPageProps) {
  // =========================================================================
  // 유틸리티 함수들
  // =========================================================================
  
  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleDateString("ko-KR", { 
      year: "numeric",
      month: "long", 
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    });
  };

  const formatTimeAgo = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffHours / 24);
    
    if (diffHours < 1) return "방금 전";
    if (diffHours < 24) return `${diffHours}시간 전`;
    if (diffDays < 7) return `${diffDays}일 전`;
    return formatTimestamp(timestamp);
  };

  const getSentimentColor = (score: number) => {
    if (score > 0.3) return "text-green-400";
    if (score < -0.3) return "text-red-400";
    return "text-yellow-400";
  };

  const getSentimentBgColor = (score: number) => {
    if (score > 0.3) return "bg-green-500/20";
    if (score < -0.3) return "bg-red-500/20";
    return "bg-yellow-500/20";
  };

  const getSentimentIcon = (score: number) => {
    if (score > 0.3) return <TrendingUp size={20} />;
    if (score < -0.3) return <TrendingDown size={20} />;
    return <Target size={20} />;
  };

  const getCategoryColor = (category: string) => {
    switch (category) {
      case "crypto": return "bg-orange-500/20 text-orange-400 border-orange-500/50";
      case "forex": return "bg-green-500/20 text-green-400 border-green-500/50";
      case "merger": return "bg-purple-500/20 text-purple-400 border-purple-500/50";
      case "general": return "bg-blue-500/20 text-blue-400 border-blue-500/50";
      default: return "bg-gray-500/20 text-gray-400 border-gray-500/50";
    }
  };

  const getCategoryDisplayName = (category: string) => {
    switch (category) {
      case "crypto": return "암호화폐";
      case "forex": return "외환";
      case "merger": return "M&A";
      case "general": return "일반";
      default: return category;
    }
  };

  const getUrl = () => {
    switch (newsItem.type) {
      case "market":
      case "financial":
      case "sentiment":
        return newsItem.url;
      case "company":
        return newsItem.news?.[0]?.url || "";
    }
  };

  // =========================================================================
  // 타입별 상세 렌더링 함수들
  // =========================================================================

  const renderMarketNews = (item: MarketNewsItem) => (
    <div className="space-y-6">
      {/* 메타 정보 - 깔끔한 카드 */}
      <div className="glass-card rounded-xl p-4">
        <div className="flex items-start justify-between mb-4">
          <div className="flex-1">
            <div className="flex items-center space-x-2 mb-2">
              <Globe size={16} className="text-blue-400" />
              <span className="font-medium text-blue-400">{item.source}</span>
            </div>
            {item.author && (
              <div className="flex items-center space-x-2 text-sm text-foreground/70">
                <User size={14} />
                <span>{item.author}</span>
              </div>
            )}
          </div>
          <div className="text-right text-sm">
            <div className="text-foreground/60">발행</div>
            <div className="font-medium">{formatTimeAgo(item.published_at)}</div>
          </div>
        </div>

        {/* 타임라인 */}
        {item.fetched_at && (
          <div className="border-t border-white/10 pt-3">
            <div className="flex items-center justify-between text-xs text-foreground/50">
              <div className="flex items-center space-x-2">
                <Clock size={12} />
                <span>수집: {formatTimeAgo(item.fetched_at)}</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* 본문 - 중복 제거된 깔끔한 구조 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        {/* 요약 (description이 있으면 표시) */}
        {item.description && (
          <div className="p-4 bg-blue-500/10 rounded-lg border border-blue-500/20">
            <h4 className="font-medium text-blue-400 mb-2 text-sm">요약</h4>
            <p className="text-foreground/90 leading-relaxed">{item.description}</p>
          </div>
        )}

        {/* 본문 내용 */}
        {item.content && item.content !== item.description && (
          <div>
            <h4 className="font-medium mb-3 text-sm text-foreground/70">본문</h4>
            <div className="prose prose-invert max-w-none">
              <p className="leading-relaxed text-foreground/90 whitespace-pre-line">
                {item.content}
              </p>
            </div>
          </div>
        )}

        {/* short_description가 별도로 있으면 표시 */}
        {item.short_description && 
         item.short_description !== item.description && 
         item.short_description !== item.content && (
          <div className="p-3 glass-subtle rounded-lg">
            <h4 className="font-medium mb-2 text-sm text-foreground/70">추가 요약</h4>
            <p className="text-sm text-foreground/80">{item.short_description}</p>
          </div>
        )}
      </div>
    </div>
  );

  const renderFinancialNews = (item: FinancialNewsItem) => (
    <div className="space-y-6">
      {/* 카테고리 및 메타 정보 */}
      <div className="glass-card rounded-xl p-4">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-3">
            <span className={`px-3 py-1.5 rounded-lg border text-sm font-medium ${getCategoryColor(item.category)}`}>
              {getCategoryDisplayName(item.category)}
            </span>
            <div className="text-xs text-foreground/60">ID: {item.news_id}</div>
          </div>
          <div className="text-right text-sm">
            <div className="text-foreground/60">발행</div>
            <div className="font-medium">{formatTimeAgo(item.datetime)}</div>
          </div>
        </div>

        <div className="flex items-center space-x-2 text-sm">
          <Globe size={14} className="text-green-400" />
          <span className="font-medium text-green-400">{item.source}</span>
        </div>
      </div>

      {/* 히어로 이미지 */}
      {item.image && item.image.trim() && (
        <div className="glass-card rounded-xl p-4">
          <div className="relative">
            <img 
              src={item.image} 
              alt={item.headline || '뉴스 이미지'}
              className="w-full rounded-lg object-cover max-h-64"
              onLoad={(e) => {
                console.log('✅ 이미지 로드 성공:', item.image);
              }}
              onError={(e) => {
                console.error('❌ 이미지 로드 실패:', item.image);
                const target = e.target as HTMLImageElement;
                const container = target.closest('.glass-card');
                if (container) {
                    (container as HTMLElement).style.display = 'none';
                  }
              }}
            />
            {/* 이미지 로딩 placeholder */}
            <div className="absolute inset-0 bg-gray-500/20 rounded-lg animate-pulse opacity-0 group-loading:opacity-100" />
          </div>
          
          {/* 이미지 정보 표시 (개발환경) */}
          {process.env.NODE_ENV === 'development' && (
            <div className="mt-2 p-2 bg-gray-800/50 rounded text-xs text-gray-400">
              <div>이미지 URL: {item.image}</div>
              <div>has_image: {item.has_image ? 'true' : 'false'}</div>
            </div>
          )}
        </div>
      )}

      {/* 이미지가 없거나 로드 실패시 대체 컨텐츠 */}
      {(!item.image || !item.image.trim()) && (
        <div className="glass-card rounded-xl p-4 border-2 border-dashed border-gray-500/30">
          <div className="flex items-center justify-center h-32 text-gray-500">
            <div className="text-center">
              <Building size={32} className="mx-auto mb-2 opacity-50" />
              <p className="text-sm">이미지 없음</p>
            </div>
          </div>
        </div>
      )}

      {/* 관련 종목 태그 */}
      {item.related_symbols && item.related_symbols.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-3 flex items-center space-x-2">
            <BarChart size={16} className="text-green-400" />
            <span>관련 종목</span>
          </h4>
          <div className="flex flex-wrap gap-2">
            {item.related_symbols.map((symbol, index) => (
              <span 
                key={index} 
                className="px-3 py-1.5 rounded-lg bg-green-500/10 text-green-400 border border-green-500/20 text-sm font-medium"
              >
                {symbol}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* 뉴스 내용 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        {/* 헤드라인과 요약 */}
        {item.summary && (
          <div className="p-4 bg-green-500/10 rounded-lg border border-green-500/20">
            <h4 className="font-medium text-green-400 mb-2 text-sm">요약</h4>
            <p className="text-foreground/90 leading-relaxed">{item.summary}</p>
          </div>
        )}

        {/* 짧은 헤드라인이 다르면 표시 */}
        {item.short_headline && item.short_headline !== item.headline && (
          <div className="p-3 glass-subtle rounded-lg">
            <h4 className="font-medium mb-2 text-sm text-foreground/70">요약 헤드라인</h4>
            <p className="text-sm text-foreground/80">{item.short_headline}</p>
          </div>
        )}
      </div>
    </div>
  );

  const renderCompanyNews = (item: CompanyNewsItem) => (
    <div className="space-y-6">
      {/* 주식 정보 카드 */}
      <div className="glass-card rounded-xl p-4">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-3">
            <div className="p-2 rounded-lg bg-purple-500/20">
              <Building size={16} className="text-purple-400" />
            </div>
            <div>
              <div className="font-bold text-lg">{item.symbol}</div>
              <div className="text-sm text-foreground/60">{item.category}</div>
            </div>
          </div>
          {item.rank_position && (
            <div className="text-right">
              <div className="text-xs text-foreground/60">순위</div>
              <div className="font-bold text-purple-400">#{item.rank_position}</div>
            </div>
          )}
        </div>

        {/* 주식 데이터 그리드 */}
        {(item.price || item.change_percentage || item.volume) && (
          <div className="grid grid-cols-3 gap-4 p-3 glass-subtle rounded-lg">
            {item.price && (
              <div className="text-center">
                <div className="text-xs text-foreground/60 mb-1">가격</div>
                <div className="font-bold">${item.price.toFixed(2)}</div>
              </div>
            )}
            {item.change_percentage && (
              <div className="text-center">
                <div className="text-xs text-foreground/60 mb-1">변화율</div>
                <div className={`font-bold ${item.change_percentage.includes('-') ? 'text-red-400' : 'text-green-400'}`}>
                  {item.change_percentage}
                </div>
              </div>
            )}
            {item.volume && (
              <div className="text-center">
                <div className="text-xs text-foreground/60 mb-1">거래량</div>
                <div className="font-bold text-xs">{item.volume.toLocaleString()}</div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* 뉴스 리스트 */}
      <div className="glass-card rounded-xl p-6">
        <h4 className="font-medium mb-4 flex items-center space-x-2">
          <Newspaper size={16} className="text-purple-400" />
          <span>관련 뉴스 ({item.news_count}개)</span>
        </h4>
        
        <div className="space-y-4">
          {item.news?.map((news, index) => (
            <div key={index} className="glass-subtle rounded-lg p-4 hover:glass transition-all">
              <div className="flex space-x-3">
                {news.image && (
                  <img 
                    src={news.image} 
                    alt={news.headline}
                    className="w-16 h-16 object-cover rounded-lg flex-shrink-0"
                    onError={(e) => {
                      (e.target as HTMLImageElement).style.display = 'none';
                    }}
                  />
                )}
                <div className="flex-1 min-w-0">
                  <h5 className="font-medium text-sm mb-2 line-clamp-2">{news.headline}</h5>
                  <p className="text-xs text-foreground/70 mb-2 line-clamp-2">{news.summary}</p>
                  <div className="flex items-center justify-between text-xs text-foreground/60">
                    <span>{news.source}</span>
                    <span>{formatTimeAgo(news.published_at)}</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );

  const renderSentimentNews = (item: SentimentNewsItem) => (
    <div className="space-y-6">
      {/* 감성 분석 메인 카드 */}
      <div className="glass-card rounded-xl p-4">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-3">
            <div className={`p-3 rounded-lg ${getSentimentBgColor(item.overall_sentiment_score)}`}>
              {getSentimentIcon(item.overall_sentiment_score)}
            </div>
            <div>
              <div className="font-bold text-lg">{item.overall_sentiment_label}</div>
              <div className={`text-sm ${getSentimentColor(item.overall_sentiment_score)}`}>
                점수: {(item.overall_sentiment_score * 100).toFixed(1)}%
              </div>
            </div>
          </div>
          
          {item.sentiment_emoji && (
            <span className="text-3xl">{item.sentiment_emoji}</span>
          )}
        </div>

        {/* 감성 점수 프로그레스 바 */}
        <div className="space-y-2">
          <div className="flex justify-between text-xs text-foreground/60">
            <span>매우 부정적</span>
            <span>중립</span>
            <span>매우 긍정적</span>
          </div>
          <div className="w-full bg-gray-700 rounded-full h-2">
            <div 
              className={`h-2 rounded-full transition-all duration-1000 ${
                item.overall_sentiment_score > 0 ? 'bg-green-400' : 'bg-red-400'
              }`}
              style={{ 
                width: `${Math.abs(item.overall_sentiment_score) * 100}%`,
                marginLeft: item.overall_sentiment_score < 0 ? 'auto' : '0'
              }}
            />
          </div>
        </div>

        <div className="flex items-center justify-between text-sm mt-3">
          <div className="flex items-center space-x-2">
            <User size={14} className="text-orange-400" />
            <span className="text-orange-400">{item.authors}</span>
          </div>
          <span className="text-foreground/60">{formatTimeAgo(item.time_published)}</span>
        </div>
      </div>

      {/* 관련 티커 분석 */}
      {item.ticker_sentiment && item.ticker_sentiment.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-4 flex items-center space-x-2">
            <BarChart size={16} className="text-orange-400" />
            <span>종목별 감성 분석</span>
          </h4>
          <div className="grid gap-3">
            {item.ticker_sentiment.map((ticker, index) => (
              <div key={index} className="glass-subtle rounded-lg p-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-bold text-lg">{ticker.ticker}</span>
                  <span className={`px-2 py-1 rounded text-xs font-medium ${getSentimentBgColor(parseFloat(ticker.ticker_sentiment_score))} ${getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}`}>
                    {ticker.ticker_sentiment_label}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <div className="text-sm text-foreground/60">
                    연관성: {(parseFloat(ticker.relevance_score) * 100).toFixed(1)}%
                  </div>
                  <div className={`text-sm font-medium ${getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}`}>
                    감성: {(parseFloat(ticker.ticker_sentiment_score) * 100).toFixed(1)}%
                  </div>
                </div>
                {/* 티커별 감성 바 */}
                <div className="w-full bg-gray-700 rounded-full h-1.5 mt-2">
                  <div 
                    className={`h-1.5 rounded-full ${
                      parseFloat(ticker.ticker_sentiment_score) > 0 ? 'bg-green-400' : 'bg-red-400'
                    }`}
                    style={{ 
                      width: `${Math.abs(parseFloat(ticker.ticker_sentiment_score)) * 100}%`
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 관련 토픽 클라우드 */}
      {item.topics && item.topics.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-4 flex items-center space-x-2">
            <Zap size={16} className="text-orange-400" />
            <span>관련 토픽</span>
          </h4>
          <div className="flex flex-wrap gap-2">
            {item.topics.map((topic, index) => {
              const relevance = parseFloat(topic.relevance_score);
              const opacity = Math.max(0.3, relevance); // 최소 30% 투명도
              const scale = 0.8 + (relevance * 0.4); // 0.8~1.2 크기
              return (
                <span 
                  key={index} 
                  className="px-3 py-2 rounded-lg bg-orange-500/20 text-orange-400 border border-orange-500/30 text-sm font-medium"
                  style={{ 
                    opacity,
                    transform: `scale(${scale})`,
                    transformOrigin: 'center'
                  }}
                >
                  {topic.topic}
                  <span className="ml-1 text-xs opacity-60">
                    {(relevance * 100).toFixed(0)}%
                  </span>
                </span>
              );
            })}
          </div>
        </div>
      )}

      {/* 뉴스 본문 */}
      <div className="glass-card rounded-xl p-6">
        <div className="p-4 bg-orange-500/10 rounded-lg border border-orange-500/20">
          <h4 className="font-medium text-orange-400 mb-2 text-sm">뉴스 요약</h4>
          <p className="text-foreground/90 leading-relaxed">{item.summary}</p>
        </div>

        {/* 배치 정보 */}
        {item.batch_id && (
          <div className="mt-4 p-3 glass-subtle rounded-lg text-xs text-foreground/60">
            <div className="flex justify-between">
              <span>배치 ID: {item.batch_id}</span>
              <span>쿼리: {item.query_type}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );

  // =========================================================================
  // 메인 렌더링
  // =========================================================================

  const getTitle = () => {
    switch (newsItem.type) {
      case "market": return newsItem.title;
      case "financial": return newsItem.headline;
      case "company": return `${newsItem.symbol} - 기업 뉴스`;
      case "sentiment": return newsItem.title;
    }
  };

  const getSourceLabel = () => {
    switch (newsItem.type) {
      case "company":
        return newsItem.news?.[0]?.source || "";
      default:
        return (newsItem as Exclude<NewsItem, CompanyNewsItem>).source;
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      <div className="space-y-4 pb-6">
        {/* 헤더 - 모바일 최적화 */}
        <div className="sticky top-0 z-10 bg-slate-900/90 backdrop-blur-sm border-b border-white/10 p-4 -m-4 mb-4">
          <div className="flex items-center justify-between">
            <button
              onClick={onBack}
              className="flex items-center space-x-2 px-3 py-2 glass-subtle rounded-lg hover:glass transition-all"
            >
              <ArrowLeft size={18} />
              <span className="text-sm">뒤로</span>
            </button>
            
            <button
              onClick={() => window.open(getUrl(), '_blank')}
              className="flex items-center space-x-2 px-3 py-2 glass-card rounded-lg hover:glass transition-all"
            >
              <ExternalLink size={16} />
              <span className="text-sm">원문</span>
            </button>
          </div>
        </div>

        {/* 제목 섹션 - 더 임팩트 있게 */}
        <div className="glass-card rounded-xl p-6">
          <div className="flex items-start justify-between mb-4">
            <h1 className="text-xl font-bold leading-tight pr-4 flex-1 text-white">{getTitle()}</h1>
            <span className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30 flex-shrink-0">
              {getSourceLabel()}
            </span>
          </div>
        </div>

        {/* 타입별 상세 내용 */}
        {newsItem.type === "market" && renderMarketNews(newsItem)}
        {newsItem.type === "financial" && renderFinancialNews(newsItem)}
        {newsItem.type === "company" && renderCompanyNews(newsItem)}
        {newsItem.type === "sentiment" && renderSentimentNews(newsItem)}

        {/* 하단 메타 정보 - 간소화 */}
        <div className="glass-subtle rounded-xl p-4">
          <div className="grid grid-cols-1 gap-2 text-xs text-foreground/60">
            <div className="flex justify-between">
              <span>뉴스 타입:</span>
              <span className="capitalize font-medium">{newsItem.type}</span>
            </div>
            {newsItem.type === "sentiment" && newsItem.created_at && (
              <div className="flex justify-between">
                <span>분석 생성:</span>
                <span>{formatTimeAgo(newsItem.created_at)}</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}