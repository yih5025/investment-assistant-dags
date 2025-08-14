import React from 'react';
import { ArrowLeft, ExternalLink, TrendingUp, TrendingDown, Target, Calendar, Building, Clock, Zap, BarChart, Newspaper, ChevronUp } from 'lucide-react';

// ============================================================================
// 타입 정의 (메인 뉴스 페이지와 동일)
// ============================================================================

// 1. Market News API
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

// 2. Financial News API
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

// 3. Company News API
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

// 4. Market News Sentiment API
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

export default function IntegratedNewsDetailPage({ newsItem, onBack }: NewsDetailPageProps) {
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

  const getSentimentColor = (score: number) => {
    if (score > 0.3) return "text-green-400";
    if (score < -0.3) return "text-red-400";
    return "text-yellow-400";
  };

  const getSentimentIcon = (score: number) => {
    if (score > 0.3) return <TrendingUp size={20} />;
    if (score < -0.3) return <TrendingDown size={20} />;
    return <Target size={20} />;
  };

  const getApiColor = (type: string) => {
    switch (type) {
      case "market": return "bg-blue-500/20 text-blue-400 border-blue-500/30";
      case "financial": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "company": return "bg-purple-500/20 text-purple-400 border-purple-500/30";
      case "sentiment": return "bg-orange-500/20 text-orange-400 border-orange-500/30";
      default: return "bg-gray-500/20 text-gray-400 border-gray-500/30";
    }
  };

  const getCategoryColor = (category: string) => {
    switch (category) {
      case "crypto": return "bg-orange-500/20 text-orange-400 border-orange-500/30";
      case "forex": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "merger": return "bg-purple-500/20 text-purple-400 border-purple-500/30";
      case "general": return "bg-blue-500/20 text-blue-400 border-blue-500/30";
      default: return "bg-gray-500/20 text-gray-400 border-gray-500/30";
    }
  };

  const getTitle = () => {
    switch (newsItem.type) {
      case "market": return newsItem.title;
      case "financial": return newsItem.headline;
      case "company": return `${newsItem.symbol} - 기업 뉴스`;
      case "sentiment": return newsItem.title;
    }
  };

  const getDescription = () => {
    switch (newsItem.type) {
      case "market": return newsItem.description;
      case "financial": return newsItem.summary;
      case "company": return `${newsItem.symbol} 관련 ${newsItem.news_count}개 뉴스`;
      case "sentiment": return newsItem.summary;
    }
  };

  const getTimestamp = () => {
    switch (newsItem.type) {
      case "market": return newsItem.published_at;
      case "financial": return newsItem.datetime;
      case "company": return newsItem.news?.[0]?.published_at || "";
      case "sentiment": return newsItem.time_published;
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

  const getSourceLabel = () => {
    switch (newsItem.type) {
      case "company":
        return newsItem.news?.[0]?.source || "";
      default:
        return (newsItem as Exclude<NewsItem, CompanyNewsItem>).source;
    }
  };

  // =========================================================================
  // 타입별 상세 렌더링 함수들
  // =========================================================================

  const renderMarketNews = (item: MarketNewsItem) => (
    <div className="space-y-6">
      {/* 메타 정보 */}
      <div className="glass-subtle rounded-xl p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Building size={16} className="text-foreground/60" />
            <span className="text-sm text-foreground/80">{item.author}</span>
          </div>
          <span className={`px-2 py-1 rounded border text-xs ${getApiColor("market")}`}>시장뉴스</span>
        </div>
        
        <div className="flex items-center space-x-2">
          <Clock size={16} className="text-foreground/60" />
          <span className="text-sm text-foreground/70">{formatTimestamp(item.published_at)}</span>
        </div>
      </div>

      {/* 본문 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        <div className="prose prose-invert max-w-none">
          <p className="leading-relaxed text-foreground/90 mb-4">
            {item.description}
          </p>
          
          {item.content && item.content !== item.description && (
            <div className="mt-4 p-4 glass-subtle rounded-lg">
              <h4 className="font-medium mb-2">전체 내용</h4>
              <p className="text-sm text-foreground/80 leading-relaxed">{item.content}</p>
            </div>
          )}

          {item.short_description && item.short_description !== item.description && (
            <div className="mt-4 p-4 glass-subtle rounded-lg">
              <h4 className="font-medium mb-2">요약</h4>
              <p className="text-sm text-foreground/80">{item.short_description}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const renderFinancialNews = (item: FinancialNewsItem) => (
    <div className="space-y-6">
      {/* 카테고리 정보 */}
      <div className="glass-subtle rounded-xl p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Zap size={16} className="text-foreground/60" />
            <span className={`px-2 py-1 rounded border text-xs ${getCategoryColor(item.category)}`}>
              {item.category_display_name || item.category}
            </span>
          </div>
          <span className="text-xs text-foreground/60">ID: {item.news_id}</span>
        </div>
        
        <div className="flex items-center space-x-2">
          <Clock size={16} className="text-foreground/60" />
          <span className="text-sm text-foreground/70">{formatTimestamp(item.datetime)}</span>
        </div>

        {item.has_image && item.image && (
          <div className="flex items-center space-x-2">
            <span className="text-xs text-green-400">✓ 이미지 포함</span>
          </div>
        )}
      </div>

      {/* 이미지 */}
      {item.image && (
        <div className="glass-card rounded-xl p-4">
          <img 
            src={item.image} 
            alt={item.headline}
            className="w-full max-w-md mx-auto rounded-lg"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = 'none';
            }}
          />
        </div>
      )}

      {/* 관련 심볼 */}
      {item.related_symbols && item.related_symbols.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-3 flex items-center space-x-2">
            <BarChart size={16} />
            <span>관련 종목</span>
          </h4>
          <div className="flex flex-wrap gap-2">
            {item.related_symbols.map((symbol, index) => (
              <span key={index} className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30">
                {symbol}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* 본문 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        <div className="space-y-3">
          <div>
            <h4 className="font-medium mb-2">헤드라인</h4>
            <p className="text-foreground/90">{item.headline}</p>
          </div>
          
          {item.short_headline && item.short_headline !== item.headline && (
            <div>
              <h4 className="font-medium mb-2">짧은 헤드라인</h4>
              <p className="text-sm text-foreground/80">{item.short_headline}</p>
            </div>
          )}

          <div>
            <h4 className="font-medium mb-2">요약</h4>
            <p className="text-sm text-foreground/80 leading-relaxed">{item.summary}</p>
          </div>
        </div>
      </div>
    </div>
  );

  const renderCompanyNews = (item: CompanyNewsItem) => (
    <div className="space-y-6">
      {/* 주식 정보 */}
      <div className="glass-subtle rounded-xl p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <TrendingUp size={16} className="text-primary" />
            <span className="font-bold text-lg">{item.symbol}</span>
            <span className={`px-2 py-1 rounded border text-xs ${getCategoryColor(item.category)}`}>
              {item.category}
            </span>
          </div>
          {item.rank_position && (
            <span className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30">
              #{item.rank_position}
            </span>
          )}
        </div>

        {/* 주식 데이터 */}
        {(item.price || item.change_percentage || item.volume) && (
          <div className="grid grid-cols-3 gap-3 mt-3">
            {item.price && (
              <div className="text-center">
                <div className="text-xs text-foreground/60">가격</div>
                <div className="font-medium">${item.price.toFixed(2)}</div>
              </div>
            )}
            {item.change_percentage && (
              <div className="text-center">
                <div className="text-xs text-foreground/60">변화율</div>
                <div className={`font-medium ${item.change_percentage.includes('-') ? 'text-red-400' : 'text-green-400'}`}>
                  {item.change_percentage}
                </div>
              </div>
            )}
            {item.volume && (
              <div className="text-center">
                <div className="text-xs text-foreground/60">거래량</div>
                <div className="font-medium text-xs">{item.volume.toLocaleString()}</div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* 뉴스 목록 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        <h4 className="font-medium mb-3 flex items-center space-x-2">
          <Newspaper size={16} />
          <span>관련 뉴스 ({item.news_count}개)</span>
        </h4>
        
        <div className="space-y-3">
          {item.news?.map((news, index) => (
            <div key={index} className="glass-subtle rounded-lg p-3">
              <div className="flex items-start space-x-3">
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
                <div className="flex-1">
                  <h5 className="font-medium text-sm mb-1">{news.headline}</h5>
                  <p className="text-xs text-foreground/70 mb-2">{news.summary}</p>
                  <div className="flex items-center justify-between text-xs text-foreground/60">
                    <span>{news.source}</span>
                    <span>{formatTimestamp(news.published_at)}</span>
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
      {/* 감성 분석 메인 */}
      <div className="glass-subtle rounded-xl p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className={`p-2 rounded-lg ${getSentimentColor(item.overall_sentiment_score)} bg-current/10`}>
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
            <span className="text-2xl">{item.sentiment_emoji}</span>
          )}
        </div>

        <div className="flex items-center space-x-2">
          <Clock size={16} className="text-foreground/60" />
          <span className="text-sm text-foreground/70">{formatTimestamp(item.time_published)}</span>
        </div>

        {item.batch_id && (
          <div className="text-xs text-foreground/60">
            배치 ID: {item.batch_id} • 쿼리: {item.query_type}
          </div>
        )}
      </div>

      {/* 관련 티커 분석 */}
      {item.ticker_sentiment && item.ticker_sentiment.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-3 flex items-center space-x-2">
            <BarChart size={16} />
            <span>관련 종목 분석</span>
          </h4>
          <div className="space-y-3">
            {item.ticker_sentiment.map((ticker, index) => (
              <div key={index} className="glass-subtle rounded-lg p-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-bold">{ticker.ticker}</span>
                  <span className={`px-2 py-1 rounded text-xs ${getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}`}>
                    {ticker.ticker_sentiment_label}
                  </span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-foreground/60">연관성: {(parseFloat(ticker.relevance_score) * 100).toFixed(1)}%</span>
                  <span className={getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}>
                    감성: {(parseFloat(ticker.ticker_sentiment_score) * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 관련 토픽 */}
      {item.topics && item.topics.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <h4 className="font-medium mb-3 flex items-center space-x-2">
            <Target size={16} />
            <span>관련 토픽</span>
          </h4>
          <div className="space-y-2">
            {item.topics.map((topic, index) => (
              <div key={index} className="flex items-center justify-between glass-subtle rounded-lg p-2">
                <span className="text-sm">{topic.topic}</span>
                <div className="text-xs text-foreground/60">
                  {(parseFloat(topic.relevance_score) * 100).toFixed(0)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 뉴스 본문 */}
      <div className="glass-card rounded-xl p-6 space-y-4">
        <div className="flex items-center space-x-2 mb-4">
          <Building size={16} className="text-foreground/60" />
          <span className="text-sm text-foreground/80">{item.authors}</span>
        </div>
        
        <div className="prose prose-invert max-w-none">
          <p className="leading-relaxed text-foreground/90">
            {item.summary}
          </p>
        </div>
      </div>
    </div>
  );

  // =========================================================================
  // 메인 렌더링
  // =========================================================================

  return (
    <div className="space-y-4">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <button
          onClick={onBack}
          className="flex items-center space-x-2 px-3 py-1.5 glass-subtle rounded-lg hover:glass transition-all"
        >
          <ArrowLeft size={16} />
          <span className="text-sm">뒤로가기</span>
        </button>
        
        <button
          onClick={() => window.open(getUrl(), '_blank')}
          className="flex items-center space-x-2 px-3 py-1.5 glass-card rounded-lg hover:glass transition-all"
        >
          <ExternalLink size={16} />
          <span className="text-sm">원문보기</span>
        </button>
      </div>

      {/* 제목 */}
      <div className="glass-card rounded-xl p-6">
        <div className="flex items-start justify-between mb-4">
          <h1 className="text-xl font-bold leading-tight pr-4">{getTitle()}</h1>
          <span className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30 flex-shrink-0">
            {getSourceLabel()}
          </span>
        </div>
        
        <p className="text-foreground/80 leading-relaxed">
          {getDescription()}
        </p>
      </div>

      {/* 타입별 상세 내용 */}
      {newsItem.type === "market" && renderMarketNews(newsItem)}
      {newsItem.type === "financial" && renderFinancialNews(newsItem)}
      {newsItem.type === "company" && renderCompanyNews(newsItem)}
      {newsItem.type === "sentiment" && renderSentimentNews(newsItem)}

      {/* 메타 정보 */}
      <div className="glass-subtle rounded-xl p-4">
        <div className="grid grid-cols-2 gap-3 text-xs text-foreground/60">
          <div>
            <span>발행:</span>
            <span className="ml-2">{formatTimestamp(getTimestamp())}</span>
          </div>
          {newsItem.type !== "sentiment" && "fetched_at" in newsItem && newsItem.fetched_at && (
            <div>
              <span>수집:</span>
              <span className="ml-2">{formatTimestamp(newsItem.fetched_at)}</span>
            </div>
          )}
          {newsItem.type === "sentiment" && newsItem.created_at && (
            <div>
              <span>생성:</span>
              <span className="ml-2">{formatTimestamp(newsItem.created_at)}</span>
            </div>
          )}
          <div>
            <span>타입:</span>
            <span className="ml-2 capitalize">{newsItem.type}</span>
          </div>
        </div>
      </div>

      {/* 개발자 디버깅 정보 */}
      {process.env.NODE_ENV === 'development' && (
        <div className="glass-card p-4 rounded-lg space-y-2 text-xs text-foreground/60">
          <div className="font-medium">디버깅 정보 (뉴스 상세):</div>
          <div>뉴스 타입: {newsItem.type}</div>
          <div>소스: {getSourceLabel()}</div>
          <div>URL: {getUrl()}</div>
          {newsItem.type === "company" && (
            <div>뉴스 개수: {newsItem.news_count}개</div>
          )}
          {newsItem.type === "sentiment" && (
            <>
              <div>감성 점수: {newsItem.overall_sentiment_score}</div>
              <div>티커 개수: {newsItem.ticker_sentiment?.length || 0}개</div>
              <div>토픽 개수: {newsItem.topics?.length || 0}개</div>
            </>
          )}
        </div>
      )}
    </div>
  );
}