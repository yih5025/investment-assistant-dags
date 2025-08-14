import React, { useState, useEffect } from 'react';
import { Search, Filter, TrendingUp, TrendingDown, Clock, Target, ExternalLink, BarChart, Newspaper, RefreshCw, Calendar, Building, Zap, ChevronDown, ChevronUp, AlertCircle } from 'lucide-react';

// ============================================================================
// 타입 정의 (제공된 백엔드 API 구조에 맞춤)
// ============================================================================

// 1. Market News API (/api/v1/market-news)
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

// 2. Financial News API (/api/v1/financial-news)  
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

// 3. Company News API (/api/v1/company-news)
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

// 4. Market News Sentiment API (/api/v1/market-news-sentiment)
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

// API 응답 타입들
interface ApiResponse<T> {
  total?: number;
  total_count?: number;
  items?: T[];
  news?: T[];
  stocks?: T[];
  batch_info?: any;
  categories?: any;
}

interface NewsPageProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
  onNewsClick?: (newsItem: NewsItem) => void;
}

// ============================================================================
// 메인 뉴스 페이지 컴포넌트
// ============================================================================

export default function IntegratedNewsPage({ isLoggedIn, onLoginPrompt, onNewsClick }: NewsPageProps) {
  // =========================================================================
  // State 관리
  // =========================================================================
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [allNews, setAllNews] = useState<NewsItem[]>([]);
  
  // 검색 및 필터
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedApi, setSelectedApi] = useState<"all" | "market" | "financial" | "company" | "sentiment">("all");
  const [selectedCategory, setSelectedCategory] = useState<"all" | "crypto" | "forex" | "merger" | "general">("all");
  const [selectedSource, setSelectedSource] = useState<string>("all");
  const [sortBy, setSortBy] = useState<"recent" | "sentiment" | "relevance">("recent");
  const [showFilters, setShowFilters] = useState(false);
  
  // 페이징
  const [currentPage, setCurrentPage] = useState(1);
  const [limit] = useState(20);
  const [hasMore, setHasMore] = useState(true);
  
  // 통계
  const [apiStats, setApiStats] = useState({
    market: 0,
    financial: 0,
    company: 0,
    sentiment: 0,
    total: 0
  });

  // 사용 가능한 소스 목록
  const [availableSources, setAvailableSources] = useState<string[]>([]);

  // =========================================================================
  // API 호출 함수들
  // =========================================================================

  // EconomicDashboard와 동일한 방식으로 API Base URL 결정
  const getAPIBaseURL = () => {
    if (typeof window !== 'undefined') {
      const envApiBase = (import.meta as any)?.env?.VITE_API_BASE_URL;
      if (envApiBase) {
        console.log("🌐 환경변수에서 API URL 사용:", envApiBase);
        return envApiBase;
      }
    }

    if (typeof window !== 'undefined') {
      const hostname = window.location.hostname;
      console.log("🔍 현재 환경 분석:", { hostname });
      if (hostname.includes('vercel.app')) {
        console.log("🌐 Vercel 환경 감지 → 외부 API 사용");
        return 'https://api.investment-assistant.site/api/v1';
      }
      if (hostname === 'localhost' || hostname === '127.0.0.1') {
        console.log("🌐 로컬 환경 감지 → 로컬 API 사용");
        return 'http://localhost:8888/api/v1';
      }
      if (hostname.includes('192.168.') || hostname.includes('10.') || hostname.includes('172.')) {
        console.log("🌐 K8s 환경 감지 → 내부 프록시 사용");
        return '/api/v1';
      }
      if (hostname.includes('investment-assistant')) {
        console.log("🌐 커스텀 도메인 감지 → 내부 프록시 사용");
        return '/api/v1';
      }
    }

    console.log("🌐 기본 외부 API URL 사용");
    return 'https://api.investment-assistant.site/api/v1';
  };

  const API_BASE_URL = getAPIBaseURL();

  // 1. Market News API 호출
  const fetchMarketNews = async (days = 7, limit = 20, offset = 0) => {
    try {
      console.log('🔄 Market News API 호출 중...');
      // Backend 스펙: page, limit, (optional) sources
      const page = Math.floor(offset / limit) + 1;
      const params = new URLSearchParams({
        page: page.toString(),
        limit: limit.toString()
      });
      if (selectedSource !== 'all') {
        params.append('sources', selectedSource);
      }

      const response = await fetch(`${API_BASE_URL}/market-news/?${params.toString()}`);
      
      if (!response.ok) {
        throw new Error(`Market News API 오류: ${response.status}`);
      }

      const data: ApiResponse<any> = await response.json();
      console.log('✅ Market News 응답:', data);
      
      const items = data.items || [];
      return items.map((item: any) => ({
        ...item,
        type: "market" as const
      }));
    } catch (error) {
      console.error('❌ Market News API 오류:', error);
      return [];
    }
  };

  // 2. Financial News API 호출
  const fetchFinancialNews = async (limit = 20, offset = 0) => {
    try {
      console.log('🔄 Financial News API 호출 중...');
      // Backend 스펙: page, limit, (optional) categories, sources
      const page = Math.floor(offset / limit) + 1;
      const params = new URLSearchParams({
        page: page.toString(),
        limit: limit.toString()
      });
      if (selectedCategory !== 'all') {
        params.append('categories', selectedCategory);
      }
      if (selectedSource !== 'all') {
        params.append('sources', selectedSource);
      }

      const response = await fetch(`${API_BASE_URL}/financial-news/?${params.toString()}`);
      
      if (!response.ok) {
        throw new Error(`Financial News API 오류: ${response.status}`);
      }

      const data: ApiResponse<any> = await response.json();
      console.log('✅ Financial News 응답:', data);
      
      const items = data.items || [];
      return items.map((item: any) => ({
        ...item,
        type: "financial" as const
      }));
    } catch (error) {
      console.error('❌ Financial News API 오류:', error);
      return [];
    }
  };

  // 3. Company News API 호출 (Trending)
  const fetchCompanyNews = async (days = 3, limit = 20, offset = 0) => {
    try {
      console.log('🔄 Company News API 호출 중...');
      const params = new URLSearchParams({
        days: days.toString(),
        limit: limit.toString(),
        offset: offset.toString()
      });

      const response = await fetch(`${API_BASE_URL}/company-news/trending?${params.toString()}`);
      
      if (!response.ok) {
        throw new Error(`Company News API 오류: ${response.status}`);
      }

      const data: ApiResponse<any> = await response.json();
      console.log('✅ Company News 응답:', data);
      
      const stocks = data.stocks || [];
      return stocks.map((stock: any) => ({
        ...stock,
        type: "company" as const
      }));
    } catch (error) {
      console.error('❌ Company News API 오류:', error);
      return [];
    }
  };

  // 4. Market News Sentiment API 호출
  const fetchSentimentNews = async (days = 7, limit = 20, offset = 0) => {
    try {
      console.log('🔄 Sentiment News API 호출 중...');
      const params = new URLSearchParams({
        days: days.toString(),
        limit: limit.toString(),
        offset: offset.toString(),
        sort_by: 'time_published',
        order: 'desc'
      });

      const response = await fetch(`${API_BASE_URL}/market-news-sentiment/?${params.toString()}`);
      
      if (!response.ok) {
        throw new Error(`Sentiment News API 오류: ${response.status}`);
      }

      const data: ApiResponse<any> = await response.json();
      console.log('✅ Sentiment News 응답:', data);
      
      const news = data.news || [];
      return news.map((item: any) => ({
        ...item,
        type: "sentiment" as const
      }));
    } catch (error) {
      console.error('❌ Sentiment News API 오류:', error);
      return [];
    }
  };

  // =========================================================================
  // 통합 데이터 로딩
  // =========================================================================

  const loadAllNewsData = async (refresh = false) => {
    setLoading(true);
    setError(null);
    
    if (refresh) {
      setAllNews([]);
      setCurrentPage(1);
    }

    try {
      console.log('🚀 모든 뉴스 API 호출 시작...');
      
      const offset = refresh ? 0 : (currentPage - 1) * limit;
      
      // 선택된 API에 따라 호출
      let results: NewsItem[] = [];
      
      if (selectedApi === "all") {
        // 모든 API 병렬 호출
        const [marketNews, financialNews, companyNews, sentimentNews] = await Promise.all([
          fetchMarketNews(7, limit / 4, offset),
          fetchFinancialNews(limit / 4, offset),
          fetchCompanyNews(3, limit / 4, offset),
          fetchSentimentNews(7, limit / 4, offset)
        ]);
        
        results = [...marketNews, ...financialNews, ...companyNews, ...sentimentNews];
      } else {
        // 특정 API만 호출
        switch (selectedApi) {
          case "market":
            results = await fetchMarketNews(7, limit, offset);
            break;
          case "financial":
            results = await fetchFinancialNews(limit, offset);
            break;
          case "company":
            results = await fetchCompanyNews(3, limit, offset);
            break;
          case "sentiment":
            results = await fetchSentimentNews(7, limit, offset);
            break;
        }
      }

      console.log(`✅ 총 ${results.length}개 뉴스 로드됨`);

      // 데이터 업데이트
      if (refresh) {
        setAllNews(results);
      } else {
        setAllNews(prev => [...prev, ...results]);
      }

      // 소스 목록 업데이트 (Company 타입은 내부 뉴스의 source 사용)
      const sources = [...new Set(results.map(item => getItemSourceLabel(item)).filter(Boolean))] as string[];
      setAvailableSources(prev => [...new Set([...prev, ...sources])]);

      // 통계 업데이트
      updateStats(refresh ? results : [...allNews, ...results]);

      // 더 보기 여부 결정
      setHasMore(results.length === limit);

    } catch (error) {
      console.error('❌ 뉴스 로딩 오류:', error);
      setError(error instanceof Error ? error.message : '뉴스를 불러오는데 실패했습니다');
    } finally {
      setLoading(false);
    }
  };

  // 통계 업데이트
  const updateStats = (newsData: NewsItem[]) => {
    const stats = {
      market: newsData.filter(n => n.type === "market").length,
      financial: newsData.filter(n => n.type === "financial").length,
      company: newsData.filter(n => n.type === "company").length,
      sentiment: newsData.filter(n => n.type === "sentiment").length,
      total: newsData.length
    };
    setApiStats(stats);
  };

  // =========================================================================
  // 필터링 및 정렬
  // =========================================================================

  const filteredNews = React.useMemo(() => {
    let filtered = allNews.filter(item => {
      // 검색어 필터
      const matchesSearch = (() => {
        const query = searchQuery.toLowerCase();
        switch (item.type) {
          case "market":
            return item.title.toLowerCase().includes(query) || 
                   item.description.toLowerCase().includes(query) ||
                   item.source.toLowerCase().includes(query);
          case "financial":
            return item.headline.toLowerCase().includes(query) || 
                   item.summary.toLowerCase().includes(query) ||
                   item.source.toLowerCase().includes(query);
          case "company":
            return item.symbol.toLowerCase().includes(query) ||
                   item.news.some(n => n.headline.toLowerCase().includes(query));
          case "sentiment":
            return item.title.toLowerCase().includes(query) || 
                   item.summary.toLowerCase().includes(query) ||
                   item.source.toLowerCase().includes(query);
          default:
            return false;
        }
      })();

      // API 타입 필터
      const matchesApi = selectedApi === "all" || item.type === selectedApi;
      
      // 카테고리 필터 (Financial News만 해당)
      const matchesCategory = selectedCategory === "all" || 
        (item.type === "financial" && item.category === selectedCategory);

      // 소스 필터 (Company 타입 고려)
      const matchesSource = selectedSource === "all" || getItemSourceLabel(item) === selectedSource;

      return matchesSearch && matchesApi && matchesCategory && matchesSource;
    });

    // 정렬
    filtered.sort((a, b) => {
      switch (sortBy) {
        case "sentiment":
          const aSentiment = a.type === "sentiment" ? a.overall_sentiment_score : 0;
          const bSentiment = b.type === "sentiment" ? b.overall_sentiment_score : 0;
          return Math.abs(bSentiment) - Math.abs(aSentiment);
        case "relevance":
          // 티커가 있는 뉴스를 우선순위로
          const aHasTicker = (a.type === "company" || a.type === "sentiment") ? 1 : 0;
          const bHasTicker = (b.type === "company" || b.type === "sentiment") ? 1 : 0;
          return bHasTicker - aHasTicker;
        default: // recent
          const aTime = new Date(getTimestamp(a)).getTime();
          const bTime = new Date(getTimestamp(b)).getTime();
          return bTime - aTime;
      }
    });

    return filtered;
  }, [allNews, searchQuery, selectedApi, selectedCategory, selectedSource, sortBy]);

  // =========================================================================
  // 유틸리티 함수들
  // =========================================================================

  const getTimestamp = (item: NewsItem) => {
    switch (item.type) {
      case "market": return item.published_at;
      case "financial": return item.datetime;
      case "company": return item.news?.[0]?.published_at || "";
      case "sentiment": return item.time_published;
      default: return "";
    }
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / 3600000);
    
    if (diffHours < 1) {
      return "방금 전";
    } else if (diffHours < 24) {
      return `${diffHours}시간 전`;
    } else {
      return date.toLocaleDateString("ko-KR", { 
        month: "short", 
        day: "numeric"
      });
    }
  };

  const getSentimentColor = (score: number) => {
    if (score > 0.3) return "text-green-400";
    if (score < -0.3) return "text-red-400";
    return "text-yellow-400";
  };

  const getSentimentIcon = (score: number) => {
    if (score > 0.3) return <TrendingUp size={16} />;
    if (score < -0.3) return <TrendingDown size={16} />;
    return <Target size={16} />;
  };

  const getApiColor = (type: string) => {
    switch (type) {
      case "market": return "bg-blue-500/20 text-blue-400";
      case "financial": return "bg-green-500/20 text-green-400";
      case "company": return "bg-purple-500/20 text-purple-400";
      case "sentiment": return "bg-orange-500/20 text-orange-400";
      default: return "bg-gray-500/20 text-gray-400";
    }
  };

  // 공통 소스 라벨
  const getItemSourceLabel = (item: NewsItem) => {
    switch (item.type) {
      case "company":
        return item.news?.[0]?.source || "";
      case "market":
      case "financial":
      case "sentiment":
        return item.source;
    }
  };

  const getCategoryColor = (category: string) => {
    switch (category) {
      case "crypto": return "bg-orange-500/20 text-orange-400";
      case "forex": return "bg-green-500/20 text-green-400";
      case "merger": return "bg-purple-500/20 text-purple-400";
      case "general": return "bg-blue-500/20 text-blue-400";
      default: return "bg-gray-500/20 text-gray-400";
    }
  };

  // =========================================================================
  // 이벤트 핸들러
  // =========================================================================

  const handleRefresh = () => {
    loadAllNewsData(true);
  };

  const handleLoadMore = () => {
    if (!loading && hasMore) {
      setCurrentPage(prev => prev + 1);
    }
  };

  const handleNewsClick = (item: NewsItem) => {
    if (onNewsClick) {
      onNewsClick(item);
    } else {
      // 뉴스 URL 결정
      let url = "";
      switch (item.type) {
        case "market":
        case "financial":
        case "sentiment":
          url = item.url;
          break;
        case "company":
          url = item.news?.[0]?.url || "";
          break;
      }
      if (url) {
        window.open(url, '_blank');
      }
    }
  };

  // =========================================================================
  // 생명주기
  // =========================================================================

  useEffect(() => {
    loadAllNewsData(true);
  }, [selectedApi, selectedCategory, selectedSource]);

  useEffect(() => {
    if (currentPage > 1) {
      loadAllNewsData(false);
    }
  }, [currentPage]);

  // =========================================================================
  // 렌더링 함수들
  // =========================================================================

  const renderNewsItem = (item: NewsItem) => {
    const getTitle = () => {
      switch (item.type) {
        case "market": return item.title;
        case "financial": return item.headline;
        case "company": return `${item.symbol} - ${item.news?.[0]?.headline || '뉴스'}`;
        case "sentiment": return item.title;
      }
    };

    const getSummary = () => {
      switch (item.type) {
        case "market": return item.description;
        case "financial": return item.summary;
        case "company": return item.news?.[0]?.summary || `${item.news_count}개 뉴스`;
        case "sentiment": return item.summary;
      }
    };

    const getImage = () => {
      switch (item.type) {
        case "financial": return item.image;
        case "company": return item.news?.[0]?.image;
        default: return null;
      }
    };

    return (
      <article
        key={`${item.type}-${getTimestamp(item)}`}
        className="glass-card p-4 rounded-xl cursor-pointer hover:glass transition-all group"
        onClick={() => handleNewsClick(item)}
      >
        <div className="flex gap-3">
          {/* 이미지 */}
          {getImage() && (
            <div className="flex-shrink-0">
              <img
                src={getImage()!}
                alt=""
                className="w-16 h-16 object-cover rounded-lg"
                loading="lazy"
                onError={(e) => {
                  (e.target as HTMLImageElement).style.display = 'none';
                }}
              />
            </div>
          )}

          {/* 콘텐츠 */}
          <div className="flex-1 min-w-0">
            {/* 헤더 */}
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm font-medium text-foreground/90">{getItemSourceLabel(item)}</span>
                
                {/* API 타입 배지 */}
                <span className={`px-2 py-1 rounded text-xs ${getApiColor(item.type)}`}>
                  {item.type}
                </span>
                
                {/* 카테고리 배지 */}
                {item.type === "financial" && (
                  <span className={`px-2 py-1 rounded text-xs ${getCategoryColor(item.category)}`}>
                    {item.category}
                  </span>
                )}
                
                {/* 심볼 배지 */}
                {item.type === "company" && (
                  <span className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30">
                    {item.symbol}
                  </span>
                )}

                {/* 감성 이모지 */}
                {item.type === "sentiment" && item.sentiment_emoji && (
                  <span className="text-sm">{item.sentiment_emoji}</span>
                )}
              </div>
              
              <span className="text-xs text-foreground/50 flex-shrink-0">
                {formatTimestamp(getTimestamp(item))}
              </span>
            </div>

            {/* 제목 */}
            <h3 className="font-medium mb-2 line-clamp-2 group-hover:text-primary transition-colors leading-tight">
              {getTitle()}
            </h3>

            {/* 요약 */}
            <p className="text-sm text-foreground/70 line-clamp-2 mb-3 leading-snug">
              {getSummary()}
            </p>

            {/* 하단 정보 */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 flex-wrap">
                {/* 감성 점수 */}
                {item.type === "sentiment" && (
                  <div className={`flex items-center gap-1 text-xs ${getSentimentColor(item.overall_sentiment_score)}`}>
                    {getSentimentIcon(item.overall_sentiment_score)}
                    <span className="font-medium">
                      {(item.overall_sentiment_score * 100).toFixed(0)}%
                    </span>
                  </div>
                )}

                {/* 티커 감성 */}
                {item.type === "sentiment" && item.ticker_sentiment?.length > 0 && (
                  <div className="flex gap-1">
                    {item.ticker_sentiment.slice(0, 2).map((ticker, index) => (
                      <span 
                        key={index} 
                        className={`px-1.5 py-0.5 rounded text-xs ${getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}`}
                      >
                        {ticker.ticker}
                      </span>
                    ))}
                  </div>
                )}

                {/* Company News 추가 정보 */}
                {item.type === "company" && (
                  <div className="flex gap-2 text-xs">
                    {item.price && (
                      <span className="text-foreground/60">${item.price.toFixed(2)}</span>
                    )}
                    {item.change_percentage && (
                      <span className={item.change_percentage.includes('-') ? 'text-red-400' : 'text-green-400'}>
                        {item.change_percentage}
                      </span>
                    )}
                  </div>
                )}

                {/* 관련 토픽 */}
                {item.type === "sentiment" && item.topics?.length > 0 && (
                  <span className="px-2 py-1 rounded text-xs bg-gray-500/20 text-gray-400 border border-gray-500/30">
                    {item.topics[0].topic}
                  </span>
                )}
              </div>

              <div className="text-xs text-foreground/50 flex-shrink-0">자세히 보기 →</div>
            </div>
          </div>
        </div>
      </article>
    );
  };

  // =========================================================================
  // 메인 렌더링
  // =========================================================================

  return (
    <div className="space-y-4">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <Newspaper size={24} className="text-primary" />
          <h2 className="text-xl font-bold">통합 뉴스</h2>
          <span className="text-sm text-foreground/60">
            ({apiStats.total}개 뉴스)
          </span>
        </div>
        
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center space-x-2 px-3 py-2 glass-card rounded-lg hover:glass transition-all disabled:opacity-50"
        >
          <RefreshCw size={16} className={loading ? "animate-spin" : ""} />
          <span className="text-sm">새로고침</span>
        </button>
      </div>

      {/* API 통계 */}
      <div className="grid grid-cols-5 gap-3">
        <div className="glass-card p-3 rounded-lg text-center">
          <div className="text-lg font-bold text-primary">{apiStats.total}</div>
          <div className="text-xs text-foreground/60">전체</div>
        </div>
        <div className="glass-card p-3 rounded-lg text-center">
          <div className="text-lg font-bold text-blue-400">{apiStats.market}</div>
          <div className="text-xs text-foreground/60">시장뉴스</div>
        </div>
        <div className="glass-card p-3 rounded-lg text-center">
          <div className="text-lg font-bold text-green-400">{apiStats.financial}</div>
          <div className="text-xs text-foreground/60">금융뉴스</div>
        </div>
        <div className="glass-card p-3 rounded-lg text-center">
          <div className="text-lg font-bold text-purple-400">{apiStats.company}</div>
          <div className="text-xs text-foreground/60">기업뉴스</div>
        </div>
        <div className="glass-card p-3 rounded-lg text-center">
          <div className="text-lg font-bold text-orange-400">{apiStats.sentiment}</div>
          <div className="text-xs text-foreground/60">감성분석</div>
        </div>
      </div>

      {/* 검색 및 필터 */}
      <div className="space-y-3">
        {/* 검색바 */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-foreground/50" size={18} />
          <input
            type="text"
            placeholder="뉴스 검색..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-3 glass-card border-white/20 rounded-lg bg-transparent placeholder:text-foreground/50 focus:outline-none focus:ring-2 focus:ring-primary/50"
          />
        </div>

        {/* 필터 토글 */}
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="flex items-center space-x-2 px-3 py-2 glass-card rounded-lg hover:glass transition-all"
        >
          <Filter size={16} />
          <span className="text-sm">필터</span>
          {showFilters ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        {/* 필터 패널 */}
        {showFilters && (
          <div className="glass-card p-4 rounded-lg space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              {/* API 선택 */}
              <div>
                <label className="block text-xs font-medium mb-1">API 타입</label>
                <select
                  value={selectedApi}
                  onChange={(e) => setSelectedApi(e.target.value as typeof selectedApi)}
                  className="w-full px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20"
                >
                  <option value="all">전체</option>
                  <option value="market">시장뉴스</option>
                  <option value="financial">금융뉴스</option>
                  <option value="company">기업뉴스</option>
                  <option value="sentiment">감성분석</option>
                </select>
              </div>

              {/* 카테고리 선택 */}
              <div>
                <label className="block text-xs font-medium mb-1">카테고리</label>
                <select
                  value={selectedCategory}
                  onChange={(e) => setSelectedCategory(e.target.value as typeof selectedCategory)}
                  className="w-full px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20"
                  disabled={selectedApi !== "all" && selectedApi !== "financial"}
                >
                  <option value="all">전체</option>
                  <option value="crypto">크립토</option>
                  <option value="forex">외환</option>
                  <option value="merger">M&A</option>
                  <option value="general">일반</option>
                </select>
              </div>

              {/* 소스 선택 */}
              <div>
                <label className="block text-xs font-medium mb-1">뉴스 소스</label>
                <select
                  value={selectedSource}
                  onChange={(e) => setSelectedSource(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20"
                >
                  <option value="all">전체 소스</option>
                  {availableSources.map(source => (
                    <option key={source} value={source}>{source}</option>
                  ))}
                </select>
              </div>

              {/* 정렬 선택 */}
              <div>
                <label className="block text-xs font-medium mb-1">정렬</label>
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
                  className="w-full px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20"
                >
                  <option value="recent">최신순</option>
                  <option value="sentiment">감성순</option>
                  <option value="relevance">연관성순</option>
                </select>
              </div>
            </div>

            {/* 필터 상태 */}
            <div className="flex items-center justify-between text-sm text-foreground/60">
              <span>
                {filteredNews.length}개 뉴스 (전체 {allNews.length}개 중)
              </span>
              <div className="flex gap-4 text-xs">
                <span>시장: {filteredNews.filter(n => n.type === "market").length}</span>
                <span>금융: {filteredNews.filter(n => n.type === "financial").length}</span>
                <span>기업: {filteredNews.filter(n => n.type === "company").length}</span>
                <span>감성: {filteredNews.filter(n => n.type === "sentiment").length}</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* 에러 표시 */}
      {error && (
        <div className="glass-card p-4 rounded-xl border border-red-500/30 bg-red-500/10">
          <div className="flex items-center space-x-2 text-red-400">
            <AlertCircle size={20} />
            <span className="font-medium">오류 발생</span>
          </div>
          <p className="text-sm text-red-300 mt-2">{error}</p>
          <button
            onClick={() => setError(null)}
            className="mt-3 px-3 py-1 text-xs bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30 transition-colors"
          >
            닫기
          </button>
        </div>
      )}

      {/* 로딩 표시 */}
      {loading && (
        <div className="glass-card p-8 text-center rounded-xl">
          <RefreshCw size={48} className="mx-auto mb-4 text-primary animate-spin" />
          <p className="text-foreground/70">뉴스를 불러오는 중...</p>
        </div>
      )}

      {/* 뉴스 목록 */}
      <div className="space-y-3">
        {filteredNews.length === 0 && !loading ? (
          <div className="glass-card p-8 text-center rounded-xl">
            <BarChart size={48} className="mx-auto mb-4 text-foreground/30" />
            <p className="text-foreground/70">
              {searchQuery ? "검색 결과가 없습니다" : "뉴스를 불러올 수 없습니다"}
            </p>
            {searchQuery && (
              <button
                onClick={() => setSearchQuery("")}
                className="mt-3 px-4 py-2 text-sm glass-card rounded-lg hover:glass transition-all"
              >
                검색어 지우기
              </button>
            )}
          </div>
        ) : (
          <>
            {filteredNews.map((item, index) => renderNewsItem(item))}

            {/* 더보기 버튼 */}
            {hasMore && !loading && filteredNews.length > 0 && (
              <div className="flex justify-center pt-4">
                <button
                  onClick={handleLoadMore}
                  className="px-6 py-3 glass-card rounded-xl hover:glass transition-all flex items-center space-x-2"
                >
                  <span>더 많은 뉴스 불러오기</span>
                  <ChevronDown size={16} />
                </button>
              </div>
            )}

            {/* 로딩 중 표시 (더보기) */}
            {loading && filteredNews.length > 0 && (
              <div className="flex justify-center pt-4">
                <div className="flex items-center space-x-2 text-foreground/60">
                  <RefreshCw size={16} className="animate-spin" />
                  <span className="text-sm">추가 뉴스 로딩 중...</span>
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* 디버깅 정보 (개발환경에서만) */}
      {process.env.NODE_ENV === 'development' && (
        <div className="glass-card p-4 rounded-lg space-y-2 text-xs text-foreground/60">
          <div className="font-medium">디버깅 정보:</div>
          <div>API Base URL: {API_BASE_URL}</div>
          <div>현재 페이지: {currentPage}</div>
          <div>로딩 상태: {loading ? "로딩중" : "완료"}</div>
          <div>더보기 가능: {hasMore ? "예" : "아니오"}</div>
          <div>전체 뉴스: {allNews.length}개</div>
          <div>필터된 뉴스: {filteredNews.length}개</div>
          <div>사용 가능한 소스: {availableSources.length}개</div>
        </div>
      )}
    </div>
  );
}