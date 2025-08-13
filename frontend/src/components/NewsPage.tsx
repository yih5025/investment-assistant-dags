import { useState } from "react";
import { Search, Filter, TrendingUp, TrendingDown, Clock, Target, ExternalLink, BarChart } from "lucide-react";
import { Input } from "./ui/input";
import { Badge } from "./ui/badge";

// 뉴스 타입 정의
interface GeneralNewsItem {
  type: "general";
  source: string;
  url: string;
  author: string;
  title: string;
  description: string;
  content: string;
  published_at: string;
}

interface CategoryNewsItem {
  type: "category";
  category: "crypto" | "forex" | "merger" | "general";
  news_id: number;
  datetime: string;
  headline: string;
  image: string;
  related: string;
  source: string;
  summary: string;
  url: string;
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
}

interface CompanyNewsItem {
  type: "company";
  symbol: string;
  report_date: string;
  category: string;
  article_id: number;
  headline: string;
  image: string;
  related: string;
  source: string;
  summary: string;
  url: string;
  published_at: string;
}

interface StockNewsItem {
  type: "stock";
  symbol: string;
  source: string;
  url: string;
  title: string;
  description: string;
  content: string;
  published_at: string;
  fetched_at: string;
}

type NewsItem = GeneralNewsItem | CategoryNewsItem | SentimentNewsItem | CompanyNewsItem | StockNewsItem;

interface NewsPageProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

// 확장된 모의 뉴스 데이터
const mockAllNewsData: NewsItem[] = [
  {
    type: "category",
    category: "crypto",
    news_id: 7498776,
    datetime: "2025-01-28 13:53:48",
    headline: "Bitcoin whale's $9.6B transfer sparks market correction concerns",
    image: "https://images.unsplash.com/photo-1518475155542-3baed9d5c875?w=200&h=120&fit=crop",
    related: "BTC",
    source: "Cointelegraph",
    summary: "An OG Bitcoin whale's $9.6 billion transfer and new regulatory requirements are sparking correction concerns among industry watchers.",
    url: "https://cointelegraph.com/news/bitcoin-whale-9-6b-correction-concerns"
  },
  {
    type: "category",
    category: "forex",
    news_id: 7498777,
    datetime: "2025-01-28 12:30:15",
    headline: "USD strengthens as Fed hints at prolonged rate stability",
    image: "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?w=200&h=120&fit=crop",
    related: "USD",
    source: "ForexLive",
    summary: "The US Dollar gained against major currencies following Federal Reserve officials' comments suggesting rates may remain elevated longer than expected.",
    url: "https://forexlive.com/usd-strengthens-fed-rate-stability"
  },
  {
    type: "sentiment",
    title: "Tesla's Revolutionary Battery Technology Drives Stock Surge",
    url: "https://example.com/tesla-battery-tech",
    time_published: "2025-01-28 14:30:15",
    authors: "Tech Analysis Team",
    summary: "Tesla's announcement of breakthrough solid-state battery technology has sent shares soaring, with analysts upgrading price targets across the board.",
    source: "TechFinance",
    overall_sentiment_score: 0.8456,
    overall_sentiment_label: "Very Bullish",
    ticker_sentiment: [
      {
        ticker: "TSLA",
        relevance_score: "0.956",
        ticker_sentiment_label: "Very Bullish",
        ticker_sentiment_score: "0.8234"
      },
      {
        ticker: "PANW",
        relevance_score: "0.234",
        ticker_sentiment_label: "Neutral",
        ticker_sentiment_score: "0.0123"
      }
    ],
    topics: [
      {
        topic: "Electric Vehicles",
        relevance_score: "0.95"
      },
      {
        topic: "Technology",
        relevance_score: "0.88"
      }
    ],
    query_type: "automotive_tech",
    query_params: "topics=automotive&sort=SENTIMENT"
  },
  {
    type: "company",
    symbol: "AAPL",
    report_date: "2025-01-29",
    category: "earnings",
    article_id: 136067578,
    headline: "Apple Q1 Earnings Preview: iPhone Sales Drive Optimistic Outlook",
    image: "https://images.unsplash.com/photo-1592179900824-cb2cb6ff2c80?w=200&h=120&fit=crop",
    related: "AAPL",
    source: "Reuters",
    summary: "Analysts expect Apple's Q1 earnings to show strong iPhone 15 sales and continued growth in the services segment, with revenue potentially exceeding $120B.",
    url: "https://reuters.com/apple-q1-earnings-preview",
    published_at: "2025-01-28 10:15:30"
  },
  {
    type: "stock",
    symbol: "NVDA",
    source: "MarketWatch",
    url: "https://marketwatch.com/nvidia-ai-breakthrough",
    title: "NVIDIA Stock Jumps on AI Chip Manufacturing Partnership",
    description: "NVIDIA announces strategic partnership with Samsung for next-gen AI chip production, boosting investor confidence in the semiconductor giant.",
    content: "NVIDIA Corporation today announced a groundbreaking partnership with Samsung Semiconductors to manufacture its next-generation AI chips, marking a significant expansion in production capacity.",
    published_at: "2025-01-28 11:45:20",
    fetched_at: "2025-01-28 11:50:30"
  },
  {
    type: "general",
    source: "Financial Times",
    url: "https://ft.com/global-markets-outlook",
    author: "Global Markets Team",
    title: "Global Markets Show Resilience Amid Geopolitical Tensions",
    description: "Despite ongoing geopolitical uncertainties, global equity markets demonstrate remarkable resilience with emerging markets leading gains.",
    content: "Global financial markets have shown remarkable resilience in the face of ongoing geopolitical tensions, with emerging market equities leading gains across major indices.",
    published_at: "2025-01-28 09:20:45"
  },
  {
    type: "category", 
    category: "merger",
    news_id: 7498778,
    datetime: "2025-01-28 08:15:30",
    headline: "Microsoft-Activision Deal Completion Boosts Gaming Sector",
    image: "https://images.unsplash.com/photo-1493711662062-fa541adb3fc8?w=200&h=120&fit=crop",
    related: "MSFT,ATVI",
    source: "GamesBeat",
    summary: "The successful completion of Microsoft's acquisition of Activision Blizzard has energized the gaming sector, with competitors seeing valuation increases.",
    url: "https://gamesbeat.com/microsoft-activision-completion-gaming-boost"
  }
];

export function NewsPage({ isLoggedIn, onLoginPrompt }: NewsPageProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedType, setSelectedType] = useState<"all" | NewsItem["type"]>("all");
  const [selectedCategory, setSelectedCategory] = useState<"all" | "crypto" | "forex" | "merger" | "general">("all");
  const [sortBy, setSortBy] = useState<"recent" | "sentiment" | "relevance">("recent");

  const filteredNews = mockAllNewsData.filter(item => {
    const matchesSearch = (() => {
      const query = searchQuery.toLowerCase();
      switch (item.type) {
        case "general":
          return item.title.toLowerCase().includes(query) || 
                 item.description.toLowerCase().includes(query) ||
                 item.source.toLowerCase().includes(query);
        case "category":
          return item.headline.toLowerCase().includes(query) || 
                 item.summary.toLowerCase().includes(query) ||
                 item.source.toLowerCase().includes(query);
        case "sentiment":
          return item.title.toLowerCase().includes(query) || 
                 item.summary.toLowerCase().includes(query) ||
                 item.source.toLowerCase().includes(query);
        case "company":
          return item.headline.toLowerCase().includes(query) || 
                 item.summary.toLowerCase().includes(query) ||
                 item.symbol.toLowerCase().includes(query);
        case "stock":
          return item.title.toLowerCase().includes(query) || 
                 item.description.toLowerCase().includes(query) ||
                 item.symbol.toLowerCase().includes(query);
        default:
          return false;
      }
    })();

    const matchesType = selectedType === "all" || item.type === selectedType;
    
    const matchesCategory = selectedCategory === "all" || 
      (item.type === "category" && item.category === selectedCategory);

    return matchesSearch && matchesType && matchesCategory;
  }).sort((a, b) => {
    switch (sortBy) {
      case "sentiment":
        const aSentiment = a.type === "sentiment" ? a.overall_sentiment_score : 0;
        const bSentiment = b.type === "sentiment" ? b.overall_sentiment_score : 0;
        return Math.abs(bSentiment) - Math.abs(aSentiment);
      case "relevance":
        // 티커가 있는 뉴스를 우선순위로
        const aHasTicker = (a.type === "company" || a.type === "stock") ? 1 : 0;
        const bHasTicker = (b.type === "company" || b.type === "stock") ? 1 : 0;
        return bHasTicker - aHasTicker;
      default:
        const aTime = new Date((() => {
          switch (a.type) {
            case "general": return a.published_at;
            case "category": return a.datetime;
            case "sentiment": return a.time_published;
            case "company": return a.published_at;
            case "stock": return a.published_at;
          }
        })()).getTime();
        
        const bTime = new Date((() => {
          switch (b.type) {
            case "general": return b.published_at;
            case "category": return b.datetime;
            case "sentiment": return b.time_published;
            case "company": return b.published_at;
            case "stock": return b.published_at;
          }
        })()).getTime();
        
        return bTime - aTime;
    }
  });

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

  const getCategoryColor = (category: string) => {
    switch (category) {
      case "crypto": return "bg-orange-500/20 text-orange-400";
      case "forex": return "bg-green-500/20 text-green-400";
      case "merger": return "bg-purple-500/20 text-purple-400";
      case "earnings": return "bg-blue-500/20 text-blue-400";
      default: return "bg-gray-500/20 text-gray-400";
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case "sentiment": return "bg-blue-500/20 text-blue-400";
      case "company": return "bg-green-500/20 text-green-400";
      case "stock": return "bg-purple-500/20 text-purple-400";
      case "category": return "bg-orange-500/20 text-orange-400";
      default: return "bg-gray-500/20 text-gray-400";
    }
  };

  const renderNewsItem = (item: NewsItem) => {
    const getTitle = () => {
      switch (item.type) {
        case "general": return item.title;
        case "category": return item.headline;
        case "sentiment": return item.title;
        case "company": return item.headline;
        case "stock": return item.title;
      }
    };

    const getSummary = () => {
      switch (item.type) {
        case "general": return item.description;
        case "category": return item.summary;
        case "sentiment": return item.summary;
        case "company": return item.summary;
        case "stock": return item.description;
      }
    };

    const getTimestamp = () => {
      switch (item.type) {
        case "general": return item.published_at;
        case "category": return item.datetime;
        case "sentiment": return item.time_published;
        case "company": return item.published_at;
        case "stock": return item.published_at;
      }
    };

    const getImage = () => {
      switch (item.type) {
        case "category": return item.image;
        case "company": return item.image;
        default: return null;
      }
    };

    return (
      <article
        key={`${item.type}-${item.url}`}
        className="glass-card p-4 rounded-xl cursor-pointer hover:glass transition-all group"
        onClick={() => window.open(item.url, '_blank')}
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
              />
            </div>
          )}

          {/* 콘텐츠 */}
          <div className="flex-1 min-w-0">
            {/* 헤더 */}
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm font-medium text-foreground/90">{item.source}</span>
                
                {/* 타입 배지 */}
                <Badge className={`text-xs ${getTypeColor(item.type)}`}>{item.type}</Badge>
                
                {/* 카테고리 배지 */}
                {item.type === "category" && (
                  <Badge className={`text-xs ${getCategoryColor(item.category)}`}>{item.category}</Badge>
                )}
                
                {/* 심볼 배지 */}
                {(item.type === "company" || item.type === "stock") && (
                  <Badge variant="outline" className="text-xs">{item.symbol}</Badge>
                )}
              </div>
              
              <span className="text-xs text-foreground/50 flex-shrink-0">
                {formatTimestamp(getTimestamp())}
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
                    <span className="hidden sm:inline">({item.overall_sentiment_label})</span>
                  </div>
                )}

                {/* 티커 감성 */}
                {item.type === "sentiment" && item.ticker_sentiment.length > 0 && (
                  <div className="flex gap-1">
                    {item.ticker_sentiment.slice(0, 2).map((ticker, index) => (
                      <Badge 
                        key={index} 
                        className={`text-xs ${getSentimentColor(parseFloat(ticker.ticker_sentiment_score))}`}
                      >
                        {ticker.ticker}
                      </Badge>
                    ))}
                  </div>
                )}

                {/* 관련 토픽 */}
                {item.type === "sentiment" && item.topics.length > 0 && (
                  <Badge variant="outline" className="text-xs">
                    {item.topics[0].topic}
                  </Badge>
                )}
              </div>

              <ExternalLink size={14} className="text-foreground/40 group-hover:text-primary transition-colors flex-shrink-0" />
            </div>
          </div>
        </div>
      </article>
    );
  };

  return (
    <div className="space-y-4">
      {/* 검색 및 필터 */}
      <div className="space-y-3">
        {/* 검색바 */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-foreground/50" size={18} />
          <Input
            placeholder="뉴스 검색..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 glass-card border-white/20 placeholder:text-foreground/50"
          />
        </div>

        {/* 필터 */}
        <div className="flex gap-2 overflow-x-auto pb-2">
          <select
            value={selectedType}
            onChange={(e) => setSelectedType(e.target.value as typeof selectedType)}
            className="px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20 min-w-[100px]"
          >
            <option value="all">전체</option>
            <option value="general">일반</option>
            <option value="category">카테고리</option>
            <option value="sentiment">감성분석</option>
            <option value="company">기업</option>
            <option value="stock">주식</option>
          </select>

          <select
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value as typeof selectedCategory)}
            className="px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20 min-w-[100px]"
          >
            <option value="all">전체</option>
            <option value="crypto">크립토</option>
            <option value="forex">외환</option>
            <option value="merger">M&A</option>
            <option value="general">일반</option>
          </select>

          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
            className="px-3 py-2 rounded-lg glass-card text-sm bg-transparent border-white/20 min-w-[100px]"
          >
            <option value="recent">최신순</option>
            <option value="sentiment">감성순</option>
            <option value="relevance">연관성순</option>
          </select>
        </div>

        {/* 통계 */}
        <div className="flex items-center justify-between text-sm text-foreground/60">
          <span>{filteredNews.length}개 뉴스</span>
          <div className="flex gap-4 text-xs">
            <span>감성: {filteredNews.filter(n => n.type === "sentiment").length}</span>
            <span>기업: {filteredNews.filter(n => n.type === "company" || n.type === "stock").length}</span>
          </div>
        </div>
      </div>

      {/* 뉴스 목록 */}
      <div className="space-y-3">
        {filteredNews.length === 0 ? (
          <div className="glass-card p-8 text-center rounded-xl">
            <BarChart size={48} className="mx-auto mb-4 text-foreground/30" />
            <p className="text-foreground/70">검색 결과가 없습니다</p>
          </div>
        ) : (
          filteredNews.map((item) => renderNewsItem(item))
        )}
      </div>

      {/* 더보기 */}
      {filteredNews.length > 0 && (
        <div className="flex justify-center pt-4">
          <button className="px-6 py-3 glass-card rounded-xl hover:glass transition-all">
            더 많은 뉴스 불러오기
          </button>
        </div>
      )}
    </div>
  );
}