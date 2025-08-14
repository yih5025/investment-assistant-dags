// NewsList.tsx
import { useState } from "react";
import { Newspaper, ExternalLink, TrendingUp, TrendingDown, Clock, Target, ArrowRight } from "lucide-react";
import { Badge } from "./ui/badge";

// 5가지 뉴스 타입 정의d
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

interface NewsListProps {
  onViewAll?: () => void;
}

const mockNewsData: NewsItem[] = [
  {
    type: "category",
    category: "crypto",
    news_id: 7498776,
    datetime: "2025-01-28 13:53:48",
    headline: "Bitcoin whale's $9.6B transfer, GENIUS Act spark correction concerns",
    image: "https://images.unsplash.com/photo-1518475155542-3baed9d5c875?w=150&h=100&fit=crop",
    related: "BTC",
    source: "Cointelegraph",
    summary: "An OG Bitcoin whale's $9.6 billion transfer and the stablecoin audit requirements imposed by the GENIUS Act are sparking correction concerns among some industry watchers.",
    url: "https://cointelegraph.com/news/bitcoin-whale-9-6b-genius-act-correction-concerns"
  },
  {
    type: "sentiment",
    title: "Tesla Stock Surges on Strong Q4 Delivery Numbers",
    url: "https://example.com/tesla-surge",
    time_published: "2025-01-28 14:30:15",
    authors: "Market Insights",
    summary: "Tesla's Q4 delivery numbers exceeded analyst expectations, driving significant positive sentiment in the electric vehicle sector.",
    source: "Financial Times",
    overall_sentiment_score: 0.7845,
    overall_sentiment_label: "Bullish",
    ticker_sentiment: [
      {
        ticker: "TSLA",
        relevance_score: "0.945",
        ticker_sentiment_label: "Very Bullish",
        ticker_sentiment_score: "0.8234"
      }
    ],
    topics: [
      {
        topic: "Electric Vehicles",
        relevance_score: "0.95"
      },
      {
        topic: "Earnings",
        relevance_score: "0.85"
      }
    ],
    query_type: "automotive",
    query_params: "topics=automotive&sort=LATEST"
  },
  {
    type: "company",
    symbol: "AAPL",
    report_date: "2025-01-29",
    category: "earnings",
    article_id: 136067578,
    headline: "Apple Prepares for Q1 Earnings: iPhone Sales Expected to Drive Growth",
    image: "https://images.unsplash.com/photo-1592179900824-cb2cb6ff2c80?w=150&h=100&fit=crop",
    related: "AAPL",
    source: "Reuters",
    summary: "Ahead of Apple's Q1 earnings announcement, analysts expect strong iPhone 15 sales and continued growth in the services segment.",
    url: "https://reuters.com/apple-q1-earnings-preview",
    published_at: "2025-01-28 10:15:30"
  },
  {
    type: "general",
    source: "Bloomberg",
    url: "https://bloomberg.com/fed-interest-rates",
    author: "Economic Team",
    title: "Federal Reserve Signals Potential Rate Cuts in Q2 2025",
    description: "The Federal Reserve hints at possible interest rate adjustments as inflation continues to moderate and economic growth remains stable.",
    content: "Federal Reserve officials have begun signaling potential interest rate cuts in the second quarter of 2025, citing continued moderation in inflation rates and stable economic growth indicators. This shift in monetary policy could have significant implications for both equity and bond markets.",
    published_at: "2025-01-28 08:45:22"
  },
  {
    type: "stock",
    symbol: "NVDA",
    source: "TechCrunch",
    url: "https://techcrunch.com/nvidia-ai-chips",
    title: "NVIDIA Unveils Next-Generation AI Chips for Data Centers",
    description: "NVIDIA announces breakthrough in AI chip technology, promising 5x performance improvement for data center applications.",
    content: "NVIDIA Corporation today unveiled its latest generation of AI chips designed specifically for data center applications, featuring unprecedented performance improvements and energy efficiency gains.",
    published_at: "2025-01-28 11:20:45",
    fetched_at: "2025-01-28 11:25:30"
  }
];

export function NewsList({ onViewAll }: NewsListProps) {
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
      return date.toLocaleDateString("ko-KR", { month: "short", day: "numeric" });
    }
  };

  const getSentimentColor = (score: number) => {
    if (score > 0.3) return "text-green-400";
    if (score < -0.3) return "text-red-400";
    return "text-yellow-400";
  };

  const getSentimentIcon = (score: number) => {
    if (score > 0.3) return <TrendingUp size={14} />;
    if (score < -0.3) return <TrendingDown size={14} />;
    return <Target size={14} />;
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

  const handleViewAllClick = () => {
    if (onViewAll) {
      onViewAll();
    } else {
      window.dispatchEvent(new CustomEvent('navigateToNews'));
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

    return (
      <div
        key={`${item.type}-${item.url}`}
        className="glass-subtle p-3 rounded-lg cursor-pointer hover:glass transition-all group"
      >
        <div className="flex items-start justify-between mb-2">
          <div className="flex items-center space-x-2">
            <span className="text-sm font-medium">{item.source}</span>
            
            {/* 타입별 배지 */}
            {item.type === "category" && (
              <Badge className={getCategoryColor(item.category)}>{item.category}</Badge>
            )}
            {item.type === "company" && item.symbol && (
              <Badge variant="outline" className="text-xs">{item.symbol}</Badge>
            )}
            {item.type === "stock" && item.symbol && (
              <Badge variant="outline" className="text-xs">{item.symbol}</Badge>
            )}
          </div>
          
          <div className="flex items-center space-x-2">
            {/* 감성 점수 표시 */}
            {item.type === "sentiment" && (
              <div className={`flex items-center space-x-1 ${getSentimentColor(item.overall_sentiment_score)}`}>
                {getSentimentIcon(item.overall_sentiment_score)}
                <span className="text-xs">{(item.overall_sentiment_score * 100).toFixed(0)}%</span>
              </div>
            )}
            
            <span className="text-xs text-foreground/60">{formatTimestamp(getTimestamp())}</span>
          </div>
        </div>

        <h4 className="font-medium text-sm mb-2 line-clamp-2">{getTitle()}</h4>
        <p className="text-xs text-foreground/70 line-clamp-2 mb-2">{getSummary()}</p>

        {/* 추가 정보 */}
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            {/* 티커 감성 정보 */}
            {item.type === "sentiment" && item.ticker_sentiment.length > 0 && (
              <div className="flex space-x-1">
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

          <ExternalLink size={14} className="text-foreground/40 group-hover:text-foreground/60 transition-colors" />
        </div>
      </div>
    );
  };

  return (
    <div className="glass-card p-4 rounded-xl">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center space-x-2">
          <Newspaper size={20} className="text-primary" />
          <h3 className="font-medium">뉴스</h3>
        </div>
        
        <button
          onClick={handleViewAllClick}
          className="flex items-center space-x-1 px-3 py-1 glass-subtle rounded-lg hover:glass transition-all text-sm"
        >
          <span>전체보기</span>
          <ArrowRight size={14} />
        </button>
      </div>

      {/* 뉴스 목록 */}
      <div className="space-y-3">
        {mockNewsData.slice(0, 4).map((item) => renderNewsItem(item))}
      </div>

      {/* 더보기 버튼 */}
      <div className="mt-4 text-center">
        <button
          onClick={handleViewAllClick}
          className="px-4 py-2 glass-subtle rounded-lg hover:glass transition-all text-sm"
        >
          더 많은 뉴스 보기
        </button>
      </div>
    </div>
  );
}