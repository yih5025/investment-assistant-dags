import { useState, useEffect } from "react";
import { TrendingUp, TrendingDown, Activity } from "lucide-react";

interface StockData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  volume?: number;
}

interface StockNewsItem {
  symbol: string;
  source: string;
  url: string;
  title: string;
  description: string;
  content: string;
  published_at: string;
  fetched_at?: string;
}

const mockGainerData: StockData[] = [
  { symbol: "TSLA", name: "Tesla Inc", price: 248.50, change: 12.30, changePercent: 5.2 },
  { symbol: "NVDA", name: "NVIDIA Corp", price: 875.20, change: 45.80, changePercent: 5.5 },
  { symbol: "AAPL", name: "Apple Inc", price: 189.95, change: 8.45, changePercent: 4.7 },
  { symbol: "AMZN", name: "Amazon.com Inc", price: 156.78, change: 6.92, changePercent: 4.6 },
  { symbol: "GOOGL", name: "Alphabet Inc", price: 142.30, change: 5.85, changePercent: 4.3 },
  { symbol: "META", name: "Meta Platforms", price: 482.15, change: 18.60, changePercent: 4.0 },
];

const mockLoserData: StockData[] = [
  { symbol: "NFLX", name: "Netflix Inc", price: 425.80, change: -22.40, changePercent: -5.0 },
  { symbol: "PYPL", name: "PayPal Holdings", price: 58.45, change: -3.15, changePercent: -5.1 },
  { symbol: "UBER", name: "Uber Technologies", price: 64.20, change: -3.50, changePercent: -5.2 },
  { symbol: "SNAP", name: "Snap Inc", price: 11.85, change: -0.75, changePercent: -5.9 },
  { symbol: "ZOOM", name: "Zoom Video", price: 68.30, change: -4.20, changePercent: -5.8 },
  { symbol: "SPOT", name: "Spotify Technology", price: 298.45, change: -16.80, changePercent: -5.3 },
];

const mockActiveData: StockData[] = [
  { symbol: "GME", name: "GameStop Corp", price: 18.45, change: 2.30, changePercent: 14.2, volume: 45000000 },
  { symbol: "AMC", name: "AMC Entertainment", price: 4.85, change: 0.42, changePercent: 9.5, volume: 38000000 },
  { symbol: "SPY", name: "SPDR S&P 500 ETF", price: 493.20, change: 1.85, changePercent: 0.4, volume: 89000000 },
  { symbol: "QQQ", name: "Invesco QQQ ETF", price: 456.78, change: 2.15, changePercent: 0.5, volume: 67000000 },
  { symbol: "VIX", name: "CBOE Volatility", price: 18.92, change: -1.45, changePercent: -7.1, volume: 25000000 },
  { symbol: "BTC-USD", name: "Bitcoin USD", price: 69420.50, change: 1250.30, changePercent: 1.8, volume: 25000000 },
];

// 주식별 관련 뉴스 (stock 타입 구조)
const stockRelatedNews: { [key: string]: StockNewsItem[] } = {
  "TSLA": [
    {
      symbol: "TSLA",
      source: "Reuters",
      url: "https://reuters.com/tesla-earnings-beat",
      title: "Tesla Q4 Earnings Beat Wall Street Expectations",
      description: "Tesla reported stronger than expected Q4 results, with vehicle deliveries reaching record highs and energy storage business showing significant growth.",
      content: "Tesla Inc posted quarterly earnings that exceeded Wall Street expectations, driven by strong vehicle deliveries and a robust energy storage business.",
      published_at: "2025-01-28 16:30:00",
      fetched_at: "2025-01-28 16:35:00"
    },
    {
      symbol: "TSLA",
      source: "Bloomberg",
      url: "https://bloomberg.com/tesla-gigafactory-expansion",
      title: "Tesla Announces Gigafactory Expansion Plans",
      description: "Elon Musk announced plans to expand Tesla's manufacturing footprint with three new Gigafactories planned for 2025.",
      content: "Tesla CEO Elon Musk revealed ambitious expansion plans during the company's earnings call, outlining new manufacturing facilities across multiple continents.",
      published_at: "2025-01-28 14:15:00"
    },
    {
      symbol: "TSLA",
      source: "TechCrunch", 
      url: "https://techcrunch.com/tesla-fsd-update",
      title: "Tesla Full Self-Driving Beta Reaches New Milestone",
      description: "Tesla's FSD beta program now includes over 400,000 drivers, marking significant progress in autonomous driving technology.",
      content: "Tesla's Full Self-Driving beta program has reached a new milestone with over 400,000 active participants, demonstrating the company's commitment to autonomous vehicle technology.",
      published_at: "2025-01-28 12:45:00"
    }
  ],
  "NVDA": [
    {
      symbol: "NVDA",
      source: "MarketWatch",
      url: "https://marketwatch.com/nvidia-ai-demand",
      title: "NVIDIA AI Chip Demand Continues to Surge",
      description: "Data center demand for NVIDIA's AI chips shows no signs of slowing, with new partnerships announced across major cloud providers.",
      content: "NVIDIA Corporation continues to see unprecedented demand for its AI acceleration chips, with major cloud service providers increasing their orders significantly.",
      published_at: "2025-01-28 15:20:00"
    },
    {
      symbol: "NVDA",
      source: "The Verge",
      url: "https://theverge.com/nvidia-blackwell-architecture",
      title: "NVIDIA Unveils Next-Generation Blackwell Architecture",
      description: "The new Blackwell GPU architecture promises 2.5x performance improvement over current generation chips for AI workloads.",
      content: "NVIDIA's latest Blackwell architecture represents a significant leap forward in AI computing performance, offering substantial improvements in both speed and efficiency.",
      published_at: "2025-01-28 11:30:00"
    }
  ],
  "NFLX": [
    {
      symbol: "NFLX",
      source: "Variety",
      url: "https://variety.com/netflix-subscriber-growth",
      title: "Netflix Faces Subscriber Growth Challenges",
      description: "Netflix reported slower subscriber growth in Q4, citing increased competition in the streaming market.",
      content: "Netflix Inc. faced headwinds in subscriber acquisition during the fourth quarter, as competition from Disney+, Apple TV+, and other streaming services intensified.",
      published_at: "2025-01-28 17:00:00"
    },
    {
      symbol: "NFLX",
      source: "WSJ",
      url: "https://wsj.com/netflix-password-sharing",
      title: "Netflix Password Sharing Crackdown Shows Mixed Results",
      description: "Netflix's efforts to monetize password sharing have shown some success, but also led to subscriber cancellations in certain markets.",
      content: "Netflix's password sharing crackdown has yielded mixed results, with new subscriber conversions offset by cancellations in some regions.",
      published_at: "2025-01-28 13:45:00"
    }
  ]
};

const banners = [
  {
    title: "급상승",
    icon: TrendingUp,
    color: "text-green-400",
    bgColor: "bg-gradient-to-br from-green-500/10 to-green-600/5",
    borderColor: "border-green-500/20",
    data: mockGainerData
  },
  {
    title: "급하락", 
    icon: TrendingDown,
    color: "text-red-400",
    bgColor: "bg-gradient-to-br from-red-500/10 to-red-600/5",
    borderColor: "border-red-500/20",
    data: mockLoserData
  },
  {
    title: "거래량",
    icon: Activity,
    color: "text-blue-400", 
    bgColor: "bg-gradient-to-br from-blue-500/10 to-blue-600/5",
    borderColor: "border-blue-500/20",
    data: mockActiveData
  }
];

interface FlipBoardProps {
  data: StockData[];
  color: string;
  bgColor: string;
  onCurrentStockChange: (stock: StockData) => void;
  onBannerClick: (stock: StockData) => void;
}

function FlipBoard({ data, color, bgColor, onCurrentStockChange, onBannerClick }: FlipBoardProps) {
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isFlipping, setIsFlipping] = useState(false);

  useEffect(() => {
    const interval = setInterval(() => {
      setIsFlipping(true);
      setTimeout(() => {
        const newIndex = (currentIndex + 1) % data.length;
        setCurrentIndex(newIndex);
        onCurrentStockChange(data[newIndex]);
        setIsFlipping(false);
      }, 300);
    }, 4000);

    return () => clearInterval(interval);
  }, [currentIndex, data, onCurrentStockChange]);

  // 초기 주식 전달
  useEffect(() => {
    onCurrentStockChange(data[currentIndex]);
  }, []);

  const currentStock = data[currentIndex];

  const formatPrice = (price: number) => {
    if (price < 1) {
      return `$${price.toFixed(4)}`;
    }
    return `$${price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatVolume = (volume?: number) => {
    if (!volume) return null;
    if (volume >= 1000000000) {
      return `${(volume / 1000000000).toFixed(1)}B`;
    } else if (volume >= 1000000) {
      return `${(volume / 1000000).toFixed(1)}M`;
    }
    return `${(volume / 1000).toFixed(0)}K`;
  };

  return (
    <div 
      className="h-full flex flex-col cursor-pointer"
      onClick={() => onBannerClick(currentStock)}
    >
      <div className={`flex-1 ${bgColor} rounded-lg ${isFlipping ? 'scale-95' : 'scale-100'} transition-transform duration-300 overflow-hidden hover:scale-105`}>
        <div className="flip-container h-full">
          <div className={`flip-content h-full ${isFlipping ? 'flipping' : ''}`}>
            <div className="h-full p-4 flex flex-col justify-center space-y-2">
              {/* 심볼 */}
              <div className="text-center">
                <div className="text-lg font-bold">{currentStock.symbol}</div>
              </div>
              
              {/* 회사명 */}
              <div className="text-center">
                <div className="text-xs text-foreground/70 truncate">{currentStock.name}</div>
              </div>
              
              {/* 주가 */}
              <div className="text-center">
                <div className="text-sm font-semibold">{formatPrice(currentStock.price)}</div>
              </div>
              
              {/* 변화율 */}
              <div className="text-center">
                <div className={`text-sm font-medium ${currentStock.changePercent >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {currentStock.changePercent >= 0 ? '+' : ''}{currentStock.changePercent.toFixed(1)}%
                </div>
              </div>
              
              {/* 거래량 (있는 경우) */}
              {currentStock.volume && (
                <div className="text-center">
                  <div className="text-xs text-foreground/60">
                    Vol: {formatVolume(currentStock.volume)}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

interface StockBannerProps {
  onStockNewsClick?: (stockNews: StockNewsItem[], symbol: string) => void;
}

export function StockBanner({ onStockNewsClick }: StockBannerProps) {
  // 각 배너의 현재 주식 상태 관리
  const [currentStocks, setCurrentStocks] = useState<{ [key: number]: StockData }>({});

  const handleCurrentStockChange = (bannerIndex: number, stock: StockData) => {
    setCurrentStocks(prev => ({
      ...prev,
      [bannerIndex]: stock
    }));
  };

  const handleBannerClick = (stock: StockData) => {
    const relatedNews = stockRelatedNews[stock.symbol] || [];
    if (onStockNewsClick) {
      onStockNewsClick(relatedNews, stock.symbol);
    } else {
      // 기본 동작: 뉴스 페이지로 이동
      window.dispatchEvent(new CustomEvent('navigateToNews'));
    }
  };

  return (
    <div className="glass-card rounded-xl overflow-hidden">
      {/* 메인 배너 섹션 */}
      <div className="grid grid-cols-3 gap-3 p-4">
        {banners.map((banner, index) => {
          const Icon = banner.icon;
          return (
            <div key={index} className="flex flex-col h-36">
              {/* 헤더 */}
              <div className={`${banner.bgColor} px-3 py-2 rounded-t-lg border-b border-white/10 flex-shrink-0`}>
                <div className="flex items-center justify-center space-x-2">
                  <Icon size={16} className={banner.color} />
                  <span className="text-sm font-medium">{banner.title}</span>
                </div>
              </div>
              
              {/* Flip Board */}
              <div className="flex-1">
                <FlipBoard 
                  data={banner.data} 
                  color={banner.color}
                  bgColor={banner.bgColor}
                  onCurrentStockChange={(stock) => handleCurrentStockChange(index, stock)}
                  onBannerClick={handleBannerClick}
                />
              </div>
            </div>
          );
        })}
      </div>

      {/* 하단 안내 텍스트 */}
      <div className="px-4 py-3 border-t border-white/10 bg-white/[0.02]">
        <div className="text-center text-xs text-foreground/60">
          배너를 클릭하면 관련 뉴스를 볼 수 있습니다
        </div>
      </div>
    </div>
  );
}