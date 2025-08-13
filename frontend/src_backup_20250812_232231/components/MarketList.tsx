import { useState, useEffect } from "react";
import { TrendingUp, TrendingDown, Search, Star } from "lucide-react";

interface MarketData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  volume: string;
  type: "stock" | "crypto";
}

interface MarketListProps {
  onStockSelect?: (symbol: string) => void;
  isLoggedIn?: boolean;
  watchlist?: string[];
  onToggleWatchlist?: (symbol: string) => void;
}

export function MarketList({ onStockSelect, isLoggedIn = false, watchlist = [], onToggleWatchlist }: MarketListProps) {
  const [filter, setFilter] = useState<"all" | "stock" | "crypto">("all");
  const [searchTerm, setSearchTerm] = useState("");
  
  // 실시간 가격 시뮬레이션을 위한 state
  const [marketData, setMarketData] = useState<MarketData[]>([
    { symbol: "AAPL", name: "Apple Inc.", price: 185.64, change: 3.45, changePercent: 1.89, volume: "45.2M", type: "stock" },
    { symbol: "TSLA", name: "Tesla Inc.", price: 248.42, change: -5.23, changePercent: -2.06, volume: "38.7M", type: "stock" },
    { symbol: "NVDA", name: "NVIDIA Corp.", price: 875.23, change: 15.67, changePercent: 1.82, volume: "52.1M", type: "stock" },
    { symbol: "META", name: "Meta Platforms", price: 352.18, change: 8.91, changePercent: 2.60, volume: "29.3M", type: "stock" },
    { symbol: "GOOGL", name: "Alphabet Inc.", price: 142.56, change: -2.34, changePercent: -1.61, volume: "41.8M", type: "stock" },
    { symbol: "MSFT", name: "Microsoft Corp.", price: 378.85, change: 4.12, changePercent: 1.10, volume: "33.5M", type: "stock" },
    { symbol: "BTC", name: "Bitcoin", price: 98745.32, change: -1250.45, changePercent: -1.25, volume: "12.4B", type: "crypto" },
    { symbol: "ETH", name: "Ethereum", price: 3842.67, change: 127.89, changePercent: 3.45, volume: "8.9B", type: "crypto" },
    { symbol: "BNB", name: "Binance Coin", price: 645.23, change: -23.45, changePercent: -3.51, volume: "2.1B", type: "crypto" },
    { symbol: "SOL", name: "Solana", price: 234.56, change: 18.92, changePercent: 8.78, volume: "3.2B", type: "crypto" },
    { symbol: "XRP", name: "Ripple", price: 2.34, change: -0.15, changePercent: -6.02, volume: "4.8B", type: "crypto" },
    { symbol: "ADA", name: "Cardano", price: 0.89, change: 0.04, changePercent: 4.71, volume: "1.2B", type: "crypto" },
  ]);

  // 실시간 가격 업데이트 시뮬레이션
  useEffect(() => {
    const interval = setInterval(() => {
      setMarketData(prev => prev.map(item => {
        const randomChange = (Math.random() - 0.5) * 0.02; // -1% to +1% random change
        const newPrice = item.price * (1 + randomChange);
        const priceChange = newPrice - item.price;
        const percentChange = (priceChange / item.price) * 100;
        
        return {
          ...item,
          price: newPrice,
          change: priceChange,
          changePercent: percentChange
        };
      }));
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  const filteredData = marketData.filter(item => {
    const matchesFilter = filter === "all" || item.type === filter;
    const matchesSearch = item.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         item.symbol.toLowerCase().includes(searchTerm.toLowerCase());
    return matchesFilter && matchesSearch;
  });

  const formatPrice = (price: number, type: string) => {
    if (type === "crypto" && price > 1000) {
      return `$${price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }
    if (type === "crypto" && price < 1) {
      return `$${price.toFixed(4)}`;
    }
    return `$${price.toFixed(2)}`;
  };

  const handleItemClick = (symbol: string) => {
    if (onStockSelect) {
      onStockSelect(symbol);
    }
  };

  const handleWatchlistToggle = (symbol: string, event: React.MouseEvent) => {
    event.stopPropagation();
    if (onToggleWatchlist) {
      onToggleWatchlist(symbol);
    }
  };

  return (
    <div className="space-y-4">
      {/* 내부 검색 및 필터 (통합 시장 페이지용) */}
      {!onStockSelect && (
        <div className="space-y-3">
          <div className="glass rounded-xl p-3 flex items-center space-x-3">
            <Search size={20} className="text-foreground/60" />
            <input
              type="text"
              placeholder="주식명, 코인명으로 검색..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="flex-1 bg-transparent placeholder-foreground/50 outline-none"
            />
          </div>
        </div>
      )}

      {/* 필터 버튼 */}
      <div className="flex space-x-2">
        {[
          { key: "all", label: "전체" },
          { key: "stock", label: "주식" },
          { key: "crypto", label: "암호화폐" }
        ].map((filterOption) => (
          <button
            key={filterOption.key}
            onClick={() => setFilter(filterOption.key as any)}
            className={`px-4 py-2 rounded-lg text-sm transition-all ${
              filter === filterOption.key
                ? "bg-primary/20 text-primary border border-primary/30"
                : "bg-white/10 text-foreground/70 hover:bg-white/20"
            }`}
          >
            {filterOption.label}
          </button>
        ))}
      </div>

      {/* 시장 데이터 리스트 */}
      <div className="space-y-2">
        {filteredData.map((item) => {
          const isInWatchlist = watchlist.includes(item.symbol);
          
          return (
            <div
              key={item.symbol}
              className="glass-card rounded-xl p-4 transition-all duration-300 hover:scale-[1.02] cursor-pointer"
              onClick={() => handleItemClick(item.symbol)}
            >
              <div className="flex items-center justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    <span className="font-semibold">{item.symbol}</span>
                    <span className={`text-xs px-2 py-1 rounded-md ${
                      item.type === "stock" 
                        ? "bg-blue-500/20 text-blue-300" 
                        : "bg-orange-500/20 text-orange-300"
                    }`}>
                      {item.type === "stock" ? "주식" : "코인"}
                    </span>
                    {isInWatchlist && (
                      <Star size={14} className="text-yellow-400 fill-current" />
                    )}
                  </div>
                  <p className="text-sm text-foreground/70 truncate">{item.name}</p>
                  <p className="text-xs text-foreground/50 mt-1">거래량: {item.volume}</p>
                </div>

                <div className="flex items-center space-x-3">
                  <div className="text-right">
                    <div className="text-lg font-semibold mb-1">
                      {formatPrice(item.price, item.type)}
                    </div>
                    <div className={`flex items-center justify-end space-x-1 ${
                      item.change >= 0 ? "text-green-400" : "text-red-400"
                    }`}>
                      {item.change >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
                      <span className="text-sm">
                        {item.change >= 0 ? "+" : ""}{item.change.toFixed(2)}
                      </span>
                      <span className="text-xs">
                        ({item.changePercent >= 0 ? "+" : ""}{item.changePercent.toFixed(2)}%)
                      </span>
                    </div>
                  </div>

                  {/* 관심 종목 토글 버튼 */}
                  {isLoggedIn && (
                    <button
                      onClick={(e) => handleWatchlistToggle(item.symbol, e)}
                      className={`p-2 rounded-lg transition-colors ${
                        isInWatchlist 
                          ? "bg-yellow-400/20 text-yellow-400 hover:bg-yellow-400/30" 
                          : "bg-white/10 text-foreground/60 hover:bg-yellow-400/20 hover:text-yellow-400"
                      }`}
                    >
                      <Star size={16} className={isInWatchlist ? "fill-current" : ""} />
                    </button>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {filteredData.length === 0 && (
        <div className="text-center py-8 text-foreground/60">
          <Search size={48} className="mx-auto mb-4 opacity-50" />
          <p>검색 결과가 없습니다</p>
        </div>
      )}

      {/* 로그인 유도 (게스트용, 하단에 간단히) */}
      {!isLoggedIn && filteredData.length > 0 && (
        <div className="glass rounded-xl p-3 text-center">
          <p className="text-sm text-foreground/70">
            <Star size={14} className="inline mr-1" />
            로그인하면 관심 종목으로 저장할 수 있어요
          </p>
        </div>
      )}
    </div>
  );
}