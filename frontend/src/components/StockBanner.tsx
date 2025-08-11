import { useState, useEffect } from "react";
import { TrendingUp, TrendingDown, Activity } from "lucide-react";

interface StockData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  type: "rising" | "active" | "falling";
}

export function StockBanner() {
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isAnimating, setIsAnimating] = useState(true);

  const stockData: StockData[] = [
    { symbol: "AAPL", name: "Apple Inc.", price: 185.64, change: 3.45, changePercent: 1.89, type: "rising" },
    { symbol: "TSLA", name: "Tesla Inc.", price: 248.42, change: -5.23, changePercent: -2.06, type: "falling" },
    { symbol: "NVDA", name: "NVIDIA Corp.", price: 875.23, change: 15.67, changePercent: 1.82, type: "active" },
    { symbol: "META", name: "Meta Platforms", price: 352.18, change: 8.91, changePercent: 2.60, type: "rising" },
    { symbol: "GOOGL", name: "Alphabet Inc.", price: 142.56, change: -2.34, changePercent: -1.61, type: "falling" },
    { symbol: "MSFT", name: "Microsoft Corp.", price: 378.85, change: 4.12, changePercent: 1.10, type: "active" },
    { symbol: "AMZN", name: "Amazon.com Inc.", price: 156.78, change: 2.89, changePercent: 1.88, type: "rising" },
    { symbol: "BTC", name: "Bitcoin", price: 98745.32, change: -1250.45, changePercent: -1.25, type: "falling" },
  ];

  // 무한 슬라이드를 위해 데이터를 3배로 복제
  const infiniteStockData = [...stockData, ...stockData, ...stockData];

  useEffect(() => {
    const interval = setInterval(() => {
      if (isAnimating) {
        setCurrentIndex((prev) => {
          const nextIndex = prev + 1;
          // 원본 데이터의 2배 지점에 도달하면 처음으로 리셋 (무한 루프 효과)
          if (nextIndex >= stockData.length * 2) {
            setTimeout(() => {
              setIsAnimating(false);
              setCurrentIndex(0);
              setTimeout(() => setIsAnimating(true), 50);
            }, 300);
            return nextIndex;
          }
          return nextIndex;
        });
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [stockData.length, isAnimating]);

  const getIcon = (type: string) => {
    switch (type) {
      case "rising":
        return <TrendingUp className="text-green-400" size={16} />;
      case "falling":
        return <TrendingDown className="text-red-400" size={16} />;
      case "active":
        return <Activity className="text-blue-400" size={16} />;
      default:
        return null;
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case "rising":
        return "급상승";
      case "falling":
        return "급하락";
      case "active":
        return "활발";
      default:
        return "";
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case "rising":
        return "from-green-500/20 to-green-600/20";
      case "falling":
        return "from-red-500/20 to-red-600/20";
      case "active":
        return "from-blue-500/20 to-blue-600/20";
      default:
        return "from-gray-500/20 to-gray-600/20";
    }
  };

  return (
    <div className="glass-card rounded-2xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold">🔥 실시간 급변동</h2>
        <div className="text-xs text-foreground/60">자동 업데이트</div>
      </div>
      
      <div className="relative overflow-hidden">
        <div 
          className={`flex transition-transform ${isAnimating ? 'duration-500 ease-in-out' : 'duration-0'}`}
          style={{ transform: `translateX(-${currentIndex * 100}%)` }}
        >
          {infiniteStockData.map((stock, index) => (
            <div
              key={`${stock.symbol}-${index}`}
              className={`w-full flex-shrink-0 glass rounded-2xl p-4 mx-2 bg-gradient-to-r ${getTypeColor(stock.type)} hover:scale-[1.02] transition-transform cursor-pointer`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  {getIcon(stock.type)}
                  <span className="text-xs text-foreground/70 font-medium">{getTypeLabel(stock.type)}</span>
                </div>
                <div className="text-xs text-foreground/60">{stock.symbol}</div>
              </div>
              <div className="mt-2">
                <h3 className="text-sm font-medium truncate">{stock.name}</h3>
                <div className="flex items-center justify-between mt-1">
                  <span className="text-lg font-semibold">
                    {stock.symbol === "BTC" 
                      ? `$${stock.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` 
                      : `$${stock.price.toFixed(2)}`}
                  </span>
                  <div className={`flex items-center space-x-1 ${
                    stock.change >= 0 ? "text-green-400" : "text-red-400"
                  }`}>
                    <span className="text-sm">
                      {stock.change >= 0 ? "+" : ""}{stock.change.toFixed(2)}
                    </span>
                    <span className="text-xs">
                      ({stock.changePercent >= 0 ? "+" : ""}{stock.changePercent.toFixed(2)}%)
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* 자동 진행 표시기 */}
      <div className="flex justify-center mt-4">
        <div className="flex space-x-1">
          {stockData.map((_, index) => (
            <div
              key={index}
              className={`h-1 transition-all duration-300 rounded-full ${
                index === (currentIndex % stockData.length) 
                  ? "bg-primary w-6" 
                  : "bg-white/30 w-2"
              }`}
            />
          ))}
        </div>
      </div>
    </div>
  );
}