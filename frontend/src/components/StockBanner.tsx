import { useState, useEffect } from "react";
import { TrendingUp, TrendingDown } from "lucide-react";

interface StockData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  volume: string;
}

export function StockBanner() {
  const [stockData, setStockData] = useState<StockData[]>([
    { symbol: "AAPL", name: "Apple", price: 185.64, change: 3.45, changePercent: 1.89, volume: "45.2M" },
    { symbol: "TSLA", name: "Tesla", price: 248.42, change: -5.23, changePercent: -2.06, volume: "38.7M" },
    { symbol: "NVDA", name: "NVIDIA", price: 875.23, change: 15.67, changePercent: 1.82, volume: "52.1M" },
    { symbol: "META", name: "Meta", price: 352.18, change: 8.91, changePercent: 2.60, volume: "29.3M" },
    { symbol: "GOOGL", name: "Google", price: 142.56, change: -2.34, changePercent: -1.61, volume: "41.8M" },
    { symbol: "MSFT", name: "Microsoft", price: 378.85, change: 4.12, changePercent: 1.10, volume: "33.5M" },
    { symbol: "AMZN", name: "Amazon", price: 156.78, change: -1.23, changePercent: -0.78, volume: "28.9M" },
    { symbol: "BTC", name: "Bitcoin", price: 98745.32, change: -1250.45, changePercent: -1.25, volume: "12.4B" },
    { symbol: "ETH", name: "Ethereum", price: 3842.67, change: 127.89, changePercent: 3.45, volume: "8.9B" },
    { symbol: "BNB", name: "Binance", price: 645.23, change: -23.45, changePercent: -3.51, volume: "2.1B" },
    { symbol: "SOL", name: "Solana", price: 234.56, change: 18.92, changePercent: 8.78, volume: "3.2B" },
    { symbol: "XRP", name: "Ripple", price: 2.34, change: -0.15, changePercent: -6.02, volume: "4.8B" }
  ]);

  // 실시간 가격 업데이트 시뮬레이션
  useEffect(() => {
    const interval = setInterval(() => {
      setStockData(prev => prev.map(stock => {
        const randomChange = (Math.random() - 0.5) * 0.02; // -1% to +1% random change
        const newPrice = stock.price * (1 + randomChange);
        const priceChange = newPrice - stock.price;
        const percentChange = (priceChange / stock.price) * 100;
        
        return {
          ...stock,
          price: newPrice,
          change: priceChange,
          changePercent: percentChange
        };
      }));
    }, 2000);

    return () => clearInterval(interval);
  }, []);

  const formatPrice = (price: number, symbol: string) => {
    if (symbol === "BTC" || symbol === "ETH") {
      return `$${price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }
    if (symbol === "XRP" || price < 10) {
      return `$${price.toFixed(2)}`;
    }
    return `$${price.toFixed(2)}`;
  };

  const StockItem = ({ stock }: { stock: StockData }) => (
    <div className="flex items-center space-x-4 px-6 py-2 glass-subtle rounded-2xl mx-2 min-w-fit">
      <div className="flex items-center space-x-3">
        <div className="text-center">
          <div className="font-semibold text-sm">{stock.symbol}</div>
          <div className="text-xs text-foreground/60">{stock.name}</div>
        </div>
        
        <div className="flex items-center space-x-2">
          <div className="text-right">
            <div className="font-semibold text-sm">
              {formatPrice(stock.price, stock.symbol)}
            </div>
            <div className="text-xs text-foreground/60">Vol: {stock.volume}</div>
          </div>
          
          <div className={`flex items-center space-x-1 text-sm font-medium ${
            stock.change >= 0 ? "text-green-400" : "text-red-400"
          }`}>
            {stock.change >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
            <div className="text-center">
              <div>{stock.change >= 0 ? "+" : ""}{stock.change.toFixed(2)}</div>
              <div className="text-xs">
                ({stock.changePercent >= 0 ? "+" : ""}{stock.changePercent.toFixed(2)}%)
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  // 데이터를 복제해서 무한 스크롤 효과 생성
  const duplicatedData = [...stockData, ...stockData];

  return (
    <div className="glass-card rounded-2xl p-3 overflow-hidden">
      <div className="flex items-center justify-between mb-2">
        <h3 className="font-semibold text-sm flex items-center">
          📈 실시간 급변동
        </h3>
        <div className="text-xs text-foreground/60">Live</div>
      </div>
      
      <div className="conveyor-container">
        <div className="conveyor-content">
          {duplicatedData.map((stock, index) => (
            <StockItem key={`${stock.symbol}-${index}`} stock={stock} />
          ))}
        </div>
      </div>
      
      <div className="mt-2 text-xs text-foreground/50 text-center">
        실시간 업데이트 • 호버로 일시정지
      </div>
    </div>
  );
}