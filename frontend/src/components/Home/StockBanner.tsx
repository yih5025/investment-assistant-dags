import { useState, useEffect } from "react";
import { TrendingUp, TrendingDown, Activity } from "lucide-react";
import { Card } from "@/components/ui/card";

interface StockData {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  type: 'rising' | 'falling' | 'active';
}

const StockBanner = () => {
  const [currentIndex, setCurrentIndex] = useState(0);

  // Mock data - 실제로는 API에서 가져올 데이터
  const stockData: StockData[] = [
    { symbol: "AAPL", name: "Apple Inc.", price: 175.43, change: 2.35, changePercent: 1.36, type: 'rising' },
    { symbol: "TSLA", name: "Tesla Inc.", price: 248.50, change: -5.20, changePercent: -2.05, type: 'falling' },
    { symbol: "NVDA", name: "NVIDIA Corp.", price: 875.28, change: 12.45, changePercent: 1.44, type: 'active' },
    { symbol: "AMZN", name: "Amazon.com Inc.", price: 155.89, change: 3.21, changePercent: 2.10, type: 'rising' },
    { symbol: "GOOGL", name: "Alphabet Inc.", price: 140.25, change: -1.85, changePercent: -1.30, type: 'falling' },
    { symbol: "MSFT", name: "Microsoft Corp.", price: 420.15, change: 8.75, changePercent: 2.13, type: 'active' }
  ];

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentIndex((prev) => (prev + 1) % stockData.length);
    }, 3000);

    return () => clearInterval(interval);
  }, [stockData.length]);

  const getStockIcon = (type: string) => {
    switch (type) {
      case 'rising':
        return <TrendingUp className="h-4 w-4 text-success" />;
      case 'falling':
        return <TrendingDown className="h-4 w-4 text-destructive" />;
      case 'active':
        return <Activity className="h-4 w-4 text-warning" />;
      default:
        return <Activity className="h-4 w-4 text-muted-foreground" />;
    }
  };

  const getStockColor = (type: string) => {
    switch (type) {
      case 'rising':
        return 'text-success';
      case 'falling':
        return 'text-destructive';
      case 'active':
        return 'text-warning';
      default:
        return 'text-foreground';
    }
  };

  const getBgGradient = (type: string) => {
    switch (type) {
      case 'rising':
        return 'from-success-light to-success/10';
      case 'falling':
        return 'from-destructive-light to-destructive/10';
      case 'active':
        return 'from-warning-light to-warning/10';
      default:
        return 'from-muted to-muted/50';
    }
  };

  return (
    <div className="w-full">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-foreground">실시간 주식 동향</h2>
        <div className="flex space-x-1">
          {stockData.map((_, index) => (
            <div
              key={index}
              className={`h-2 w-2 rounded-full transition-colors ${
                index === currentIndex ? 'bg-primary' : 'bg-muted'
              }`}
            />
          ))}
        </div>
      </div>

      <Card className={`p-4 transition-all duration-500 bg-gradient-to-r ${getBgGradient(stockData[currentIndex].type)}`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            {getStockIcon(stockData[currentIndex].type)}
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-bold text-lg">{stockData[currentIndex].symbol}</span>
                <span className="text-sm text-muted-foreground">
                  {stockData[currentIndex].name}
                </span>
              </div>
              <div className="mt-1 flex items-center space-x-2">
                <span className="text-2xl font-bold">
                  ${stockData[currentIndex].price.toFixed(2)}
                </span>
                <span className={`text-sm font-medium ${getStockColor(stockData[currentIndex].type)}`}>
                  {stockData[currentIndex].change > 0 ? '+' : ''}
                  {stockData[currentIndex].change.toFixed(2)} 
                  ({stockData[currentIndex].changePercent > 0 ? '+' : ''}
                  {stockData[currentIndex].changePercent.toFixed(2)}%)
                </span>
              </div>
            </div>
          </div>
          
          <div className="text-right">
            <div className="text-xs text-muted-foreground uppercase tracking-wide">
              {stockData[currentIndex].type === 'rising' && '상승 중'}
              {stockData[currentIndex].type === 'falling' && '하락 중'}
              {stockData[currentIndex].type === 'active' && '활발한 거래'}
            </div>
            <div className="text-xs text-muted-foreground mt-1">
              실시간
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
};

export default StockBanner;