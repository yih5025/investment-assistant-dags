import { useMemo } from "react";
import { TrendingUp, TrendingDown } from "lucide-react";
import { useTopGainersWS } from "../hooks/useTopGainersWS";

export function StockBanner() {
  const { data: wsData } = useTopGainersWS();
  const stockData = useMemo(() => {
    const list = (wsData || []).slice(0, 50).map((it) => ({
      symbol: it.symbol,
      name: it.name || '',
      price: typeof it.price === 'number' ? it.price : NaN,
      changePercent: typeof it.changePercent === 'number' ? it.changePercent : 0,
    }));
    // 두 번 이어붙여 무한 스크롤 효과
    return [...list, ...list];
  }, [wsData]);

  const formatPrice = (price: number) => isNaN(price) ? '-' : `$${price.toFixed(2)}`;

  const StockItem = ({ stock }: { stock: { symbol: string; name?: string; price?: number; changePercent?: number } }) => (
    <div className="flex items-center space-x-4 px-6 py-2 glass-subtle rounded-2xl mx-2 min-w-fit">
      <div className="flex items-center space-x-3">
        <div className="text-center">
          <div className="font-semibold text-sm">{stock.symbol}</div>
          <div className="text-xs text-foreground/60">{stock.name || ''}</div>
        </div>
        
        <div className="flex items-center space-x-2">
          <div className="text-right">
            <div className="font-semibold text-sm">
              {formatPrice(stock.price ?? NaN)}
            </div>
            <div className="text-xs text-foreground/60">&nbsp;</div>
          </div>
          
          <div className={`flex items-center space-x-1 text-sm font-medium ${
            (stock.changePercent ?? 0) >= 0 ? "text-green-400" : "text-red-400"
          }`}>
            {(stock.changePercent ?? 0) >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
            <div className="text-center">
              <div className="text-xs">
                {(stock.changePercent ?? 0) >= 0 ? "+" : ""}{Math.abs(stock.changePercent ?? 0).toFixed(2)}%
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

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
          {stockData.map((stock, index) => (
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