import { useState } from "react";
import { Search, ArrowLeft, Star, Heart, TrendingUp, Lock } from "lucide-react";
import { MarketList } from "./MarketList";
import { SearchDetail } from "./SearchDetail";

interface IntegratedMarketProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

export function IntegratedMarket({ isLoggedIn, onLoginPrompt }: IntegratedMarketProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const [watchlist, setWatchlist] = useState<string[]>(isLoggedIn ? ["AAPL", "TSLA", "NVDA"] : []);

  const handleSearch = (query: string) => {
    if (query.trim()) {
      setSearchQuery(query);
      setSelectedStock(query.toUpperCase());
    }
  };

  const handleStockSelect = (symbol: string) => {
    setSelectedStock(symbol);
  };

  const handleBackToMarket = () => {
    setSelectedStock(null);
    setSearchQuery("");
  };

  const toggleWatchlist = (symbol: string) => {
    if (!isLoggedIn) {
      onLoginPrompt();
      return;
    }

    setWatchlist(prev => 
      prev.includes(symbol) 
        ? prev.filter(s => s !== symbol)
        : [...prev, symbol]
    );
  };

  if (selectedStock) {
    return (
      <SearchDetail 
        symbol={selectedStock} 
        onBack={handleBackToMarket}
        isLoggedIn={isLoggedIn}
        onLoginPrompt={onLoginPrompt}
        isInWatchlist={watchlist.includes(selectedStock)}
        onToggleWatchlist={() => toggleWatchlist(selectedStock)}
      />
    );
  }

  return (
    <div className="space-y-6">
      {/* 검색 헤더 */}
      <div className="glass-card rounded-2xl p-4">
        <div className="glass rounded-xl p-3 flex items-center space-x-3">
          <Search size={20} className="text-foreground/60" />
          <input 
            type="text" 
            placeholder="주식, 코인, 기업명을 검색하세요..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch(searchQuery)}
            className="flex-1 bg-transparent placeholder-foreground/50 outline-none"
          />
          <button 
            onClick={() => handleSearch(searchQuery)}
            className="px-4 py-1 bg-primary/20 text-primary rounded-lg text-sm hover:bg-primary/30 transition-colors"
          >
            검색
          </button>
        </div>
        
        {/* 인기 검색어 */}
        <div className="mt-4">
          <h3 className="font-medium mb-3">🔥 인기 검색어</h3>
          <div className="flex flex-wrap gap-2">
            {["AAPL", "TSLA", "NVDA", "BTC", "ETH", "META", "GOOGL", "MSFT"].map((symbol) => (
              <button
                key={symbol}
                onClick={() => handleSearch(symbol)}
                className="px-3 py-1.5 glass rounded-lg text-sm hover:bg-white/10 transition-colors flex items-center space-x-1"
              >
                <span>{symbol}</span>
                {isLoggedIn && watchlist.includes(symbol) && (
                  <Star size={12} className="text-yellow-400 fill-current" />
                )}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* 관심 종목 (로그인한 사용자만) */}
      {isLoggedIn && watchlist.length > 0 && (
        <div className="glass-card rounded-2xl p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-medium flex items-center">
              <Star className="mr-2 text-yellow-400" size={18} />
              내 관심 종목
            </h3>
            <span className="text-xs text-foreground/60">{watchlist.length}개</span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {watchlist.map((symbol) => (
              <button
                key={symbol}
                onClick={() => handleStockSelect(symbol)}
                className="glass rounded-lg p-2 text-center hover:bg-white/10 transition-colors"
              >
                <div className="text-sm font-medium">{symbol}</div>
                <div className="text-xs text-green-400">+2.4%</div>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* 로그인 유도 (게스트용) */}
      {!isLoggedIn && (
        <div className="glass-card rounded-2xl p-4 border border-primary/30">
          <div className="flex items-start space-x-3">
            <Heart className="text-primary mt-1" size={20} />
            <div className="flex-1">
              <h3 className="font-medium mb-1">관심 종목 추적하기</h3>
              <p className="text-sm text-foreground/70 mb-3">
                로그인하면 관심 있는 주식과 코인을 저장하고 실시간으로 추적할 수 있어요.
              </p>
              <button
                onClick={onLoginPrompt}
                className="px-3 py-1.5 bg-primary/20 text-primary rounded-lg text-sm hover:bg-primary/30 transition-colors"
              >
                로그인하고 시작하기
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 실시간 시장 데이터 */}
      <MarketList 
        onStockSelect={handleStockSelect} 
        isLoggedIn={isLoggedIn}
        watchlist={watchlist}
        onToggleWatchlist={toggleWatchlist}
      />

      {/* 프리미엄 기능 안내 (게스트용) */}
      {!isLoggedIn && (
        <div className="glass-card rounded-2xl p-4">
          <div className="text-center">
            <TrendingUp className="mx-auto mb-3 text-primary" size={32} />
            <h3 className="font-medium mb-2">고급 시장 분석</h3>
            <p className="text-sm text-foreground/70 mb-4">
              로그인하면 더 상세한 차트, 기술적 분석, 실시간 알림 등<br/>
              전문가 수준의 도구를 사용할 수 있습니다.
            </p>
            <div className="grid grid-cols-2 gap-3 text-xs text-foreground/60 mb-4">
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                <span>실시간 가격 알림</span>
              </div>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                <span>고급 차트 분석</span>
              </div>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                <span>포트폴리오 추적</span>
              </div>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-primary rounded-full"></div>
                <span>AI 투자 조언</span>
              </div>
            </div>
            <button
              onClick={onLoginPrompt}
              className="px-4 py-2 bg-primary/20 text-primary rounded-xl font-medium hover:bg-primary/30 transition-colors"
            >
              프리미엄 기능 사용하기
            </button>
          </div>
        </div>
      )}
    </div>
  );
}