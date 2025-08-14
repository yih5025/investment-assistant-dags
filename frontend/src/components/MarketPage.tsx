import React, { useState, useEffect, useRef } from 'react';
import { Search, TrendingUp, TrendingDown, Star, Wifi, WifiOff } from 'lucide-react';

// 타입 정의
interface CryptoData {
  market: string;
  trade_price: number;
  signed_change_rate: number;
  signed_change_price: number;
  trade_volume: number;
  acc_trade_volume_24h: number;
  change: 'RISE' | 'FALL' | 'EVEN';
  source: string;
}

interface SP500Data {
  symbol: string;
  price: number;
  volume: number;
  timestamp_ms: number;
  category?: string;
  source: string;
}

interface MarketItem {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  volume: string;
  type: 'crypto' | 'stock';
  marketCap?: string;
}

// WebSocket 메시지 타입
interface WebSocketMessage {
  type: string;
  data?: CryptoData[] | SP500Data[];
  timestamp: string;
}

const MarketPage: React.FC = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [filter, setFilter] = useState<'all' | 'stock' | 'crypto'>('all');
  const [marketData, setMarketData] = useState<MarketItem[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'connected' | 'disconnected'>('disconnected');
  const [watchlist, setWatchlist] = useState<string[]>(['AAPL', 'BTC']);
  
  // WebSocket 연결 관리
  const cryptoWs = useRef<WebSocket | null>(null);
  const sp500Ws = useRef<WebSocket | null>(null);
  const reconnectTimeout = useRef<NodeJS.Timeout | null>(null);

  // 암호화폐 이름 매핑 (실제로는 API에서 가져와야 함)
  const cryptoNames: { [key: string]: string } = {
    'KRW-BTC': 'Bitcoin',
    'KRW-ETH': 'Ethereum',
    'KRW-ADA': 'Cardano',
    'KRW-XRP': 'Ripple',
    'KRW-DOT': 'Polkadot',
    'KRW-DOGE': 'Dogecoin',
    'KRW-SOL': 'Solana',
    'KRW-AVAX': 'Avalanche',
  };

  // 주식 이름 매핑 (실제로는 API에서 가져와야 함)
  const stockNames: { [key: string]: string } = {
    'AAPL': 'Apple Inc.',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc.',
    'AMZN': 'Amazon.com Inc.',
    'TSLA': 'Tesla Inc.',
    'META': 'Meta Platforms Inc.',
    'NVDA': 'NVIDIA Corporation',
    'NFLX': 'Netflix Inc.',
  };

  // 현재 호스트 기반 WebSocket URL 생성
  const buildWsUrl = (path: string) => {
    // 1) 절대 WS URL 환경변수 우선 (예: wss://api.investment-assistant.site/api/v1/ws)
    const abs = (import.meta as any)?.env?.VITE_WS_URL;
    if (abs) return `${abs}${path}`;

    if (typeof window === 'undefined') return path;

    // 2) Vercel과 같은 정적 호스팅에서는 동일 출처에 WS 엔드포인트가 없으므로 공개 API 도메인 사용
    const hostname = window.location.hostname;
    if (hostname.includes('vercel.app')) {
      return `wss://api.investment-assistant.site/api/v1/ws${path}`;
    }

    // 3) 기본: 동일 출처 프록시 경로 사용 (K8s/Nginx 프록시)
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const host = window.location.host;
    const wsBase = (import.meta as any)?.env?.VITE_WS_BASE || '/ws';
    return `${protocol}://${host}${wsBase}${path}`;
  };

  // WebSocket 연결 초기화
  const initializeWebSockets = () => {
    try {
      setConnectionStatus('connecting');

      // 암호화폐 WebSocket 연결
      cryptoWs.current = new WebSocket(buildWsUrl('/crypto'));
      
      cryptoWs.current.onopen = () => {
        console.log('🟢 암호화폐 WebSocket 연결됨');
        setConnectionStatus('connected');
      };

      cryptoWs.current.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);
          if (message.type === 'crypto_update' && message.data) {
            handleCryptoUpdate(message.data as CryptoData[]);
          }
        } catch (error) {
          console.error('암호화폐 메시지 파싱 오류:', error);
        }
      };

      cryptoWs.current.onclose = () => {
        console.log('🔴 암호화폐 WebSocket 연결 종료');
        setConnectionStatus('disconnected');
        scheduleReconnect();
      };

      // S&P 500 WebSocket 연결
      sp500Ws.current = new WebSocket(buildWsUrl('/stocks/sp500'));
      
      sp500Ws.current.onopen = () => {
        console.log('🟢 S&P 500 WebSocket 연결됨');
      };

      sp500Ws.current.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);
          if (message.type === 'sp500_update' && message.data) {
            handleSP500Update(message.data as SP500Data[]);
          }
        } catch (error) {
          console.error('S&P 500 메시지 파싱 오류:', error);
        }
      };

      sp500Ws.current.onclose = () => {
        console.log('🔴 S&P 500 WebSocket 연결 종료');
        scheduleReconnect();
      };

    } catch (error) {
      console.error('WebSocket 연결 오류:', error);
      setConnectionStatus('disconnected');
      scheduleReconnect();
    }
  };

  // 자동 재연결
  const scheduleReconnect = () => {
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current);
    }
    
    reconnectTimeout.current = setTimeout(() => {
      console.log('🔄 WebSocket 재연결 시도...');
      initializeWebSockets();
    }, 5000);
  };

  // 암호화폐 데이터 업데이트 처리
  const handleCryptoUpdate = (cryptoData: CryptoData[]) => {
    const newCryptoItems: MarketItem[] = cryptoData.map(crypto => {
      const symbol = crypto.market.replace('KRW-', '');
      const name = cryptoNames[crypto.market] || symbol;
      
      return {
        symbol,
        name,
        price: crypto.trade_price || 0,
        change: crypto.signed_change_price || 0,
        changePercent: (crypto.signed_change_rate || 0) * 100,
        volume: formatVolume(crypto.acc_trade_volume_24h || 0),
        type: 'crypto' as const,
        marketCap: formatVolume((crypto.trade_price || 0) * 21000000) // 임시 시총 계산
      };
    });

    updateMarketData(newCryptoItems, 'crypto');
  };

  // S&P 500 데이터 업데이트 처리
  const handleSP500Update = (sp500Data: SP500Data[]) => {
    const newStockItems: MarketItem[] = sp500Data.map(stock => {
      const name = stockNames[stock.symbol] || `${stock.symbol} Corp.`;
      
      return {
        symbol: stock.symbol,
        name,
        price: stock.price || 0,
        change: 0, // 변화량은 별도 계산 필요
        changePercent: 0, // 변화율은 별도 계산 필요
        volume: formatVolume(stock.volume || 0),
        type: 'stock' as const
      };
    });

    updateMarketData(newStockItems, 'stock');
  };

  // 마켓 데이터 업데이트
  const updateMarketData = (newItems: MarketItem[], type: 'crypto' | 'stock') => {
    setMarketData(prevData => {
      const filteredPrevData = prevData.filter(item => item.type !== type);
      return [...filteredPrevData, ...newItems];
    });
  };

  // 볼륨 포맷팅
  const formatVolume = (volume: number): string => {
    if (volume >= 1e12) return `${(volume / 1e12).toFixed(1)}T`;
    if (volume >= 1e9) return `${(volume / 1e9).toFixed(1)}B`;
    if (volume >= 1e6) return `${(volume / 1e6).toFixed(1)}M`;
    if (volume >= 1e3) return `${(volume / 1e3).toFixed(1)}K`;
    return volume.toFixed(0);
  };

  // 가격 포맷팅
  const formatPrice = (price: number, type: string): string => {
    if (type === 'crypto') {
      if (price >= 1000000) return `₩${(price / 1000000).toFixed(1)}M`;
      if (price >= 1000) return `₩${price.toLocaleString('ko-KR', { maximumFractionDigits: 0 })}`;
      return `₩${price.toFixed(2)}`;
    } else {
      return `$${price.toFixed(2)}`;
    }
  };

  // 검색 필터링
  const filteredData = marketData.filter(item => {
    const matchesFilter = filter === 'all' || item.type === filter;
    const matchesSearch = item.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                         item.symbol.toLowerCase().includes(searchQuery.toLowerCase());
    return matchesFilter && matchesSearch;
  });

  // 관심종목 토글
  const toggleWatchlist = (symbol: string) => {
    setWatchlist(prev => 
      prev.includes(symbol) 
        ? prev.filter(s => s !== symbol)
        : [...prev, symbol]
    );
  };

  // 컴포넌트 마운트/언마운트 시 WebSocket 관리
  useEffect(() => {
    initializeWebSockets();

    return () => {
      if (cryptoWs.current) {
        cryptoWs.current.close();
      }
      if (sp500Ws.current) {
        sp500Ws.current.close();
      }
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current);
      }
    };
  }, []);

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
            className="flex-1 bg-transparent placeholder-foreground/50 outline-none"
          />
        </div>
        
        {/* 연결 상태 */}
        <div className="mt-3 flex items-center justify-end space-x-2">
          {connectionStatus === 'connected' && (
            <>
              <Wifi className="text-green-400" size={16} />
              <span className="text-green-400 text-xs">실시간 연결</span>
            </>
          )}
          {connectionStatus === 'connecting' && (
            <>
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-yellow-400"></div>
              <span className="text-yellow-400 text-xs">연결 중...</span>
            </>
          )}
          {connectionStatus === 'disconnected' && (
            <>
              <WifiOff className="text-red-400" size={16} />
              <span className="text-red-400 text-xs">연결 끊김</span>
            </>
          )}
        </div>
      </div>

      {/* 관심 종목 */}
      {watchlist.length > 0 && (
        <div className="glass-card rounded-2xl p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-medium flex items-center">
              <Star className="mr-2 text-yellow-400" size={18} />
              내 관심 종목
            </h3>
            <span className="text-xs text-foreground/60">{watchlist.length}개</span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {watchlist.map((symbol) => {
              const item = marketData.find(data => data.symbol === symbol);
              return (
                <button
                  key={symbol}
                  className="glass rounded-lg p-2 text-center hover:glass-strong transition-all"
                >
                  <div className="text-sm font-medium">{symbol}</div>
                  {item && (
                    <div className={`text-xs ${item.change >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                      {item.change >= 0 ? '+' : ''}{item.changePercent.toFixed(2)}%
                    </div>
                  )}
                </button>
              );
            })}
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
        {filteredData.length === 0 ? (
          <div className="text-center py-8 text-foreground/60">
            <Search size={48} className="mx-auto mb-4 opacity-50" />
            <p>검색 결과가 없습니다</p>
          </div>
        ) : (
          filteredData.map((item) => {
            const isInWatchlist = watchlist.includes(item.symbol);
            
            return (
              <div
                key={`${item.type}-${item.symbol}`}
                className="glass-card rounded-xl p-4 transition-all duration-300 hover:scale-[1.02] cursor-pointer"
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
                          {item.change >= 0 ? "+" : ""}{formatPrice(Math.abs(item.change), item.type)}
                        </span>
                        <span className="text-xs">
                          ({item.changePercent >= 0 ? "+" : ""}{item.changePercent.toFixed(2)}%)
                        </span>
                      </div>
                    </div>

                    {/* 관심 종목 토글 버튼 */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleWatchlist(item.symbol);
                      }}
                      className={`p-2 rounded-lg transition-colors ${
                        isInWatchlist 
                          ? "bg-yellow-400/20 text-yellow-400 hover:bg-yellow-400/30" 
                          : "bg-white/10 text-foreground/60 hover:bg-yellow-400/20 hover:text-yellow-400"
                      }`}
                    >
                      <Star size={16} className={isInWatchlist ? "fill-current" : ""} />
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* 로그인 유도 (게스트용, 하단에 간단히) */}
      {filteredData.length > 0 && (
        <div className="glass rounded-xl p-3 text-center">
          <p className="text-sm text-foreground/70">
            <Star size={14} className="inline mr-1" />
            로그인하면 관심 종목으로 저장할 수 있어요
          </p>
        </div>
      )}
    </div>
  );
};

export default MarketPage;