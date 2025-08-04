import { create } from 'zustand';
import type { WebSocketMessage } from '../types/api';
import type { Stock, Crypto } from '../types/api';

interface WebSocketState {
  // Connection state
  connected: boolean;
  connecting: boolean;
  error: string | null;
  reconnectCount: number;
  lastActivity: Date | null;

  // Subscriptions
  stockSubscriptions: Set<string>;
  cryptoSubscriptions: Set<string>;

  // Real-time data
  stockPrices: Map<string, Stock>;
  cryptoPrices: Map<string, Crypto>;
  recentMessages: WebSocketMessage[];
  
  // Statistics
  messagesReceived: number;
  messagesPerSecond: number;
  connectionUptime: number;
}

interface WebSocketStore extends WebSocketState {
  // Connection actions
  setConnected: (connected: boolean) => void;
  setConnecting: (connecting: boolean) => void;
  setError: (error: string | null) => void;
  incrementReconnectCount: () => void;
  resetReconnectCount: () => void;
  updateLastActivity: () => void;

  // Subscription actions
  addStockSubscription: (symbol: string) => void;
  removeStockSubscription: (symbol: string) => void;
  addCryptoSubscription: (symbol: string) => void;
  removeCryptoSubscription: (symbol: string) => void;
  clearSubscriptions: () => void;

  // Data actions
  updateStockPrice: (symbol: string, data: Stock) => void;
  updateCryptoPrice: (symbol: string, data: Crypto) => void;
  addMessage: (message: WebSocketMessage) => void;
  clearMessages: () => void;

  // Statistics
  incrementMessageCount: () => void;
  updateMessagesPerSecond: (rate: number) => void;
  updateConnectionUptime: (uptime: number) => void;
  resetStatistics: () => void;

  // Utility
  getStockPrice: (symbol: string) => Stock | undefined;
  getCryptoPrice: (symbol: string) => Crypto | undefined;
  getSubscriptionCount: () => number;
  isSubscribed: (symbol: string, type: 'stock' | 'crypto') => boolean;
}

export const useWebSocketStore = create<WebSocketStore>((set, get) => ({
  // Initial state
  connected: false,
  connecting: false,
  error: null,
  reconnectCount: 0,
  lastActivity: null,
  
  stockSubscriptions: new Set(),
  cryptoSubscriptions: new Set(),
  
  stockPrices: new Map(),
  cryptoPrices: new Map(),
  recentMessages: [],
  
  messagesReceived: 0,
  messagesPerSecond: 0,
  connectionUptime: 0,

  // Connection actions
  setConnected: (connected) => {
    set({ 
      connected,
      connecting: false,
      error: connected ? null : get().error,
      lastActivity: connected ? new Date() : get().lastActivity,
    });
  },

  setConnecting: (connecting) => {
    set({ connecting });
  },

  setError: (error) => {
    set({ 
      error,
      connected: false,
      connecting: false,
    });
  },

  incrementReconnectCount: () => {
    set({ reconnectCount: get().reconnectCount + 1 });
  },

  resetReconnectCount: () => {
    set({ reconnectCount: 0 });
  },

  updateLastActivity: () => {
    set({ lastActivity: new Date() });
  },

  // Subscription actions
  addStockSubscription: (symbol) => {
    const subscriptions = new Set(get().stockSubscriptions);
    subscriptions.add(symbol.toUpperCase());
    set({ stockSubscriptions: subscriptions });
  },

  removeStockSubscription: (symbol) => {
    const subscriptions = new Set(get().stockSubscriptions);
    subscriptions.delete(symbol.toUpperCase());
    set({ stockSubscriptions: subscriptions });
  },

  addCryptoSubscription: (symbol) => {
    const subscriptions = new Set(get().cryptoSubscriptions);
    subscriptions.add(symbol.toUpperCase());
    set({ cryptoSubscriptions: subscriptions });
  },

  removeCryptoSubscription: (symbol) => {
    const subscriptions = new Set(get().cryptoSubscriptions);
    subscriptions.delete(symbol.toUpperCase());
    set({ cryptoSubscriptions: subscriptions });
  },

  clearSubscriptions: () => {
    set({ 
      stockSubscriptions: new Set(),
      cryptoSubscriptions: new Set(),
    });
  },

  // Data actions
  updateStockPrice: (symbol, data) => {
    const stockPrices = new Map(get().stockPrices);
    stockPrices.set(symbol.toUpperCase(), data);
    set({ stockPrices });
    get().updateLastActivity();
  },

  updateCryptoPrice: (symbol, data) => {
    const cryptoPrices = new Map(get().cryptoPrices);
    cryptoPrices.set(symbol.toUpperCase(), data);
    set({ cryptoPrices });
    get().updateLastActivity();
  },

  addMessage: (message) => {
    const recentMessages = [message, ...get().recentMessages].slice(0, 100); // 최근 100개만 보관
    set({ recentMessages });
    get().incrementMessageCount();
    get().updateLastActivity();
  },

  clearMessages: () => {
    set({ recentMessages: [] });
  },

  // Statistics
  incrementMessageCount: () => {
    set({ messagesReceived: get().messagesReceived + 1 });
  },

  updateMessagesPerSecond: (rate) => {
    set({ messagesPerSecond: rate });
  },

  updateConnectionUptime: (uptime) => {
    set({ connectionUptime: uptime });
  },

  resetStatistics: () => {
    set({
      messagesReceived: 0,
      messagesPerSecond: 0,
      connectionUptime: 0,
    });
  },

  // Utility functions
  getStockPrice: (symbol) => {
    return get().stockPrices.get(symbol.toUpperCase());
  },

  getCryptoPrice: (symbol) => {
    return get().cryptoPrices.get(symbol.toUpperCase());
  },

  getSubscriptionCount: () => {
    return get().stockSubscriptions.size + get().cryptoSubscriptions.size;
  },

  isSubscribed: (symbol, type) => {
    const normalizedSymbol = symbol.toUpperCase();
    if (type === 'stock') {
      return get().stockSubscriptions.has(normalizedSymbol);
    } else {
      return get().cryptoSubscriptions.has(normalizedSymbol);
    }
  },
}));

// WebSocket 메시지 타입별 처리를 위한 헬퍼 함수들
export const processWebSocketMessage = (message: WebSocketMessage) => {
  const store = useWebSocketStore.getState();
  
  switch (message.type) {
    case 'stock_price':
      if (message.data && message.data.symbol) {
        store.updateStockPrice(message.data.symbol, message.data);
      }
      break;
      
    case 'crypto_price':
      if (message.data && message.data.symbol) {
        store.updateCryptoPrice(message.data.symbol, message.data);
      }
      break;
      
    case 'news':
    case 'economic_data':
      // 뉴스나 경제 데이터는 별도 처리 또는 대시보드 스토어로 전달
      break;
  }
  
  store.addMessage(message);
};

// 구독 관리를 위한 헬퍼 함수들
export const subscribeToSymbol = (symbol: string, type: 'stock' | 'crypto') => {
  const store = useWebSocketStore.getState();
  
  if (type === 'stock') {
    store.addStockSubscription(symbol);
  } else {
    store.addCryptoSubscription(symbol);
  }
};

export const unsubscribeFromSymbol = (symbol: string, type: 'stock' | 'crypto') => {
  const store = useWebSocketStore.getState();
  
  if (type === 'stock') {
    store.removeStockSubscription(symbol);
  } else {
    store.removeCryptoSubscription(symbol);
  }
};

// 성능 모니터링을 위한 헬퍼
export const startPerformanceMonitoring = () => {
  let messageCount = 0;
  let lastCount = 0;
  
  const interval = setInterval(() => {
    const store = useWebSocketStore.getState();
    messageCount = store.messagesReceived;
    const messagesPerSecond = messageCount - lastCount;
    store.updateMessagesPerSecond(messagesPerSecond);
    lastCount = messageCount;
  }, 1000);
  
  return () => clearInterval(interval);
};