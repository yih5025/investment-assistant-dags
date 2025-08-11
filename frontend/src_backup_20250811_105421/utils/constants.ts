// API 엔드포인트
export const API_ENDPOINTS = {
  // 인증
  AUTH: {
    LOGIN: '/auth/login',
    REGISTER: '/auth/register',
    REFRESH: '/auth/refresh',
    LOGOUT: '/auth/logout',
    PROFILE: '/auth/profile',
  },
  
  // 주식
  STOCKS: {
    LIST: '/stocks',
    DETAIL: (symbol: string) => `/stocks/${symbol}`,
    PRICES: (symbol: string) => `/stocks/${symbol}/prices`,
    SEARCH: '/stocks/search',
  },
  
  // 암호화폐
  CRYPTO: {
    LIST: '/crypto',
    DETAIL: (symbol: string) => `/crypto/${symbol}`,
    PRICES: (symbol: string) => `/crypto/${symbol}/prices`,
  },
  
  // 뉴스
  NEWS: {
    LIST: '/news',
    DETAIL: (id: string) => `/news/${id}`,
    SEARCH: '/news/search',
  },
  
  // 경제지표
  ECONOMIC: {
    INDICATORS: '/economic/indicators',
    CALENDAR: '/economic/calendar',
  },
  
  // 대시보드
  DASHBOARD: {
    LAYOUT: '/dashboard/layout',
    WIDGETS: '/dashboard/widgets',
  },
} as const;

// WebSocket 이벤트
export const WS_EVENTS = {
  CONNECT: 'connect',
  DISCONNECT: 'disconnect',
  STOCK_PRICE_UPDATE: 'stock_price_update',
  CRYPTO_PRICE_UPDATE: 'crypto_price_update',
  NEWS_UPDATE: 'news_update',
  ECONOMIC_UPDATE: 'economic_update',
} as const;

// 로컬 스토리지 키
export const STORAGE_KEYS = {
  AUTH_TOKEN: 'investment_auth_token',
  REFRESH_TOKEN: 'investment_refresh_token',
  USER_PREFERENCES: 'investment_user_preferences',
  DASHBOARD_LAYOUT: 'investment_dashboard_layout',
  WATCHLIST: 'investment_watchlist',
} as const;

// 차트 설정
export const CHART_COLORS = {
  PRIMARY: '#2563eb',
  SUCCESS: '#10b981',
  DANGER: '#ef4444',
  WARNING: '#f59e0b',
  INFO: '#3b82f6',
  GRAY: '#6b7280',
} as const;

// 페이지네이션 기본값
export const PAGINATION = {
  DEFAULT_PAGE: 1,
  DEFAULT_LIMIT: 20,
  MAX_LIMIT: 100,
} as const;

// 데이터 업데이트 간격 (밀리초)
export const UPDATE_INTERVALS = {
  REALTIME: 1000,      // 1초
  FREQUENT: 5000,      // 5초
  NORMAL: 30000,       // 30초
  SLOW: 300000,        // 5분
} as const;

// 통화 및 포맷팅
export const CURRENCY = {
  USD: 'USD',
  KRW: 'KRW',
  BTC: 'BTC',
  ETH: 'ETH',
} as const;

// 날짜 형식
export const DATE_FORMATS = {
  SHORT_DATE: 'yyyy-MM-dd',
  LONG_DATE: 'yyyy-MM-dd HH:mm:ss',
  TIME_ONLY: 'HH:mm:ss',
  MONTH_YEAR: 'MMM yyyy',
} as const;