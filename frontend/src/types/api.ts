// API 공통 응답 타입
export interface ApiResponse<T> {
  success: boolean;
  data: T;
  message?: string;
  error?: string;
}

// 페이지네이션 타입
export interface PaginationMeta {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
}

export interface PaginatedResponse<T> extends ApiResponse<T[]> {
  meta: PaginationMeta;
}

// 주식 관련 타입
export interface Stock {
  symbol: string;
  name: string;
  price: number;
  change: number;
  changePercent: number;
  volume: number;
  marketCap?: number;
  sector?: string;
  industry?: string;
}

export interface StockPrice {
  symbol: string;
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// 암호화폐 관련 타입
export interface Crypto {
  symbol: string;
  name: string;
  price: number;
  change24h: number;
  changePercent24h: number;
  volume24h: number;
  marketCap: number;
  rank: number;
}

// 뉴스 관련 타입
export interface News {
  id: string;
  title: string;
  content: string;
  summary?: string;
  source: string;
  publishedAt: string;
  url: string;
  imageUrl?: string;
  tags: string[];
  sentiment?: 'positive' | 'negative' | 'neutral';
}

// 경제지표 관련 타입
export interface EconomicIndicator {
  id: string;
  name: string;
  value: number;
  unit: string;
  date: string;
  previousValue?: number;
  change?: number;
  changePercent?: number;
}

// WebSocket 메시지 타입
export interface WebSocketMessage {
  type: 'stock_price' | 'crypto_price' | 'news' | 'economic_data';
  data: any;
  timestamp: string;
}

// 차트 데이터 타입
export interface ChartDataPoint {
  timestamp: string;
  value: number;
  label?: string;
}

export interface CandlestickData {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}