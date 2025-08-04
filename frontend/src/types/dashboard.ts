import type { Stock } from './api';

// 대시보드 위젯 타입
export type WidgetType = 
  | 'market_overview' 
  | 'stock_watchlist' 
  | 'crypto_prices' 
  | 'recent_news' 
  | 'economic_calendar'
  | 'portfolio_summary';

export interface Widget {
  id: string;
  type: WidgetType;
  title: string;
  position: {
    x: number;
    y: number;
    width: number;
    height: number;
  };
  config?: Record<string, any>;
  isVisible: boolean;
}

// 대시보드 레이아웃
export interface DashboardLayout {
  id: string;
  name: string;
  widgets: Widget[];
  isDefault: boolean;
}

// 시장 개요 데이터
export interface MarketOverview {
  indices: {
    name: string;
    symbol: string;
    value: number;
    change: number;
    changePercent: number;
  }[];
  topGainers: Stock[];
  topLosers: Stock[];
  mostActive: Stock[];
}

// 포트폴리오 관련 타입 (향후 확장용)
export interface PortfolioItem {
  symbol: string;
  name: string;
  quantity: number;
  avgPrice: number;
  currentPrice: number;
  totalValue: number;
  gainLoss: number;
  gainLossPercent: number;
}

export interface Portfolio {
  id: string;
  name: string;
  totalValue: number;
  dayChange: number;
  dayChangePercent: number;
  items: PortfolioItem[];
}

// 알림 관련 타입
export interface Alert {
  id: string;
  type: 'price' | 'news' | 'economic' | 'system';
  title: string;
  message: string;
  severity: 'info' | 'warning' | 'error' | 'success';
  timestamp: string;
  isRead: boolean;
  data?: Record<string, any>;
}