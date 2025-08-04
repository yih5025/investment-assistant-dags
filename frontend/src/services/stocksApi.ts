import { ApiService } from './api';
import { API_ENDPOINTS } from '../utils/constants';
import { buildQueryString } from '../utils/helpers';
import type {
  Stock,
  StockPrice,
  ApiResponse,
  PaginatedResponse,
  ChartDataPoint,
  CandlestickData
} from '../types/api';

export interface StockSearchParams {
  query?: string;
  sector?: string;
  industry?: string;
  minPrice?: number;
  maxPrice?: number;
  minVolume?: number;
  minMarketCap?: number;
  page?: number;
  limit?: number;
  sortBy?: 'symbol' | 'name' | 'price' | 'change' | 'volume' | 'marketCap';
  sortOrder?: 'asc' | 'desc';
}

export interface StockPriceParams {
  period?: '1d' | '5d' | '1m' | '3m' | '6m' | '1y' | '5y' | 'max';
  interval?: '1m' | '5m' | '15m' | '30m' | '1h' | '1d' | '1w' | '1M';
  startDate?: string;
  endDate?: string;
}

export class StocksApiService extends ApiService {
  // 주식 목록 조회
  async getStocks(params: StockSearchParams = {}): Promise<PaginatedResponse<Stock>> {
    const queryString = buildQueryString(params);
    const url = queryString ? `${API_ENDPOINTS.STOCKS.LIST}?${queryString}` : API_ENDPOINTS.STOCKS.LIST;
    return this.get(url);
  }

  // 특정 주식 상세 정보
  async getStock(symbol: string): Promise<ApiResponse<Stock>> {
    return this.get(API_ENDPOINTS.STOCKS.DETAIL(symbol));
  }

  // 주식 가격 데이터 조회
  async getStockPrices(
    symbol: string, 
    params: StockPriceParams = {}
  ): Promise<ApiResponse<StockPrice[]>> {
    const queryString = buildQueryString(params);
    const url = queryString 
      ? `${API_ENDPOINTS.STOCKS.PRICES(symbol)}?${queryString}` 
      : API_ENDPOINTS.STOCKS.PRICES(symbol);
    return this.get(url);
  }

  // 주식 차트 데이터 (라인 차트용)
  async getStockChart(
    symbol: string, 
    params: StockPriceParams = {}
  ): Promise<ApiResponse<ChartDataPoint[]>> {
    const queryString = buildQueryString({ ...params, format: 'chart' });
    return this.get(`${API_ENDPOINTS.STOCKS.PRICES(symbol)}?${queryString}`);
  }

  // 주식 캔들스틱 데이터
  async getStockCandlestick(
    symbol: string, 
    params: StockPriceParams = {}
  ): Promise<ApiResponse<CandlestickData[]>> {
    const queryString = buildQueryString({ ...params, format: 'candlestick' });
    return this.get(`${API_ENDPOINTS.STOCKS.PRICES(symbol)}?${queryString}`);
  }

  // 주식 검색
  async searchStocks(query: string, limit: number = 10): Promise<ApiResponse<Stock[]>> {
    const queryString = buildQueryString({ query, limit });
    return this.get(`${API_ENDPOINTS.STOCKS.SEARCH}?${queryString}`);
  }

  // 인기 주식 (가장 많이 거래된)
  async getPopularStocks(limit: number = 20): Promise<ApiResponse<Stock[]>> {
    return this.get(`${API_ENDPOINTS.STOCKS.LIST}/popular?limit=${limit}`);
  }

  // 상승/하락 주식
  async getTopMovers(): Promise<ApiResponse<{
    gainers: Stock[];
    losers: Stock[];
    mostActive: Stock[];
  }>> {
    return this.get(`${API_ENDPOINTS.STOCKS.LIST}/movers`);
  }

  // 섹터별 주식
  async getStocksBySector(sector: string, limit: number = 20): Promise<ApiResponse<Stock[]>> {
    return this.get(`${API_ENDPOINTS.STOCKS.LIST}/sector/${sector}?limit=${limit}`);
  }

  // 주식 통계 정보
  async getStockStats(symbol: string): Promise<ApiResponse<{
    symbol: string;
    fiftyTwoWeekHigh: number;
    fiftyTwoWeekLow: number;
    avgVolume: number;
    beta: number;
    eps: number;
    pe: number;
    dividend: number;
    dividendYield: number;
  }>> {
    return this.get(`${API_ENDPOINTS.STOCKS.DETAIL(symbol)}/stats`);
  }

  // 주식 뉴스
  async getStockNews(symbol: string, limit: number = 10): Promise<ApiResponse<any[]>> {
    return this.get(`${API_ENDPOINTS.STOCKS.DETAIL(symbol)}/news?limit=${limit}`);
  }

  // 관련 주식 추천
  async getRelatedStocks(symbol: string, limit: number = 5): Promise<ApiResponse<Stock[]>> {
    return this.get(`${API_ENDPOINTS.STOCKS.DETAIL(symbol)}/related?limit=${limit}`);
  }
}

// 싱글톤 인스턴스
export const stocksApi = new StocksApiService();