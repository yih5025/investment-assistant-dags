import { ApiService } from './api';
import { API_ENDPOINTS } from '../utils/constants';
import { buildQueryString } from '../utils/helpers';
import type {
  Crypto,
  ApiResponse,
  PaginatedResponse,
  ChartDataPoint,
  CandlestickData
} from '../types/api';

export interface CryptoSearchParams {
  query?: string;
  minPrice?: number;
  maxPrice?: number;
  minVolume?: number;
  minMarketCap?: number;
  page?: number;
  limit?: number;
  sortBy?: 'rank' | 'symbol' | 'name' | 'price' | 'change24h' | 'volume24h' | 'marketCap';
  sortOrder?: 'asc' | 'desc';
}

export interface CryptoPriceParams {
  period?: '1h' | '24h' | '7d' | '30d' | '90d' | '1y' | 'max';
  interval?: '1m' | '5m' | '15m' | '30m' | '1h' | '4h' | '1d' | '1w';
  startDate?: string;
  endDate?: string;
}

export class CryptoApiService extends ApiService {
  // 암호화폐 목록 조회
  async getCryptos(params: CryptoSearchParams = {}): Promise<PaginatedResponse<Crypto>> {
    const queryString = buildQueryString(params);
    const url = queryString ? `${API_ENDPOINTS.CRYPTO.LIST}?${queryString}` : API_ENDPOINTS.CRYPTO.LIST;
    return this.get(url);
  }

  // 특정 암호화폐 상세 정보
  async getCrypto(symbol: string): Promise<ApiResponse<Crypto>> {
    return this.get(API_ENDPOINTS.CRYPTO.DETAIL(symbol));
  }

  // 암호화폐 가격 데이터 조회
  async getCryptoPrices(
    symbol: string, 
    params: CryptoPriceParams = {}
  ): Promise<ApiResponse<any[]>> {
    const queryString = buildQueryString(params);
    const url = queryString 
      ? `${API_ENDPOINTS.CRYPTO.PRICES(symbol)}?${queryString}` 
      : API_ENDPOINTS.CRYPTO.PRICES(symbol);
    return this.get(url);
  }

  // 암호화폐 차트 데이터 (라인 차트용)
  async getCryptoChart(
    symbol: string, 
    params: CryptoPriceParams = {}
  ): Promise<ApiResponse<ChartDataPoint[]>> {
    const queryString = buildQueryString({ ...params, format: 'chart' });
    return this.get(`${API_ENDPOINTS.CRYPTO.PRICES(symbol)}?${queryString}`);
  }

  // 암호화폐 캔들스틱 데이터
  async getCryptoCandlestick(
    symbol: string, 
    params: CryptoPriceParams = {}
  ): Promise<ApiResponse<CandlestickData[]>> {
    const queryString = buildQueryString({ ...params, format: 'candlestick' });
    return this.get(`${API_ENDPOINTS.CRYPTO.PRICES(symbol)}?${queryString}`);
  }

  // 암호화폐 검색
  async searchCryptos(query: string, limit: number = 10): Promise<ApiResponse<Crypto[]>> {
    const queryString = buildQueryString({ query, limit });
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/search?${queryString}`);
  }

  // 상위 암호화폐 (시가총액 기준)
  async getTopCryptos(limit: number = 20): Promise<ApiResponse<Crypto[]>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/top?limit=${limit}`);
  }

  // 트렌딩 암호화폐
  async getTrendingCryptos(limit: number = 10): Promise<ApiResponse<Crypto[]>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/trending?limit=${limit}`);
  }

  // 상승/하락 암호화폐
  async getCryptoMovers(): Promise<ApiResponse<{
    gainers: Crypto[];
    losers: Crypto[];
    mostActive: Crypto[];
  }>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/movers`);
  }

  // 암호화폐 도미넌스 정보
  async getCryptoDominance(): Promise<ApiResponse<{
    btcDominance: number;
    ethDominance: number;
    totalMarketCap: number;
    totalVolume24h: number;
  }>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/dominance`);
  }

  // Fear & Greed Index
  async getFearGreedIndex(): Promise<ApiResponse<{
    value: number;
    classification: string;
    timestamp: string;
    previousValue?: number;
  }>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/fear-greed`);
  }

  // 암호화폐 뉴스
  async getCryptoNews(symbol: string, limit: number = 10): Promise<ApiResponse<any[]>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.DETAIL(symbol)}/news?limit=${limit}`);
  }

  // 암호화폐 소셜 데이터
  async getCryptoSocial(symbol: string): Promise<ApiResponse<{
    twitterFollowers: number;
    redditSubscribers: number;
    githubStars: number;
    sentiment: 'positive' | 'negative' | 'neutral';
  }>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.DETAIL(symbol)}/social`);
  }

  // DeFi 프로토콜 정보
  async getDefiProtocols(limit: number = 20): Promise<ApiResponse<{
    name: string;
    symbol: string;
    tvl: number;
    change24h: number;
    chains: string[];
  }[]>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/defi?limit=${limit}`);
  }

  // NFT 컬렉션 정보
  async getNftCollections(limit: number = 20): Promise<ApiResponse<{
    name: string;
    floorPrice: number;
    volume24h: number;
    change24h: number;
    blockchain: string;
  }[]>> {
    return this.get(`${API_ENDPOINTS.CRYPTO.LIST}/nft?limit=${limit}`);
  }
}

// 싱글톤 인스턴스
export const cryptoApi = new CryptoApiService();