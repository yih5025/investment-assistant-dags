import { ApiService } from './api';
import { API_ENDPOINTS } from '../utils/constants';
import { buildQueryString } from '../utils/helpers';
import type {
  News,
  ApiResponse,
  PaginatedResponse
} from '../types/api';

export interface NewsSearchParams {
  query?: string;
  category?: 'general' | 'business' | 'technology' | 'crypto' | 'stocks' | 'economic';
  source?: string;
  sentiment?: 'positive' | 'negative' | 'neutral';
  startDate?: string;
  endDate?: string;
  page?: number;
  limit?: number;
  sortBy?: 'publishedAt' | 'relevance' | 'popularity';
  sortOrder?: 'asc' | 'desc';
}

export interface NewsAnalysis {
  sentiment: 'positive' | 'negative' | 'neutral';
  confidence: number;
  keywords: string[];
  entities: {
    stocks: string[];
    cryptos: string[];
    companies: string[];
    people: string[];
  };
  impact: 'high' | 'medium' | 'low';
}

export class NewsApiService extends ApiService {
  // 뉴스 목록 조회
  async getNews(params: NewsSearchParams = {}): Promise<PaginatedResponse<News>> {
    const queryString = buildQueryString(params);
    const url = queryString ? `${API_ENDPOINTS.NEWS.LIST}?${queryString}` : API_ENDPOINTS.NEWS.LIST;
    return this.get(url);
  }

  // 특정 뉴스 상세 정보
  async getNewsDetail(id: string): Promise<ApiResponse<News & { analysis?: NewsAnalysis }>> {
    return this.get(API_ENDPOINTS.NEWS.DETAIL(id));
  }

  // 뉴스 검색
  async searchNews(query: string, params: Omit<NewsSearchParams, 'query'> = {}): Promise<PaginatedResponse<News>> {
    const queryString = buildQueryString({ query, ...params });
    return this.get(`${API_ENDPOINTS.NEWS.SEARCH}?${queryString}`);
  }

  // 카테고리별 뉴스
  async getNewsByCategory(
    category: NewsSearchParams['category'], 
    params: Omit<NewsSearchParams, 'category'> = {}
  ): Promise<PaginatedResponse<News>> {
    const queryString = buildQueryString({ category, ...params });
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/category?${queryString}`);
  }

  // 트렌딩 뉴스
  async getTrendingNews(limit: number = 10): Promise<ApiResponse<News[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/trending?limit=${limit}`);
  }

  // 최신 뉴스
  async getLatestNews(limit: number = 20): Promise<ApiResponse<News[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/latest?limit=${limit}`);
  }

  // 주식 관련 뉴스
  async getStockNews(symbol: string, limit: number = 10): Promise<ApiResponse<News[]>> {
    const queryString = buildQueryString({ symbol, limit });
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/stocks?${queryString}`);
  }

  // 암호화폐 관련 뉴스
  async getCryptoNews(symbol?: string, limit: number = 10): Promise<ApiResponse<News[]>> {
    const queryString = buildQueryString({ symbol, limit });
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/crypto?${queryString}`);
  }

  // 경제 뉴스
  async getEconomicNews(limit: number = 10): Promise<ApiResponse<News[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/economic?limit=${limit}`);
  }

  // 뉴스 소스 목록
  async getNewsSources(): Promise<ApiResponse<{
    id: string;
    name: string;
    description?: string;
    url: string;
    category: string;
    language: string;
    country: string;
  }[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/sources`);
  }

  // 뉴스 분석 (감정 분석, 키워드 추출 등)
  async analyzeNews(id: string): Promise<ApiResponse<NewsAnalysis>> {
    return this.get(`${API_ENDPOINTS.NEWS.DETAIL(id)}/analyze`);
  }

  // 뉴스 요약
  async summarizeNews(id: string): Promise<ApiResponse<{
    summary: string;
    keyPoints: string[];
  }>> {
    return this.get(`${API_ENDPOINTS.NEWS.DETAIL(id)}/summary`);
  }

  // 관련 뉴스 추천
  async getRelatedNews(id: string, limit: number = 5): Promise<ApiResponse<News[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.DETAIL(id)}/related?limit=${limit}`);
  }

  // 뉴스 북마크 추가/제거
  async bookmarkNews(id: string): Promise<ApiResponse<void>> {
    return this.post(`${API_ENDPOINTS.NEWS.DETAIL(id)}/bookmark`);
  }

  async removeBookmark(id: string): Promise<ApiResponse<void>> {
    return this.delete(`${API_ENDPOINTS.NEWS.DETAIL(id)}/bookmark`);
  }

  // 북마크된 뉴스 목록
  async getBookmarkedNews(params: Omit<NewsSearchParams, 'category'> = {}): Promise<PaginatedResponse<News>> {
    const queryString = buildQueryString(params);
    const url = queryString ? `${API_ENDPOINTS.NEWS.LIST}/bookmarks?${queryString}` : `${API_ENDPOINTS.NEWS.LIST}/bookmarks`;
    return this.get(url);
  }

  // 뉴스 피드 개인화 설정
  async updateNewsPreferences(preferences: {
    categories: string[];
    sources: string[];
    keywords: string[];
    excludeKeywords: string[];
  }): Promise<ApiResponse<void>> {
    return this.put(`${API_ENDPOINTS.NEWS.LIST}/preferences`, preferences);
  }

  // 개인화된 뉴스 피드
  async getPersonalizedNews(limit: number = 20): Promise<ApiResponse<News[]>> {
    return this.get(`${API_ENDPOINTS.NEWS.LIST}/personalized?limit=${limit}`);
  }
}

// 싱글톤 인스턴스
export const newsApi = new NewsApiService();