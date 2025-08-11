import axios, { type AxiosInstance, type AxiosRequestConfig, type AxiosResponse } from 'axios';
import { STORAGE_KEYS } from '../utils/constants';
import { safeLocalStorage, normalizeError } from '../utils/helpers';

// API 클라이언트 설정
const createApiClient = (): AxiosInstance => {
  const baseURL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:30888';
  
  const client = axios.create({
    baseURL,
    timeout: 10000,
    headers: {
      'Content-Type': 'application/json',
    },
  });

  // 요청 인터셉터 - 인증 토큰 추가
  client.interceptors.request.use(
    (config) => {
      const token = safeLocalStorage.getItem(STORAGE_KEYS.AUTH_TOKEN);
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // 응답 인터셉터 - 에러 처리 및 토큰 갱신
  client.interceptors.response.use(
    (response: AxiosResponse) => {
      return response;
    },
    async (error) => {
      const originalRequest = error.config;

      if (error.response?.status === 401 && !originalRequest._retry) {
        originalRequest._retry = true;

        try {
          const refreshToken = safeLocalStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN);
          if (refreshToken) {
            const response = await axios.post(`${baseURL}/auth/refresh`, {
              refresh_token: refreshToken,
            });

            const { access_token } = response.data;
            safeLocalStorage.setItem(STORAGE_KEYS.AUTH_TOKEN, access_token);

            originalRequest.headers.Authorization = `Bearer ${access_token}`;
            return client(originalRequest);
          }
        } catch (refreshError) {
          // 토큰 갱신 실패 시 로그아웃 처리
          safeLocalStorage.removeItem(STORAGE_KEYS.AUTH_TOKEN);
          safeLocalStorage.removeItem(STORAGE_KEYS.REFRESH_TOKEN);
          window.location.href = '/login';
        }
      }

      return Promise.reject(error);
    }
  );

  return client;
};

// API 클라이언트 인스턴스
export const apiClient = createApiClient();

// 공통 API 메서드들
export class ApiService {
  protected client: AxiosInstance;

  constructor(client: AxiosInstance = apiClient) {
    this.client = client;
  }

  // GET 요청
  protected async get<T>(
    url: string, 
    config?: AxiosRequestConfig
  ): Promise<T> {
    try {
      const response = await this.client.get<T>(url, config);
      return response.data;
    } catch (error) {
      throw new Error(normalizeError(error));
    }
  }

  // POST 요청
  protected async post<T, D = any>(
    url: string, 
    data?: D, 
    config?: AxiosRequestConfig
  ): Promise<T> {
    try {
      const response = await this.client.post<T>(url, data, config);
      return response.data;
    } catch (error) {
      throw new Error(normalizeError(error));
    }
  }

  // PUT 요청
  protected async put<T, D = any>(
    url: string, 
    data?: D, 
    config?: AxiosRequestConfig
  ): Promise<T> {
    try {
      const response = await this.client.put<T>(url, data, config);
      return response.data;
    } catch (error) {
      throw new Error(normalizeError(error));
    }
  }

  // DELETE 요청
  protected async delete<T>(
    url: string, 
    config?: AxiosRequestConfig
  ): Promise<T> {
    try {
      const response = await this.client.delete<T>(url, config);
      return response.data;
    } catch (error) {
      throw new Error(normalizeError(error));
    }
  }

  // PATCH 요청
  protected async patch<T, D = any>(
    url: string, 
    data?: D, 
    config?: AxiosRequestConfig
  ): Promise<T> {
    try {
      const response = await this.client.patch<T>(url, data, config);
      return response.data;
    } catch (error) {
      throw new Error(normalizeError(error));
    }
  }
}

export default apiClient;