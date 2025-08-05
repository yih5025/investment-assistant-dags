import { ApiService } from './api';
import { API_ENDPOINTS } from '../utils/constants';
import type { ApiResponse } from '../types/api';
import type { 
  LoginCredentials, 
  RegisterData, 
  User,
  AuthTokens
} from '../types/auth';

export class AuthApiService extends ApiService {
  // 로그인
  async login(credentials: LoginCredentials): Promise<ApiResponse<AuthTokens & { user: User }>> {
    return this.post(API_ENDPOINTS.AUTH.LOGIN, credentials);
  }

  // 회원가입
  async register(data: RegisterData): Promise<ApiResponse<{ user: User }>> {
    return this.post(API_ENDPOINTS.AUTH.REGISTER, data);
  }

  // 토큰 갱신
  async refreshToken(refreshToken: string): Promise<ApiResponse<AuthTokens>> {
    return this.post(API_ENDPOINTS.AUTH.REFRESH, {
      refresh_token: refreshToken,
    });
  }

  // 로그아웃
  async logout(): Promise<ApiResponse<void>> {
    return this.post(API_ENDPOINTS.AUTH.LOGOUT);
  }

  // 사용자 프로필 조회
  async getProfile(): Promise<ApiResponse<User>> {
    return this.get(API_ENDPOINTS.AUTH.PROFILE);
  }

  // 사용자 프로필 업데이트
  async updateProfile(data: Partial<User>): Promise<ApiResponse<User>> {
    return this.put(API_ENDPOINTS.AUTH.PROFILE, data);
  }

  // 비밀번호 변경
  async changePassword(data: {
    currentPassword: string;
    newPassword: string;
  }): Promise<ApiResponse<void>> {
    return this.put(`${API_ENDPOINTS.AUTH.PROFILE}/password`, data);
  }

  // 비밀번호 재설정 요청
  async requestPasswordReset(email: string): Promise<ApiResponse<void>> {
    return this.post('/auth/password-reset/request', { email });
  }

  // 비밀번호 재설정
  async resetPassword(data: {
    token: string;
    newPassword: string;
  }): Promise<ApiResponse<void>> {
    return this.post('/auth/password-reset/confirm', data);
  }

  // 이메일 인증 요청
  async requestEmailVerification(): Promise<ApiResponse<void>> {
    return this.post('/auth/email/verify/request');
  }

  // 이메일 인증 확인
  async verifyEmail(token: string): Promise<ApiResponse<void>> {
    return this.post('/auth/email/verify/confirm', { token });
  }
}

// 싱글톤 인스턴스
export const authApi = new AuthApiService();