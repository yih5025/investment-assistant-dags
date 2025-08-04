import { useCallback } from 'react';
import { useAuthStore } from '../store/authStore';
import { authApi } from '../services/authApi';
import { STORAGE_KEYS } from '../utils/constants';
import { safeLocalStorage, normalizeError } from '../utils/helpers';
import type { LoginCredentials, RegisterData, User } from '../types/auth';

export function useAuth() {
  const {
    isAuthenticated,
    user,
    tokens,
    loading,
    error,
    setLoading,
    setError,
    setAuth,
    clearAuth,
  } = useAuthStore();

  // 로그인
  const login = useCallback(async (credentials: LoginCredentials) => {
    setLoading(true);
    setError(null);

    try {
      const response = await authApi.login(credentials);
      
      if (response.success && response.data) {
        const { user, accessToken, refreshToken, expiresIn } = response.data;
        
        // 토큰 저장
        safeLocalStorage.setItem(STORAGE_KEYS.AUTH_TOKEN, accessToken);
        safeLocalStorage.setItem(STORAGE_KEYS.REFRESH_TOKEN, refreshToken);
        
        // 상태 업데이트
        setAuth({
          user,
          tokens: { accessToken, refreshToken, expiresIn },
        });
        
        return { success: true, user };
      } else {
        throw new Error(response.message || '로그인에 실패했습니다.');
      }
    } catch (error) {
      const errorMessage = normalizeError(error);
      setError(errorMessage);
      throw new Error(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [setLoading, setError, setAuth]);

  // 회원가입
  const register = useCallback(async (data: RegisterData) => {
    setLoading(true);
    setError(null);

    try {
      const response = await authApi.register(data);
      
      if (response.success && response.data) {
        return { success: true, user: response.data.user };
      } else {
        throw new Error(response.message || '회원가입에 실패했습니다.');
      }
    } catch (error) {
      const errorMessage = normalizeError(error);
      setError(errorMessage);
      throw new Error(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [setLoading, setError]);

  // 로그아웃
  const logout = useCallback(async () => {
    setLoading(true);
    
    try {
      // 서버에 로그아웃 요청
      await authApi.logout();
    } catch (error) {
      // 로그아웃 요청 실패해도 계속 진행
      console.warn('로그아웃 요청 실패:', error);
    } finally {
      // 로컬 상태 및 토큰 정리
      safeLocalStorage.removeItem(STORAGE_KEYS.AUTH_TOKEN);
      safeLocalStorage.removeItem(STORAGE_KEYS.REFRESH_TOKEN);
      clearAuth();
      setLoading(false);
    }
  }, [setLoading, clearAuth]);

  // 토큰 갱신
  const refreshToken = useCallback(async () => {
    const refreshToken = safeLocalStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN);
    
    if (!refreshToken) {
      clearAuth();
      return false;
    }

    try {
      const response = await authApi.refreshToken(refreshToken);
      
      if (response.success && response.data) {
        const { accessToken, refreshToken: newRefreshToken, expiresIn } = response.data;
        
        // 새 토큰 저장
        safeLocalStorage.setItem(STORAGE_KEYS.AUTH_TOKEN, accessToken);
        safeLocalStorage.setItem(STORAGE_KEYS.REFRESH_TOKEN, newRefreshToken);
        
        // 상태 업데이트 (사용자 정보는 유지)
        setAuth({
          user: user!,
          tokens: { accessToken, refreshToken: newRefreshToken, expiresIn },
        });
        
        return true;
      } else {
        throw new Error('토큰 갱신에 실패했습니다.');
      }
    } catch (error) {
      clearAuth();
      return false;
    }
  }, [user, setAuth, clearAuth]);

  // 사용자 프로필 로드
  const loadProfile = useCallback(async () => {
    const token = safeLocalStorage.getItem(STORAGE_KEYS.AUTH_TOKEN);
    
    if (!token) {
      clearAuth();
      return;
    }

    setLoading(true);
    
    try {
      const response = await authApi.getProfile();
      
      if (response.success && response.data) {
        setAuth({
          user: response.data,
          tokens: tokens || { 
            accessToken: token, 
            refreshToken: safeLocalStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN) || '', 
            expiresIn: 0 
          },
        });
      } else {
        clearAuth();
      }
    } catch (error) {
      console.error('프로필 로드 실패:', error);
      clearAuth();
    } finally {
      setLoading(false);
    }
  }, [tokens, setAuth, clearAuth, setLoading]);

  // 프로필 업데이트
  const updateProfile = useCallback(async (data: Partial<User>) => {
    if (!user) return;

    // 토큰 유효성 확인
    const token = safeLocalStorage.getItem(STORAGE_KEYS.AUTH_TOKEN);
    if (!token) {
      clearAuth();
      throw new Error('인증 토큰이 없습니다. 다시 로그인해주세요.');
    }

    setLoading(true);
    setError(null);

    try {
      const response = await authApi.updateProfile(data);
      
      if (response.success && response.data) {
        setAuth({
          user: response.data,
          tokens: tokens || {
            accessToken: token,
            refreshToken: safeLocalStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN) || '',
            expiresIn: 0
          },
        });
        
        return { success: true, user: response.data };
      } else {
        throw new Error(response.message || '프로필 업데이트에 실패했습니다.');
      }
    } catch (error) {
      const errorMessage = normalizeError(error);
      setError(errorMessage);
      throw new Error(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [user, tokens, setAuth, setLoading, setError, clearAuth]);

  // 비밀번호 변경
  const changePassword = useCallback(async (currentPassword: string, newPassword: string) => {
    setLoading(true);
    setError(null);

    try {
      const response = await authApi.changePassword({
        currentPassword,
        newPassword,
      });
      
      if (response.success) {
        return { success: true };
      } else {
        throw new Error(response.message || '비밀번호 변경에 실패했습니다.');
      }
    } catch (error) {
      const errorMessage = normalizeError(error);
      setError(errorMessage);
      throw new Error(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [setLoading, setError]);

  // 인증 상태 확인
  const checkAuth = useCallback(() => {
    const token = safeLocalStorage.getItem(STORAGE_KEYS.AUTH_TOKEN);
    
    if (token && !isAuthenticated) {
      loadProfile();
    } else if (!token && isAuthenticated) {
      clearAuth();
    }
  }, [isAuthenticated, loadProfile, clearAuth]);

  // 권한 확인
  const hasPermission = useCallback((_permission: string) => {
    // 실제 구현에서는 사용자의 권한을 확인
    // 현재는 로그인 여부만 확인
    return isAuthenticated;
  }, [isAuthenticated]);

  // 역할 확인
  const hasRole = useCallback((_role: string) => {
    // 실제 구현에서는 사용자의 역할을 확인
    // 현재는 단순화된 구현
    return isAuthenticated && user?.username === 'admin';
  }, [isAuthenticated, user]);

  return {
    // 상태
    isAuthenticated,
    user,
    tokens,
    loading,
    error,
    
    // 액션
    login,
    register,
    logout,
    refreshToken,
    loadProfile,
    updateProfile,
    changePassword,
    checkAuth,
    
    // 유틸리티
    hasPermission,
    hasRole,
  };
}