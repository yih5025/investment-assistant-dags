import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { AuthState, User, AuthTokens } from '../types/auth';
import { STORAGE_KEYS } from '../utils/constants';

interface AuthStore extends AuthState {
  // Actions
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setAuth: (data: { user: User; tokens: AuthTokens }) => void;
  clearAuth: () => void;
  updateUser: (user: Partial<User>) => void;
  setTokens: (tokens: AuthTokens) => void;
}

export const useAuthStore = create<AuthStore>()(
  persist(
    (set, get) => ({
      // Initial state
      isAuthenticated: false,
      user: null,
      tokens: null,
      loading: false,
      error: null,

      // Actions
      setLoading: (loading) => 
        set({ loading }),

      setError: (error) => 
        set({ error }),

      setAuth: ({ user, tokens }) =>
        set({
          isAuthenticated: true,
          user,
          tokens,
          loading: false,
          error: null,
        }),

      clearAuth: () =>
        set({
          isAuthenticated: false,
          user: null,
          tokens: null,
          loading: false,
          error: null,
        }),

      updateUser: (userData) => {
        const currentUser = get().user;
        if (currentUser) {
          set({
            user: { ...currentUser, ...userData },
          });
        }
      },

      setTokens: (tokens) => {
        const currentState = get();
        if (currentState.isAuthenticated) {
          set({ tokens });
        }
      },
    }),
    {
      name: STORAGE_KEYS.AUTH_TOKEN,
      // 민감한 정보는 localStorage에 저장하지 않음
      partialize: (state) => ({
        isAuthenticated: state.isAuthenticated,
        user: state.user,
        // tokens는 별도로 관리
      }),
    }
  )
);