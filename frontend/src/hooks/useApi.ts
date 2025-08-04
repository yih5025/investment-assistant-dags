import { useState, useEffect, useCallback } from 'react';
import useSWR, { type SWRConfiguration, type SWRResponse, mutate } from 'swr';
import { normalizeError } from '../utils/helpers';

// API 호출 상태 타입
export interface ApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

// useApi 훅 - 기본 API 호출
export function useApi<T>(
  fetcher: () => Promise<T>,
  dependencies: any[] = []
): ApiState<T> & {
  refetch: () => Promise<void>;
  reset: () => void;
} {
  const [state, setState] = useState<ApiState<T>>({
    data: null,
    loading: false,
    error: null,
  });

  const refetch = useCallback(async () => {
    setState(prev => ({ ...prev, loading: true, error: null }));
    
    try {
      const data = await fetcher();
      setState({ data, loading: false, error: null });
    } catch (error) {
      setState(prev => ({ 
        ...prev, 
        loading: false, 
        error: normalizeError(error) 
      }));
    }
  }, dependencies);

  const reset = useCallback(() => {
    setState({ data: null, loading: false, error: null });
  }, []);

  useEffect(() => {
    refetch();
  }, dependencies);

  return {
    ...state,
    refetch,
    reset,
  };
}

// useSWRApi 훅 - SWR을 사용한 API 호출
export function useSWRApi<T>(
  key: string | null,
  fetcher: () => Promise<T>,
  options?: SWRConfiguration
): SWRResponse<T, string> {
  return useSWR(
    key,
    async () => {
      try {
        return await fetcher();
      } catch (error) {
        throw normalizeError(error);
      }
    },
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: true,
      ...options,
    }
  );
}

// useMutation 훅 - 데이터 변경 작업
export function useMutation<T, P = void>(
  mutationFn: (params: P) => Promise<T>
) {
  const [state, setState] = useState<ApiState<T>>({
    data: null,
    loading: false,
    error: null,
  });

  const mutate = useCallback(async (params: P) => {
    setState(prev => ({ ...prev, loading: true, error: null }));
    
    try {
      const data = await mutationFn(params);
      setState({ data, loading: false, error: null });
      return data;
    } catch (error) {
      const errorMessage = normalizeError(error);
      setState(prev => ({ 
        ...prev, 
        loading: false, 
        error: errorMessage 
      }));
      throw new Error(errorMessage);
    }
  }, [mutationFn]);

  const reset = useCallback(() => {
    setState({ data: null, loading: false, error: null });
  }, []);

  return {
    ...state,
    mutate,
    reset,
  };
}

// useInfiniteApi 훅 - 무한 스크롤용 API 호출
export function useInfiniteApi<T>(
  getKey: (pageIndex: number, previousPageData: T | null) => string | null,
  fetcher: (key: string) => Promise<T>,
  options?: SWRConfiguration
) {
  const [hasMore, setHasMore] = useState(true);

  const swr = useSWR(
    getKey,
    async (key: string) => {
      try {
        const data = await fetcher(key);
        // 데이터가 비어있거나 예상보다 적으면 더 이상 로드할 데이터가 없음
        if (!data || (Array.isArray(data) && data.length === 0)) {
          setHasMore(false);
        }
        return data;
      } catch (error) {
        throw normalizeError(error);
      }
    },
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: true,
      ...options,
    }
  );

  const loadMore = useCallback(() => {
    if (!swr.isLoading && hasMore) {
      swr.mutate();
    }
  }, [swr.isLoading, hasMore, swr.mutate]);

  return {
    ...swr,
    hasMore,
    loadMore,
  };
}

// useOptimisticUpdate 훅 - 낙관적 업데이트
export function useOptimisticUpdate<T>(
  key: string,
  updateFn: () => Promise<T>
) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const update = useCallback(async (optimisticData: T) => {
    setLoading(true);
    setError(null);

    // 낙관적 업데이트
    mutate(key, optimisticData, false);

    try {
      const result = await updateFn();
      // 실제 결과로 업데이트
      mutate(key, result);
      setLoading(false);
      return result;
    } catch (error) {
      // 오류 발생 시 원래 데이터로 롤백
      mutate(key);
      const errorMessage = normalizeError(error);
      setError(errorMessage);
      setLoading(false);
      throw new Error(errorMessage);
    }
  }, [key, updateFn]);

  return {
    update,
    loading,
    error,
  };
}

// useDebounceApi 훅 - 디바운스된 API 호출
export function useDebounceApi<T>(
  fetcher: () => Promise<T>,
  delay: number = 300,
  dependencies: any[] = []
): ApiState<T> {
  const [state, setState] = useState<ApiState<T>>({
    data: null,
    loading: false,
    error: null,
  });

  useEffect(() => {
    setState(prev => ({ ...prev, loading: true, error: null }));

    const timeoutId = setTimeout(async () => {
      try {
        const data = await fetcher();
        setState({ data, loading: false, error: null });
      } catch (error) {
        setState(prev => ({ 
          ...prev, 
          loading: false, 
          error: normalizeError(error) 
        }));
      }
    }, delay);

    return () => clearTimeout(timeoutId);
  }, [...dependencies, delay]);

  return state;
}