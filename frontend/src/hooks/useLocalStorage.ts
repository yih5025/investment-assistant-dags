import { useState, useCallback, useEffect } from 'react';
import { safeLocalStorage } from '../utils/helpers';

// 로컬스토리지 훅의 옵션 타입
export interface UseLocalStorageOptions<T> {
  serializer?: {
    read: (value: string) => T;
    write: (value: T) => string;
  };
  syncAcrossTabs?: boolean;
  onError?: (error: Error) => void;
}

// 기본 직렬화기
const defaultSerializer = {
  read: (value: string) => {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  },
  write: (value: any) => JSON.stringify(value),
};

// useLocalStorage 훅
export function useLocalStorage<T>(
  key: string,
  initialValue: T,
  options: UseLocalStorageOptions<T> = {}
): [T, (value: T | ((prev: T) => T)) => void, () => void] {
  const {
    serializer = defaultSerializer,
    syncAcrossTabs = true,
    onError = console.error,
  } = options;

  // 초기값을 로컬스토리지에서 읽어오기
  const readValue = useCallback((): T => {
    try {
      const item = safeLocalStorage.getItem(key);
      if (item === null) {
        return initialValue;
      }
      return serializer.read(item);
    } catch (error) {
      onError(error as Error);
      return initialValue;
    }
  }, [key, initialValue, serializer, onError]);

  const [storedValue, setStoredValue] = useState<T>(readValue);

  // 값 업데이트 함수
  const setValue = useCallback(
    (value: T | ((prev: T) => T)) => {
      try {
        const newValue = value instanceof Function ? value(storedValue) : value;
        setStoredValue(newValue);
        
        if (newValue === undefined) {
          safeLocalStorage.removeItem(key);
        } else {
          safeLocalStorage.setItem(key, serializer.write(newValue));
        }
      } catch (error) {
        onError(error as Error);
      }
    },
    [key, storedValue, serializer, onError]
  );

  // 값 삭제 함수
  const removeValue = useCallback(() => {
    try {
      safeLocalStorage.removeItem(key);
      setStoredValue(initialValue);
    } catch (error) {
      onError(error as Error);
    }
  }, [key, initialValue, onError]);

  // 탭 간 동기화를 위한 storage 이벤트 리스너
  useEffect(() => {
    if (!syncAcrossTabs) return;

    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === key && e.newValue !== null) {
        try {
          setStoredValue(serializer.read(e.newValue));
        } catch (error) {
          onError(error as Error);
        }
      } else if (e.key === key && e.newValue === null) {
        setStoredValue(initialValue);
      }
    };

    window.addEventListener('storage', handleStorageChange);
    return () => window.removeEventListener('storage', handleStorageChange);
  }, [key, initialValue, serializer, syncAcrossTabs, onError]);

  // 컴포넌트 마운트 시 최신 값으로 동기화
  useEffect(() => {
    setStoredValue(readValue());
  }, [readValue]);

  return [storedValue, setValue, removeValue];
}

// 간단한 boolean 값을 위한 특화된 훅
export function useLocalStorageBoolean(
  key: string,
  initialValue: boolean = false
): [boolean, () => void, () => void, () => void, () => void] {
  const [value, setValue, removeValue] = useLocalStorage(key, initialValue);

  const setTrue = useCallback(() => setValue(true), [setValue]);
  const setFalse = useCallback(() => setValue(false), [setValue]);
  const toggle = useCallback(() => setValue(prev => !prev), [setValue]);

  return [value, setTrue, setFalse, toggle, removeValue];
}

// 배열을 위한 특화된 훅
export function useLocalStorageArray<T>(
  key: string,
  initialValue: T[] = []
): [
  T[], 
  (item: T) => void, 
  (index: number) => void, 
  (predicate: (item: T) => boolean) => void,
  () => void,
  () => void
] {
  const [array, setArray, removeValue] = useLocalStorage<T[]>(key, initialValue);

  const addItem = useCallback((item: T) => {
    setArray(prev => [...prev, item]);
  }, [setArray]);

  const removeItem = useCallback((index: number) => {
    setArray(prev => prev.filter((_, i) => i !== index));
  }, [setArray]);

  const removeItemByPredicate = useCallback((predicate: (item: T) => boolean) => {
    setArray(prev => prev.filter(item => !predicate(item)));
  }, [setArray]);

  const clearArray = useCallback(() => {
    setArray([]);
  }, [setArray]);

  return [array, addItem, removeItem, removeItemByPredicate, clearArray, removeValue];
}

// 객체를 위한 특화된 훅
export function useLocalStorageObject<T extends Record<string, any>>(
  key: string,
  initialValue: T
): [T, (updates: Partial<T>) => void, (key: keyof T) => void, () => void] {
  const [object, setObject, removeValue] = useLocalStorage<T>(key, initialValue);

  const updateObject = useCallback((updates: Partial<T>) => {
    setObject(prev => ({ ...prev, ...updates }));
  }, [setObject]);

  const removeProperty = useCallback((propertyKey: keyof T) => {
    setObject(prev => {
      const newObject = { ...prev };
      delete newObject[propertyKey];
      return newObject;
    });
  }, [setObject]);

  return [object, updateObject, removeProperty, removeValue];
}

// 만료 시간이 있는 로컬스토리지 훅
export function useLocalStorageWithExpiry<T>(
  key: string,
  initialValue: T,
  expiryTime: number // 밀리초
): [T | null, (value: T) => void, () => void] {
  const [storedValue, setStoredValue] = useState<T | null>(null);

  // 값 읽기 (만료 시간 확인)
  const readValue = useCallback((): T | null => {
    try {
      const item = safeLocalStorage.getItem(key);
      if (!item) return initialValue;

      const { value, timestamp } = JSON.parse(item);
      const now = Date.now();

      if (now - timestamp > expiryTime) {
        safeLocalStorage.removeItem(key);
        return initialValue;
      }

      return value;
    } catch {
      return initialValue;
    }
  }, [key, initialValue, expiryTime]);

  // 값 설정 (타임스탬프와 함께)
  const setValue = useCallback((value: T) => {
    try {
      const item = {
        value,
        timestamp: Date.now(),
      };
      safeLocalStorage.setItem(key, JSON.stringify(item));
      setStoredValue(value);
    } catch (error) {
      console.error('Error setting localStorage value:', error);
    }
  }, [key]);

  // 값 삭제
  const removeValue = useCallback(() => {
    safeLocalStorage.removeItem(key);
    setStoredValue(initialValue);
  }, [key, initialValue]);

  // 초기 로드
  useEffect(() => {
    setStoredValue(readValue());
  }, [readValue]);

  return [storedValue, setValue, removeValue];
}