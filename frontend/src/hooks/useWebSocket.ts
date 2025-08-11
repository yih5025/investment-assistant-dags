import { useState, useEffect, useCallback } from 'react';

export interface WebSocketState {
  connected: boolean;
  connecting: boolean;
  error: string | null;
  lastMessage: any;
  reconnectCount: number;
}

export interface UseWebSocketOptions {
  autoConnect?: boolean;
  reconnectAttempts?: number;
  reconnectDelay?: number;
}

// Socket.IO 비활성화: 기존 API와 호환되는 빈 구현을 제공합니다.
export function useWebSocket(_url?: string, options: UseWebSocketOptions = {}) {
  const { autoConnect = false } = options;
  const [state, setState] = useState<WebSocketState>({
    connected: false,
    connecting: false,
    error: null,
    lastMessage: null,
    reconnectCount: 0,
  });

  const connect = useCallback(() => {
    setState((s) => ({ ...s, connecting: false, connected: false }));
  }, []);
  const disconnect = useCallback(() => {
    setState((s) => ({ ...s, connecting: false, connected: false }));
  }, []);
  const emit = useCallback((_event: string, _data?: any) => false, []);
  const on = useCallback((_event: string, _listener: (...args: any[]) => void) => {
    return () => {};
  }, []);
  const off = useCallback((_event: string, _listener?: (...args: any[]) => void) => {}, []);

  useEffect(() => {
    if (autoConnect) connect();
    return () => disconnect();
  }, [autoConnect, connect, disconnect]);

  return { ...state, connect, disconnect, emit, on, off };
}

export function useWebSocketEvent<T>(_event: string, _handler: (data: T) => void, _dependencies: any[] = []) {
  // no-op
}

export function useWebSocketSubscription<T>(_event: string, initialValue: T | null = null) {
  const [data] = useState<T | null>(initialValue);
  const [lastUpdated] = useState<Date | null>(null);
  return { data, lastUpdated };
}