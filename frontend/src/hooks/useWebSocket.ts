import { useState, useEffect, useRef, useCallback } from 'react';
import { io, Socket, SocketOptions } from 'socket.io-client';
import { WS_EVENTS } from '../utils/constants';
import { normalizeError } from '../utils/helpers';

export interface WebSocketState {
  connected: boolean;
  connecting: boolean;
  error: string | null;
  lastMessage: any;
  reconnectCount: number;
}

export interface UseWebSocketOptions extends SocketOptions {
  autoConnect?: boolean;
  reconnectAttempts?: number;
  reconnectDelay?: number;
}

// useWebSocket 훅
export function useWebSocket(
  url?: string,
  options: UseWebSocketOptions = {}
) {
  const {
    autoConnect = true,
    reconnectAttempts = 5,
    reconnectDelay = 1000,
    ...socketOptions
  } = options;

  const [state, setState] = useState<WebSocketState>({
    connected: false,
    connecting: false,
    error: null,
    lastMessage: null,
    reconnectCount: 0,
  });

  const socketRef = useRef<Socket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const eventListenersRef = useRef<Map<string, Function[]>>(new Map());

  // WebSocket URL 설정
  const wsUrl = url || import.meta.env.VITE_WS_BASE_URL || 'ws://localhost:8000';

  // 연결 함수
  const connect = useCallback(() => {
    if (socketRef.current?.connected) return;

    setState(prev => ({ ...prev, connecting: true, error: null }));

    try {
      const socket = io(wsUrl, {
        autoConnect: false,
        ...socketOptions,
      });

      socket.on(WS_EVENTS.CONNECT, () => {
        setState(prev => ({
          ...prev,
          connected: true,
          connecting: false,
          error: null,
          reconnectCount: 0,
        }));
      });

      socket.on(WS_EVENTS.DISCONNECT, (reason) => {
        setState(prev => ({
          ...prev,
          connected: false,
          connecting: false,
          error: `연결이 끊어졌습니다: ${reason}`,
        }));

        // 자동 재연결
        if (reason === 'io server disconnect') {
          // 서버에서 연결을 끊은 경우 수동으로 재연결
          handleReconnect();
        }
      });

      socket.on('connect_error', (error) => {
        setState(prev => ({
          ...prev,
          connected: false,
          connecting: false,
          error: normalizeError(error),
        }));

        handleReconnect();
      });

      // 등록된 이벤트 리스너들 재등록
      eventListenersRef.current.forEach((listeners, event) => {
        listeners.forEach(listener => {
          socket.on(event, listener);
        });
      });

      socket.connect();
      socketRef.current = socket;
    } catch (error) {
      setState(prev => ({
        ...prev,
        connecting: false,
        error: normalizeError(error),
      }));
    }
  }, [wsUrl, socketOptions]);

  // 재연결 처리
  const handleReconnect = useCallback(() => {
    if (state.reconnectCount >= reconnectAttempts) {
      setState(prev => ({
        ...prev,
        error: '최대 재연결 시도 횟수를 초과했습니다.',
      }));
      return;
    }

    setState(prev => ({ ...prev, reconnectCount: prev.reconnectCount + 1 }));

    reconnectTimeoutRef.current = setTimeout(() => {
      connect();
    }, reconnectDelay * Math.pow(2, state.reconnectCount)); // 지수 백오프
  }, [state.reconnectCount, reconnectAttempts, reconnectDelay, connect]);

  // 연결 해제 함수
  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }

    if (socketRef.current) {
      socketRef.current.disconnect();
      socketRef.current = null;
    }

    setState({
      connected: false,
      connecting: false,
      error: null,
      lastMessage: null,
      reconnectCount: 0,
    });
  }, []);

  // 메시지 전송 함수
  const emit = useCallback((event: string, data?: any) => {
    if (socketRef.current?.connected) {
      socketRef.current.emit(event, data);
      return true;
    }
    return false;
  }, []);

  // 이벤트 리스너 등록
  const on = useCallback((event: string, listener: Function) => {
    // 리스너 목록에 추가
    const listeners = eventListenersRef.current.get(event) || [];
    listeners.push(listener);
    eventListenersRef.current.set(event, listeners);

    // 소켓이 연결되어 있으면 즉시 등록
    if (socketRef.current) {
      socketRef.current.on(event, listener);
    }

    // cleanup 함수 반환
    return () => {
      const updatedListeners = eventListenersRef.current.get(event) || [];
      const index = updatedListeners.indexOf(listener);
      if (index > -1) {
        updatedListeners.splice(index, 1);
        eventListenersRef.current.set(event, updatedListeners);
      }

      if (socketRef.current) {
        socketRef.current.off(event, listener);
      }
    };
  }, []);

  // 이벤트 리스너 제거
  const off = useCallback((event: string, listener?: Function) => {
    if (listener) {
      const listeners = eventListenersRef.current.get(event) || [];
      const index = listeners.indexOf(listener);
      if (index > -1) {
        listeners.splice(index, 1);
        eventListenersRef.current.set(event, listeners);
      }

      if (socketRef.current) {
        socketRef.current.off(event, listener);
      }
    } else {
      eventListenersRef.current.delete(event);
      if (socketRef.current) {
        socketRef.current.off(event);
      }
    }
  }, []);

  // 컴포넌트 마운트/언마운트 처리
  useEffect(() => {
    if (autoConnect) {
      connect();
    }

    return () => {
      disconnect();
    };
  }, [autoConnect, connect, disconnect]);

  return {
    ...state,
    connect,
    disconnect,
    emit,
    on,
    off,
  };
}

// 특정 이벤트에 대한 간단한 훅
export function useWebSocketEvent<T>(
  event: string,
  handler: (data: T) => void,
  dependencies: any[] = []
) {
  const { on } = useWebSocket();

  useEffect(() => {
    const cleanup = on(event, handler);
    return cleanup;
  }, [event, on, ...dependencies]);
}

// 실시간 데이터 구독 훅
export function useWebSocketSubscription<T>(
  event: string,
  initialValue: T | null = null
) {
  const [data, setData] = useState<T | null>(initialValue);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const { on } = useWebSocket();

  useEffect(() => {
    const cleanup = on(event, (newData: T) => {
      setData(newData);
      setLastUpdated(new Date());
    });

    return cleanup;
  }, [event, on]);

  return {
    data,
    lastUpdated,
  };
}