import { useCallback, useEffect, useRef, useState } from 'react';

interface NativeWsState {
  connected: boolean;
  connecting: boolean;
  error: string | null;
}

export function useNativeWebSocket(pathOrUrl: string, autoConnect: boolean = true) {
  const [state, setState] = useState<NativeWsState>({ connected: false, connecting: false, error: null });
  const wsRef = useRef<WebSocket | null>(null);
  const listenersRef = useRef<Array<(data: any) => void>>([]);

  const resolveUrl = useCallback(() => {
    // if absolute ws url provided use as is; else treat as relative path under current host
    if (/^wss?:\/\//i.test(pathOrUrl)) return pathOrUrl;
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const host = window.location.host; // includes port
    const base = pathOrUrl.startsWith('/') ? pathOrUrl : `/${pathOrUrl}`;
    return `${protocol}://${host}${base}`;
  }, [pathOrUrl]);

  const connect = useCallback(() => {
    if (wsRef.current && (wsRef.current.readyState === WebSocket.OPEN || wsRef.current.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const url = resolveUrl();
    setState({ connected: false, connecting: true, error: null });
    try {
      const ws = new WebSocket(url);
      ws.onopen = () => setState({ connected: true, connecting: false, error: null });
      ws.onerror = () => setState(prev => ({ ...prev, error: '웹소켓 오류' }));
      ws.onclose = () => setState({ connected: false, connecting: false, error: null });
      ws.onmessage = (ev) => {
        let payload: any = ev.data;
        try { payload = JSON.parse(ev.data); } catch {}
        listenersRef.current.forEach(fn => fn(payload));
      };
      wsRef.current = ws;
    } catch (e: any) {
      setState({ connected: false, connecting: false, error: e?.message || '연결 실패' });
    }
  }, [resolveUrl]);

  const disconnect = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
  }, []);

  const onMessage = useCallback((fn: (data: any) => void) => {
    listenersRef.current.push(fn);
    return () => {
      listenersRef.current = listenersRef.current.filter(f => f !== fn);
    };
  }, []);

  useEffect(() => {
    if (autoConnect) connect();
    return () => disconnect();
  }, [autoConnect, connect, disconnect]);

  return { ...state, connect, disconnect, onMessage };
}


