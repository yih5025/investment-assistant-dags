import { useEffect, useState } from 'react';
import { useNativeWebSocket } from './useNativeWebSocket';

export interface TopGainer {
  symbol: string;
  name?: string;
  price?: number;
  changePercent?: number;
}

export function useTopGainersWS() {
  // 상대 경로로 접속: 프론트 Nginx 프록시(/ws → /api/v1/ws) 경유
  const { onMessage, connected } = useNativeWebSocket('/ws/stocks/topgainers');
  const [data, setData] = useState<TopGainer[]>([]);

  useEffect(() => {
    const cleanup = onMessage((payload: any) => {
      try {
        const msg = typeof payload === 'string' ? JSON.parse(payload) : payload;
        const items = Array.isArray(msg?.data) ? msg.data : [];
        const mapped: TopGainer[] = items.map((it: any) => ({
          symbol: it.symbol ?? it.ticker,
          name: it.name,
          price: it.price ?? it.last,
          changePercent: it.changePercent ?? it.pct_change,
        }));
        setData(mapped);
      } catch {}
    });
    return cleanup;
  }, [onMessage]);

  return { data, connected };
}


