import { useEffect, useState } from 'react';
import { useWebSocket } from './useWebSocket';

export interface TopGainer {
  symbol: string;
  name?: string;
  price?: number;
  changePercent?: number;
}

export function useTopGainersWS() {
  const { on, connected, connect } = useWebSocket(`${import.meta.env.VITE_WS_BASE_URL || ''}/stocks/topgainers`);
  const [data, setData] = useState<TopGainer[]>([]);

  useEffect(() => {
    connect();
  }, [connect]);

  useEffect(() => {
    const cleanup = on('message', (payload: any) => {
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
  }, [on]);

  return { data, connected };
}


