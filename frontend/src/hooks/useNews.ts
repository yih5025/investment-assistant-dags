import { useEffect, useState } from 'react';
import { marketNewsApi, type MarketNewsItem } from '../services/marketNewsApi';

export function useNews() {
  const [data, setData] = useState<MarketNewsItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const items = await marketNewsApi.getRecent();
        if (!cancelled) setData(items);
      } catch (e: any) {
        if (!cancelled) setError(e?.message || '뉴스 로드 실패');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  return { data, loading, error };
}


