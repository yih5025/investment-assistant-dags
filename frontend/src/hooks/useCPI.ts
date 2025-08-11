import { useEffect, useState } from 'react';
import { ApiService } from '../services/api';

export interface CPIItem {
  date: string;
  value: number;
}

class CpiApi extends ApiService {
  async list(): Promise<CPIItem[]> {
    // GET /api/v1/cpi
    const res = await this.get<any>(`/cpi`);
    const items = Array.isArray(res?.data || res) ? (res.data || res) : [];
    return items.map((d: any) => ({
      date: d.date ?? d.period ?? d.observation_date,
      value: Number(d.value ?? d.cpi ?? d.index ?? 0),
    }));
  }
  async chart(): Promise<CPIItem[]> {
    const res = await this.get<any>(`/cpi/chart`);
    const points = res?.data?.points || res?.points || [];
    return points.map((p: any) => ({ date: p.timestamp ?? p.date, value: Number(p.value) }));
  }
}

const cpiApi = new CpiApi();

export function useCPI() {
  const [data, setData] = useState<CPIItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancel = false;
    (async () => {
      setLoading(true);
      try {
        const points = await cpiApi.chart();
        if (!cancel) setData(points);
      } catch (e: any) {
        if (!cancel) setError(e?.message || 'CPI 로드 실패');
      } finally {
        if (!cancel) setLoading(false);
      }
    })();
    return () => { cancel = true; };
  }, []);

  return { data, loading, error };
}


