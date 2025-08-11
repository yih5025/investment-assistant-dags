import { useEffect, useState } from 'react';
import { ApiService } from '../services/api';

export interface EarningsItem {
  symbol: string;
  reportDate: string;
  companyName?: string;
  epsActual?: number;
  epsEstimate?: number;
}

class EarningsApi extends ApiService {
  async today(): Promise<EarningsItem[]> {
    const res = await this.get<any>(`/earnings-calendar/today`);
    const items = Array.isArray(res?.data || res) ? (res.data || res) : [];
    return items.map((it: any) => ({
      symbol: it.symbol ?? it.ticker,
      reportDate: it.report_date ?? it.date,
      companyName: it.company_name ?? it.name,
      epsActual: it.eps_actual ?? it.epsActual,
      epsEstimate: it.eps_estimate ?? it.epsEstimate,
    }));
  }
}

const earningsApi = new EarningsApi();

export function useEarningsCalendar() {
  const [data, setData] = useState<EarningsItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancel = false;
    (async () => {
      setLoading(true);
      try {
        const rows = await earningsApi.today();
        if (!cancel) setData(rows);
      } catch (e: any) {
        if (!cancel) setError(e?.message || '실적 캘린더 로드 실패');
      } finally {
        if (!cancel) setLoading(false);
      }
    })();
    return () => { cancel = true; };
  }, []);

  return { data, loading, error };
}


