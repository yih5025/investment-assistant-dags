import { ApiService } from './api';

export interface MarketNewsItem {
  id?: string;
  title: string;
  summary?: string;
  source?: string;
  url?: string;
  publishedAt?: string;
  category?: string;
}

export class MarketNewsApiService extends ApiService {
  async getRecent(): Promise<MarketNewsItem[]> {
    // backend: GET /api/v1/market-news/recent
    const res = await this.get<any>(`/market-news/recent`);
    // Normalize best-effort to MarketNewsItem[]
    const items: MarketNewsItem[] = Array.isArray(res?.data || res)
      ? (res.data || res).map((n: any) => ({
          id: n.id ?? n.news_id ?? n.url,
          title: n.title ?? n.headline ?? '',
          summary: n.summary ?? n.description ?? '',
          source: n.source ?? n.publisher ?? n.provider,
          url: n.url ?? n.link,
          publishedAt: n.published_at ?? n.publishedAt ?? n.time_published,
          category: n.category,
        }))
      : [];
    return items;
  }
}

export const marketNewsApi = new MarketNewsApiService();


