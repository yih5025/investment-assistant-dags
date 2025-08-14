import { useEffect, useMemo, useState } from "react";
import { apiUrl } from "../utils/api";

export interface EconomicIndicatorYearlyRow {
  year: number;          // 파생: period에서 추출한 연도
  period?: string;       // 'YYYY-MM' 형태의 월 키
  treasuryRate?: number; // 10Y yield (%)
  fedRate?: number;      // federal funds rate (%)
  cpi?: number;          // CPI index (pt)
  inflation?: number;    // yearly inflation (%), 월별 행에도 연도값을 반복 매핑
}

interface UseEconomicIndicatorsOptions {
  startYear?: number;
  endYear?: number;
}

type AnyRecord = Record<string, any>;

function parseDate(value: any): Date | null {
  if (!value) return null;
  try {
    // Support "YYYY-MM-DD" or ISO string
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
  } catch {
    return null;
  }
}

function pickNumber(obj: AnyRecord, fields: string[]): number | undefined {
  for (const key of fields) {
    const v = obj[key];
    if (typeof v === "number" && !Number.isNaN(v)) return v;
    if (typeof v === "string") {
      const n = Number(v);
      if (!Number.isNaN(n)) return n;
    }
  }
  return undefined;
}

function groupLastValuePerYear<T extends AnyRecord>(
  items: T[],
  dateFieldCandidates: string[],
  valueFieldCandidates: string[]
): Map<number, number> {
  const byYear = new Map<number, { ts: number; value: number }>();

  for (const item of items) {
    // find date
    let date: Date | null = null;
    for (const f of dateFieldCandidates) {
      date = parseDate(item[f]);
      if (date) break;
    }
    if (!date) continue;

    // find value
    const value = pickNumber(item, valueFieldCandidates);
    if (typeof value !== "number") continue;

    const year = date.getUTCFullYear();
    const ts = date.getTime();
    const prev = byYear.get(year);
    // keep the latest in the year (e.g., December or last available)
    if (!prev || ts > prev.ts) {
      byYear.set(year, { ts, value });
    }
  }

  const result = new Map<number, number>();
  for (const [year, entry] of byYear.entries()) {
    result.set(year, entry.value);
  }
  return result;
}

export function useEconomicIndicators(options: UseEconomicIndicatorsOptions = {}) {
  const startYear = options.startYear ?? 2014;
  const endYear = options.endYear ?? new Date().getUTCFullYear();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<EconomicIndicatorYearlyRow[]>([]);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);
    setError(null);

    async function fetchAll() {
      try {
    // 1) Treasury: 전체 기간 한 번에(monthly 저장 스키마 가정)
    const treasuryResPromise = fetch(apiUrl(`/treasury-yield/?maturity=10year&start_date=${startYear}-01-01&end_date=${endYear}-12-31&size=1000`));

    // 2) Fed/CPI: 전체 목록(정렬 파라미터 불필요)
    const fedResPromise = fetch(apiUrl(`/federal-funds-rate`));
    const cpiResPromise = fetch(apiUrl(`/cpi`));

    // 3) Inflation: 연도 범위 조회(연도별)
    const inflationResPromise = fetch(apiUrl(`/inflation/range?start_year=${startYear}&end_year=${endYear}`));

    const [treasuryRes, fedRes, cpiRes, inflationRes] = await Promise.all([
      treasuryResPromise,
      fedResPromise,
      cpiResPromise,
      inflationResPromise,
    ]);

    // Validate
    if (!treasuryRes.ok) throw new Error(`treasury ${treasuryRes.status}`);
    if (!fedRes.ok) throw new Error(`federal ${fedRes.status}`);
    if (!cpiRes.ok) throw new Error(`cpi ${cpiRes.status}`);
    if (!inflationRes.ok) throw new Error(`inflation ${inflationRes.status}`);

    const [treasuryJson, fedJson, cpiJson, inflationJson] = await Promise.all([
      treasuryRes.json(),
      fedRes.json(),
      cpiRes.json(),
      inflationRes.json(),
    ]);

        // Normalize arrays from possible shapes
        const treasuryItems: AnyRecord[] = Array.isArray(treasuryJson?.items)
          ? treasuryJson.items
          : Array.isArray(treasuryJson)
            ? treasuryJson
            : [];
        const fedItems: AnyRecord[] = Array.isArray(fedJson?.items)
          ? fedJson.items
          : Array.isArray(fedJson)
            ? fedJson
            : [];
        const cpiItems: AnyRecord[] = Array.isArray(cpiJson?.items)
          ? cpiJson.items
          : Array.isArray(cpiJson)
            ? cpiJson
            : [];
        const inflationItems: AnyRecord[] = Array.isArray(inflationJson?.items)
          ? inflationJson.items
          : Array.isArray(inflationJson)
            ? inflationJson
            : [];

        // Build monthly map keyed by 'YYYY-MM'
        const byMonth = new Map<string, EconomicIndicatorYearlyRow>();

        function monthKeyOf(d: Date) {
          const y = d.getUTCFullYear();
          const m = d.getUTCMonth() + 1;
          const mm = m < 10 ? `0${m}` : `${m}`;
          return `${y}-${mm}`;
        }

        // Treasury monthly
        for (const it of treasuryItems) {
          const dt = parseDate(it["date"] ?? it["as_of_date"] ?? it["recorded_at"]);
          if (!dt) continue;
          const key = monthKeyOf(dt);
          const year = dt.getUTCFullYear();
          const row = byMonth.get(key) ?? { year, period: key };
          row.treasuryRate = pickNumber(it, ["yield", "yield_value", "rate", "value"]);
          row.year = year;
          row.period = key;
          byMonth.set(key, row);
        }

        // Fed monthly
        for (const it of fedItems) {
          const dt = parseDate(it["date"] ?? it["month"] ?? it["as_of_date"] ?? it["recorded_at"]);
          if (!dt) continue;
          const key = monthKeyOf(dt);
          const year = dt.getUTCFullYear();
          const row = byMonth.get(key) ?? { year, period: key };
          row.fedRate = pickNumber(it, ["rate", "federal_funds_rate", "value"]);
          row.year = year;
          row.period = key;
          byMonth.set(key, row);
        }

        // CPI monthly
        for (const it of cpiItems) {
          const dt = parseDate(it["date"] ?? it["month"] ?? it["as_of_date"] ?? it["recorded_at"]);
          if (!dt) continue;
          const key = monthKeyOf(dt);
          const year = dt.getUTCFullYear();
          const row = byMonth.get(key) ?? { year, period: key };
          row.cpi = pickNumber(it, ["cpi_value", "value", "cpi"]);
          row.year = year;
          row.period = key;
          byMonth.set(key, row);
        }

        // Inflation yearly -> spread to months
        const inflationYearMap = new Map<number, number>();
        for (const it of inflationItems) {
          const y = pickNumber(it, ["year"]);
          const val = pickNumber(it, ["inflation", "inflation_rate", "value", "rate"]);
          if (typeof y === "number" && typeof val === "number") inflationYearMap.set(y, val);
        }

        for (const [key, row] of byMonth) {
          if (row.year != null && inflationYearMap.has(row.year)) {
            row.inflation = inflationYearMap.get(row.year);
          }
        }

        // Compose rows in ascending month order within range
        const composed: EconomicIndicatorYearlyRow[] = Array.from(byMonth.values())
          .filter(r => r.year >= startYear && r.year <= endYear)
          .sort((a, b) => (a.period! < b.period! ? -1 : a.period! > b.period! ? 1 : 0));

        // If empty, keep previous behavior
        if (isMounted) {
          setRows(composed);
          setLoading(false);
        }

        if (isMounted) {
          setRows(composed);
          setLoading(false);
        }
      } catch (e: any) {
        if (isMounted) {
          setError(e?.message ?? "failed to load indicators");
          setLoading(false);
        }
      }
    }

    fetchAll();
    return () => {
      isMounted = false;
    };
  }, [startYear, endYear]);

  const isEmpty = useMemo(() => rows.length === 0 || rows.every(r => r.treasuryRate == null && r.fedRate == null && r.cpi == null && r.inflation == null), [rows]);

  return { loading, error, rows, isEmpty } as const;
}


