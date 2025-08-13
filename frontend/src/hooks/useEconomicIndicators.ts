import { useEffect, useMemo, useState } from "react";

export interface EconomicIndicatorYearlyRow {
  year: number;
  treasuryRate?: number; // 10Y yield (%)
  fedRate?: number;      // federal funds rate (%)
  cpi?: number;          // CPI index (pt)
  inflation?: number;    // yearly inflation (%)
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
        // 1) Treasury: 연도별 범위 요청(연말 값 확보 목적)
        const treasuryYearPromises: Promise<Response>[] = [];
        for (let y = startYear; y <= endYear; y += 1) {
          const url = `/api/v1/treasury-yield?maturity=10year&start_date=${y}-01-01&end_date=${y}-12-31&size=1000`;
          treasuryYearPromises.push(fetch(url));
        }

        // 2) Fed/CPI: 전체목록 asc 정렬로 받아서 연말 값 선택
        const fedResPromise = fetch(`/api/v1/federal-funds-rate?order_by=asc`);
        const cpiResPromise = fetch(`/api/v1/cpi?order_by=asc`);

        // 3) Inflation: 연도 범위 조회
        const inflationResPromise = fetch(`/api/v1/inflation/range?start_year=${startYear}&end_year=${endYear}`);

        const [treasuryResponses, fedRes, cpiRes, inflationRes] = await Promise.all([
          Promise.all(treasuryYearPromises),
          fedResPromise,
          cpiResPromise,
          inflationResPromise,
        ]);

        // Validate
        for (const [idx, r] of treasuryResponses.entries()) {
          if (!r.ok) throw new Error(`treasury[${startYear + idx}] ${r.status}`);
        }
        if (!fedRes.ok) throw new Error(`federal ${fedRes.status}`);
        if (!cpiRes.ok) throw new Error(`cpi ${cpiRes.status}`);
        if (!inflationRes.ok) throw new Error(`inflation ${inflationRes.status}`);

        const [treasuryJsonList, fedJson, cpiJson, inflationJson] = await Promise.all([
          Promise.all(treasuryResponses.map(r => r.json())),
          fedRes.json(),
          cpiRes.json(),
          inflationRes.json(),
        ]);

        // Normalize arrays from possible shapes
        // 연도별 응답을 하나의 배열로 평탄화
        const treasuryItems: AnyRecord[] = treasuryJsonList.flatMap((tj: any) =>
          Array.isArray(tj?.items) ? tj.items : Array.isArray(tj) ? tj : []
        );
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

        // Group by year with robust field selection
        const treasuryByYear = groupLastValuePerYear(
          treasuryItems,
          ["date", "as_of_date", "recorded_at"],
          ["yield", "yield_value", "rate", "value"]
        );

        const fedByYear = groupLastValuePerYear(
          fedItems,
          ["date", "month", "as_of_date", "recorded_at"],
          ["rate", "federal_funds_rate", "value"]
        );

        const cpiByYear = groupLastValuePerYear(
          cpiItems,
          ["date", "month", "as_of_date", "recorded_at"],
          ["cpi_value", "value", "cpi"]
        );

        // Inflation expected yearly already; accept year/value varieties
        const inflationByYear = new Map<number, number>();
        for (const it of inflationItems) {
          const year = pickNumber(it, ["year"])?.valueOf();
          const val = pickNumber(it, ["inflation", "inflation_rate", "value", "rate"]);
          if (typeof year === "number" && typeof val === "number") {
            inflationByYear.set(year, val);
          }
        }

        // Compose final rows for full year range
        const composed: EconomicIndicatorYearlyRow[] = [];
        for (let y = startYear; y <= endYear; y += 1) {
          composed.push({
            year: y,
            treasuryRate: treasuryByYear.get(y),
            fedRate: fedByYear.get(y),
            cpi: cpiByYear.get(y),
            inflation: inflationByYear.get(y),
          });
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


