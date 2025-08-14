import { useMemo, useState, useEffect } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { TrendingUp, TrendingDown, Info, BarChart3, GitCompare, Lock, TestTube, CheckCircle, XCircle, RefreshCw } from "lucide-react";

interface EconomicDashboardProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

interface EconomicIndicator {
  year: number;
  treasuryRate: number;
  fedRate: number;
  cpi: number;
  inflation: number;
}

// 백엔드 API 응답 타입 정
interface APIResponse<T> {
  total_count?: number;
  items: T[];
}

interface FederalFundsRateData {
  date: string;
  rate: number;
}

interface InflationData {
  date: string;
  inflation_rate: number;
}

interface CPIData {
  date: string;
  cpi_value: number;
}

interface TreasuryYieldData {
  date: string;
  yield?: number;
  yield_value?: number;
  rate?: number;
  value?: number;
  maturity: string;
}

// API 테스트 결과 타입
interface APITestResult {
  name: string;
  url: string;
  status: 'loading' | 'success' | 'error';
  statusCode?: number;
  data?: any;
  error?: string;
  responseTime?: number;
}

// 통합 경제 지표 데이터 타입
interface EconomicIndicatorRow {
  year: number;
  period: string; // 'YYYY-MM'
  treasuryRate?: number;
  fedRate?: number;
  cpi?: number;
  inflation?: number;
}

export function EconomicDashboard({ isLoggedIn, onLoginPrompt }: EconomicDashboardProps) {
  const [selectedIndicator, setSelectedIndicator] = useState<string>("treasuryRate");
  const [correlationMode, setCorrelationMode] = useState<boolean>(false);
  const [correlationPair, setCorrelationPair] = useState<{first: string, second: string}>({
    first: "fedRate",
    second: "treasuryRate"
  });

  // API 테스트 관련 상태
  const [showAPITest, setShowAPITest] = useState<boolean>(false);
  const [apiTestResults, setApiTestResults] = useState<APITestResult[]>([]);
  const [isTestRunning, setIsTestRunning] = useState<boolean>(false);

  // 데이터 상태
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [economicData, setEconomicData] = useState<EconomicIndicatorRow[]>([]);

  // API 기본 URL: 환경변수 우선, 없으면 K8s 프론트 Nginx 프록시(/api) 사용
  const API_BASE_URL = (import.meta as any)?.env?.VITE_API_BASE || '/api';

  // 숫자 값 추출 함수 (여러 필드명 대응)
  const extractNumber = (item: any, fields: string[]): number | undefined => {
    for (const field of fields) {
      const value = item[field];
      if (typeof value === 'number' && !isNaN(value)) return value;
      if (typeof value === 'string') {
        const parsed = parseFloat(value);
        if (!isNaN(parsed)) return parsed;
      }
    }
    return undefined;
  };

  // 날짜를 YYYY-MM 형식으로 변환하는 함수
  const formatPeriod = (dateStr: string): string => {
    const date = new Date(dateStr);
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    return `${year}-${month}`;
  };

  // 여러 API 데이터를 월별로 병합하는 함수
  const combineEconomicData = (
    fedData: FederalFundsRateData[],
    inflationData: InflationData[],
    cpiData: CPIData[],
    treasuryData: TreasuryYieldData[]
  ): EconomicIndicatorRow[] => {
    const monthlyData = new Map<string, EconomicIndicatorRow>();

    // 연방기금금리 데이터 처리
    fedData.forEach(item => {
      if (item.date && typeof item.rate === 'number') {
        const period = formatPeriod(item.date);
        const year = parseInt(period.split('-')[0]);
        
        if (!monthlyData.has(period)) {
          monthlyData.set(period, { year, period });
        }
        
        const existing = monthlyData.get(period)!;
        existing.fedRate = item.rate;
      }
    });

    // 국채수익률 데이터 처리 (10년 국채)
    treasuryData.forEach(item => {
      if (item.date) {
        const period = formatPeriod(item.date);
        const year = parseInt(period.split('-')[0]);
        
        const yieldValue = extractNumber(item, ['yield', 'yield_value', 'rate', 'value']);
        
        if (yieldValue !== undefined) {
          if (!monthlyData.has(period)) {
            monthlyData.set(period, { year, period });
          }
          
          const existing = monthlyData.get(period)!;
          existing.treasuryRate = yieldValue;
        }
      }
    });

    // 인플레이션 데이터 처리 (연도별 데이터를 월별로 분배)
    inflationData.forEach(item => {
      if (item.date && typeof item.inflation_rate === 'number') {
        const date = new Date(item.date);
        const year = date.getFullYear();
        
        for (let month = 1; month <= 12; month++) {
          const period = `${year}-${month.toString().padStart(2, '0')}`;
          
          if (!monthlyData.has(period)) {
            monthlyData.set(period, { year, period });
          }
          
          const existing = monthlyData.get(period)!;
          existing.inflation = item.inflation_rate;
        }
      }
    });

    // CPI 데이터 처리
    cpiData.forEach(item => {
      if (item.date && typeof item.cpi_value === 'number') {
        const period = formatPeriod(item.date);
        const year = parseInt(period.split('-')[0]);
        
        if (!monthlyData.has(period)) {
          monthlyData.set(period, { year, period });
        }
        
        const existing = monthlyData.get(period)!;
        existing.cpi = item.cpi_value;
      }
    });

    return Array.from(monthlyData.values())
      .filter(item => item.year >= 2014)
      .sort((a, b) => a.period.localeCompare(b.period));
  };

  // 안전한 JSON 파서 (HTML/빈 응답 방지)
  const parseJsonSafe = async (response: Response, url: string) => {
    try {
      const contentType = response.headers.get('content-type') || '';
      if (!contentType.includes('application/json')) {
        const text = await response.text();
        throw new Error(`Unexpected content-type for ${url}: ${contentType} (snippet: ${text.slice(0, 120)})`);
      }
      return await response.json();
    } catch (e: any) {
      throw new Error(e?.message || `Failed to parse JSON from ${url}`);
    }
  };

  // 백엔드 API에서 데이터 가져오는 함수
  const fetchEconomicData = async () => {
    try {
      setLoading(true);
      setError(null);

      console.log("📊 경제 데이터 로딩 시작...");

      const noCacheInit: RequestInit = { method: 'GET', cache: 'no-store', headers: { 'Accept': 'application/json' } };
      const [fedResponse, inflationResponse, cpiResponse, treasuryResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/federal-funds-rate/`, noCacheInit),
        fetch(`${API_BASE_URL}/inflation/`, noCacheInit),
        fetch(`${API_BASE_URL}/cpi/`, noCacheInit),
        fetch(`${API_BASE_URL}/treasury-yield/`, noCacheInit)
      ]);

      if (!fedResponse.ok) throw new Error(`연방기금금리 API 오류: ${fedResponse.status}`);
      if (!inflationResponse.ok) throw new Error(`인플레이션 API 오류: ${inflationResponse.status}`);
      if (!cpiResponse.ok) throw new Error(`CPI API 오류: ${cpiResponse.status}`);
      if (!treasuryResponse.ok) throw new Error(`국채수익률 API 오류: ${treasuryResponse.status}`);

      const [fedData, inflationData, cpiData, treasuryData] = await Promise.all([
        parseJsonSafe(fedResponse, `${API_BASE_URL}/federal-funds-rate/`) as Promise<APIResponse<FederalFundsRateData>>,
        parseJsonSafe(inflationResponse, `${API_BASE_URL}/inflation/`) as Promise<APIResponse<InflationData>>,
        parseJsonSafe(cpiResponse, `${API_BASE_URL}/cpi/`) as Promise<APIResponse<CPIData>>,
        parseJsonSafe(treasuryResponse, `${API_BASE_URL}/treasury-yield/`) as Promise<APIResponse<TreasuryYieldData>>
      ]);

      console.log("✅ API 응답 받음:", {
        fed: fedData.items?.length || 0,
        inflation: inflationData.items?.length || 0,
        cpi: cpiData.items?.length || 0,
        treasury: treasuryData.items?.length || 0
      });

      const combinedData = combineEconomicData(
        fedData.items || [],
        inflationData.items || [],
        cpiData.items || [],
        treasuryData.items || []
      );

      console.log("🔄 데이터 변환 완료:", combinedData.length + "개 항목");
      setEconomicData(combinedData);

    } catch (err: any) {
      console.error("❌ 경제 데이터 로딩 실패:", err);
      setError(err.message || '데이터를 불러올 수 없습니다');
    } finally {
      setLoading(false);
    }
  };

  // API 테스트 함수
  const runAPITest = async () => {
    setIsTestRunning(true);
    setShowAPITest(true);
    
    const tests: Omit<APITestResult, 'status'>[] = [
      {
        name: "프론트 헬스",
        url: `/health`
      },
      {
        name: "연방기금금리",
        url: `${API_BASE_URL}/federal-funds-rate/`
      },
      {
        name: "인플레이션",
        url: `${API_BASE_URL}/inflation/`
      },
      {
        name: "소비자물가지수",
        url: `${API_BASE_URL}/cpi/`
      },
      {
        name: "국채수익률",
        url: `${API_BASE_URL}/treasury-yield/?maturity=10year&size=5`
      }
    ];

    const results: APITestResult[] = tests.map(test => ({ ...test, status: 'loading' }));
    setApiTestResults([...results]);

    for (let i = 0; i < tests.length; i++) {
      const test = tests[i];
      const startTime = Date.now();
      
      try {
        const response = await fetch(test.url, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          }
        });
        
        const responseTime = Date.now() - startTime;
        let data;
        const cloned = response.clone();
        try {
          data = await response.json();
        } catch (parseError) {
          data = await cloned.text();
        }
        
        results[i] = {
          ...test,
          status: response.ok ? 'success' : 'error',
          statusCode: response.status,
          data: data,
          responseTime
        };
        
      } catch (error: any) {
        const responseTime = Date.now() - startTime;
        results[i] = {
          ...test,
          status: 'error',
          error: error.message,
          responseTime
        };
      }
      
      setApiTestResults([...results]);
      
      if (i < tests.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    
    setIsTestRunning(false);
  };

  // 컴포넌트 마운트 시 데이터 로드
  useEffect(() => {
    fetchEconomicData();
  }, []);

  // 선택 지표의 최신값과 전년 동월(YoY) 값 추출
  const getLatestPair = (key: keyof EconomicIndicator): { value?: number; prev?: number } => {
    const available = economicData.filter(r => (r as any)[key] != null && r.period);
    if (available.length === 0) return { value: undefined, prev: undefined };
    
    const last = available[available.length - 1];
    const period = last.period as string;
    const [yy, mm] = period.split('-').map(n => parseInt(n, 10));
    const prevPeriod = `${yy - 1}-${mm < 10 ? `0${mm}` : `${mm}`}`;
    
    const periodMap = new Map(economicData.filter(r => r.period).map(r => [r.period as string, r]));
    const prevRow = periodMap.get(prevPeriod);
    
    return {
      value: (last as any)[key] as number | undefined,
      prev: prevRow ? ((prevRow as any)[key] as number | undefined) : undefined,
    };
  };

  const indicators = [
    {
      key: "treasuryRate",
      name: "미국 국채 10년",
      unit: "%",
      color: "#3b82f6",
      description: "장기 금리 동향을 나타내는 핵심 지표",
      impact: "높아지면 대출금리 상승, 주식보다 채권 매력도 증가"
    },
    {
      key: "fedRate",
      name: "연준 기준금리",
      unit: "%",
      color: "#10b981",
      description: "미국 연방준비제도의 기준 금리",
      impact: "경기 과열시 올리고, 침체시 낮춰서 경기 조절"
    },
    {
      key: "inflation",
      name: "인플레이션율",
      unit: "%",
      color: "#f59e0b",
      description: "물가 상승률을 나타내는 지표",
      impact: "높아지면 돈의 가치 하락, 연준이 금리 인상 고려",
      premium: !isLoggedIn
    },
    {
      key: "cpi",
      name: "소비자물가지수",
      unit: "pt",
      color: "#8b5cf6",
      description: "소비자가 구매하는 상품과 서비스의 가격 수준",
      impact: "CPI 상승률이 인플레이션율과 직결됨",
      premium: !isLoggedIn
    }
  ];

  const correlationPairs = [
    { first: "fedRate", second: "treasuryRate", description: "연준 금리와 국채 수익률은 강한 양의 상관관계" },
    { first: "inflation", second: "fedRate", description: "인플레이션 상승 시 연준이 금리를 올리는 패턴", premium: !isLoggedIn },
    { first: "cpi", second: "inflation", description: "CPI 변화율이 인플레이션을 직접 반영", premium: !isLoggedIn },
    { first: "treasuryRate", second: "inflation", description: "인플레이션 기대가 장기 금리에 반영됨", premium: !isLoggedIn }
  ];

  const currentIndicator = indicators.find(ind => ind.key === selectedIndicator);
  const latestPair = getLatestPair(selectedIndicator as keyof EconomicIndicator);
  const currentValue = latestPair.value ?? 0;
  const previousValue = latestPair.prev ?? 0;
  const hasPrev = latestPair.prev != null;
  const change = hasPrev ? currentValue - (previousValue as number) : 0;
  const changePercent = hasPrev && previousValue !== 0 ? ((change / (previousValue as number)) * 100) : 0;

  // 차트 데이터: 선택 지표가 있는 연도만
  const chartData = useMemo(() => {
    return economicData
      .filter((r) => (r as any)[selectedIndicator] != null)
      .map((r) => ({
        year: r.year,
        period: r.period,
        treasuryRate: r.treasuryRate,
        fedRate: r.fedRate,
        cpi: r.cpi,
        inflation: r.inflation,
      }));
  }, [economicData, selectedIndicator]);

  const formatTooltipValue = (value: any, name: string) => {
    const indicator = indicators.find(ind => ind.key === name);
    return [`${value}${indicator?.unit || ""}`, indicator?.name || name];
  };

  const handleIndicatorClick = (indicator: any) => {
    if (indicator.premium) {
      onLoginPrompt();
      return;
    }
    setSelectedIndicator(indicator.key);
  };

  // API 테스트 결과 렌더링
  const renderAPITestResult = (result: APITestResult) => {
    const getStatusIcon = () => {
      switch (result.status) {
        case 'loading':
          return <div className="w-4 h-4 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />;
        case 'success':
          return <CheckCircle size={16} className="text-green-400" />;
        case 'error':
          return <XCircle size={16} className="text-red-400" />;
      }
    };

    const getStatusColor = () => {
      switch (result.status) {
        case 'loading': return 'text-blue-400';
        case 'success': return 'text-green-400';
        case 'error': return 'text-red-400';
      }
    };

    return (
      <div key={result.name} className="glass rounded-lg p-3">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center space-x-2">
            {getStatusIcon()}
            <span className="font-medium">{result.name}</span>
          </div>
          {result.responseTime && (
            <span className="text-xs text-foreground/60">{result.responseTime}ms</span>
          )}
        </div>
        
        <div className="text-xs text-foreground/70 mb-1">
          {result.url}
        </div>
        
        {result.status === 'success' && (
          <div className={`text-xs ${getStatusColor()}`}>
            ✅ HTTP {result.statusCode} - 
            {result.data && typeof result.data === 'object' && result.data.total_count 
              ? ` ${result.data.total_count}개 데이터` 
              : result.data && typeof result.data === 'object' && result.data.items?.length
              ? ` ${result.data.items.length}개 데이터`
              : ' 응답 성공'
            }
          </div>
        )}
        
        {result.status === 'error' && (
          <div className={`text-xs ${getStatusColor()}`}>
            ❌ {result.statusCode ? `HTTP ${result.statusCode}` : '네트워크 오류'} - {result.error}
          </div>
        )}
      </div>
    );
  };

  // 에러 상태 처리
  if (error) {
    return (
      <div className="space-y-6">
        <div className="glass-card rounded-2xl p-6">
          <div className="text-center">
            <XCircle className="mx-auto mb-4 text-red-400" size={48} />
            <h3 className="font-semibold mb-2 text-red-400">데이터 로딩 실패</h3>
            <p className="text-sm text-foreground/70 mb-4">{error}</p>
            <div className="flex justify-center space-x-3">
              <button 
                onClick={fetchEconomicData}
                className="px-4 py-2 bg-primary/20 text-primary rounded-xl hover:bg-primary/30 transition-colors"
              >
                다시 시도
              </button>
              <button 
                onClick={runAPITest}
                className="px-4 py-2 bg-blue-500/20 text-blue-400 rounded-xl hover:bg-blue-500/30 transition-colors"
              >
                🧪 연결 테스트
              </button>
            </div>
          </div>
        </div>

        {showAPITest && (
          <div className="glass-card rounded-2xl p-6">
            <div className="flex items-center space-x-2 mb-4">
              <TestTube size={20} className="text-blue-400" />
              <h3 className="font-semibold">API 연결 테스트 결과</h3>
              {isTestRunning && <div className="text-sm text-blue-400">진행 중...</div>}
            </div>
            
            <div className="space-y-3">
              {apiTestResults.length > 0 ? (
                apiTestResults.map(renderAPITestResult)
              ) : (
                <div className="text-sm text-foreground/60">테스트를 실행하려면 '🧪 연결 테스트' 버튼을 클릭하세요.</div>
              )}
            </div>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="glass-card rounded-2xl p-6">
        <h2 className="text-lg font-semibold mb-4 flex items-center">
          <BarChart3 className="mr-2" size={20} />
          📊 경제 지표 대시보드
          {loading && <div className="ml-2 text-sm text-blue-400">데이터 로딩 중...</div>}
        </h2>
        
        {/* API 테스트 및 새로고침 버튼 */}
        <div className="flex items-center space-x-4 mb-4">
          <button
            onClick={runAPITest}
            disabled={isTestRunning}
            className={`flex items-center space-x-2 px-4 py-2 rounded-xl font-medium transition-colors ${
              isTestRunning 
                ? 'bg-gray-500/20 text-gray-400 cursor-not-allowed'
                : 'bg-blue-500/20 text-blue-400 hover:bg-blue-500/30'
            }`}
          >
            <TestTube size={16} />
            <span>{isTestRunning ? '테스트 중...' : '🧪 API 연결 테스트'}</span>
          </button>

          <button
            onClick={fetchEconomicData}
            disabled={loading}
            className="flex items-center space-x-2 px-4 py-2 bg-green-500/20 text-green-400 rounded-xl font-medium hover:bg-green-500/30 transition-colors disabled:opacity-50"
          >
            <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
            <span>데이터 새로고침</span>
          </button>
          
          {showAPITest && (
            <button
              onClick={() => setShowAPITest(false)}
              className="px-3 py-2 text-sm text-foreground/60 hover:text-foreground/80"
            >
              테스트 결과 숨기기
            </button>
          )}
        </div>
        
        <p className="text-sm text-foreground/70">
          미국의 주요 경제 지표들의 실시간 데이터를 확인하고, 각 지표들 간의 상관관계를 이해해보세요.
          {economicData.length > 0 && (
            <span className="ml-2 text-primary">
              • {economicData.length}개 데이터 포인트 로딩됨
            </span>
          )}
        </p>
        
        {!isLoggedIn && (
          <div className="mt-4 p-3 glass rounded-xl border border-primary/30">
            <div className="flex items-center space-x-2">
              <Lock className="text-primary" size={16} />
              <p className="text-sm">
                <span className="font-medium">로그인하면</span> 모든 경제 지표와 상관관계 분석을 이용할 수 있어요.
              </p>
              <button
                onClick={onLoginPrompt}
                className="px-3 py-1 bg-primary/20 text-primary rounded-lg text-sm hover:bg-primary/30 transition-colors"
              >
                로그인
              </button>
            </div>
          </div>
        )}
      </div>

      {/* API 테스트 결과 */}
      {showAPITest && (
        <div className="glass-card rounded-2xl p-6">
          <div className="flex items-center space-x-2 mb-4">
            <TestTube size={20} className="text-blue-400" />
            <h3 className="font-semibold">API 연결 테스트 결과</h3>
            {isTestRunning && <div className="text-sm text-blue-400">진행 중...</div>}
          </div>
          
          <div className="space-y-3">
            {apiTestResults.length > 0 ? (
              apiTestResults.map(renderAPITestResult)
            ) : (
              <div className="text-sm text-foreground/60">테스트를 실행하려면 '🧪 API 연결 테스트' 버튼을 클릭하세요.</div>
            )}
          </div>
        </div>
      )}

      {/* 차트 모드 선택 */}
      <div className="flex space-x-3">
        <button
          onClick={() => setCorrelationMode(false)}
          className={`flex-1 glass rounded-xl p-3 transition-all ${
            !correlationMode ? "bg-primary/20 border border-primary/30" : "hover:bg-white/10"
          }`}
        >
          <div className="flex items-center justify-center space-x-2">
            <BarChart3 size={16} />
            <span className="text-sm font-medium">개별 지표</span>
          </div>
        </button>
        <button
          onClick={() => setCorrelationMode(true)}
          className={`flex-1 glass rounded-xl p-3 transition-all ${
            correlationMode ? "bg-primary/20 border border-primary/30" : "hover:bg-white/10"
          }`}
        >
          <div className="flex items-center justify-center space-x-2">
            <GitCompare size={16} />
            <span className="text-sm font-medium">상관관계</span>
            {!isLoggedIn && <Lock size={12} className="text-yellow-400" />}
          </div>
        </button>
      </div>

      {!correlationMode ? (
        <>
          {/* 지표 선택 */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {indicators.map((indicator) => {
              const isSelected = selectedIndicator === indicator.key;
              const pair = getLatestPair(indicator.key as keyof EconomicIndicator);
              const value = pair.value;
              const prevValue = pair.prev;
              const indicatorChange = value != null && prevValue != null ? value - prevValue : 0;
              
              return (
                <button
                  key={indicator.key}
                  onClick={() => handleIndicatorClick(indicator)}
                  className={`glass-card rounded-xl p-4 text-left transition-all relative ${
                    isSelected ? "bg-primary/20 border border-primary/30" : "hover:bg-white/10"
                  } ${indicator.premium ? "cursor-pointer" : ""}`}
                >
                  {indicator.premium && (
                    <div className="absolute top-2 right-2">
                      <Lock size={12} className="text-yellow-400" />
                    </div>
                  )}
                  
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium">{indicator.name}</span>
                    <div className={`flex items-center text-xs ${
                      indicatorChange >= 0 ? "text-green-400" : "text-red-400"
                    }`}>
                      {indicatorChange >= 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                    </div>
                  </div>
                  <div className="text-lg font-semibold" style={{ color: indicator.color }}>
                    {indicator.premium ? "••••" : (value != null ? `${value.toFixed(2)}${indicator.unit}` : "--")}
                  </div>
                  <div className={`text-xs ${
                    indicator.premium ? "text-foreground/40" : value != null && prevValue != null ? (indicatorChange >= 0 ? "text-green-400" : "text-red-400") : "text-foreground/40"
                  }`}>
                    {indicator.premium ? "로그인 필요" : value != null && prevValue != null ?
                      `${indicatorChange >= 0 ? "+" : ""}${indicatorChange.toFixed(2)}${indicator.unit}` : "데이터 없음"}
                  </div>
                </button>
              );
            })}
          </div>

          {/* 선택된 지표 상세 정보 */}
          {currentIndicator && !currentIndicator.premium && (
            <div className="glass-card rounded-2xl p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold" style={{ color: currentIndicator.color }}>
                  {currentIndicator.name}
                </h3>
                <div className="text-right">
                  <div className="text-2xl font-bold">
                    {loading ? "…" : `${currentValue.toFixed(2)}${currentIndicator.unit}`}
                  </div>
                  <div className={`text-sm flex items-center justify-end ${
                    hasPrev ? (change >= 0 ? "text-green-400" : "text-red-400") : "text-foreground/60"
                  }`}>
                    {hasPrev ? (change >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />) : null}
                    <span className="ml-1">
                      {hasPrev ? `${change >= 0 ? "+" : ""}${change.toFixed(2)}${currentIndicator.unit} (${changePercent >= 0 ? "+" : ""}${changePercent.toFixed(1)}%)` : "이전 데이터 없음"}
                    </span>
                  </div>
                </div>
              </div>

              <div className="glass rounded-xl p-4 mb-4">
                <div className="flex items-start space-x-2">
                  <Info size={16} className="text-blue-400 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium mb-1">이 지표는?</p>
                    <p className="text-xs text-foreground/70 mb-2">{currentIndicator.description}</p>
                    <p className="text-xs text-foreground/60">
                      <span className="font-medium">시장 영향:</span> {currentIndicator.impact}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* 개별 지표 차트 */}
          <div className={`glass-card rounded-2xl p-6 ${(!currentIndicator || currentIndicator.premium || loading) ? 'relative' : ''}`}>
            {(!currentIndicator || currentIndicator.premium) && (
              <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-2xl z-10 flex items-center justify-center">
                <div className="text-center">
                  <Lock className="mx-auto mb-2 text-primary" size={32} />
                  <p className="font-medium mb-1">상세 차트 분석</p>
                  <p className="text-sm text-foreground/70 mb-3">
                    모든 경제 지표의 추이를 확인하세요
                  </p>
                  <button
                    onClick={onLoginPrompt}
                    className="px-4 py-2 bg-primary/20 text-primary rounded-xl text-sm hover:bg-primary/30 transition-colors"
                  >
                    로그인하고 보기
                  </button>
                </div>
              </div>
            )}
            {loading && currentIndicator && !currentIndicator.premium && (
              <div className="absolute inset-0 bg-black/30 backdrop-blur-sm rounded-2xl z-10 flex items-center justify-center">
                <div className="flex items-center space-x-2">
                  <div className="w-4 h-4 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />
                  <div className="text-sm text-foreground/80">차트 데이터 로딩 중...</div>
                </div>
              </div>
            )}

            <h3 className="font-semibold mb-4">실시간 추이 차트</h3>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                  <XAxis 
                    dataKey="period" 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
                    tickFormatter={(value) => {
                      return value.replace('-', '.');
                    }}
                  />
                  <YAxis 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
                    tickFormatter={(value) => `${value}${currentIndicator?.unit || ""}`}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(0,0,0,0.8)",
                      border: "1px solid rgba(255,255,255,0.2)",
                      borderRadius: "8px",
                      color: "white"
                    }}
                    formatter={(value: any, name: string) => formatTooltipValue(value, name)}
                    labelFormatter={(label) => `기간: ${label.replace('-', '년 ')}월`}
                  />
                  <Line
                    type="monotone"
                    dataKey={selectedIndicator}
                    stroke={currentIndicator?.color || "#3b82f6"}
                    strokeWidth={3}
                    dot={{ fill: currentIndicator?.color, strokeWidth: 2, r: 4 }}
                    activeDot={{ r: 6, fill: currentIndicator?.color }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
            
            {/* 차트 하단 데이터 요약 */}
            {!loading && chartData.length > 0 && currentIndicator && !currentIndicator.premium && (
              <div className="mt-4 grid grid-cols-3 gap-4 text-center">
                <div className="glass rounded-lg p-3">
                  <div className="text-xs text-foreground/60 mb-1">데이터 포인트</div>
                  <div className="font-semibold">{chartData.length}개</div>
                </div>
                <div className="glass rounded-lg p-3">
                  <div className="text-xs text-foreground/60 mb-1">기간</div>
                  <div className="font-semibold">
                    {chartData.length > 0 && chartData[0].period && chartData[chartData.length - 1].period
                      ? `${chartData[0].period.split('-')[0]} ~ ${chartData[chartData.length - 1].period.split('-')[0]}`
                      : '데이터 없음'
                    }
                  </div>
                </div>
                <div className="glass rounded-lg p-3">
                  <div className="text-xs text-foreground/60 mb-1">최신 업데이트</div>
                  <div className="font-semibold">
                    {chartData.length > 0 && chartData[chartData.length - 1].period
                      ? chartData[chartData.length - 1].period.replace('-', '년 ') + '월'
                      : '없음'
                    }
                  </div>
                </div>
              </div>
            )}
          </div>
        </>
      ) : (
        <>
          {/* 상관관계 모드는 로그인 필요 */}
          <div className="glass-card rounded-2xl p-6">
            <div className="text-center py-8">
              <GitCompare className="mx-auto mb-3 text-primary" size={48} />
              <h3 className="font-semibold mb-2">상관관계 분석</h3>
              <p className="text-sm text-foreground/70 mb-4">
                경제 지표들 간의 상관관계를 분석하고<br/>
                투자 인사이트를 얻어보세요.
              </p>
              <button
                onClick={onLoginPrompt}
                className="px-4 py-2 bg-primary/20 text-primary rounded-xl font-medium hover:bg-primary/30 transition-colors"
              >
                로그인하고 분석하기
              </button>
            </div>
          </div>
        </>
      )}

      {/* 지표 간 관계 설명 */}
      <div className="glass-card rounded-2xl p-6">
        <h3 className="font-semibold mb-4 flex items-center">
          🔗 경제 지표들의 상관관계
        </h3>
        
        <div className="space-y-4">
          <div className="glass rounded-xl p-4">
            <h4 className="font-medium mb-2 text-blue-400">연준 금리 ↔ 국채 수익률</h4>
            <p className="text-sm text-foreground/70">
              연준이 기준금리를 올리면 국채 수익률도 따라 오르는 경향이 있어요. 
              둘 다 경제의 '기준 금리' 역할을 하기 때문입니다.
            </p>
          </div>
          
          <div className={`glass rounded-xl p-4 ${!isLoggedIn ? 'relative' : ''}`}>
            {!isLoggedIn && (
              <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-xl z-10 flex items-center justify-center">
                <Lock className="text-primary" size={20} />
              </div>
            )}
            <h4 className="font-medium mb-2 text-yellow-400">인플레이션 ↔ 금리</h4>
            <p className="text-sm text-foreground/70">
              인플레이션이 높아지면 연준이 금리를 올려서 경기를 식히려 해요. 
              반대로 인플레이션이 낮으면 금리를 내려서 경기를 부양합니다.
            </p>
          </div>
          
          <div className={`glass rounded-xl p-4 ${!isLoggedIn ? 'relative' : ''}`}>
            {!isLoggedIn && (
              <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-xl z-10 flex items-center justify-center">
                <Lock className="text-primary" size={20} />
              </div>
            )}
            <h4 className="font-medium mb-2 text-green-400">💡 투자 시사점</h4>
            <p className="text-sm text-foreground/70">
              • 금리 상승기: 채권 매력도 증가, 성장주 부담<br/>
              • 금리 하락기: 주식 매력도 증가, 특히 성장주 유리<br/>
              • 고인플레이션: 실물자산(부동산, 원자재) 선호<br/>
              • 저인플레이션: 금융자산(주식, 채권) 선호<br/>
              • 국채수익률 역전: 경기침체 신호, 방어적 투자 전략 필요
            </p>
          </div>
        </div>
      </div>

      {/* 데이터 상태 표시 (하단) */}
      {!loading && economicData.length > 0 && (
        <div className="glass-card rounded-xl p-4">
          <div className="flex items-center justify-between text-sm text-foreground/60">
            <div className="flex items-center space-x-4">
              <span>✅ 실시간 연동 완료</span>
              <span>📊 {economicData.length}개 데이터 포인트</span>
              <span>🔄 마지막 업데이트: {new Date().toLocaleTimeString('ko-KR')}</span>
            </div>
            <button
              onClick={fetchEconomicData}
              className="text-primary hover:text-primary/80 transition-colors"
            >
              새로고침
            </button>
          </div>
        </div>
      )}
    </div>
  );
}