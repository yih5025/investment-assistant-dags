import { useMemo, useState, useEffect } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { TrendingUp, TrendingDown, Info, BarChart3, GitCompare, Lock, TestTube, CheckCircle, XCircle, RefreshCw, Globe, AlertTriangle } from "lucide-react";

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

// 백엔드 API 응답 타입 정의
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
  const [apiConnectionStatus, setApiConnectionStatus] = useState<'unknown' | 'checking' | 'success' | 'failed'>('unknown');

  // 🔧 강화된 API URL 설정
  const getAPIBaseURL = () => {
    // 1. 환경변수 우선 확인
    if (typeof window !== 'undefined') {
      const envApiBase = import.meta.env?.VITE_API_BASE_URL;
      if (envApiBase) {
        console.log("🌐 환경변수에서 API URL 사용:", envApiBase);
        return envApiBase;
      }
    }

    // 2. 현재 도메인 기반 스마트 판단
    if (typeof window !== 'undefined') {
      const hostname = window.location.hostname;
      
      console.log("🔍 현재 환경 분석:", { hostname });

      // Vercel 배포 환경 감지
      if (hostname.includes('vercel.app')) {
        console.log("🌐 Vercel 환경 감지 → 외부 API 사용");
        return 'https://api.investment-assistant.site/api/v1';
      }
      
      // 로컬 개발 환경
      if (hostname === 'localhost' || hostname === '127.0.0.1') {
        console.log("🌐 로컬 환경 감지 → 로컬 API 사용");
        return 'http://localhost:8888/api/v1';
      }
      
      // K8s 내부 환경 (IP 기반)
      if (hostname.includes('192.168.') || hostname.includes('10.') || hostname.includes('172.')) {
        console.log("🌐 K8s 환경 감지 → 내부 프록시 사용");
        return '/api/v1';
      }

      // 사용자 정의 도메인
      if (hostname.includes('investment-assistant')) {
        console.log("🌐 커스텀 도메인 감지 → 내부 프록시 사용");
        return '/api/v1';
      }
    }

    // 3. 안전한 기본값
    console.log("🌐 기본 외부 API URL 사용");
    return 'https://api.investment-assistant.site/api/v1';
  };

  const API_BASE_URL = getAPIBaseURL();

  // 🔧 CORS 문제 해결을 위한 강화된 fetch 함수
  const fetchWithRetry = async (url: string, options: RequestInit = {}, retries: number = 3): Promise<Response> => {
    const defaultOptions: RequestInit = {
      method: 'GET',
      mode: 'cors',
      credentials: 'omit', // CORS 단순화
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      cache: 'no-store'
    };

    const finalOptions = { ...defaultOptions, ...options };
    
    console.log(`🔄 API 요청 시도: ${url}`);

    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        console.log(`🎯 시도 ${attempt + 1}/${retries + 1}: ${url}`);
        
        const response = await fetch(url, finalOptions);
        
        console.log(`📥 응답 받음:`, {
          status: response.status,
          statusText: response.statusText,
          contentType: response.headers.get('content-type'),
          corsHeaders: {
            'access-control-allow-origin': response.headers.get('access-control-allow-origin'),
          }
        });

        // Content-Type 엄격 검증
        const contentType = response.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
          const textResponse = await response.clone().text();
          
          console.error(`❌ 예상치 못한 Content-Type: ${contentType}`);
          console.error(`📄 응답 내용 (첫 500자):`, textResponse.substring(0, 500));
          
          // HTML 응답 특별 처리
          if (contentType.includes('text/html')) {
            throw new Error(`서버가 HTML 페이지를 반환했습니다. API 엔드포인트가 잘못되었거나 서버에서 요청을 처리하지 못했습니다.`);
          }
          
          // Cloudflare 에러 페이지 감지
          if (textResponse.includes('cloudflare') || textResponse.includes('error code')) {
            throw new Error(`Cloudflare에서 요청을 차단했습니다. CORS 설정이나 보안 정책을 확인해주세요.`);
          }
          
          throw new Error(`JSON 응답을 기대했지만 ${contentType}을 받았습니다.`);
        }

        // HTTP 상태 코드 검증
        if (!response.ok) {
          const errorText = await response.text();
          console.error(`❌ HTTP ${response.status}:`, errorText);
          
          switch (response.status) {
            case 404:
              throw new Error(`API 엔드포인트를 찾을 수 없습니다 (404)`);
            case 403:
              throw new Error(`API 접근이 거부되었습니다 (403). CORS 정책을 확인해주세요.`);
            case 429:
              throw new Error(`너무 많은 요청입니다 (429). 잠시 후 다시 시도해주세요.`);
            case 500:
              throw new Error(`서버 내부 오류 (500)`);
            case 502:
              throw new Error(`Bad Gateway (502). 서버가 일시적으로 불안정합니다.`);
            case 503:
              throw new Error(`서비스 일시 중단 (503)`);
            default:
              throw new Error(`HTTP ${response.status}: ${errorText || response.statusText}`);
          }
        }

        console.log(`✅ 요청 성공: ${url}`);
        return response;

      } catch (error: any) {
        console.error(`💥 API 요청 실패 (시도 ${attempt + 1}/${retries + 1}):`, error.message);
        
        // 마지막 시도가 아니라면 재시도
        if (attempt < retries) {
          const delay = Math.min(Math.pow(2, attempt) * 1000, 5000);
          console.log(`⏳ ${delay}ms 후 재시도...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        
        throw error;
      }
    }

    throw new Error('모든 재시도 실패');
  };

  // 🔧 안전한 JSON 파서
  const parseJsonSafe = async (response: Response): Promise<any> => {
    try {
      const text = await response.clone().text();
      
      if (!text || text.trim() === '') {
        throw new Error('서버에서 빈 응답을 반환했습니다');
      }
      
      const parsedData = JSON.parse(text);
      console.log(`✅ JSON 파싱 성공:`, typeof parsedData);
      return parsedData;
      
    } catch (parseError: any) {
      console.error('❌ JSON 파싱 실패:', parseError);
      const textResponse = await response.text();
      console.error('📄 파싱 실패한 원본 응답:', textResponse.substring(0, 1000));
      throw new Error(`JSON 파싱 실패: ${parseError.message}`);
    }
  };

  // 숫자 값 추출 함수
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

  // 날짜를 YYYY-MM 형식으로 변환
  const formatPeriod = (dateStr: string): string => {
    const date = new Date(dateStr);
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    return `${year}-${month}`;
  };

  // 여러 API 데이터를 월별로 병합
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

    // 국채수익률 데이터 처리
    treasuryData.forEach(item => {
      if (item.date) {
        const period = formatPeriod(item.date);
        const year = parseInt(period.split('-')[0]);
        
        const yieldValue = extractNumber(item, ['yield', 'yield_value', 'yield_rate', 'rate', 'value']);
        
        if (yieldValue !== undefined) {
          if (!monthlyData.has(period)) {
            monthlyData.set(period, { year, period });
          }
          
          const existing = monthlyData.get(period)!;
          existing.treasuryRate = yieldValue;
        }
      }
    });

    // 인플레이션 데이터 처리
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

  // 🔧 순차적 API 호출로 안정성 확보
  const fetchEconomicData = async () => {
    try {
      setLoading(true);
      setError(null);
      setApiConnectionStatus('checking');

      console.log("📊 경제 데이터 로딩 시작...");
      console.log("🌐 사용할 API URL:", API_BASE_URL);

      const endpoints = [
        { name: '연방기금금리', path: '/federal-funds-rate/', key: 'fedData' },
        { name: '인플레이션', path: '/inflation/', key: 'inflationData' },
        { name: 'CPI', path: '/cpi/', key: 'cpiData' },
        { name: '국채수익률', path: '/treasury-yield/?maturity=10year&size=1000', key: 'treasuryData' }
      ];

      const apiResults: { [key: string]: any } = {};
      let successCount = 0;
      const errors: string[] = [];

      // 순차적으로 API 호출
      for (const endpoint of endpoints) {
        try {
          console.log(`🔄 ${endpoint.name} 데이터 요청 중...`);
          
          const response = await fetchWithRetry(`${API_BASE_URL}${endpoint.path}`);
          const data = await parseJsonSafe(response);
          
          apiResults[endpoint.key] = data;
          successCount++;
          
          console.log(`✅ ${endpoint.name} 데이터 수신 완료:`, data.items?.length || 0, '개 항목');
          
          await new Promise(resolve => setTimeout(resolve, 200));
          
        } catch (error: any) {
          console.error(`❌ ${endpoint.name} 데이터 로딩 실패:`, error);
          errors.push(`${endpoint.name}: ${error.message}`);
          apiResults[endpoint.key] = { items: [] };
        }
      }

      // 결과 처리
      if (successCount === 0) {
        setApiConnectionStatus('failed');
        throw new Error(`모든 API 요청이 실패했습니다:\n${errors.join('\n')}`);
      }

      setApiConnectionStatus('success');

      // 데이터 결합
      const combinedData = combineEconomicData(
        apiResults.fedData?.items || [],
        apiResults.inflationData?.items || [],
        apiResults.cpiData?.items || [],
        apiResults.treasuryData?.items || []
      );

      console.log("🔄 데이터 변환 완료:", combinedData.length + "개 항목");
      setEconomicData(combinedData);

      if (combinedData.length === 0) {
        throw new Error('변환된 데이터가 없습니다. API 응답 형식을 확인해주세요.');
      }

      // 부분적 실패 경고 표시
      if (errors.length > 0) {
        setError(`일부 데이터 로딩 실패: ${errors.join(', ')}`);
        setTimeout(() => setError(null), 3000);
      }

    } catch (err: any) {
      console.error("❌ 경제 데이터 로딩 실패:", err);
      setApiConnectionStatus('failed');
      setError(`데이터 로딩 실패: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // 🔧 강화된 API 테스트 함수
  const runAPITest = async () => {
    setIsTestRunning(true);
    setShowAPITest(true);
    setApiConnectionStatus('checking');
    
    const tests: Omit<APITestResult, 'status'>[] = [
      { name: "🌐 기본 연결", url: `${API_BASE_URL.replace('/api/v1', '')}/health` },
      { name: "🧪 CORS 테스트", url: `${API_BASE_URL.replace('/api/v1', '')}/cors-test` },
      { name: "📊 API 정보", url: `${API_BASE_URL}/` },
      { name: "💰 연방기금금리", url: `${API_BASE_URL}/federal-funds-rate/?limit=3` },
      { name: "📈 인플레이션", url: `${API_BASE_URL}/inflation/?limit=3` },
      { name: "🛒 CPI", url: `${API_BASE_URL}/cpi/?limit=3` },
      { name: "🏛️ 국채수익률", url: `${API_BASE_URL}/treasury-yield/?limit=3` }
    ];

    const results: APITestResult[] = tests.map(test => ({ ...test, status: 'loading' }));
    setApiTestResults([...results]);

    let successCount = 0;

    for (let i = 0; i < tests.length; i++) {
      const test = tests[i];
      const startTime = Date.now();
      
      try {
        const response = await fetchWithRetry(test.url, {}, 1);
        const responseTime = Date.now() - startTime;
        const data = await parseJsonSafe(response);
        
        results[i] = {
          ...test,
          status: 'success',
          statusCode: response.status,
          data: data,
          responseTime
        };
        
        successCount++;
        
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
        await new Promise(resolve => setTimeout(resolve, 300));
      }
    }
    
    setApiConnectionStatus(successCount >= 3 ? 'success' : 'failed');
    setIsTestRunning(false);
  };

  // 컴포넌트 마운트 시 데이터 로드
  useEffect(() => {
    console.log("🚀 EconomicDashboard 컴포넌트 마운트됨");
    fetchEconomicData();
  }, []);

  // 선택 지표의 최신값과 전년 동월 값 추출
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

  // 🔧 모든 지표를 로그인 없이 이용 가능하도록 수정
  const indicators = [
    {
      key: "treasuryRate",
      name: "미국 국채 10년",
      unit: "%",
      color: "#3b82f6",
      description: "장기 금리 동향을 나타내는 핵심 지표",
      impact: "높아지면 대출금리 상승, 주식보다 채권 매력도 증가"
      // premium 속성 제거 - 모든 사용자가 이용 가능
    },
    {
      key: "fedRate",
      name: "연준 기준금리",
      unit: "%",
      color: "#10b981",
      description: "미국 연방준비제도의 기준 금리",
      impact: "경기 과열시 올리고, 침체시 낮춰서 경기 조절"
      // premium 속성 제거
    },
    {
      key: "inflation",
      name: "인플레이션율",
      unit: "%",
      color: "#f59e0b",
      description: "물가 상승률을 나타내는 지표",
      impact: "높아지면 돈의 가치 하락, 연준이 금리 인상 고려"
      // premium 속성 제거 - 이제 로그인 없이도 이용 가능
    },
    {
      key: "cpi",
      name: "소비자물가지수",
      unit: "pt",
      color: "#8b5cf6",
      description: "소비자가 구매하는 상품과 서비스의 가격 수준",
      impact: "CPI 상승률이 인플레이션율과 직결됨"
      // premium 속성 제거 - 이제 로그인 없이도 이용 가능
    }
  ];

  // 🔧 상관관계 분석 옵션들 (모든 사용자 이용 가능)
  const correlationPairs = [
    { first: "fedRate", second: "treasuryRate", description: "연준 금리와 국채 수익률은 강한 양의 상관관계" },
    { first: "inflation", second: "fedRate", description: "인플레이션 상승 시 연준이 금리를 올리는 패턴" },
    { first: "cpi", second: "inflation", description: "CPI 변화율이 인플레이션을 직접 반영" },
    { first: "treasuryRate", second: "inflation", description: "인플레이션 기대가 장기 금리에 반영됨" }
  ];

  const currentIndicator = indicators.find(ind => ind.key === selectedIndicator);
  const latestPair = getLatestPair(selectedIndicator as keyof EconomicIndicator);
  const currentValue = latestPair.value ?? 0;
  const previousValue = latestPair.prev ?? 0;
  const hasPrev = latestPair.prev != null;
  const change = hasPrev ? currentValue - (previousValue as number) : 0;
  const changePercent = hasPrev && previousValue !== 0 ? ((change / (previousValue as number)) * 100) : 0;

  // 🔧 상관관계 차트 데이터 생성 함수
  const getCorrelationData = () => {
    const firstKey = correlationPair.first as keyof EconomicIndicatorRow;
    const secondKey = correlationPair.second as keyof EconomicIndicatorRow;
    
    return economicData.filter(r => 
      (r as any)[firstKey] != null && (r as any)[secondKey] != null
    ).map(r => ({
      period: r.period,
      year: r.year,
      [firstKey]: (r as any)[firstKey],
      [secondKey]: (r as any)[secondKey]
    }));
  };

  // 차트 데이터
  const chartData = useMemo(() => {
    if (correlationMode) {
      return getCorrelationData();
    }
    
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
  }, [economicData, selectedIndicator, correlationMode, correlationPair]);

  const formatTooltipValue = (value: any, name: string) => {
    const indicator = indicators.find(ind => ind.key === name);
    return [`${value}${indicator?.unit || ""}`, indicator?.name || name];
  };

  const handleIndicatorClick = (indicator: any) => {
    // premium 체크 제거 - 모든 지표 자유 이용
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
        
        <div className="text-xs text-foreground/70 mb-1 break-all">
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

  // API 연결 상태 표시
  const renderConnectionStatus = () => {
    const getStatusColor = () => {
      switch (apiConnectionStatus) {
        case 'checking': return 'text-blue-400';
        case 'success': return 'text-green-400';
        case 'failed': return 'text-red-400';
        default: return 'text-gray-400';
      }
    };

    const getStatusIcon = () => {
      switch (apiConnectionStatus) {
        case 'checking':
          return <div className="w-3 h-3 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />;
        case 'success':
          return <CheckCircle size={14} className="text-green-400" />;
        case 'failed':
          return <XCircle size={14} className="text-red-400" />;
        default:
          return <Globe size={14} className="text-gray-400" />;
      }
    };

    const getStatusText = () => {
      switch (apiConnectionStatus) {
        case 'checking': return '연결 확인 중...';
        case 'success': return '연결됨';
        case 'failed': return '연결 실패';
        default: return '미확인';
      }
    };

    return (
      <div className={`flex items-center space-x-1 text-xs ${getStatusColor()}`}>
        {getStatusIcon()}
        <span>{getStatusText()}</span>
      </div>
    );
  };

  // 에러 상태 처리
  if (error && economicData.length === 0) {
    return (
      <div className="space-y-6">
        <div className="glass-card rounded-2xl p-6">
          <div className="text-center">
            <XCircle className="mx-auto mb-4 text-red-400" size={48} />
            <h3 className="font-semibold mb-2 text-red-400">데이터 로딩 실패</h3>
            <p className="text-sm text-foreground/70 mb-4 whitespace-pre-line">{error}</p>
            
            <div className="glass rounded-xl p-3 mb-4 text-left">
              <h4 className="font-medium mb-2 text-blue-400">🔍 연결 정보</h4>
              <div className="text-xs text-foreground/70 space-y-1">
                <div>API URL: {API_BASE_URL}</div>
                <div>도메인: {typeof window !== 'undefined' ? window.location.hostname : 'SSR'}</div>
                <div className="flex items-center space-x-2">
                  <span>상태:</span> {renderConnectionStatus()}
                </div>
              </div>
            </div>
            
            <div className="flex justify-center space-x-3">
              <button 
                onClick={fetchEconomicData}
                disabled={loading}
                className="px-4 py-2 bg-primary/20 text-primary rounded-xl hover:bg-primary/30 transition-colors disabled:opacity-50"
              >
                {loading ? '재시도 중...' : '다시 시도'}
              </button>
              <button 
                onClick={runAPITest}
                disabled={isTestRunning}
                className="px-4 py-2 bg-blue-500/20 text-blue-400 rounded-xl hover:bg-blue-500/30 transition-colors disabled:opacity-50"
              >
                {isTestRunning ? '테스트 중...' : '🧪 연결 진단'}
              </button>
            </div>
          </div>
        </div>

        {showAPITest && (
          <div className="glass-card rounded-2xl p-6">
            <div className="flex items-center space-x-2 mb-4">
              <TestTube size={20} className="text-blue-400" />
              <h3 className="font-semibold">API 연결 진단 결과</h3>
              {isTestRunning && <div className="text-sm text-blue-400">진행 중...</div>}
            </div>
            
            <div className="space-y-3">
              {apiTestResults.length > 0 ? (
                apiTestResults.map(renderAPITestResult)
              ) : (
                <div className="text-sm text-foreground/60">진단을 실행하려면 '🧪 연결 진단' 버튼을 클릭하세요.</div>
              )}
            </div>
            
            {apiTestResults.length > 0 && !isTestRunning && (
              <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="glass rounded-xl p-3">
                  <h4 className="font-medium mb-2 text-green-400">✅ 성공 시</h4>
                  <div className="text-xs text-foreground/70 space-y-1">
                    <div>• API 서버가 정상 응답</div>
                    <div>• CORS 설정이 올바름</div>
                    <div>• 데이터 형식이 정확함</div>
                  </div>
                </div>
                <div className="glass rounded-xl p-3">
                  <h4 className="font-medium mb-2 text-red-400">❌ 실패 시 해결법</h4>
                  <div className="text-xs text-foreground/70 space-y-1">
                    <div>• CORS: 서버 설정 확인</div>
                    <div>• 404: URL 경로 확인</div>
                    <div>• HTML 응답: Cloudflare 설정</div>
                    <div>• 네트워크: 방화벽 확인</div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold flex items-center">
            <BarChart3 className="mr-2" size={20} />
            📊 경제 지표 대시보드
            {loading && <div className="ml-2 text-sm text-blue-400">데이터 로딩 중...</div>}
          </h2>
          {renderConnectionStatus()}
        </div>
        
        {/* 부분적 에러 알림 */}
        {error && economicData.length > 0 && (
          <div className="mb-4 p-3 glass rounded-xl border border-yellow-500/30">
            <div className="flex items-center space-x-2">
              <AlertTriangle className="text-yellow-400" size={16} />
              <div className="text-sm">
                <span className="font-medium text-yellow-400">부분적 데이터 로딩</span>
                <p className="text-foreground/70 mt-1">{error}</p>
              </div>
            </div>
          </div>
        )}
        
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
            <span>{isTestRunning ? '진단 중...' : '🧪 API 연결 진단'}</span>
          </button>

          <button
            onClick={fetchEconomicData}
            disabled={loading}
            className="flex items-center space-x-2 px-4 py-2 bg-green-500/20 text-green-400 rounded-xl font-medium hover:bg-green-500/30 transition-colors disabled:opacity-50"
          >
            <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
            <span>{loading ? '로딩 중...' : '데이터 새로고침'}</span>
          </button>
          
          {showAPITest && (
            <button
              onClick={() => setShowAPITest(false)}
              className="px-3 py-2 text-sm text-foreground/60 hover:text-foreground/80"
            >
              진단 결과 숨기기
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
        
        {/* 🔧 로그인 프롬프트 제거 - 모든 기능 무료 이용 */}
        <div className="mt-4 p-3 glass rounded-xl border border-green-500/30">
          <div className="flex items-center space-x-2">
            <CheckCircle className="text-green-400" size={16} />
            <p className="text-sm">
              <span className="font-medium text-green-400">모든 기능 이용 가능!</span> 
              <span className="text-foreground/70 ml-2">경제 지표와 상관관계 분석을 자유롭게 사용하세요.</span>
            </p>
          </div>
        </div>
      </div>

      {/* API 진단 결과 */}
      {showAPITest && (
        <div className="glass-card rounded-2xl p-6">
          <div className="flex items-center space-x-2 mb-4">
            <TestTube size={20} className="text-blue-400" />
            <h3 className="font-semibold">API 연결 진단 결과</h3>
            {isTestRunning && <div className="text-sm text-blue-400">진행 중...</div>}
          </div>
          
          <div className="space-y-3">
            {apiTestResults.length > 0 ? (
              apiTestResults.map(renderAPITestResult)
            ) : (
              <div className="text-sm text-foreground/60">진단을 실행하려면 '🧪 API 연결 진단' 버튼을 클릭하세요.</div>
            )}
          </div>
          
          {apiTestResults.length > 0 && !isTestRunning && (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="glass rounded-xl p-3">
                <h4 className="font-medium mb-2 text-green-400">✅ 성공 시</h4>
                <div className="text-xs text-foreground/70 space-y-1">
                  <div>• API 서버가 정상 응답</div>
                  <div>• CORS 설정이 올바름</div>
                  <div>• 데이터 형식이 정확함</div>
                </div>
              </div>
              <div className="glass rounded-xl p-3">
                <h4 className="font-medium mb-2 text-red-400">❌ 실패 시 해결법</h4>
                <div className="text-xs text-foreground/70 space-y-1">
                  <div>• CORS: 서버 설정 확인</div>
                  <div>• 404: URL 경로 확인</div>
                  <div>• HTML 응답: Cloudflare 설정</div>
                  <div>• 네트워크: 방화벽 확인</div>
                </div>
              </div>
            </div>
          )}
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
            {/* 🔧 Lock 아이콘 제거 - 모든 사용자 이용 가능 */}
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
                  }`}
                >
                  {/* 🔧 Lock 아이콘 제거 - 모든 지표 자유 이용 */}
                  
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium">{indicator.name}</span>
                    <div className={`flex items-center text-xs ${
                      indicatorChange >= 0 ? "text-green-400" : "text-red-400"
                    }`}>
                      {indicatorChange >= 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                    </div>
                  </div>
                  <div className="text-lg font-semibold" style={{ color: indicator.color }}>
                    {/* 🔧 premium 체크 제거 - 모든 데이터 표시 */}
                    {value != null ? `${value.toFixed(2)}${indicator.unit}` : "--"}
                  </div>
                  <div className={`text-xs ${
                    value != null && prevValue != null ? (indicatorChange >= 0 ? "text-green-400" : "text-red-400") : "text-foreground/40"
                  }`}>
                    {/* 🔧 premium 체크 제거 - 모든 변화량 표시 */}
                    {value != null && prevValue != null ?
                      `${indicatorChange >= 0 ? "+" : ""}${indicatorChange.toFixed(2)}${indicator.unit}` : "데이터 없음"}
                  </div>
                </button>
              );
            })}
          </div>

          {/* 선택된 지표 상세 정보 */}
          {currentIndicator && (
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

          {/* 개별 지표 차트 - 🔧 premium 체크 제거 */}
          <div className={`glass-card rounded-2xl p-6 ${loading ? 'relative' : ''}`}>
            {loading && (
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
                    tickFormatter={(value) => value.replace('-', '.')}
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
                    dot={false}  // 🔧 포인트 제거
                    activeDot={{ r: 6, fill: currentIndicator?.color }} // 호버 시에만 포인트 표시
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
            
            {/* 차트 하단 데이터 요약 */}
            {!loading && chartData.length > 0 && currentIndicator && (
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
          {/* 🔧 상관관계 모드 - 로그인 제한 제거하고 실제 기능 구현 */}
          <div className="glass-card rounded-2xl p-6">
            <h3 className="font-semibold mb-4 flex items-center">
              <GitCompare className="mr-2" size={20} />
              📈 경제 지표 상관관계 분석
            </h3>

            {/* 상관관계 쌍 선택 */}
            <div className="mb-6">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {correlationPairs.map((pair, index) => (
                  <button
                    key={index}
                    onClick={() => setCorrelationPair({ first: pair.first, second: pair.second })}
                    className={`glass rounded-xl p-4 text-left transition-all ${
                      correlationPair.first === pair.first && correlationPair.second === pair.second
                        ? "bg-primary/20 border border-primary/30" 
                        : "hover:bg-white/10"
                    }`}
                  >
                    <div className="flex items-center space-x-2 mb-2">
                      <GitCompare size={16} className="text-primary" />
                      <span className="font-medium text-sm">
                        {indicators.find(i => i.key === pair.first)?.name} ↔ {indicators.find(i => i.key === pair.second)?.name}
                      </span>
                    </div>
                    <p className="text-xs text-foreground/70">{pair.description}</p>
                  </button>
                ))}
              </div>
            </div>

            {/* 상관관계 차트 */}
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={getCorrelationData()}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                  <XAxis 
                    dataKey="period" 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
                    tickFormatter={(value) => value.replace('-', '.')}
                  />
                  <YAxis 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(0,0,0,0.8)",
                      border: "1px solid rgba(255,255,255,0.2)",
                      borderRadius: "8px",
                      color: "white"
                    }}
                    labelFormatter={(label) => `기간: ${label.replace('-', '년 ')}월`}
                  />
                  <Line
                    type="monotone"
                    dataKey={correlationPair.first}
                    stroke={indicators.find(i => i.key === correlationPair.first)?.color || "#3b82f6"}
                    strokeWidth={3}
                    dot={false}  // 🔧 포인트 제거
                    name={indicators.find(i => i.key === correlationPair.first)?.name}
                  />
                  <Line
                    type="monotone"
                    dataKey={correlationPair.second}
                    stroke={indicators.find(i => i.key === correlationPair.second)?.color || "#10b981"}
                    strokeWidth={3}
                    dot={false}  // 🔧 포인트 제거
                    name={indicators.find(i => i.key === correlationPair.second)?.name}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* 상관관계 분석 정보 */}
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="glass rounded-xl p-4">
                <h4 className="font-medium mb-2" style={{ color: indicators.find(i => i.key === correlationPair.first)?.color }}>
                  {indicators.find(i => i.key === correlationPair.first)?.name}
                </h4>
                <div className="text-sm text-foreground/70">
                  {indicators.find(i => i.key === correlationPair.first)?.description}
                </div>
              </div>
              <div className="glass rounded-xl p-4">
                <h4 className="font-medium mb-2" style={{ color: indicators.find(i => i.key === correlationPair.second)?.color }}>
                  {indicators.find(i => i.key === correlationPair.second)?.name}
                </h4>
                <div className="text-sm text-foreground/70">
                  {indicators.find(i => i.key === correlationPair.second)?.description}
                </div>
              </div>
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
          
          {/* 🔧 Lock 아이콘 제거 - 모든 정보 공개 */}
          <div className="glass rounded-xl p-4">
            <h4 className="font-medium mb-2 text-yellow-400">인플레이션 ↔ 금리</h4>
            <p className="text-sm text-foreground/70">
              인플레이션이 높아지면 연준이 금리를 올려서 경기를 식히려 해요. 
              반대로 인플레이션이 낮으면 금리를 내려서 경기를 부양합니다.
            </p>
          </div>
          
          <div className="glass rounded-xl p-4">
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
              {renderConnectionStatus()}
            </div>
            <button
              onClick={fetchEconomicData}
              disabled={loading}
              className="text-primary hover:text-primary/80 transition-colors disabled:opacity-50"
            >
              새로고침
            </button>
          </div>
        </div>
      )}

      {/* 디버깅 정보 (개발 환경에서만 표시) */}
      {typeof window !== 'undefined' && window.location.hostname.includes('localhost') && (
        <div className="glass-card rounded-xl p-4">
          <details className="text-sm">
            <summary className="cursor-pointer text-foreground/60 hover:text-foreground/80">
              🔧 개발자 디버깅 정보
            </summary>
            <div className="mt-2 space-y-2 text-xs text-foreground/70">
              <div><strong>환경:</strong> {window.location.hostname}</div>
              <div><strong>API URL:</strong> {API_BASE_URL}</div>
              <div><strong>연결 상태:</strong> {apiConnectionStatus}</div>
              <div><strong>로딩 상태:</strong> {loading ? '로딩 중' : '완료'}</div>
              <div><strong>데이터 개수:</strong> {economicData.length}</div>
              <div><strong>선택된 지표:</strong> {selectedIndicator}</div>
              <div><strong>상관관계 모드:</strong> {correlationMode ? 'ON' : 'OFF'}</div>
              {correlationMode && (
                <div><strong>상관관계 쌍:</strong> {correlationPair.first} ↔ {correlationPair.second}</div>
              )}
              <div><strong>User Agent:</strong> {navigator.userAgent.slice(0, 50)}...</div>
            </div>
          </details>
        </div>
      )}
    </div>
  );
} 