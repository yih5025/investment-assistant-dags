import React, { useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { 
  TrendingUp, 
  TrendingDown, 
  DollarSign, 
  Activity,
  RefreshCw
} from 'lucide-react';
import { useDashboardStore } from '../store/dashboardStore';
import { useWebSocket } from '../hooks/useWebSocket';
import { LoadingSpinner } from '../components/common/LoadingSpinner';
import { formatCurrency, formatPercent, formatRelativeTime } from '../utils/formatters';

export const Dashboard: React.FC = () => {
  const {
    marketOverview,
    loading,
    error,
    lastUpdated,
    refreshData,
    setLoading,
    setError,
  } = useDashboardStore();

  const { connected: wsConnected } = useWebSocket();

  useEffect(() => {
    // 대시보드 데이터 초기 로드
    loadDashboardData();
  }, []);

  const loadDashboardData = async () => {
    setLoading(true);
    setError(null);

    try {
      // 실제 구현에서는 API 호출
      // 현재는 모의 데이터
      setTimeout(() => {
        // Mock data
        setLoading(false);
      }, 1000);
    } catch (error) {
      setError('데이터를 불러오는 중 오류가 발생했습니다.');
      setLoading(false);
    }
  };

  if (loading && !marketOverview) {
    return (
      <div className="p-6">
        <LoadingSpinner />
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">대시보드</h1>
          <p className="text-muted-foreground">
            투자 포트폴리오와 시장 현황을 한눈에 확인하세요
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <div className="flex items-center space-x-2 text-sm text-muted-foreground">
            <div className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-green-500' : 'bg-red-500'}`} />
            <span>{wsConnected ? '연결됨' : '연결 끊김'}</span>
          </div>
          {lastUpdated && (
            <span className="text-sm text-muted-foreground">
              마지막 업데이트: {formatRelativeTime(lastUpdated)}
            </span>
          )}
          <Button
            variant="outline"
            size="sm"
            onClick={refreshData}
            disabled={loading}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
            새로고침
          </Button>
        </div>
      </div>

      {/* 오류 메시지 */}
      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4">
            <p className="text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {/* 주요 지표 카드들 */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              총 포트폴리오 가치
            </CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatCurrency(125420000, 'KRW')}
            </div>
            <p className="text-xs text-green-600 flex items-center mt-1">
              <TrendingUp className="h-3 w-3 mr-1" />
              {formatPercent(2.3)} 오늘
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              일일 손익
            </CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">
              {formatCurrency(2850000, 'KRW')}
            </div>
            <p className="text-xs text-green-600 flex items-center mt-1">
              <TrendingUp className="h-3 w-3 mr-1" />
              {formatPercent(2.3)}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              KOSPI
            </CardTitle>
            <TrendingUp className="h-4 w-4 text-green-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">2,640.52</div>
            <p className="text-xs text-green-600 flex items-center mt-1">
              <TrendingUp className="h-3 w-3 mr-1" />
              +15.24 (+0.58%)
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              비트코인
            </CardTitle>
            <TrendingDown className="h-4 w-4 text-red-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatCurrency(94250000, 'KRW')}
            </div>
            <p className="text-xs text-red-600 flex items-center mt-1">
              <TrendingDown className="h-3 w-3 mr-1" />
              {formatPercent(-1.2)} 24시간
            </p>
          </CardContent>
        </Card>
      </div>

      {/* 위젯 영역 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
        {/* 관심 종목 */}
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>관심 종목</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {[
                { symbol: 'AAPL', name: 'Apple Inc.', price: 180.25, change: 2.15, changePercent: 1.21 },
                { symbol: 'MSFT', name: 'Microsoft Corp.', price: 415.30, change: -3.20, changePercent: -0.76 },
                { symbol: 'GOOGL', name: 'Alphabet Inc.', price: 142.65, change: 1.85, changePercent: 1.31 },
                { symbol: 'TSLA', name: 'Tesla Inc.', price: 248.50, change: -5.75, changePercent: -2.26 },
              ].map((stock) => (
                <div key={stock.symbol} className="flex items-center justify-between p-3 rounded-lg border">
                  <div className="flex items-center space-x-3">
                    <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
                      <span className="font-semibold text-sm">{stock.symbol.slice(0, 2)}</span>
                    </div>
                    <div>
                      <p className="font-medium">{stock.symbol}</p>
                      <p className="text-sm text-muted-foreground">{stock.name}</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="font-semibold">${stock.price}</p>
                    <p className={`text-sm flex items-center ${stock.change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {stock.change >= 0 ? (
                        <TrendingUp className="h-3 w-3 mr-1" />
                      ) : (
                        <TrendingDown className="h-3 w-3 mr-1" />
                      )}
                      {stock.change >= 0 ? '+' : ''}{stock.change} ({formatPercent(stock.changePercent)})
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* 최근 뉴스 */}
        <Card>
          <CardHeader>
            <CardTitle>최근 뉴스</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {[
                { title: 'Apple, 새로운 AI 기능 발표', time: '2시간 전', category: '기술' },
                { title: '연준, 금리 동결 결정', time: '4시간 전', category: '경제' },
                { title: '테슬라, Q4 실적 발표', time: '6시간 전', category: '기업' },
                { title: '비트코인, 규제 우려로 하락', time: '8시간 전', category: '암호화폐' },
              ].map((news, index) => (
                <div key={index} className="pb-3 border-b last:border-b-0">
                  <h4 className="font-medium text-sm mb-1 line-clamp-2">
                    {news.title}
                  </h4>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">{news.time}</span>
                    <span className="text-xs bg-muted px-2 py-1 rounded">
                      {news.category}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* 차트 영역 (향후 추가 예정) */}
      <Card>
        <CardHeader>
          <CardTitle>포트폴리오 성과</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center text-muted-foreground">
            차트 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};