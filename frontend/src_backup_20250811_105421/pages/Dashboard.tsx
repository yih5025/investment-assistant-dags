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
    <div className="min-h-screen bg-background">
      <main className="container mx-auto px-4 py-6 space-y-8">
        {/* Hero Section */}
        <div className="text-center space-y-4">
          <h1 className="text-3xl md:text-4xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
            Smart Investment Dashboard
          </h1>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            포트폴리오와 시장 현황을 실시간으로 확인하고, 데이터 기반의 투자 결정을 내리세요.
          </p>
          
          {/* Status Bar */}
          <div className="flex items-center justify-center space-x-6 text-sm">
            <div className="flex items-center space-x-2">
              <div className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-success' : 'bg-destructive'}`} />
              <span className="text-muted-foreground">{wsConnected ? '실시간 연결됨' : '연결 끊김'}</span>
            </div>
            {lastUpdated && (
              <span className="text-muted-foreground">
                업데이트: {formatRelativeTime(lastUpdated)}
              </span>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={refreshData}
              disabled={loading}
              className="h-8"
            >
              <RefreshCw className={`h-3 w-3 mr-2 ${loading ? 'animate-spin' : ''}`} />
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
        <section>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <Card className="border-0 shadow-soft hover:shadow-medium transition-all duration-300">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  총 포트폴리오 가치
                </CardTitle>
                <div className="p-2 bg-primary/10 rounded-lg">
                  <DollarSign className="h-4 w-4 text-primary" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {formatCurrency(125420000, 'KRW')}
                </div>
                <p className="text-xs text-bull flex items-center mt-1">
                  <TrendingUp className="h-3 w-3 mr-1" />
                  {formatPercent(2.3)} 오늘
                </p>
              </CardContent>
            </Card>

            <Card className="border-0 shadow-soft hover:shadow-medium transition-all duration-300">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  일일 손익
                </CardTitle>
                <div className="p-2 bg-success/10 rounded-lg">
                  <Activity className="h-4 w-4 text-success" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-bull">
                  {formatCurrency(2850000, 'KRW')}
                </div>
                <p className="text-xs text-bull flex items-center mt-1">
                  <TrendingUp className="h-3 w-3 mr-1" />
                  {formatPercent(2.3)}
                </p>
              </CardContent>
            </Card>

            <Card className="border-0 shadow-soft hover:shadow-medium transition-all duration-300">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  KOSPI
                </CardTitle>
                <div className="p-2 bg-bull/10 rounded-lg">
                  <TrendingUp className="h-4 w-4 text-bull" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">2,640.52</div>
                <p className="text-xs text-bull flex items-center mt-1">
                  <TrendingUp className="h-3 w-3 mr-1" />
                  +15.24 (+0.58%)
                </p>
              </CardContent>
            </Card>

            <Card className="border-0 shadow-soft hover:shadow-medium transition-all duration-300">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  비트코인
                </CardTitle>
                <div className="p-2 bg-bear/10 rounded-lg">
                  <TrendingDown className="h-4 w-4 text-bear" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {formatCurrency(94250000, 'KRW')}
                </div>
                <p className="text-xs text-bear flex items-center mt-1">
                  <TrendingDown className="h-3 w-3 mr-1" />
                  {formatPercent(-1.2)} 24시간
                </p>
              </CardContent>
            </Card>
          </div>
        </section>

        {/* Main Content Grid */}
        <div className="grid lg:grid-cols-2 gap-8">
          {/* Left Column */}
          <div className="space-y-8">
            <section>
              <Card className="border-0 shadow-soft">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Activity className="h-5 w-5 text-primary" />
                    <span>관심 종목</span>
                  </CardTitle>
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
            </section>
          </div>

          {/* Right Column */}
          <div>
            <section>
              <Card className="border-0 shadow-soft">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Activity className="h-5 w-5 text-accent" />
                    <span>시장 뉴스</span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {[
                      { title: 'Apple, 새로운 AI 기능 발표', time: '2시간 전', category: '기술' },
                      { title: '연준, 금리 동결 결정', time: '4시간 전', category: '경제' },
                      { title: '테슬라, Q4 실적 발표', time: '6시간 전', category: '기업' },
                      { title: '비트코인, 규제 우려로 하락', time: '8시간 전', category: '암호화폐' },
                    ].map((news, index) => (
                      <div key={index} className="pb-3 border-b last:border-b-0 hover:bg-muted/30 -mx-2 px-2 rounded transition-colors">
                        <h4 className="font-medium text-sm mb-1 line-clamp-2">
                          {news.title}
                        </h4>
                        <div className="flex items-center justify-between">
                          <span className="text-xs text-muted-foreground">{news.time}</span>
                          <span className="text-xs bg-primary/10 text-primary px-2 py-1 rounded">
                            {news.category}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </section>
          </div>
        </div>
      </main>
    </div>
  );
};