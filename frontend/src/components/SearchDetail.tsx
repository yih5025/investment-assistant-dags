import { useState } from "react";
import { ArrowLeft, TrendingUp, TrendingDown, Star, Bell, Building, DollarSign, PieChart, Calculator, Info } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from "recharts";

interface SearchDetailProps {
  symbol: string;
  onBack: () => void;
  isLoggedIn?: boolean;
  onLoginPrompt?: () => void;
  isInWatchlist?: boolean;
  onToggleWatchlist?: () => void;
}

export function SearchDetail({ 
  symbol, 
  onBack, 
  isLoggedIn = false, 
  onLoginPrompt, 
  isInWatchlist = false, 
  onToggleWatchlist 
}: SearchDetailProps) {
  const [timeframe, setTimeframe] = useState("1D");
  const [alertEnabled, setAlertEnabled] = useState(false);

  // 재무제표가 있는 회사 목록
  const companiesWithFinancials = ["AAPL", "TSLA", "NVDA", "META", "GOOGL", "MSFT", "AMZN"];
  const hasFinancials = companiesWithFinancials.includes(symbol);

  // 모의 주식 데이터
  const stockInfo = {
    AAPL: { name: "Apple Inc.", sector: "Technology", price: 185.64, change: 3.45, changePercent: 1.89 },
    TSLA: { name: "Tesla Inc.", sector: "Automotive", price: 248.42, change: -5.23, changePercent: -2.06 },
    NVDA: { name: "NVIDIA Corp.", sector: "Semiconductors", price: 875.23, change: 15.67, changePercent: 1.82 },
    BTC: { name: "Bitcoin", sector: "Cryptocurrency", price: 98745.32, change: -1250.45, changePercent: -1.25 },
    ETH: { name: "Ethereum", sector: "Cryptocurrency", price: 3842.67, change: 127.89, changePercent: 3.45 }
  };

  const stock = stockInfo[symbol as keyof typeof stockInfo] || {
    name: `${symbol} Corporation`,
    sector: "Technology",
    price: 125.50,
    change: 2.35,
    changePercent: 1.91
  };

  // 차트 데이터 (시간대별로 다르게)
  const chartData = timeframe === "1D" ? [
    { time: "09:30", price: stock.price - 5 },
    { time: "10:00", price: stock.price - 3 },
    { time: "11:00", price: stock.price - 1 },
    { time: "12:00", price: stock.price + 1 },
    { time: "13:00", price: stock.price + 2 },
    { time: "14:00", price: stock.price - 1 },
    { time: "15:00", price: stock.price + 3 },
    { time: "16:00", price: stock.price }
  ] : [
    { time: "1월", price: stock.price - 20 },
    { time: "2월", price: stock.price - 15 },
    { time: "3월", price: stock.price - 10 },
    { time: "4월", price: stock.price - 5 },
    { time: "5월", price: stock.price },
    { time: "6월", price: stock.price + 5 }
  ];

  // 재무 데이터 (Apple 기준 모의 데이터)
  const financialData = {
    revenue: [
      { year: "2022", value: 394.3 },
      { year: "2023", value: 383.3 },
      { year: "2024", value: 391.0 }
    ],
    metrics: {
      revenue: { value: "391.0B", description: "매출액", explanation: "회사가 제품과 서비스를 판매해서 얻은 총 수익이에요. 높을수록 좋아요!" },
      netIncome: { value: "101.5B", description: "순이익", explanation: "매출에서 모든 비용을 뺀 실제 남은 돈이에요. 회사가 얼마나 효율적인지 보여줘요." },
      totalAssets: { value: "352.8B", description: "총자산", explanation: "회사가 가지고 있는 모든 재산의 총합이에요. 현금, 건물, 기계 등 모든 것이 포함돼요." },
      totalDebt: { value: "110.2B", description: "총부채", explanation: "회사가 빌린 돈의 총합이에요. 너무 많으면 위험하지만, 적당히 있으면 성장에 도움이 돼요." },
      debtToEquity: { value: "31.2%", description: "부채비율", explanation: "자본 대비 부채의 비율이에요. 30% 미만이면 안전, 50% 이상이면 주의가 필요해요." },
      roe: { value: "26.4%", description: "ROE", explanation: "주주가 투자한 돈으로 얼마나 이익을 냈는지 보여줘요. 15% 이상이면 우수한 기업이에요!" },
      pe: { value: "28.5", description: "P/E 비율", explanation: "주가가 적정한지 보는 지표예요. 업계 평균과 비교해서 판단해야 해요." },
      marketCap: { value: "2.85T", description: "시가총액", explanation: "회사의 전체 가치예요. 주가 × 총 발행주식수로 계산해요." }
    }
  };

  const handleWatchlistToggle = () => {
    if (!isLoggedIn) {
      onLoginPrompt?.();
      return;
    }
    onToggleWatchlist?.();
  };

  const handleAlertToggle = () => {
    if (!isLoggedIn) {
      onLoginPrompt?.();
      return;
    }
    setAlertEnabled(!alertEnabled);
  };

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <button
            onClick={onBack}
            className="p-2 rounded-lg glass hover:glass-strong transition-all"
          >
            <ArrowLeft size={20} />
          </button>
          <div>
            <h1 className="text-xl font-bold">{symbol}</h1>
            <p className="text-sm text-foreground/70">{stock.name}</p>
          </div>
        </div>

        <div className="flex items-center space-x-2">
          {isLoggedIn && (
            <>
              <button
                onClick={handleAlertToggle}
                className={`p-2 rounded-lg transition-all ${
                  alertEnabled 
                    ? "glass text-blue-400" 
                    : "glass hover:glass-strong"
                }`}
              >
                <Bell size={20} />
              </button>
              <button
                onClick={handleWatchlistToggle}
                className={`p-2 rounded-lg transition-all ${
                  isInWatchlist 
                    ? "glass text-yellow-400" 
                    : "glass hover:glass-strong"
                }`}
              >
                <Star size={20} className={isInWatchlist ? "fill-current" : ""} />
              </button>
            </>
          )}
        </div>
      </div>

      {/* 가격 정보 */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <div className="text-3xl font-bold mb-1">
              ${stock.price.toFixed(2)}
            </div>
            <div className={`flex items-center space-x-2 ${
              stock.change >= 0 ? "text-green-400" : "text-red-400"
            }`}>
              {stock.change >= 0 ? <TrendingUp size={20} /> : <TrendingDown size={20} />}
              <span className="text-lg">
                {stock.change >= 0 ? "+" : ""}{stock.change.toFixed(2)}
              </span>
              <span className="text-sm">
                ({stock.changePercent >= 0 ? "+" : ""}{stock.changePercent.toFixed(2)}%)
              </span>
            </div>
          </div>
          <div className="text-right">
            <div className="text-sm text-foreground/60">{stock.sector}</div>
          </div>
        </div>
      </div>

      {/* 차트 */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold">가격 차트</h3>
          <div className="flex space-x-1">
            {["1D", "1W", "1M", "3M", "1Y"].map((period) => (
              <button
                key={period}
                onClick={() => setTimeframe(period)}
                className={`px-3 py-1 text-sm rounded-lg transition-all ${
                  timeframe === period
                    ? "glass text-primary"
                    : "hover:glass-subtle"
                }`}
              >
                {period}
              </button>
            ))}
          </div>
        </div>

        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
              <XAxis 
                dataKey="time" 
                stroke="rgba(255,255,255,0.6)"
                fontSize={12}
              />
              <YAxis 
                stroke="rgba(255,255,255,0.6)"
                fontSize={12}
                domain={['dataMin - 5', 'dataMax + 5']}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgba(0,0,0,0.8)",
                  border: "1px solid rgba(255,255,255,0.2)",
                  borderRadius: "8px",
                  color: "white"
                }}
              />
              <Line
                type="monotone"
                dataKey="price"
                stroke={stock.change >= 0 ? "#10b981" : "#ef4444"}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: stock.change >= 0 ? "#10b981" : "#ef4444" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* 기본 정보 */}
      <div className="glass-card rounded-2xl p-6">
        <h3 className="font-semibold mb-4 flex items-center">
          <Building className="mr-2" size={18} />
          기본 정보
        </h3>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <div className="text-sm text-foreground/60">섹터</div>
            <div className="font-medium">{stock.sector}</div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">현재가</div>
            <div className="font-medium">${stock.price.toFixed(2)}</div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">변동</div>
            <div className={`font-medium ${stock.change >= 0 ? "text-green-400" : "text-red-400"}`}>
              {stock.change >= 0 ? "+" : ""}{stock.changePercent.toFixed(2)}%
            </div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">거래량</div>
            <div className="font-medium">45.2M</div>
          </div>
        </div>
      </div>

      {/* 상세 분석 */}
      <div className="glass-card rounded-2xl p-6">
        <h3 className="font-semibold mb-4 flex items-center">
          <PieChart className="mr-2" size={18} />
          상세 분석
        </h3>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <div className="text-sm text-foreground/60">52주 최고</div>
            <div className="font-medium">$198.23</div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">52주 최저</div>
            <div className="font-medium">$124.17</div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">P/E 비율</div>
            <div className="font-medium">28.5</div>
          </div>
          <div>
            <div className="text-sm text-foreground/60">시가총액</div>
            <div className="font-medium">$2.85T</div>
          </div>
        </div>
      </div>

      {/* 재무제표 정보 (재무제표가 있는 회사만) */}
      {hasFinancials ? (
        <div className="space-y-6">
          {/* 재무 헤더 */}
          <div className="glass-card rounded-2xl p-6">
            <h3 className="font-semibold mb-4 flex items-center">
              <Calculator className="mr-2" size={18} />
              📈 재무제표 분석
            </h3>
            <div className="glass rounded-xl p-4 bg-blue-500/10">
              <div className="flex items-start space-x-2">
                <Info size={16} className="text-blue-400 mt-0.5" />
                <div>
                  <p className="text-sm font-medium mb-1">💡 재무제표란?</p>
                  <p className="text-xs text-foreground/70">
                    회사의 돈과 관련된 모든 상황을 보여주는 보고서예요. 
                    마치 개인의 가계부처럼 회사가 얼마나 건강한지 알 수 있어요!
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* 핵심 재무 지표 */}
          <div className="glass-card rounded-2xl p-6">
            <h4 className="font-semibold mb-4">💰 핵심 재무 지표</h4>
            <div className="grid grid-cols-1 gap-4">
              {Object.entries(financialData.metrics).map(([key, metric]) => (
                <div key={key} className="glass rounded-xl p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <DollarSign size={16} className="text-green-400" />
                      <span className="font-medium">{metric.description}</span>
                    </div>
                    <span className="text-lg font-semibold">${metric.value}</span>
                  </div>
                  <div className="glass rounded-lg p-3 bg-primary/5">
                    <p className="text-sm text-foreground/80 leading-relaxed">
                      {metric.explanation}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* 매출 추이 차트 */}
          <div className="glass-card rounded-2xl p-6">
            <h4 className="font-semibold mb-4">📊 매출 성장 추이</h4>
            <div className="h-48 mb-4">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={financialData.revenue}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                  <XAxis 
                    dataKey="year" 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
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
                  />
                  <Bar
                    dataKey="value"
                    fill="url(#revenueGradient)"
                    radius={[4, 4, 0, 0]}
                  />
                  <defs>
                    <linearGradient id="revenueGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.8} />
                      <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.3} />
                    </linearGradient>
                  </defs>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div className="glass rounded-xl p-3 bg-green-500/10">
              <p className="text-sm text-foreground/80">
                <span className="font-medium">📈 분석:</span> 최근 3년간 매출이 안정적으로 유지되고 있어요. 
                2024년에는 전년 대비 2% 성장하며 회복세를 보이고 있습니다.
              </p>
            </div>
          </div>

          {/* 재무 건전성 요약 */}
          <div className="glass-card rounded-2xl p-6">
            <h4 className="font-semibold mb-4">🏥 재무 건전성 진단</h4>
            <div className="space-y-4">
              <div className="glass rounded-xl p-4 bg-green-500/10 border-l-4 border-l-green-400">
                <h5 className="font-medium text-green-400 mb-2">✅ 강점</h5>
                <ul className="text-sm text-foreground/80 space-y-1">
                  <li>• 높은 수익성 (ROE 26.4%)</li>
                  <li>• 안정적인 부채 비율 (31.2%)</li>
                  <li>• 강력한 현금 창출 능력</li>
                </ul>
              </div>
              
              <div className="glass rounded-xl p-4 bg-yellow-500/10 border-l-4 border-l-yellow-400">
                <h5 className="font-medium text-yellow-400 mb-2">⚠️ 주의점</h5>
                <ul className="text-sm text-foreground/80 space-y-1">
                  <li>• P/E 비율이 다소 높음 (28.5)</li>
                  <li>• 시장 경쟁 심화로 인한 성장률 둔화</li>
                  <li>• 거시경제 변화에 따른 영향 주시 필요</li>
                </ul>
              </div>

              <div className="glass rounded-xl p-4 bg-blue-500/10 border-l-4 border-l-blue-400">
                <h5 className="font-medium text-blue-400 mb-2">📊 종합 평가</h5>
                <p className="text-sm text-foreground/80 leading-relaxed">
                  <span className="font-medium">우수한 재무 건전성</span>을 보이는 기업입니다. 
                  안정적인 수익 창출과 낮은 부채 비율로 투자 매력도가 높습니다. 
                  다만 현재 주가가 다소 높은 수준이므로 적절한 매수 시점을 고려해보세요.
                </p>
              </div>
            </div>
          </div>
        </div>
      ) : (
        /* 재무제표가 없는 회사 */
        <div className="glass-card rounded-2xl p-6">
          <h3 className="font-semibold mb-4 flex items-center">
            <Calculator className="mr-2" size={18} />
            재무 정보
          </h3>
          <div className="text-center py-8">
            <Building className="mx-auto mb-3 text-foreground/40" size={48} />
            <h4 className="font-medium mb-2">재무제표 정보 없음</h4>
            <p className="text-sm text-foreground/70">
              이 종목에 대한 상세한 재무제표 정보는<br/>
              현재 제공되지 않습니다.
            </p>
            {symbol.includes("BTC") || symbol.includes("ETH") ? (
              <div className="mt-4 glass rounded-xl p-4 bg-orange-500/10">
                <p className="text-sm text-foreground/80">
                  💡 암호화폐는 기업이 아니기 때문에 재무제표가 없어요. 
                  대신 시장 데이터와 기술적 분석을 참고하세요!
                </p>
              </div>
            ) : (
              <div className="mt-4 text-xs text-foreground/60">
                주식 종목의 경우 추후 재무 데이터가 추가될 예정입니다.
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}