import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line } from "recharts";
import { TrendingUp, TrendingDown, DollarSign, Users, Building, PieChart, Lock } from "lucide-react";

interface FinancialAnalysisProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

export function FinancialAnalysis({ isLoggedIn, onLoginPrompt }: FinancialAnalysisProps) {
  const [selectedStock, setSelectedStock] = useState("AAPL");
  const [selectedMetric, setSelectedMetric] = useState("revenue");

  const stocks = [
    { symbol: "AAPL", name: "Apple Inc." },
    { symbol: "MSFT", name: "Microsoft" },
    { symbol: "GOOGL", name: "Alphabet" },
    { symbol: "TSLA", name: "Tesla" }
  ];

  const financialData = {
    revenue: [
      { year: "2020", value: 274.5, growth: 5.5 },
      { year: "2021", value: 365.8, growth: 33.3 },
      { year: "2022", value: 394.3, growth: 7.8 },
      { year: "2023", value: 383.3, growth: -2.8 },
      { year: "2024", value: 391.0, growth: 2.0 }
    ],
    profit: [
      { year: "2020", value: 57.4, margin: 20.9 },
      { year: "2021", value: 94.7, margin: 25.9 },
      { year: "2022", value: 99.8, margin: 25.3 },
      { year: "2023", value: 97.0, margin: 25.3 },
      { year: "2024", value: 101.5, margin: 26.0 }
    ]
  };

  const keyMetrics = [
    { 
      icon: DollarSign, 
      label: "매출액", 
      value: "$391.0B", 
      change: "+2.0%", 
      positive: true,
      description: "전년 대비 성장률"
    },
    { 
      icon: TrendingUp, 
      label: "순이익", 
      value: "$101.5B", 
      change: "+4.6%", 
      positive: true,
      description: "순이익률 26.0%"
    },
    { 
      icon: Users, 
      label: "P/E 비율", 
      value: "28.5", 
      change: "-5.2%", 
      positive: false,
      description: "업계 평균 대비"
    },
    { 
      icon: Building, 
      label: "부채비율", 
      value: "31.2%", 
      change: "-2.1%", 
      positive: true,
      description: "안정적 재무구조"
    }
  ];

  return (
    <div className="space-y-6">
      {/* 로그인 유도 배너 (게스트용) */}
      {!isLoggedIn && (
        <div className="glass-card rounded-2xl p-4 border border-primary/30">
          <div className="text-center">
            <PieChart className="mx-auto mb-3 text-primary" size={32} />
            <h3 className="font-semibold mb-2">📊 전문가급 재무 분석</h3>
            <p className="text-sm text-foreground/70 mb-4">
              로그인하면 상세한 재무제표 분석, 비교 분석, AI 투자 조언 등을 이용할 수 있어요.
            </p>
            <button
              onClick={onLoginPrompt}
              className="px-4 py-2 bg-primary/20 text-primary rounded-xl text-sm font-medium hover:bg-primary/30 transition-colors"
            >
              로그인하고 전체 분석 보기
            </button>
          </div>
        </div>
      )}

      {/* 종목 선택 */}
      <div className="glass-card rounded-2xl p-4">
        <h3 className="font-semibold mb-3">📈 종목 선택</h3>
        <div className="grid grid-cols-2 gap-2">
          {stocks.map((stock) => (
            <button
              key={stock.symbol}
              onClick={() => setSelectedStock(stock.symbol)}
              className={`p-3 rounded-xl text-left transition-all ${
                selectedStock === stock.symbol
                  ? "bg-primary/20 text-primary border border-primary/30"
                  : "glass hover:bg-white/10"
              }`}
            >
              <div className="font-medium">{stock.symbol}</div>
              <div className="text-sm text-foreground/70">{stock.name}</div>
            </button>
          ))}
        </div>
      </div>

      {/* 핵심 지표 */}
      <div className="glass-card rounded-2xl p-4">
        <h3 className="font-semibold mb-4">💰 핵심 재무 지표</h3>
        <div className="grid grid-cols-2 gap-4">
          {keyMetrics.map((metric, index) => {
            const Icon = metric.icon;
            return (
              <div key={index} className={`glass rounded-xl p-4 ${!isLoggedIn && index >= 2 ? 'relative' : ''}`}>
                {!isLoggedIn && index >= 2 && (
                  <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-xl z-10 flex items-center justify-center">
                    <Lock className="text-primary" size={20} />
                  </div>
                )}
                
                <div className="flex items-center space-x-2 mb-2">
                  <Icon size={16} className="text-primary" />
                  <span className="text-sm font-medium">{metric.label}</span>
                </div>
                <div className="text-lg font-semibold mb-1">{metric.value}</div>
                <div className={`flex items-center space-x-1 text-sm ${
                  metric.positive ? "text-green-400" : "text-red-400"
                }`}>
                  {metric.positive ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
                  <span>{metric.change}</span>
                </div>
                <div className="text-xs text-foreground/60 mt-1">{metric.description}</div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 매출 추이 차트 */}
      <div className={`glass-card rounded-2xl p-6 ${!isLoggedIn ? 'relative' : ''}`}>
        {!isLoggedIn && (
          <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-2xl z-10 flex items-center justify-center">
            <div className="text-center">
              <Lock className="mx-auto mb-2 text-primary" size={32} />
              <p className="font-medium mb-1">상세 차트 분석</p>
              <p className="text-sm text-foreground/70 mb-3">
                5년간 재무 데이터와 성장 추이 분석
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

        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold">📊 매출 성장 추이</h3>
          <div className="flex space-x-2">
            <button
              onClick={() => setSelectedMetric("revenue")}
              className={`px-3 py-1 text-sm rounded-lg transition-colors ${
                selectedMetric === "revenue"
                  ? "bg-primary/20 text-primary"
                  : "hover:bg-white/10"
              }`}
            >
              매출
            </button>
            <button
              onClick={() => setSelectedMetric("profit")}
              className={`px-3 py-1 text-sm rounded-lg transition-colors ${
                selectedMetric === "profit"
                  ? "bg-primary/20 text-primary"
                  : "hover:bg-white/10"
              }`}
            >
              순이익
            </button>
          </div>
        </div>

        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={financialData[selectedMetric as keyof typeof financialData]}>
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
                fill="url(#gradient)"
                radius={[4, 4, 0, 0]}
              />
              <defs>
                <linearGradient id="gradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.8} />
                  <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.3} />
                </linearGradient>
              </defs>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* AI 분석 (프리미엄) */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold flex items-center">
            🤖 AI 재무 분석
          </h3>
          {!isLoggedIn && (
            <div className="flex items-center space-x-1 text-xs text-yellow-400 bg-yellow-400/20 px-2 py-1 rounded-md">
              <Lock size={10} />
              <span>프리미엄</span>
            </div>
          )}
        </div>

        {isLoggedIn ? (
          <div className="space-y-4">
            <div className="glass rounded-xl p-4">
              <h4 className="font-medium mb-2 text-green-400">✅ 강점</h4>
              <ul className="text-sm text-foreground/80 space-y-1">
                <li>• 안정적인 현금흐름과 높은 수익성</li>
                <li>• 강력한 브랜드 파워와 생태계</li>
                <li>• 지속적인 혁신과 R&D 투자</li>
              </ul>
            </div>
            
            <div className="glass rounded-xl p-4">
              <h4 className="font-medium mb-2 text-yellow-400">⚠️ 주의점</h4>
              <ul className="text-sm text-foreground/80 space-y-1">
                <li>• 중국 시장 의존도 높음</li>
                <li>• 성장률 둔화 우려</li>
                <li>• 경쟁 심화로 인한 마진 압박</li>
              </ul>
            </div>

            <div className="glass rounded-xl p-4">
              <h4 className="font-medium mb-2 text-blue-400">📈 투자 의견</h4>
              <p className="text-sm text-foreground/80">
                <strong>추천 (BUY)</strong> - 목표주가 $210, 현재가 대비 13% 상승 여력
              </p>
            </div>
          </div>
        ) : (
          <div className="text-center py-8">
            <Lock className="mx-auto mb-3 text-primary" size={32} />
            <h4 className="font-medium mb-2">AI 재무 분석</h4>
            <p className="text-sm text-foreground/70 mb-4">
              인공지능이 분석한 종목별 투자 의견과<br/>
              상세한 재무 건전성 평가를 확인하세요.
            </p>
            <button
              onClick={onLoginPrompt}
              className="px-4 py-2 bg-primary/20 text-primary rounded-xl text-sm hover:bg-primary/30 transition-colors"
            >
              AI 분석 보기
            </button>
          </div>
        )}
      </div>
    </div>
  );
}