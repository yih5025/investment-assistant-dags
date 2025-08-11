import { useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { TrendingUp, TrendingDown, Info, BarChart3, GitCompare, Lock } from "lucide-react";

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

export function EconomicDashboard({ isLoggedIn, onLoginPrompt }: EconomicDashboardProps) {
  const [selectedIndicator, setSelectedIndicator] = useState<string>("treasuryRate");
  const [correlationMode, setCorrelationMode] = useState<boolean>(false);
  const [correlationPair, setCorrelationPair] = useState<{first: string, second: string}>({
    first: "fedRate",
    second: "treasuryRate"
  });

  // 10년간 경제 지표 모의 데이터
  const economicData: EconomicIndicator[] = [
    { year: 2014, treasuryRate: 2.54, fedRate: 0.25, cpi: 236.7, inflation: 0.1 },
    { year: 2015, treasuryRate: 2.14, fedRate: 0.25, cpi: 237.0, inflation: 0.1 },
    { year: 2016, treasuryRate: 2.45, fedRate: 0.50, cpi: 240.0, inflation: 1.3 },
    { year: 2017, treasuryRate: 2.33, fedRate: 1.50, cpi: 245.1, inflation: 2.1 },
    { year: 2018, treasuryRate: 2.91, fedRate: 2.25, cpi: 251.1, inflation: 2.4 },
    { year: 2019, treasuryRate: 2.14, fedRate: 1.75, cpi: 255.7, inflation: 1.8 },
    { year: 2020, treasuryRate: 0.89, fedRate: 0.25, cpi: 258.8, inflation: 1.2 },
    { year: 2021, treasuryRate: 1.44, fedRate: 0.25, cpi: 270.9, inflation: 4.7 },
    { year: 2022, treasuryRate: 3.01, fedRate: 4.25, cpi: 292.7, inflation: 8.0 },
    { year: 2023, treasuryRate: 4.05, fedRate: 5.25, cpi: 307.0, inflation: 4.9 },
    { year: 2024, treasuryRate: 4.25, fedRate: 5.25, cpi: 310.3, inflation: 3.2 },
  ];

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
  const currentValue = economicData[economicData.length - 1][selectedIndicator as keyof EconomicIndicator];
  const previousValue = economicData[economicData.length - 2][selectedIndicator as keyof EconomicIndicator];
  const change = currentValue - previousValue;
  const changePercent = ((change / previousValue) * 100);

  const formatTooltipValue = (value: any, name: string) => {
    const indicator = indicators.find(ind => ind.key === name);
    return [`${value}${indicator?.unit || ""}`, indicator?.name || name];
  };

  const getCorrelationDescription = () => {
    const pair = correlationPairs.find(p => 
      (p.first === correlationPair.first && p.second === correlationPair.second) ||
      (p.first === correlationPair.second && p.second === correlationPair.first)
    );
    return pair?.description || "두 지표 간의 관계를 분석해보세요.";
  };

  const handleIndicatorClick = (indicator: any) => {
    if (indicator.premium) {
      onLoginPrompt();
      return;
    }
    setSelectedIndicator(indicator.key);
  };

  const handleCorrelationClick = (pair: any) => {
    if (pair.premium) {
      onLoginPrompt();
      return;
    }
    setCorrelationPair({ first: pair.first, second: pair.second });
  };

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="glass-card rounded-2xl p-6">
        <h2 className="text-lg font-semibold mb-4 flex items-center">
          <BarChart3 className="mr-2" size={20} />
          📊 경제 지표 대시보드
        </h2>
        <p className="text-sm text-foreground/70">
          미국의 주요 경제 지표들의 10년간 추이를 확인하고, 각 지표들 간의 상관관계를 이해해보세요.
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
          <div className="grid grid-cols-2 gap-3">
            {indicators.map((indicator) => {
              const isSelected = selectedIndicator === indicator.key;
              const value = economicData[economicData.length - 1][indicator.key as keyof EconomicIndicator];
              const prevValue = economicData[economicData.length - 2][indicator.key as keyof EconomicIndicator];
              const indicatorChange = value - prevValue;
              
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
                    {indicator.premium ? "••••" : `${value}${indicator.unit}`}
                  </div>
                  <div className={`text-xs ${
                    indicator.premium ? "text-foreground/40" : 
                    indicatorChange >= 0 ? "text-green-400" : "text-red-400"
                  }`}>
                    {indicator.premium ? "로그인 필요" : 
                     `${indicatorChange >= 0 ? "+" : ""}${indicatorChange.toFixed(2)}${indicator.unit}`}
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
                    {currentValue}{currentIndicator.unit}
                  </div>
                  <div className={`text-sm flex items-center justify-end ${
                    change >= 0 ? "text-green-400" : "text-red-400"
                  }`}>
                    {change >= 0 ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
                    <span className="ml-1">
                      {change >= 0 ? "+" : ""}{change.toFixed(2)}{currentIndicator.unit} 
                      ({changePercent >= 0 ? "+" : ""}{changePercent.toFixed(1)}%)
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
          <div className={`glass-card rounded-2xl p-6 ${!currentIndicator || currentIndicator.premium ? 'relative' : ''}`}>
            {(!currentIndicator || currentIndicator.premium) && (
              <div className="absolute inset-0 bg-black/50 backdrop-blur-sm rounded-2xl z-10 flex items-center justify-center">
                <div className="text-center">
                  <Lock className="mx-auto mb-2 text-primary" size={32} />
                  <p className="font-medium mb-1">상세 차트 분석</p>
                  <p className="text-sm text-foreground/70 mb-3">
                    모든 경제 지표의 10년 추이를 확인하세요
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

            <h3 className="font-semibold mb-4">10년간 추이</h3>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={economicData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                  <XAxis 
                    dataKey="year" 
                    stroke="rgba(255,255,255,0.6)"
                    fontSize={12}
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
              • 저인플레이션: 금융자산(주식, 채권) 선호
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}