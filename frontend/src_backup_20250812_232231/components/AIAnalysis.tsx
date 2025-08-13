import { useState, useEffect } from "react";
import { Bot, TrendingUp, TrendingDown, MessageSquare, Search, Eye, Lock, Lightbulb, Target, BarChart3, Users, Brain, Zap } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

interface AIAnalysisProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

interface SentimentData {
  symbol: string;
  sentiment: "positive" | "negative" | "neutral";
  score: number;
  posts: number;
  influencer: string;
  keywords: string[];
  marketImpact: "high" | "medium" | "low";
}

interface AIRecommendation {
  symbol: string;
  name: string;
  action: "buy" | "sell" | "hold";
  confidence: number;
  reasoning: string[];
  targetPrice: number;
  currentPrice: number;
  timeframe: string;
}

export function AIAnalysis({ isLoggedIn, onLoginPrompt }: AIAnalysisProps) {
  const [activeTab, setActiveTab] = useState<"sentiment" | "recommendations" | "process">("sentiment");
  const [selectedStock, setSelectedStock] = useState<string>("TSLA");
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  // 감성 분석 데이터
  const sentimentData: SentimentData[] = [
    {
      symbol: "TSLA",
      sentiment: "positive",
      score: 0.82,
      posts: 1247,
      influencer: "일론 머스크",
      keywords: ["자율주행", "FSD", "혁신", "미래"],
      marketImpact: "high"
    },
    {
      symbol: "AAPL",
      sentiment: "neutral",
      score: 0.15,
      posts: 892,
      influencer: "팀 쿡",
      keywords: ["아이폰", "실적", "안정성"],
      marketImpact: "medium"
    },
    {
      symbol: "NVDA",
      sentiment: "positive",
      score: 0.75,
      posts: 634,
      influencer: "젠슨 황",
      keywords: ["AI", "칩", "성장", "혁신"],
      marketImpact: "high"
    },
    {
      symbol: "META",
      sentiment: "negative",
      score: -0.34,
      posts: 445,
      influencer: "마크 저커버그",
      keywords: ["메타버스", "투자", "손실"],
      marketImpact: "medium"
    }
  ];

  // AI 추천 데이터
  const recommendations: AIRecommendation[] = [
    {
      symbol: "NVDA",
      name: "NVIDIA Corp.",
      action: "buy",
      confidence: 78,
      reasoning: [
        "AI 칩 수요 급증으로 매출 성장 지속",
        "경쟁사 대비 기술적 우위 확보",
        "데이터센터 사업 확장 가속화"
      ],
      targetPrice: 950,
      currentPrice: 875.23,
      timeframe: "3-6개월"
    },
    {
      symbol: "TSLA",
      name: "Tesla Inc.",
      action: "hold",
      confidence: 65,
      reasoning: [
        "자율주행 기술 발전으로 장기 성장 동력 확보",
        "단기적으로 경쟁 심화와 가격 압박 존재",
        "중국 시장 회복 여부가 핵심 변수"
      ],
      targetPrice: 280,
      currentPrice: 248.42,
      timeframe: "6-12개월"
    },
    {
      symbol: "META",
      name: "Meta Platforms",
      action: "sell",
      confidence: 45,
      reasoning: [
        "메타버스 투자 대비 수익 창출 지연",
        "광고 시장 경쟁 심화",
        "규제 리스크 증가"
      ],
      targetPrice: 320,
      currentPrice: 352.18,
      timeframe: "1-3개월"
    }
  ];

  // AI 분석 과정 데이터
  const analysisSteps = [
    {
      step: 1,
      title: "데이터 수집",
      description: "SNS, 뉴스, 재무 데이터 등 다양한 소스에서 정보 수집",
      status: "completed",
      sources: ["Twitter/X", "Reddit", "뉴스 기사", "재무제표", "시장 데이터"]
    },
    {
      step: 2,
      title: "텍스트 분석",
      description: "자연어 처리를 통한 감성 분석 및 키워드 추출",
      status: "completed",
      details: "10,000+ 포스트 분석, 85% 정확도"
    },
    {
      step: 3,
      title: "시장 영향도 계산",
      description: "인플루언서 영향력과 시장 반응 상관관계 분석",
      status: "completed",
      accuracy: "78%"
    },
    {
      step: 4,
      title: "예측 모델 실행",
      description: "머신러닝 모델을 통한 가격 예측 및 추천 생성",
      status: "processing",
      confidence: "65-78%"
    }
  ];

  const sentimentChartData = sentimentData.map(item => ({
    name: item.symbol,
    value: item.score * 100,
    posts: item.posts
  }));

  const confidenceData = [
    { name: "높은 신뢰도", value: 35, color: "#10b981" },
    { name: "보통 신뢰도", value: 45, color: "#f59e0b" },
    { name: "낮은 신뢰도", value: 20, color: "#ef4444" }
  ];

  const getSentimentColor = (sentiment: string, score: number) => {
    if (sentiment === "positive") return "text-green-400";
    if (sentiment === "negative") return "text-red-400";
    return "text-yellow-400";
  };

  const getActionColor = (action: string) => {
    switch (action) {
      case "buy": return "text-green-400";
      case "sell": return "text-red-400";
      case "hold": return "text-yellow-400";
      default: return "text-foreground";
    }
  };

  const getActionIcon = (action: string) => {
    switch (action) {
      case "buy": return <TrendingUp size={16} className="text-green-400" />;
      case "sell": return <TrendingDown size={16} className="text-red-400" />;
      case "hold": return <Target size={16} className="text-yellow-400" />;
      default: return null;
    }
  };

  const startAnalysis = () => {
    if (!isLoggedIn) {
      onLoginPrompt();
      return;
    }
    setIsAnalyzing(true);
    setTimeout(() => setIsAnalyzing(false), 3000);
  };

  return (
    <div className="space-y-6">
      {/* AI 소개 */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center space-x-3 mb-4">
          <Bot className="text-primary" size={24} />
          <h2 className="text-lg font-semibold">🤖 W.E.I AI 분석 시스템</h2>
        </div>
        <p className="text-sm text-foreground/70 mb-4">
          SNS 감성 분석, 시장 데이터, 그리고 투명한 분석 과정을 통해 투자 인사이트를 제공합니다.
        </p>
        
        <div className="glass rounded-xl p-4 bg-orange-500/10 border-l-4 border-l-orange-400">
          <p className="text-sm text-foreground/80">
            <span className="font-medium">⚠️ 중요:</span> AI도 인간의 광기는 예상하지 못합니다. 
            모든 분석 결과는 참고용이며, 투자 결정은 신중하게 하세요!
          </p>
        </div>

        {!isLoggedIn && (
          <div className="mt-4 p-4 glass rounded-xl border border-primary/30">
            <div className="flex items-center space-x-2">
              <Lock className="text-primary" size={16} />
              <p className="text-sm flex-1">
                <span className="font-medium">로그인하면</span> 모든 AI 분석 기능을 무제한으로 이용할 수 있어요.
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

      {/* 탭 네비게이션 */}
      <div className="flex space-x-1 glass-card rounded-2xl p-1">
        {[
          { key: "sentiment", label: "감성 분석", icon: MessageSquare },
          { key: "recommendations", label: "AI 추천", icon: Target },
          { key: "process", label: "분석 과정", icon: Search }
        ].map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key as any)}
              className={`flex-1 flex items-center justify-center space-x-2 py-3 px-4 rounded-xl transition-all ${
                activeTab === tab.key
                  ? "bg-primary/20 text-primary border border-primary/30"
                  : "hover:bg-white/10"
              }`}
            >
              <Icon size={16} />
              <span className="text-sm font-medium">{tab.label}</span>
            </button>
          );
        })}
      </div>

      {/* 감성 분석 탭 */}
      {activeTab === "sentiment" && (
        <div className="space-y-6">
          <div className="glass-card rounded-2xl p-6">
            <h3 className="font-semibold mb-4 flex items-center">
              <MessageSquare className="mr-2" size={18} />
              📱 SNS 키워드 감성 분석
            </h3>
            
            {isLoggedIn ? (
              <div className="space-y-4">
                {sentimentData.map((item) => (
                  <div key={item.symbol} className="glass rounded-xl p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-3">
                        <span className="font-semibold">{item.symbol}</span>
                        <span className={`text-sm ${getSentimentColor(item.sentiment, item.score)}`}>
                          {item.sentiment === "positive" ? "긍정적" : 
                           item.sentiment === "negative" ? "부정적" : "중립적"}
                        </span>
                      </div>
                      <div className="text-right">
                        <div className="text-lg font-semibold">
                          {item.score > 0 ? "+" : ""}{(item.score * 100).toFixed(0)}%
                        </div>
                        <div className="text-xs text-foreground/60">{item.posts} 포스트</div>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4 mb-3">
                      <div>
                        <div className="text-xs text-foreground/60">주요 인플루언서</div>
                        <div className="text-sm font-medium">{item.influencer}</div>
                      </div>
                      <div>
                        <div className="text-xs text-foreground/60">시장 영향도</div>
                        <div className={`text-sm font-medium ${
                          item.marketImpact === "high" ? "text-red-400" : 
                          item.marketImpact === "medium" ? "text-yellow-400" : "text-green-400"
                        }`}>
                          {item.marketImpact === "high" ? "높음" : 
                           item.marketImpact === "medium" ? "보통" : "낮음"}
                        </div>
                      </div>
                    </div>

                    <div className="flex flex-wrap gap-1">
                      {item.keywords.map((keyword) => (
                        <span
                          key={keyword}
                          className="text-xs px-2 py-1 bg-primary/20 text-primary rounded-md"
                        >
                          #{keyword}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <MessageSquare className="mx-auto mb-3 text-primary" size={48} />
                <h4 className="font-medium mb-2">실시간 감성 분석</h4>
                <p className="text-sm text-foreground/70 mb-4">
                  일론 머스크, 트럼프 등 영향력 있는 인물들의<br/>
                  포스트를 실시간으로 분석해서 시장 영향도를 측정해요.
                </p>
                <button
                  onClick={onLoginPrompt}
                  className="px-4 py-2 bg-primary/20 text-primary rounded-xl font-medium hover:bg-primary/30 transition-colors"
                >
                  감성 분석 보기
                </button>
              </div>
            )}
          </div>

          {/* 감성 점수 차트 */}
          {isLoggedIn && (
            <div className="glass-card rounded-2xl p-6">
              <h4 className="font-semibold mb-4">📊 감성 점수 분포</h4>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={sentimentChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                    <XAxis dataKey="name" stroke="rgba(255,255,255,0.6)" fontSize={12} />
                    <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
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
                      dataKey="value"
                      stroke="#3b82f6"
                      strokeWidth={3}
                      dot={{ fill: "#3b82f6", strokeWidth: 2, r: 6 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
        </div>
      )}

      {/* AI 추천 탭 */}
      {activeTab === "recommendations" && (
        <div className="space-y-6">
          <div className="glass-card rounded-2xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="font-semibold flex items-center">
                <Target className="mr-2" size={18} />
                🎯 AI 투자 추천
              </h3>
              <button
                onClick={startAnalysis}
                disabled={isAnalyzing}
                className="px-3 py-1.5 bg-primary/20 text-primary rounded-lg text-sm hover:bg-primary/30 transition-colors disabled:opacity-50"
              >
                {isAnalyzing ? (
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 border border-primary border-t-transparent rounded-full animate-spin"></div>
                    <span>분석 중...</span>
                  </div>
                ) : (
                  "새로고침"
                )}
              </button>
            </div>

            {isLoggedIn ? (
              <div className="space-y-4">
                {recommendations.map((rec) => (
                  <div key={rec.symbol} className="glass rounded-xl p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-3">
                        <span className="font-semibold">{rec.symbol}</span>
                        <span className="text-sm text-foreground/70">{rec.name}</span>
                      </div>
                      <div className="flex items-center space-x-2">
                        {getActionIcon(rec.action)}
                        <span className={`font-medium uppercase ${getActionColor(rec.action)}`}>
                          {rec.action === "buy" ? "매수" : rec.action === "sell" ? "매도" : "보유"}
                        </span>
                      </div>
                    </div>

                    <div className="grid grid-cols-3 gap-4 mb-4">
                      <div>
                        <div className="text-xs text-foreground/60">신뢰도</div>
                        <div className="text-sm font-medium">{rec.confidence}%</div>
                      </div>
                      <div>
                        <div className="text-xs text-foreground/60">목표가</div>
                        <div className="text-sm font-medium">${rec.targetPrice}</div>
                      </div>
                      <div>
                        <div className="text-xs text-foreground/60">기간</div>
                        <div className="text-sm font-medium">{rec.timeframe}</div>
                      </div>
                    </div>

                    <div className="glass rounded-lg p-3 bg-blue-500/10">
                      <h5 className="text-sm font-medium mb-2">💡 분석 근거</h5>
                      <ul className="text-xs text-foreground/80 space-y-1">
                        {rec.reasoning.map((reason, index) => (
                          <li key={index}>• {reason}</li>
                        ))}
                      </ul>
                    </div>

                    <div className="mt-3 flex items-center justify-between text-xs text-foreground/60">
                      <span>현재가: ${rec.currentPrice}</span>
                      <span className={
                        rec.targetPrice > rec.currentPrice ? "text-green-400" : "text-red-400"
                      }>
                        {rec.targetPrice > rec.currentPrice ? "+" : ""}
                        {(((rec.targetPrice - rec.currentPrice) / rec.currentPrice) * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <Target className="mx-auto mb-3 text-primary" size={48} />
                <h4 className="font-medium mb-2">AI 투자 추천</h4>
                <p className="text-sm text-foreground/70 mb-4">
                  데이터 분석 기반의 신중한 투자 추천을<br/>
                  신뢰도와 함께 제공해드려요.
                </p>
                <button
                  onClick={onLoginPrompt}
                  className="px-4 py-2 bg-primary/20 text-primary rounded-xl font-medium hover:bg-primary/30 transition-colors"
                >
                  AI 추천 보기
                </button>
              </div>
            )}
          </div>

          {/* 신뢰도 분포 */}
          {isLoggedIn && (
            <div className="glass-card rounded-2xl p-6">
              <h4 className="font-semibold mb-4">📈 추천 신뢰도 분포</h4>
              <div className="h-48">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={confidenceData}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={80}
                      paddingAngle={5}
                      dataKey="value"
                    >
                      {confidenceData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="flex justify-center space-x-4 mt-4">
                {confidenceData.map((item) => (
                  <div key={item.name} className="flex items-center space-x-2">
                    <div 
                      className="w-3 h-3 rounded-full" 
                      style={{ backgroundColor: item.color }}
                    ></div>
                    <span className="text-sm">{item.name}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* 분석 과정 탭 */}
      {activeTab === "process" && (
        <div className="space-y-6">
          <div className="glass-card rounded-2xl p-6">
            <h3 className="font-semibold mb-4 flex items-center">
              <Search className="mr-2" size={18} />
              🔍 분석 과정 투명화
            </h3>

            {isLoggedIn ? (
              <div className="space-y-4">
                {analysisSteps.map((step) => (
                  <div key={step.step} className="glass rounded-xl p-4">
                    <div className="flex items-center space-x-3 mb-3">
                      <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium ${
                        step.status === "completed" ? "bg-green-500/20 text-green-400" :
                        step.status === "processing" ? "bg-blue-500/20 text-blue-400" :
                        "bg-gray-500/20 text-gray-400"
                      }`}>
                        {step.step}
                      </div>
                      <div className="flex-1">
                        <h4 className="font-medium">{step.title}</h4>
                        <p className="text-sm text-foreground/70">{step.description}</p>
                      </div>
                      <div className={`text-xs px-2 py-1 rounded-md ${
                        step.status === "completed" ? "bg-green-500/20 text-green-400" :
                        step.status === "processing" ? "bg-blue-500/20 text-blue-400" :
                        "bg-gray-500/20 text-gray-400"
                      }`}>
                        {step.status === "completed" ? "완료" :
                         step.status === "processing" ? "진행중" : "대기"}
                      </div>
                    </div>

                    {step.sources && (
                      <div className="glass rounded-lg p-3 bg-primary/5">
                        <div className="text-xs text-foreground/60 mb-2">데이터 소스:</div>
                        <div className="flex flex-wrap gap-1">
                          {step.sources.map((source) => (
                            <span key={source} className="text-xs px-2 py-1 bg-primary/20 text-primary rounded-md">
                              {source}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {step.details && (
                      <div className="text-xs text-foreground/60 mt-2">
                        📊 {step.details}
                      </div>
                    )}

                    {step.accuracy && (
                      <div className="text-xs text-foreground/60 mt-2">
                        🎯 정확도: {step.accuracy}
                      </div>
                    )}

                    {step.confidence && (
                      <div className="text-xs text-foreground/60 mt-2">
                        💪 신뢰도: {step.confidence}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <Search className="mx-auto mb-3 text-primary" size={48} />
                <h4 className="font-medium mb-2">투명한 분석 과정</h4>
                <p className="text-sm text-foreground/70 mb-4">
                  AI가 어떻게 분석하는지 모든 과정을<br/>
                  투명하게 공개해서 신뢰도를 높여요.
                </p>
                <button
                  onClick={onLoginPrompt}
                  className="px-4 py-2 bg-primary/20 text-primary rounded-xl font-medium hover:bg-primary/30 transition-colors"
                >
                  분석 과정 보기
                </button>
              </div>
            )}
          </div>

          {/* AI 한계점 및 주의사항 */}
          <div className="glass-card rounded-2xl p-6">
            <h4 className="font-semibold mb-4 flex items-center">
              <Brain className="mr-2 text-orange-400" size={18} />
              🧠 AI의 한계점
            </h4>
            <div className="space-y-4">
              <div className="glass rounded-xl p-4 bg-red-500/10 border-l-4 border-l-red-400">
                <h5 className="font-medium text-red-400 mb-2">⚠️ 예측 불가능한 요소들</h5>
                <ul className="text-sm text-foreground/80 space-y-1">
                  <li>• 갑작스러운 시장 충격 (블랙 스완 이벤트)</li>
                  <li>• 지정학적 리스크와 정치적 변화</li>
                  <li>• 투자자들의 감정적 반응과 패닉</li>
                  <li>• 규제 변화 및 정책 전환</li>
                </ul>
              </div>

              <div className="glass rounded-xl p-4 bg-yellow-500/10 border-l-4 border-l-yellow-400">
                <h5 className="font-medium text-yellow-400 mb-2">🎯 올바른 활용법</h5>
                <ul className="text-sm text-foreground/80 space-y-1">
                  <li>• AI 분석은 참고 자료로만 활용하세요</li>
                  <li>• 여러 정보원을 비교 검토하세요</li>
                  <li>• 본인만의 투자 원칙을 세우세요</li>
                  <li>• 손실 감수 가능한 범위 내에서 투자하세요</li>
                </ul>
              </div>

              <div className="glass rounded-xl p-4 bg-blue-500/10 border-l-4 border-l-blue-400">
                <h5 className="font-medium text-blue-400 mb-2">💡 지속적인 개선</h5>
                <p className="text-sm text-foreground/80">
                  W.E.I AI는 시장 피드백과 새로운 데이터를 바탕으로 지속적으로 학습하고 개선됩니다. 
                  더 정확하고 신뢰할 수 있는 분석을 위해 노력하고 있어요.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}