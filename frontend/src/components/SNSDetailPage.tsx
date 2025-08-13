import { useState } from "react";
import { ArrowLeft, ExternalLink, TrendingUp, TrendingDown, BarChart, Clock, Target, AlertTriangle } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart as ReBarChart, Bar } from "recharts";
import { Badge } from "./ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";

interface SNSPost {
  id: string;
  content: string;
  author: string;
  platform: "X" | "Truth Social";
  category?: string;
  timestamp: string;
  likes?: number;
  retweets?: number;
  replies?: number;
  verified: boolean;
  profileImage: string;
  hasMarketImpact: boolean;
  impactScore?: number;
}

interface MarketImpactData {
  post_analysis: {
    post_id: string;
    post_content: string;
    post_time: string;
    author: string;
    affected_symbols: string[];
    price_analysis: {
      [symbol: string]: {
        price_before_30m: number;
        price_at_post: number;
        price_after_1h: number;
        price_after_6h: number;
        price_after_24h: number;
        change_1h: number;
        change_6h: number;
        change_24h: number;
        volume_before: number;
        volume_after: number;
        volume_spike: number;
        volatility_increase: number;
      };
    };
  };
  statistical_analysis: {
    [symbol: string]: {
      correlation_analysis: {
        pearson_correlation: number;
        p_value: number;
        is_significant: boolean;
        confidence_level: number;
      };
      time_series_analysis: {
        trend_break: boolean;
        change_point: string;
        anomaly_score: number;
        pattern_deviation: string;
      };
      control_group_comparison: {
        same_time_last_week: number;
        difference: number;
        comparison_result: string;
      };
    };
  };
}

interface SNSDetailPageProps {
  post: SNSPost;
  onBack: () => void;
}

// 모의 시장 영향도 데이터
const mockMarketData: MarketImpactData = {
  post_analysis: {
    post_id: "1951082319277859202",
    post_content: "The future of cryptocurrency is looking incredibly promising. Major institutional adoption is accelerating faster than ever before. 🚀 #Crypto #Bitcoin",
    post_time: "2025-01-28T09:47:28.000Z",
    author: "elonmusk",
    affected_symbols: ["TSLA", "DOGE", "BTC-USD"],
    price_analysis: {
      "TSLA": {
        price_before_30m: 248.50,
        price_at_post: 247.80,
        price_after_1h: 245.20,
        price_after_6h: 243.10,
        price_after_24h: 241.90,
        change_1h: -1.05,
        change_6h: -2.08,
        change_24h: -2.65,
        volume_before: 15234000,
        volume_after: 23891000,
        volume_spike: 56.8,
        volatility_increase: 23.4
      },
      "DOGE": {
        price_before_30m: 0.1234,
        price_at_post: 0.1229,
        price_after_1h: 0.1189,
        price_after_6h: 0.1156,
        price_after_24h: 0.1198,
        change_1h: -3.25,
        change_6h: -5.93,
        change_24h: -2.52,
        volume_before: 892341000,
        volume_after: 1590876000,
        volume_spike: 78.2,
        volatility_increase: 45.7
      },
      "BTC-USD": {
        price_before_30m: 67840.50,
        price_at_post: 67920.30,
        price_after_1h: 68450.80,
        price_after_6h: 69120.40,
        price_after_24h: 69890.20,
        change_1h: 0.78,
        change_6h: 1.77,
        change_24h: 2.90,
        volume_before: 28451000000,
        volume_after: 45678000000,
        volume_spike: 60.5,
        volatility_increase: 18.9
      }
    }
  },
  statistical_analysis: {
    "TSLA": {
      correlation_analysis: {
        pearson_correlation: -0.73,
        p_value: 0.0042,
        is_significant: true,
        confidence_level: 99.58
      },
      time_series_analysis: {
        trend_break: true,
        change_point: "2025-01-28T09:50:00",
        anomaly_score: 87.3,
        pattern_deviation: "strong_negative"
      },
      control_group_comparison: {
        same_time_last_week: 0.12,
        difference: -1.17,
        comparison_result: "significantly_different"
      }
    },
    "DOGE": {
      correlation_analysis: {
        pearson_correlation: 0.68,
        p_value: 0.0089,
        is_significant: true,
        confidence_level: 99.11
      },
      time_series_analysis: {
        trend_break: true,
        change_point: "2025-01-28T09:52:00",
        anomaly_score: 92.6,
        pattern_deviation: "strong_positive"
      },
      control_group_comparison: {
        same_time_last_week: -0.35,
        difference: 2.88,
        comparison_result: "significantly_different"
      }
    },
    "BTC-USD": {
      correlation_analysis: {
        pearson_correlation: 0.85,
        p_value: 0.0001,
        is_significant: true,
        confidence_level: 99.99
      },
      time_series_analysis: {
        trend_break: true,
        change_point: "2025-01-28T09:48:00",
        anomaly_score: 78.9,
        pattern_deviation: "moderate_positive"
      },
      control_group_comparison: {
        same_time_last_week: 0.45,
        difference: 2.45,
        comparison_result: "significantly_different"
      }
    }
  }
};

// 차트용 가격 데이터 생성
const generatePriceChartData = (symbol: string, priceData: any) => {
  const baseTime = new Date("2025-01-28T09:47:28.000Z").getTime();
  return [
    { time: "3일 전", price: priceData.price_before_30m * 0.98, volume: priceData.volume_before * 0.9 },
    { time: "2일 전", price: priceData.price_before_30m * 0.995, volume: priceData.volume_before * 0.95 },
    { time: "1일 전", price: priceData.price_before_30m * 1.01, volume: priceData.volume_before * 1.05 },
    { time: "게시 30분 전", price: priceData.price_before_30m, volume: priceData.volume_before },
    { time: "게시 시점", price: priceData.price_at_post, volume: priceData.volume_before },
    { time: "1시간 후", price: priceData.price_after_1h, volume: priceData.volume_after },
    { time: "6시간 후", price: priceData.price_after_6h, volume: priceData.volume_after * 1.1 },
    { time: "24시간 후", price: priceData.price_after_24h, volume: priceData.volume_after * 0.8 },
    { time: "1일 후", price: priceData.price_after_24h * 1.02, volume: priceData.volume_after * 0.7 },
    { time: "2일 후", price: priceData.price_after_24h * 1.01, volume: priceData.volume_after * 0.65 },
    { time: "3일 후", price: priceData.price_after_24h * 0.99, volume: priceData.volume_after * 0.6 }
  ];
};

// SNS 원본 링크 생성 함수
const generateSNSLink = (post: SNSPost): string => {
  if (post.platform === "X") {
    // X(Twitter) 링크 형식: https://x.com/username/status/postid
    return `https://x.com/${post.author}/status/${post.id}`;
  } else if (post.platform === "Truth Social") {
    // Truth Social 링크 형식: https://truthsocial.com/@username/posts/postid
    return `https://truthsocial.com/@${post.author.replace(/\s+/g, '')}/posts/${post.id}`;
  }
  return "#";
};

export function SNSDetailPage({ post, onBack }: SNSDetailPageProps) {
  const [activeTab, setActiveTab] = useState("overview");

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleString("ko-KR", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    });
  };

  const formatPrice = (price: number, symbol: string) => {
    if (symbol.includes("USD") || symbol === "BTC-USD") {
      return price.toLocaleString("en-US", { style: "currency", currency: "USD" });
    }
    return `$${price.toFixed(symbol === "DOGE" ? 4 : 2)}`;
  };

  const getChangeColor = (change: number) => {
    return change >= 0 ? "text-green-400" : "text-red-400";
  };

  const getChangeIcon = (change: number) => {
    return change >= 0 ? <TrendingUp size={16} /> : <TrendingDown size={16} />;
  };

  const renderCorrelationBadge = (correlation: number) => {
    const abs = Math.abs(correlation);
    let label = "";
    let color = "";
    
    if (abs >= 0.8) {
      label = "매우 강한 상관관계";
      color = "bg-red-500/20 text-red-400";
    } else if (abs >= 0.6) {
      label = "강한 상관관계";
      color = "bg-orange-500/20 text-orange-400";
    } else if (abs >= 0.4) {
      label = "중간 상관관계";
      color = "bg-yellow-500/20 text-yellow-400";
    } else {
      label = "약한 상관관계";
      color = "bg-gray-500/20 text-gray-400";
    }

    return <Badge className={color}>{label}</Badge>;
  };

  const snsLink = generateSNSLink(post);

  return (
    <div className="space-y-6">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <button
          onClick={onBack}
          className="p-2 rounded-lg glass hover:glass-strong transition-all"
        >
          <ArrowLeft size={20} />
        </button>
        <h1 className="text-xl font-bold">SNS 시장 영향 분석</h1>
        <a
          href={snsLink}
          target="_blank"
          rel="noopener noreferrer"
          className="p-2 rounded-lg glass hover:glass-strong transition-all"
        >
          <ExternalLink size={20} />
        </a>
      </div>

      {/* 게시글 정보 */}
      <div className="glass-card p-4 rounded-xl">
        <div className="flex items-start space-x-3 mb-4">
          <div className="relative">
            <img
              src={post.profileImage}
              alt={post.author}
              className="w-12 h-12 rounded-full object-cover"
            />
            {post.verified && (
              <div className="absolute -bottom-1 -right-1 w-5 h-5 bg-blue-500 rounded-full flex items-center justify-center">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="white">
                  <path d="M9 12l2 2 4-4" stroke="white" strokeWidth="2" fill="none"/>
                </svg>
              </div>
            )}
          </div>
          <div className="flex-1">
            <div className="flex items-center space-x-2 mb-1">
              <p className="font-medium">{post.author}</p>
              <Badge variant="secondary">{post.platform}</Badge>
              {post.category && (
                <Badge variant="outline" className="text-xs">
                  {post.category.replace("_", " ")}
                </Badge>
              )}
            </div>
            <p className="text-sm text-foreground/60">{formatTimestamp(post.timestamp)}</p>
          </div>
        </div>
        
        <p className="text-sm mb-4">{post.content}</p>
        
        {/* SNS 원본 링크 */}
        <div className="mb-4 p-3 glass-subtle rounded-lg">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <ExternalLink size={16} className="text-primary" />
              <span className="text-sm text-foreground/70">원본 게시글 보기</span>
            </div>
            <a
              href={snsLink}
              target="_blank"
              rel="noopener noreferrer"
              className="px-3 py-1 glass rounded-lg text-sm hover:glass-strong transition-all"
            >
              {post.platform}에서 보기
            </a>
          </div>
        </div>
        
        {post.hasMarketImpact && (
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <TrendingUp size={18} className="text-primary" />
              <span className="text-sm font-medium">시장 영향도: {post.impactScore?.toFixed(1)}%</span>
            </div>
            <Badge className="bg-primary/20 text-primary">
              분석 완료
            </Badge>
          </div>
        )}
      </div>

      {/* 영향받은 심볼 개요 */}
      <div className="glass-card p-4 rounded-xl">
        <h3 className="font-medium mb-3">영향받은 자산</h3>
        <div className="grid grid-cols-3 gap-3">
          {mockMarketData.post_analysis.affected_symbols.map((symbol) => {
            const priceData = mockMarketData.post_analysis.price_analysis[symbol];
            return (
              <div key={symbol} className="glass-subtle p-3 rounded-lg text-center">
                <p className="font-medium text-sm mb-1">{symbol}</p>
                <p className="text-xs text-foreground/60 mb-2">
                  {formatPrice(priceData.price_after_24h, symbol)}
                </p>
                <div className={`flex items-center justify-center space-x-1 text-xs ${getChangeColor(priceData.change_24h)}`}>
                  {getChangeIcon(priceData.change_24h)}
                  <span>{priceData.change_24h.toFixed(2)}%</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 상세 분석 탭 */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-3 glass-card">
          <TabsTrigger value="overview">개요</TabsTrigger>
          <TabsTrigger value="charts">차트</TabsTrigger>
          <TabsTrigger value="analysis">분석</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-4">
          {/* 가격 변화 요약 */}
          {mockMarketData.post_analysis.affected_symbols.map((symbol) => {
            const priceData = mockMarketData.post_analysis.price_analysis[symbol];
            return (
              <div key={symbol} className="glass-card p-4 rounded-xl">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-medium">{symbol}</h4>
                  <Badge variant="outline">{formatPrice(priceData.price_after_24h, symbol)}</Badge>
                </div>
                
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-foreground/60">1시간 후</p>
                    <div className={`flex items-center space-x-1 ${getChangeColor(priceData.change_1h)}`}>
                      {getChangeIcon(priceData.change_1h)}
                      <span>{priceData.change_1h.toFixed(2)}%</span>
                    </div>
                  </div>
                  <div>
                    <p className="text-foreground/60">6시간 후</p>
                    <div className={`flex items-center space-x-1 ${getChangeColor(priceData.change_6h)}`}>
                      {getChangeIcon(priceData.change_6h)}
                      <span>{priceData.change_6h.toFixed(2)}%</span>
                    </div>
                  </div>
                  <div>
                    <p className="text-foreground/60">24시간 후</p>
                    <div className={`flex items-center space-x-1 ${getChangeColor(priceData.change_24h)}`}>
                      {getChangeIcon(priceData.change_24h)}
                      <span>{priceData.change_24h.toFixed(2)}%</span>
                    </div>
                  </div>
                </div>

                <div className="mt-3 pt-3 border-t border-white/10">
                  <div className="flex justify-between text-xs text-foreground/60">
                    <span>거래량 증가: +{priceData.volume_spike.toFixed(1)}%</span>
                    <span>변동성 증가: +{priceData.volatility_increase.toFixed(1)}%</span>
                  </div>
                </div>
              </div>
            );
          })}
        </TabsContent>

        <TabsContent value="charts" className="space-y-4">
          {mockMarketData.post_analysis.affected_symbols.map((symbol) => {
            const priceData = mockMarketData.post_analysis.price_analysis[symbol];
            const chartData = generatePriceChartData(symbol, priceData);
            
            return (
              <div key={symbol} className="glass-card p-4 rounded-xl">
                <h4 className="font-medium mb-4">{symbol} 가격 변화</h4>
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                      <XAxis 
                        dataKey="time" 
                        tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.6)' }}
                        angle={-45}
                        textAnchor="end"
                        height={60}
                      />
                      <YAxis 
                        tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.6)' }}
                        domain={['dataMin - 5', 'dataMax + 5']}
                      />
                      <Tooltip 
                        contentStyle={{ 
                          backgroundColor: 'rgba(0,0,0,0.8)', 
                          border: '1px solid rgba(255,255,255,0.2)',
                          borderRadius: '8px'
                        }}
                        formatter={(value: any) => [formatPrice(value, symbol), '가격']}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="price" 
                        stroke="#60a5fa" 
                        strokeWidth={2}
                        dot={{ fill: '#60a5fa', strokeWidth: 2, r: 3 }}
                        activeDot={{ r: 5, stroke: '#60a5fa', strokeWidth: 2 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
                
                <div className="mt-4">
                  <h5 className="text-sm font-medium mb-2">거래량 변화</h5>
                  <div className="h-32">
                    <ResponsiveContainer width="100%" height="100%">
                      <ReBarChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                        <XAxis 
                          dataKey="time" 
                          tick={{ fontSize: 8, fill: 'rgba(255,255,255,0.6)' }}
                          angle={-45}
                          textAnchor="end"
                          height={40}
                        />
                        <YAxis tick={{ fontSize: 8, fill: 'rgba(255,255,255,0.6)' }} />
                        <Tooltip 
                          contentStyle={{ 
                            backgroundColor: 'rgba(0,0,0,0.8)', 
                            border: '1px solid rgba(255,255,255,0.2)',
                            borderRadius: '8px'
                          }}
                          formatter={(value: any) => [value.toLocaleString(), '거래량']}
                        />
                        <Bar dataKey="volume" fill="#60a5fa" opacity={0.6} />
                      </ReBarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            );
          })}
        </TabsContent>

        <TabsContent value="analysis" className="space-y-4">
          {mockMarketData.post_analysis.affected_symbols.map((symbol) => {
            const statData = mockMarketData.statistical_analysis[symbol];
            if (!statData) return null;
            
            return (
              <div key={symbol} className="glass-card p-4 rounded-xl">
                <h4 className="font-medium mb-4">{symbol} 통계 분석</h4>
                
                {/* 상관관계 분석 */}
                <div className="mb-4">
                  <div className="flex items-center justify-between mb-2">
                    <h5 className="text-sm font-medium">상관관계 분석</h5>
                    {renderCorrelationBadge(statData.correlation_analysis.pearson_correlation)}
                  </div>
                  <div className="glass-subtle p-3 rounded-lg">
                    <div className="grid grid-cols-2 gap-3 text-xs">
                      <div>
                        <p className="text-foreground/60">피어슨 상관계수</p>
                        <p className="font-medium">{statData.correlation_analysis.pearson_correlation.toFixed(3)}</p>
                      </div>
                      <div>
                        <p className="text-foreground/60">신뢰도</p>
                        <p className="font-medium">{statData.correlation_analysis.confidence_level.toFixed(2)}%</p>
                      </div>
                    </div>
                    <div className="mt-2">
                      <p className="text-xs text-foreground/60">
                        P-value: {statData.correlation_analysis.p_value.toFixed(4)}
                        {statData.correlation_analysis.is_significant && (
                          <Badge className="ml-2 bg-green-500/20 text-green-400 text-xs">통계적 유의</Badge>
                        )}
                      </p>
                    </div>
                  </div>
                </div>

                {/* 시계열 분석 */}
                <div className="mb-4">
                  <h5 className="text-sm font-medium mb-2">시계열 분석</h5>
                  <div className="glass-subtle p-3 rounded-lg">
                    <div className="grid grid-cols-2 gap-3 text-xs">
                      <div>
                        <p className="text-foreground/60">이상 점수</p>
                        <p className="font-medium">{statData.time_series_analysis.anomaly_score.toFixed(1)}</p>
                      </div>
                      <div>
                        <p className="text-foreground/60">패턴 편차</p>
                        <p className="font-medium">{statData.time_series_analysis.pattern_deviation.replace("_", " ")}</p>
                      </div>
                    </div>
                    {statData.time_series_analysis.trend_break && (
                      <div className="mt-2 flex items-center space-x-1">
                        <AlertTriangle size={14} className="text-orange-400" />
                        <p className="text-xs text-orange-400">트렌드 변곡점 감지</p>
                      </div>
                    )}
                  </div>
                </div>

                {/* 대조군 비교 */}
                <div>
                  <h5 className="text-sm font-medium mb-2">대조군 비교</h5>
                  <div className="glass-subtle p-3 rounded-lg">
                    <div className="grid grid-cols-2 gap-3 text-xs">
                      <div>
                        <p className="text-foreground/60">지난주 같은 시간</p>
                        <p className="font-medium">{statData.control_group_comparison.same_time_last_week.toFixed(2)}%</p>
                      </div>
                      <div>
                        <p className="text-foreground/60">차이</p>
                        <p className={`font-medium ${getChangeColor(statData.control_group_comparison.difference)}`}>
                          {statData.control_group_comparison.difference.toFixed(2)}%p
                        </p>
                      </div>
                    </div>
                    <div className="mt-2">
                      <Badge className="bg-primary/20 text-primary text-xs">
                        {statData.control_group_comparison.comparison_result.replace("_", " ")}
                      </Badge>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </TabsContent>
      </Tabs>
    </div>
  );
}