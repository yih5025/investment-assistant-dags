import { useState } from "react";
import { Clock, TrendingUp, AlertCircle, ExternalLink, Building, DollarSign, Users, Filter, Lock, Bell } from "lucide-react";

interface NewsPageProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
}

interface NewsItem {
  id: string;
  title: string;
  summary: string;
  content: string;
  source: string;
  timestamp: string;
  category: "corporate" | "economy" | "politics" | "tech" | "crypto";
  importance: "high" | "medium" | "low";
  url: string;
  imageUrl?: string;
  premium?: boolean;
}

export function NewsPage({ isLoggedIn, onLoginPrompt }: NewsPageProps) {
  const [selectedCategory, setSelectedCategory] = useState<"all" | "corporate" | "economy" | "politics" | "tech" | "crypto">("all");
  const [selectedNews, setSelectedNews] = useState<NewsItem | null>(null);

  const news: NewsItem[] = [
    {
      id: "1",
      title: "연준, 기준금리 0.25%p 인하 결정... 경기 부양 신호",
      summary: "미국 연방준비제도(Fed)가 기준금리를 0.25%포인트 인하하며 완화적 통화정책을 유지하기로 했습니다.",
      content: "제롬 파월 연준 의장은 기자회견에서 '인플레이션이 목표치인 2%에 근접하고 있으며, 고용시장도 안정적'이라고 밝혔습니다. 이번 결정으로 연방기금금리는 4.75-5.00%에서 4.50-4.75%로 조정되었습니다.",
      source: "Reuters",
      timestamp: "30분 전",
      category: "economy",
      importance: "high",
      url: "https://example.com/news1"
    },
    {
      id: "2",
      title: "테슬라, Q4 실적 기대치 상회... 전기차 판매량 사상 최고",
      summary: "테슬라가 4분기 실적에서 시장 기대치를 상회하는 성과를 기록했다고 발표했습니다.",
      content: "일론 머스크 CEO는 '2024년 전기차 판매량이 사상 최고치를 기록했으며, 자율주행 기술 개발도 순조롭게 진행되고 있다'고 밝혔습니다. 특히 모델 Y의 강세가 두드러졌습니다.",
      source: "Bloomberg",
      timestamp: "1시간 전",
      category: "corporate",
      importance: "high",
      url: "https://example.com/news2"
    },
    {
      id: "3",
      title: "[프리미엄] AI 반도체 시장 전망 분석 리포트",
      summary: "2025년 AI 칩 시장의 성장 전망과 주요 기업들의 경쟁 구도를 심층 분석합니다.",
      content: "전문가들은 AI 칩 시장이 향후 5년간 연평균 35% 성장할 것으로 예측한다고 밝혔습니다. NVIDIA, AMD, Intel 등 주요 기업들의 전략과 시장 점유율 변화를 자세히 살펴봅니다.",
      source: "W.E.I 분석팀",
      timestamp: "2시간 전",
      category: "tech",
      importance: "medium",
      url: "https://example.com/news3",
      premium: true
    },
    {
      id: "4",
      title: "비트코인, 10만 달러 돌파 임박... 기관투자자 유입 급증",
      summary: "비트코인이 사상 최고치를 경신하며 10만 달러 돌파가 임박한 것으로 보입니다.",
      content: "블랙록, 피델리티 등 대형 자산운용사들의 비트코인 ETF 자금 유입이 지속되고 있으며, 기관투자자들의 관심이 크게 증가하고 있습니다.",
      source: "CoinDesk",
      timestamp: "3시간 전",
      category: "crypto",
      importance: "medium",
      url: "https://example.com/news4"
    },
    {
      id: "5",
      title: "바이든 정부, 반도체 지원법 추가 예산 승인",
      summary: "미국 정부가 국내 반도체 제조업 지원을 위한 추가 예산을 승인했습니다.",
      content: "CHIPS 법에 따른 추가 지원으로 미국 내 반도체 생산 능력을 확대하고, 중국에 대한 기술 의존도를 줄이는 것이 목표입니다. 인텔, AMD, 엔비디아 등이 주요 수혜 기업으로 예상됩니다.",
      source: "The Wall Street Journal",
      timestamp: "4시간 전",
      category: "politics",
      importance: "medium",
      url: "https://example.com/news5"
    },
    {
      id: "6",
      title: "[프리미엄] 글로벌 경제 리스크 전망 2025",
      summary: "2025년 주요 경제 리스크 요인들과 투자 전략을 전문가가 분석합니다.",
      content: "지정학적 리스크, 인플레이션 재상승 가능성, 중앙은행들의 정책 변화 등 2025년 투자자들이 주목해야 할 주요 리스크들을 종합 분석했습니다.",
      source: "W.E.I 이코노미스트",
      timestamp: "5시간 전",
      category: "economy",
      importance: "high",
      url: "https://example.com/news6",
      premium: true
    }
  ];

  const categories = [
    { key: "all", label: "전체", icon: Filter, color: "text-gray-400" },
    { key: "corporate", label: "기업", icon: Building, color: "text-blue-400" },
    { key: "economy", label: "경제", icon: DollarSign, color: "text-green-400" },
    { key: "politics", label: "정치", icon: Users, color: "text-purple-400" },
    { key: "tech", label: "기술", icon: TrendingUp, color: "text-orange-400" },
    { key: "crypto", label: "암호화폐", icon: AlertCircle, color: "text-yellow-400" }
  ];

  const filteredNews = selectedCategory === "all" 
    ? news 
    : news.filter(item => item.category === selectedCategory);

  const getCategoryIcon = (category: string) => {
    const cat = categories.find(c => c.key === category);
    if (!cat) return null;
    const Icon = cat.icon;
    return <Icon size={16} className={cat.color} />;
  };

  const getCategoryLabel = (category: string) => {
    const cat = categories.find(c => c.key === category);
    return cat?.label || category;
  };

  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case "high": return "border-l-red-400";
      case "medium": return "border-l-yellow-400";
      case "low": return "border-l-green-400";
      default: return "border-l-gray-400";
    }
  };

  const handleNewsClick = (newsItem: NewsItem) => {
    if (newsItem.premium && !isLoggedIn) {
      onLoginPrompt();
      return;
    }
    setSelectedNews(newsItem);
  };

  if (selectedNews) {
    return (
      <div className="space-y-6">
        {/* 뉴스 상세 헤더 */}
        <div className="flex items-center space-x-3">
          <button
            onClick={() => setSelectedNews(null)}
            className="p-2 rounded-lg glass hover:bg-white/10 transition-colors"
          >
            <Clock size={20} />
          </button>
          <div>
            <h1 className="text-xl font-bold">뉴스 상세</h1>
            <p className="text-sm text-foreground/70">{selectedNews.source}</p>
          </div>
        </div>

        {/* 뉴스 상세 내용 */}
        <div className="glass-card rounded-2xl p-6">
          <div className="flex items-center space-x-2 mb-4">
            {getCategoryIcon(selectedNews.category)}
            <span className="text-xs text-foreground/60 bg-white/10 px-2 py-1 rounded-md">
              {getCategoryLabel(selectedNews.category)}
            </span>
            <span className="text-xs text-foreground/50">·</span>
            <span className="text-xs text-foreground/50">{selectedNews.timestamp}</span>
            {selectedNews.premium && (
              <>
                <span className="text-xs text-foreground/50">·</span>
                <span className="text-xs text-yellow-400 bg-yellow-400/20 px-2 py-1 rounded-md">프리미엄</span>
              </>
            )}
          </div>

          <h1 className="text-xl font-bold mb-4 leading-relaxed">{selectedNews.title}</h1>
          
          <div className="glass rounded-xl p-4 mb-4 bg-primary/5">
            <p className="text-sm text-foreground/80 leading-relaxed">{selectedNews.summary}</p>
          </div>

          <div className="prose prose-sm max-w-none text-foreground/80 leading-relaxed mb-6">
            <p>{selectedNews.content}</p>
          </div>

          <div className="flex items-center justify-between pt-4 border-t border-white/10">
            <span className="text-sm text-foreground/50">출처: {selectedNews.source}</span>
            <button 
              onClick={() => window.open(selectedNews.url, '_blank')}
              className="flex items-center space-x-1 text-sm text-primary hover:text-primary/80 bg-primary/10 px-3 py-1.5 rounded-lg transition-colors"
            >
              <span>원문 보기</span>
              <ExternalLink size={14} />
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* 프리미엄 뉴스 알림 (게스트용) */}
      {!isLoggedIn && (
        <div className="glass-card rounded-2xl p-4 border border-primary/30">
          <div className="flex items-center space-x-3">
            <Bell className="text-primary" size={20} />
            <div className="flex-1">
              <h3 className="font-medium mb-1">📈 프리미엄 분석 레포트</h3>
              <p className="text-sm text-foreground/70">
                전문가의 심층 분석과 독점 리포트를 로그인하고 확인하세요.
              </p>
            </div>
            <button
              onClick={onLoginPrompt}
              className="px-3 py-1.5 bg-primary/20 text-primary rounded-lg text-sm hover:bg-primary/30 transition-colors"
            >
              로그인
            </button>
          </div>
        </div>
      )}

      {/* 카테고리 필터 */}
      <div className="glass-card rounded-2xl p-4">
        <h3 className="font-semibold mb-3">카테고리</h3>
        <div className="grid grid-cols-3 gap-2">
          {categories.map((category) => {
            const Icon = category.icon;
            const isSelected = selectedCategory === category.key;
            
            return (
              <button
                key={category.key}
                onClick={() => setSelectedCategory(category.key as any)}
                className={`flex flex-col items-center space-y-1 p-3 rounded-xl transition-all ${
                  isSelected 
                    ? "bg-primary/20 text-primary border border-primary/30" 
                    : "glass hover:bg-white/10"
                }`}
              >
                <Icon size={20} className={isSelected ? "text-primary" : category.color} />
                <span className="text-xs font-medium">{category.label}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* 뉴스 리스트 */}
      <div className="space-y-3">
        {filteredNews.map((item) => (
          <div 
            key={item.id} 
            className={`glass-card rounded-xl p-4 border-l-4 ${getImportanceColor(item.importance)} cursor-pointer hover:bg-white/5 transition-all relative`}
            onClick={() => handleNewsClick(item)}
          >
            {/* 프리미엄 오버레이 */}
            {item.premium && !isLoggedIn && (
              <div className="absolute top-2 right-2">
                <div className="flex items-center space-x-1 bg-yellow-400/20 text-yellow-400 px-2 py-1 rounded-md text-xs">
                  <Lock size={10} />
                  <span>프리미엄</span>
                </div>
              </div>
            )}

            <div className="flex items-start justify-between mb-3">
              <div className="flex items-center space-x-2">
                {getCategoryIcon(item.category)}
                <span className="text-xs text-foreground/60 bg-white/10 px-2 py-1 rounded-md">
                  {getCategoryLabel(item.category)}
                </span>
                <span className="text-xs text-foreground/50">·</span>
                <span className="text-xs text-foreground/50">{item.timestamp}</span>
              </div>
              <ExternalLink size={14} className="text-foreground/40" />
            </div>

            <h3 className="font-medium mb-2 line-clamp-2 leading-relaxed">{item.title}</h3>
            <p className={`text-sm text-foreground/70 line-clamp-2 leading-relaxed mb-3 ${
              item.premium && !isLoggedIn ? 'blur-sm' : ''
            }`}>
              {item.summary}
            </p>
            
            <div className="flex items-center justify-between">
              <span className="text-xs text-foreground/50">{item.source}</span>
              <div className="flex items-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${
                  item.importance === "high" ? "bg-red-400" : 
                  item.importance === "medium" ? "bg-yellow-400" : "bg-green-400"
                }`} />
                <span className="text-xs text-foreground/50">
                  {item.importance === "high" ? "중요" : 
                   item.importance === "medium" ? "보통" : "일반"}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {filteredNews.length === 0 && (
        <div className="text-center py-8 text-foreground/60">
          <Clock size={48} className="mx-auto mb-4 opacity-50" />
          <p>해당 카테고리의 뉴스가 없습니다</p>
        </div>
      )}
    </div>
  );
}