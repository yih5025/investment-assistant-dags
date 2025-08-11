import { Clock, TrendingUp, AlertCircle, ExternalLink } from "lucide-react";

interface NewsItem {
  id: string;
  title: string;
  summary: string;
  source: string;
  timestamp: string;
  category: "market" | "crypto" | "economy" | "tech";
  importance: "high" | "medium" | "low";
  url: string;
}

export function NewsList() {
  const news: NewsItem[] = [
    {
      id: "1",
      title: "연준, 기준금리 0.25%p 인하 결정",
      summary: "미국 연방준비제도(Fed)가 기준금리를 0.25%포인트 인하하며 완화적 통화정책을 유지하기로 했습니다.",
      source: "Reuters",
      timestamp: "30분 전",
      category: "economy",
      importance: "high",
      url: "https://example.com/news1"
    },
    {
      id: "2",
      title: "테슬라, Q4 실적 기대치 상회",
      summary: "테슬라가 4분기 실적에서 시장 기대치를 상회하는 성과를 기록했다고 발표했습니다.",
      source: "Bloomberg",
      timestamp: "1시간 전",
      category: "market",
      importance: "high",
      url: "https://example.com/news2"
    },
    {
      id: "3",
      title: "비트코인, 10만 달러 돌파 임박",
      summary: "비트코인이 사상 최고치를 경신하며 10만 달러 돌파가 임박한 것으로 보입니다.",
      source: "CoinDesk",
      timestamp: "2시간 전",
      category: "crypto",
      importance: "medium",
      url: "https://example.com/news3"
    },
    {
      id: "4",
      title: "애플, 새로운 AI 칩 개발 발표",
      summary: "애플이 차세대 인공지능 처리를 위한 새로운 칩셋 개발을 공식 발표했습니다.",
      source: "TechCrunch",
      timestamp: "3시간 전",
      category: "tech",
      importance: "medium",
      url: "https://example.com/news4"
    },
    {
      id: "5",
      title: "글로벌 인플레이션 둔화세 지속",
      summary: "주요국의 인플레이션율이 지속적으로 하락하며 경제 안정화 신호를 보이고 있습니다.",
      source: "Financial Times",
      timestamp: "4시간 전",
      category: "economy",
      importance: "medium",
      url: "https://example.com/news5"
    }
  ];

  const getCategoryIcon = (category: string) => {
    switch (category) {
      case "market":
        return <TrendingUp size={16} className="text-green-400" />;
      case "crypto":
        return <AlertCircle size={16} className="text-yellow-400" />;
      case "economy":
        return <Clock size={16} className="text-blue-400" />;
      case "tech":
        return <TrendingUp size={16} className="text-purple-400" />;
      default:
        return null;
    }
  };

  const getCategoryLabel = (category: string) => {
    switch (category) {
      case "market": return "증시";
      case "crypto": return "암호화폐";
      case "economy": return "경제";
      case "tech": return "기술";
      default: return category;
    }
  };

  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case "high": return "border-l-red-400";
      case "medium": return "border-l-yellow-400";
      case "low": return "border-l-green-400";
      default: return "border-l-gray-400";
    }
  };

  return (
    <div className="glass-card rounded-2xl p-4">
      <h2 className="text-lg font-semibold mb-4 flex items-center">
        <Clock className="mr-2" size={20} />
        실시간 뉴스
      </h2>

      <div className="space-y-3">
        {news.map((item) => (
          <div 
            key={item.id} 
            className={`glass rounded-xl p-3 border-l-4 ${getImportanceColor(item.importance)} cursor-pointer hover:bg-white/5 transition-all`}
            onClick={() => window.open(item.url, '_blank')}
          >
            <div className="flex items-start justify-between mb-2">
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

            <h3 className="font-medium mb-2 line-clamp-2">{item.title}</h3>
            <p className="text-sm text-foreground/70 line-clamp-2 mb-2">{item.summary}</p>
            
            <div className="flex items-center justify-between">
              <span className="text-xs text-foreground/50">{item.source}</span>
              <div className={`w-2 h-2 rounded-full ${
                item.importance === "high" ? "bg-red-400" : 
                item.importance === "medium" ? "bg-yellow-400" : "bg-green-400"
              }`} />
            </div>
          </div>
        ))}
      </div>

      <button className="w-full mt-4 py-2 text-sm text-primary hover:text-primary/80 transition-colors">
        더 많은 뉴스 보기
      </button>
    </div>
  );
}