import { Clock, TrendingUp, AlertCircle, ExternalLink } from "lucide-react";
import { useNews } from "../hooks/useNews";

export function NewsList() {
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

  const { data, loading, error } = useNews();

  return (
    <div className="glass-card rounded-2xl p-4">
      <h2 className="text-lg font-semibold mb-4 flex items-center">
        <Clock className="mr-2" size={20} />
        실시간 뉴스
      </h2>

      {error && (
        <div className="text-sm text-red-400 mb-2">{error}</div>
      )}
      {loading && (
        <div className="text-sm text-foreground/60 mb-2">불러오는 중...</div>
      )}

      <div className="space-y-3">
        {data.map((item, idx) => (
          <div 
            key={item.id ?? idx}
            className={`glass rounded-xl p-3 border-l-4 ${getImportanceColor('medium')} cursor-pointer hover:bg-white/5 transition-all`}
            onClick={() => item.url && window.open(item.url, '_blank')}
          >
            <div className="flex items-start justify-between mb-2">
              <div className="flex items-center space-x-2">
                {getCategoryIcon(item.category || 'market')}
                <span className="text-xs text-foreground/60 bg-white/10 px-2 py-1 rounded-md">
                  {getCategoryLabel(item.category || 'market')}
                </span>
                <span className="text-xs text-foreground/50">·</span>
                <span className="text-xs text-foreground/50">{item.publishedAt || ''}</span>
              </div>
              <ExternalLink size={14} className="text-foreground/40" />
            </div>

            <h3 className="font-medium mb-2 line-clamp-2">{item.title}</h3>
            <p className="text-sm text-foreground/70 line-clamp-2 mb-2">{item.summary || ''}</p>
            
            <div className="flex items-center justify-between">
              <span className="text-xs text-foreground/50">{item.source || ''}</span>
              <div className={`w-2 h-2 rounded-full ${
                'bg-yellow-400'
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