import { useState } from "react";
import { Heart, MessageCircle, Share, TrendingUp, ExternalLink, Eye } from "lucide-react";

interface SocialFeedProps {
  isLoggedIn: boolean;
}

interface Post {
  id: string;
  author: string;
  avatar: string;
  time: string;
  content: string;
  image?: string;
  likes: number;
  comments: number;
  shares: number;
  liked: boolean;
  stocks: string[];
  type: "tweet" | "analysis" | "news";
}

export function SocialFeed({ isLoggedIn }: SocialFeedProps) {
  const [posts, setPosts] = useState<Post[]>([
    {
      id: "1",
      author: "일론 머스크",
      avatar: "🚀",
      time: "2시간 전",
      content: "Tesla의 자율주행 기술이 새로운 단계에 진입했습니다. Full Self-Driving v12가 곧 출시됩니다!",
      likes: 15420,
      comments: 2341,
      shares: 892,
      liked: false,
      stocks: ["TSLA"],
      type: "tweet"
    },
    {
      id: "2",
      author: "워렌 버핏",
      avatar: "👴",
      time: "4시간 전",
      content: "장기 투자의 힘을 믿어야 합니다. 시장의 단기 변동에 휘둘리지 말고, 좋은 기업을 찾아 꾸준히 투자하세요.",
      likes: 8932,
      comments: 1205,
      shares: 543,
      liked: isLoggedIn,
      stocks: ["BRK.A", "AAPL"],
      type: "analysis"
    },
    {
      id: "3",
      author: "AI 투자 분석가",
      avatar: "🤖",
      time: "6시간 전",
      content: "NVIDIA의 Q4 실적 분석: AI 칩 수요 급증으로 매출 예상치 상회. 주가 상승 모멘텀 지속 전망.",
      likes: 3421,
      comments: 567,
      shares: 234,
      liked: false,
      stocks: ["NVDA"],
      type: "analysis"
    },
    {
      id: "4",
      author: "도널드 트럼프",
      avatar: "🇺🇸",
      time: "8시간 전",
      content: "미국의 경제가 다시 한 번 세계 최강임을 보여주고 있습니다. 주식시장 신고점 경신!",
      likes: 12789,
      comments: 4532,
      shares: 1876,
      liked: false,
      stocks: ["SPY", "QQQ"],
      type: "tweet"
    },
    {
      id: "5",
      author: "제롬 파월",
      avatar: "🏦",
      time: "12시간 전",
      content: "연준은 인플레이션 목표 달성을 위해 신중한 통화정책을 지속할 것입니다. 경제 데이터를 면밀히 모니터링하고 있습니다.",
      likes: 5634,
      comments: 876,
      shares: 432,
      liked: false,
      stocks: ["SPY", "TLT"],
      type: "analysis"
    },
    {
      id: "6",
      author: "팀 쿡",
      avatar: "🍎",
      time: "1일 전",
      content: "Apple Vision Pro의 혁신적인 기술이 새로운 컴퓨팅 시대를 열어가고 있습니다. 미래가 여기에 있습니다.",
      likes: 7842,
      comments: 1432,
      shares: 654,
      liked: false,
      stocks: ["AAPL"],
      type: "tweet"
    }
  ]);

  const toggleLike = (postId: string) => {
    setPosts(prev => prev.map(post => 
      post.id === postId 
        ? { 
            ...post, 
            liked: !post.liked,
            likes: post.liked ? post.likes - 1 : post.likes + 1
          }
        : post
    ));
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case "analysis":
        return <TrendingUp size={14} className="text-blue-400" />;
      case "news":
        return <ExternalLink size={14} className="text-green-400" />;
      default:
        return null;
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case "analysis":
        return "분석";
      case "news":
        return "뉴스";
      case "tweet":
        return "트윗";
      default:
        return "";
    }
  };

  return (
    <div className="glass-card rounded-2xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold">📱 영향력 있는 인물들</h2>
        <div className="text-xs text-foreground/60">실시간 업데이트</div>
      </div>

      <div className="space-y-4">
        {posts.map((post) => (
          <div key={post.id} className="glass rounded-xl p-4 hover:glass-strong transition-all">
            {/* 헤더 */}
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center space-x-3">
                <div className="text-2xl">{post.avatar}</div>
                <div>
                  <div className="flex items-center space-x-2">
                    <span className="font-medium">{post.author}</span>
                    <div className="flex items-center space-x-1">
                      {getTypeIcon(post.type)}
                      <span className="text-xs text-foreground/60">{getTypeLabel(post.type)}</span>
                    </div>
                  </div>
                  <span className="text-xs text-foreground/50">{post.time}</span>
                </div>
              </div>
            </div>

            {/* 관련 주식 */}
            <div className="flex flex-wrap gap-1 mb-3">
              {post.stocks.map((stock) => (
                <span
                  key={stock}
                  className="text-xs px-2 py-1 bg-primary/20 text-primary rounded-md"
                >
                  ${stock}
                </span>
              ))}
            </div>

            {/* 콘텐츠 */}
            <p className="text-sm mb-4 leading-relaxed">
              {post.content}
            </p>

            {/* 액션 버튼들 */}
            <div className="flex items-center justify-between pt-3 border-t border-white/10">
              <div className="flex items-center space-x-4">
                <button
                  onClick={() => toggleLike(post.id)}
                  className={`flex items-center space-x-1 text-sm transition-colors ${
                    post.liked 
                      ? "text-red-400 hover:text-red-300" 
                      : "text-foreground/60 hover:text-red-400"
                  }`}
                >
                  <Heart 
                    size={16} 
                    className={post.liked ? "fill-current" : ""} 
                  />
                  <span>{post.likes.toLocaleString()}</span>
                </button>

                <button className="flex items-center space-x-1 text-sm text-foreground/60 hover:text-blue-400 transition-colors">
                  <MessageCircle size={16} />
                  <span>{post.comments.toLocaleString()}</span>
                </button>

                <button className="flex items-center space-x-1 text-sm text-foreground/60 hover:text-green-400 transition-colors">
                  <Share size={16} />
                  <span>{post.shares.toLocaleString()}</span>
                </button>
              </div>

              <div className="flex items-center space-x-1 text-xs text-foreground/50">
                <Eye size={12} />
                <span>실시간</span>
              </div>
            </div>
          </div>
        ))}

        {/* 더보기 섹션 */}
        <div className="glass-subtle rounded-xl p-4 text-center">
          <h3 className="font-medium mb-2">💡 투자 인사이트</h3>
          <p className="text-sm text-foreground/70 mb-3">
            전 세계 투자 구루들의 실시간 의견과 분석을 한 곳에서 만나보세요.
          </p>
          {isLoggedIn ? (
            <div className="text-xs text-foreground/60">
              🎯 맞춤형 피드 • 📊 심화 분석 • 🔔 실시간 알림 활성화됨
            </div>
          ) : (
            <div className="text-xs text-foreground/60">
              🌟 모든 투자 인사이트를 무료로 이용하세요
            </div>
          )}
        </div>
      </div>
    </div>
  );
}