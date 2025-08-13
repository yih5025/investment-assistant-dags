import { useState } from "react";
import { MessageSquare, TrendingUp, ExternalLink } from "lucide-react";
import { Badge } from "./ui/badge";

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

interface SocialFeedProps {
  isLoggedIn: boolean;
  onPostClick?: (post: SNSPost) => void;
}

const mockSNSPosts: SNSPost[] = [
  {
    id: "1951082319277859202",
    content: "The future of cryptocurrency is looking incredibly promising. Major institutional adoption is accelerating faster than ever before. 🚀 #Crypto #Bitcoin",
    author: "elonmusk",
    platform: "X",
    category: "tech_ceo",
    timestamp: "2025-01-28T09:47:28.000Z",
    likes: 45672,
    retweets: 12843,
    replies: 3421,
    verified: true,
    profileImage: "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?w=40&h=40&fit=crop&crop=face",
    hasMarketImpact: true,
    impactScore: 87.3
  },
  {
    id: "1951083421277859203",
    content: "Despite the fake news media's constant attacks, America's economy is stronger than ever! Our policies are working and the American people are winning! 🇺🇸",
    author: "Donald J. Trump",
    platform: "Truth Social",
    timestamp: "2025-01-28T08:32:15.000Z",
    verified: true,
    profileImage: "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=40&h=40&fit=crop&crop=face",
    hasMarketImpact: true,
    impactScore: 73.2
  },
  {
    id: "1951084522277859204",
    content: "Tesla's Q4 delivery numbers exceeded all expectations. The sustainable energy revolution is unstoppable! 🔋⚡",
    author: "elonmusk",
    platform: "X", 
    category: "tech_ceo",
    timestamp: "2025-01-28T06:44:18.000Z",
    likes: 67421,
    retweets: 19832,
    replies: 4567,
    verified: true,
    profileImage: "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?w=40&h=40&fit=crop&crop=face",
    hasMarketImpact: true,
    impactScore: 91.5
  }
];

export function SocialFeed({ isLoggedIn, onPostClick }: SocialFeedProps) {
  const [selectedPlatform, setSelectedPlatform] = useState<"all" | "X" | "Truth Social">("all");

  const filteredPosts = mockSNSPosts.filter(post => 
    selectedPlatform === "all" || post.platform === selectedPlatform
  );

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    
    if (diffMins < 60) {
      return `${diffMins}분 전`;
    } else if (diffHours < 24) {
      return `${diffHours}시간 전`;
    } else {
      return date.toLocaleDateString("ko-KR", { month: "short", day: "numeric" });
    }
  };

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + "M";
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + "K";
    }
    return num.toString();
  };

  const handlePostClick = (post: SNSPost) => {
    if (onPostClick) {
      onPostClick(post);
    }
  };

  const handleViewAllClick = () => {
    // App.tsx에서 SNS 탭으로 이동하는 로직이 필요함
    window.dispatchEvent(new CustomEvent('navigateToSNS'));
  };

  return (
    <div className="glass-card p-4 rounded-xl">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center space-x-2">
          <MessageSquare size={20} className="text-primary" />
          <h3 className="font-medium">SNS</h3>
        </div>
        
        <button
          onClick={handleViewAllClick}
          className="flex items-center space-x-1 px-3 py-1 glass-subtle rounded-lg hover:glass transition-all text-sm"
        >
          <span>전체보기</span>
          <ExternalLink size={14} />
        </button>
      </div>

      {/* 플랫폼 필터 */}
      <div className="flex space-x-2 mb-4">
        {["all", "X", "Truth Social"].map((platform) => (
          <button
            key={platform}
            onClick={() => setSelectedPlatform(platform as typeof selectedPlatform)}
            className={`px-3 py-1.5 rounded-lg text-sm transition-all ${
              selectedPlatform === platform
                ? "glass text-primary"
                : "glass-subtle text-foreground/70 hover:text-foreground"
            }`}
          >
            {platform === "all" ? "전체" : platform}
          </button>
        ))}
      </div>

      {/* SNS 피드 목록 */}
      <div className="space-y-3">
        {filteredPosts.slice(0, 3).map((post) => (
          <div
            key={post.id}
            onClick={() => handlePostClick(post)}
            className="glass-subtle p-3 rounded-lg cursor-pointer hover:glass transition-all group"
          >
            {/* 헤더 */}
            <div className="flex items-start justify-between mb-2">
              <div className="flex items-center space-x-2">
                <div className="relative">
                  <img
                    src={post.profileImage}
                    alt={post.author}
                    className="w-8 h-8 rounded-full object-cover"
                  />
                  {post.verified && (
                    <div className="absolute -bottom-1 -right-1 w-3 h-3 bg-blue-500 rounded-full flex items-center justify-center">
                      <svg width="8" height="8" viewBox="0 0 24 24" fill="white">
                        <path d="M9 12l2 2 4-4" stroke="white" strokeWidth="2" fill="none"/>
                      </svg>
                    </div>
                  )}
                </div>
                <div>
                  <div className="flex items-center space-x-1">
                    <p className="text-sm font-medium">{post.author}</p>
                    <Badge variant="secondary" className="text-xs">
                      {post.platform}
                    </Badge>
                  </div>
                  <p className="text-xs text-foreground/60">{formatTimestamp(post.timestamp)}</p>
                </div>
              </div>

              {post.hasMarketImpact && (
                <div className="flex items-center space-x-1">
                  <TrendingUp size={14} className="text-primary" />
                  <span className="text-xs text-primary font-medium">
                    {post.impactScore?.toFixed(1)}%
                  </span>
                </div>
              )}
            </div>

            {/* 내용 */}
            <p className="text-sm mb-2 line-clamp-2">{post.content}</p>

            {/* 인게이지먼트 */}
            <div className="flex items-center justify-between">
              {post.platform === "X" && (
                <div className="flex space-x-3 text-foreground/60 text-xs">
                  <span>💬 {formatNumber(post.replies || 0)}</span>
                  <span>🔄 {formatNumber(post.retweets || 0)}</span>
                  <span>❤️ {formatNumber(post.likes || 0)}</span>
                </div>
              )}
              
              {post.hasMarketImpact && (
                <Badge className="bg-primary/20 text-primary text-xs ml-auto">
                  시장 영향 분석
                </Badge>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* 더보기 버튼 */}
      <div className="mt-4 text-center">
        <button
          onClick={handleViewAllClick}
          className="px-4 py-2 glass-subtle rounded-lg hover:glass transition-all text-sm"
        >
          더 많은 SNS 피드 보기
        </button>
      </div>
    </div>
  );
}