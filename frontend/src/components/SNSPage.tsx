import { useState } from "react";
import { Search, Filter, X, MessageSquare, TrendingUp, Calendar, ExternalLink } from "lucide-react";
import { Input } from "./ui/input";
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

interface SNSPageProps {
  isLoggedIn: boolean;
  onLoginPrompt: () => void;
  onPostClick: (post: SNSPost) => void;
}

const xCategories = ["crypto", "core_investors", "tech_ceo", "institutional", "corporate"];
const truthSocialUsers = ["Donald J. Trump", "Donald Trump Jr.", "The White House"];

const mockSNSData: SNSPost[] = [
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
    content: "BlackRock's Bitcoin ETF has now reached $25 billion in assets under management. This is institutional adoption at unprecedented scale.",
    author: "ARKInvest",
    platform: "X",
    category: "institutional",
    timestamp: "2025-01-28T07:15:42.000Z",
    likes: 28391,
    retweets: 8765,
    replies: 1892,
    verified: true,
    profileImage: "https://images.unsplash.com/photo-1494790108755-2616b88b9b2c?w=40&h=40&fit=crop&crop=face",
    hasMarketImpact: true,
    impactScore: 65.8
  },
  {
    id: "1951085623277859205",
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
  },
  {
    id: "1951086724277859206",
    content: "The White House continues to work tirelessly for the American people. Today's economic indicators show remarkable progress across all sectors.",
    author: "The White House",
    platform: "Truth Social",
    timestamp: "2025-01-28T05:28:33.000Z",
    verified: true,
    profileImage: "https://images.unsplash.com/photo-1560250097-0b93528c311a?w=40&h=40&fit=crop&crop=face",
    hasMarketImpact: false
  }
];

export function SNSPage({ isLoggedIn, onLoginPrompt, onPostClick }: SNSPageProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedPlatform, setSelectedPlatform] = useState<"all" | "X" | "Truth Social">("all");
  const [selectedCategory, setSelectedCategory] = useState<string>("all");
  const [showFilters, setShowFilters] = useState(false);
  const [sortBy, setSortBy] = useState<"recent" | "impact" | "engagement">("recent");

  const filteredPosts = mockSNSData.filter(post => {
    const matchesSearch = post.content.toLowerCase().includes(searchQuery.toLowerCase()) ||
                         post.author.toLowerCase().includes(searchQuery.toLowerCase());
    
    const matchesPlatform = selectedPlatform === "all" || post.platform === selectedPlatform;
    
    let matchesCategory = true;
    if (selectedCategory !== "all") {
      if (post.platform === "X") {
        matchesCategory = post.category === selectedCategory;
      } else if (post.platform === "Truth Social") {
        matchesCategory = post.author === selectedCategory;
      }
    }
    
    return matchesSearch && matchesPlatform && matchesCategory;
  }).sort((a, b) => {
    switch (sortBy) {
      case "impact":
        return (b.impactScore || 0) - (a.impactScore || 0);
      case "engagement":
        const aEngagement = (a.likes || 0) + (a.retweets || 0) + (a.replies || 0);
        const bEngagement = (b.likes || 0) + (b.retweets || 0) + (b.replies || 0);
        return bEngagement - aEngagement;
      default:
        return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
    }
  });

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

  return (
    <div className="space-y-4">
      {/* 검색 및 필터 헤더 */}
      <div className="space-y-3">
        {/* 검색바 */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-foreground/50" size={20} />
          <Input
            placeholder="사용자명 또는 게시글 내용 검색..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 glass-card border-white/20 placeholder:text-foreground/50"
          />
        </div>

        {/* 플랫폼 및 정렬 선택 */}
        <div className="flex items-center justify-between">
          <div className="flex space-x-2">
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

          <div className="flex items-center space-x-2">
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
              className="px-3 py-1.5 rounded-lg glass-card text-sm bg-transparent border-white/20"
            >
              <option value="recent">최신순</option>
              <option value="impact">영향도순</option>
              <option value="engagement">인기순</option>
            </select>

            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`p-2 rounded-lg transition-all ${
                showFilters ? "glass text-primary" : "glass-subtle text-foreground/70"
              }`}
            >
              <Filter size={18} />
            </button>
          </div>
        </div>

        {/* 카테고리 필터 */}
        {showFilters && (
          <div className="glass-card p-4 rounded-xl space-y-3">
            <h3 className="font-medium">카테고리 필터</h3>
            
            {selectedPlatform !== "Truth Social" && (
              <div>
                <p className="text-sm text-foreground/70 mb-2">X 카테고리</p>
                <div className="flex flex-wrap gap-2">
                  <button
                    onClick={() => setSelectedCategory("all")}
                    className={`px-3 py-1 rounded-lg text-xs transition-all ${
                      selectedCategory === "all" ? "glass text-primary" : "glass-subtle"
                    }`}
                  >
                    전체
                  </button>
                  {xCategories.map((category) => (
                    <button
                      key={category}
                      onClick={() => setSelectedCategory(category)}
                      className={`px-3 py-1 rounded-lg text-xs transition-all ${
                        selectedCategory === category ? "glass text-primary" : "glass-subtle"
                      }`}
                    >
                      {category.replace("_", " ")}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {selectedPlatform !== "X" && (
              <div>
                <p className="text-sm text-foreground/70 mb-2">Truth Social 인물</p>
                <div className="flex flex-wrap gap-2">
                  <button
                    onClick={() => setSelectedCategory("all")}
                    className={`px-3 py-1 rounded-lg text-xs transition-all ${
                      selectedCategory === "all" ? "glass text-primary" : "glass-subtle"
                    }`}
                  >
                    전체
                  </button>
                  {truthSocialUsers.map((user) => (
                    <button
                      key={user}
                      onClick={() => setSelectedCategory(user)}
                      className={`px-3 py-1 rounded-lg text-xs transition-all ${
                        selectedCategory === user ? "glass text-primary" : "glass-subtle"
                      }`}
                    >
                      {user}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* SNS 피드 목록 */}
      <div className="space-y-3">
        {filteredPosts.length === 0 ? (
          <div className="glass-card p-8 text-center">
            <MessageSquare size={48} className="mx-auto mb-4 text-foreground/30" />
            <p className="text-foreground/70">검색 결과가 없습니다</p>
          </div>
        ) : (
          filteredPosts.map((post) => (
            <div
              key={post.id}
              onClick={() => onPostClick(post)}
              className="glass-card p-4 rounded-xl cursor-pointer hover:glass transition-all group"
            >
              {/* 헤더 */}
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-3">
                  <div className="relative">
                    <img
                      src={post.profileImage}
                      alt={post.author}
                      className="w-10 h-10 rounded-full object-cover"
                    />
                    {post.verified && (
                      <div className="absolute -bottom-1 -right-1 w-4 h-4 bg-blue-500 rounded-full flex items-center justify-center">
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="white">
                          <path d="M9 12l2 2 4-4" stroke="white" strokeWidth="2" fill="none"/>
                        </svg>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="flex items-center space-x-2">
                      <p className="font-medium">{post.author}</p>
                      <Badge variant="secondary" className="text-xs">
                        {post.platform}
                      </Badge>
                      {post.category && (
                        <Badge variant="outline" className="text-xs">
                          {post.category.replace("_", " ")}
                        </Badge>
                      )}
                    </div>
                    <p className="text-xs text-foreground/60">{formatTimestamp(post.timestamp)}</p>
                  </div>
                </div>

                {post.hasMarketImpact && (
                  <div className="flex items-center space-x-1">
                    <TrendingUp size={16} className="text-primary" />
                    <span className="text-xs text-primary font-medium">
                      {post.impactScore?.toFixed(1)}%
                    </span>
                  </div>
                )}
              </div>

              {/* 내용 */}
              <p className="text-sm mb-3 line-clamp-3">{post.content}</p>

              {/* 인게이지먼트 */}
              {post.platform === "X" && (
                <div className="flex items-center justify-between text-foreground/60 text-xs">
                  <div className="flex space-x-4">
                    <span>💬 {formatNumber(post.replies || 0)}</span>
                    <span>🔄 {formatNumber(post.retweets || 0)}</span>
                    <span>❤️ {formatNumber(post.likes || 0)}</span>
                  </div>
                  {post.hasMarketImpact && (
                    <Badge className="bg-primary/20 text-primary text-xs">
                      시장 영향 분석 가능
                    </Badge>
                  )}
                </div>
              )}

              {post.platform === "Truth Social" && post.hasMarketImpact && (
                <div className="flex justify-end">
                  <Badge className="bg-primary/20 text-primary text-xs">
                    시장 영향 분석 가능
                  </Badge>
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* 로딩 더보기 버튼 */}
      <div className="flex justify-center pt-4">
        <button className="px-6 py-2 glass-card rounded-xl hover:glass transition-all">
          더 많은 게시글 보기
        </button>
      </div>
    </div>
  );
}