import { useState, useEffect } from "react";
import { BottomNavigation } from "./components/BottomNavigation";
import { StockBanner } from "./components/StockBanner";
import { EventCalendar } from "./components/EventCalendar";
import { SocialFeed } from "./components/SocialFeed";
import { NewsList } from "./components/NewsList";
// import { IntegratedMarket } from "./components/IntegratedMarket";
import MarketPage from "./components/MarketPage";
import { NewsPage } from "./components/NewsPage";
import NewsDetailPage, { NewsItem as DetailNewsItem } from "./components/NewsDetailPage";
import { SNSPage } from "./components/SNSPage";
import { SNSDetailPage } from "./components/SNSDetailPage";
import { AIAnalysis } from "./components/AIAnalysis";
import { EconomicDashboard } from "./components/EconomicDashboard";
import { LoginPage } from "./components/auth/LoginPage";
import { SignupPage } from "./components/auth/SignupPage";
import { UserProfile } from "./components/user/UserProfile";
import { NotificationSystem } from "./components/notifications/NotificationSystem";
import { TrendingUp, MessageSquare, Newspaper, Bot, BarChart3, Bell, User, LogIn, ArrowLeft } from "lucide-react";

type AuthState = "guest" | "login" | "signup" | "authenticated";
type ViewState = "main" | "auth" | "profile" | "sns-detail" | "stock-news" | "news-detail";

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

interface StockNewsItem {
  symbol: string;
  source: string;
  url: string;
  title: string;
  description: string;
  content: string;
  published_at: string;
  fetched_at?: string;
}

export default function App() {
  const [activeTab, setActiveTab] = useState("home");
  const [authState, setAuthState] = useState<AuthState>("guest"); // 게스트 모드로 시작
  const [viewState, setViewState] = useState<ViewState>("main");
  const [showNotifications, setShowNotifications] = useState(false);
  const [selectedSNSPost, setSelectedSNSPost] = useState<SNSPost | null>(null);
  const [selectedStockNews, setSelectedStockNews] = useState<{ news: StockNewsItem[], symbol: string } | null>(null);
  const [selectedNewsItem, setSelectedNewsItem] = useState<DetailNewsItem | null>(null);
  
  // 모의 사용자 데이터
  const [user] = useState({
    name: "김투자",
    email: "investor@wei.com",
    phone: "010-1234-5678",
    profileImage: "",
    memberSince: "2023년 3월",
    totalReturn: 12.8,
    portfolioValue: 125000
  });

  const isLoggedIn = authState === "authenticated";

  // 네비게이션 이벤트 리스너들
  useEffect(() => {
    const handleNavigateToSNS = () => {
      setActiveTab("sns");
    };

    const handleNavigateToNews = () => {
      setActiveTab("news");
    };

    window.addEventListener('navigateToSNS', handleNavigateToSNS);
    window.addEventListener('navigateToNews', handleNavigateToNews);
    
    return () => {
      window.removeEventListener('navigateToSNS', handleNavigateToSNS);
      window.removeEventListener('navigateToNews', handleNavigateToNews);
    };
  }, []);

  const handleLogin = (email: string, password: string) => {
    console.log("로그인:", email, password);
    setAuthState("authenticated");
    setViewState("main");
  };

  const handleSignup = (userData: any) => {
    console.log("회원가입:", userData);
    setAuthState("authenticated");
    setViewState("main");
  };

  const handleLogout = () => {
    setAuthState("guest");
    setViewState("main");
    setActiveTab("home");
    setShowNotifications(false);
  };

  const handleLoginClick = () => {
    setAuthState("login");
    setViewState("auth");
  };

  const handleUserIconClick = () => {
    if (isLoggedIn) {
      setViewState("profile");
    } else {
      handleLoginClick();
    }
  };

  const handleNotificationClick = () => {
    if (isLoggedIn) {
      setShowNotifications(true);
    } else {
      handleLoginClick();
    }
  };

  const handleBackToMain = () => {
    setViewState("main");
    setSelectedSNSPost(null);
    setSelectedStockNews(null);
    if (!isLoggedIn) {
      setAuthState("guest");
    }
  };

  const handleSNSPostClick = (post: SNSPost) => {
    if (post.hasMarketImpact) {
      setSelectedSNSPost(post);
      setViewState("sns-detail");
    }
  };

  const handleStockNewsClick = (stockNews: StockNewsItem[], symbol: string) => {
    setSelectedStockNews({ news: stockNews, symbol });
    setViewState("stock-news");
  };

  const handleNewsClick = (newsItem: DetailNewsItem) => {
    setSelectedNewsItem(newsItem);
    setViewState("news-detail");
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / 3600000);
    
    if (diffHours < 1) {
      return "방금 전";
    } else if (diffHours < 24) {
      return `${diffHours}시간 전`;
    } else {
      return date.toLocaleDateString("ko-KR", { 
        month: "short", 
        day: "numeric"
      });
    }
  };

  // 인증 페이지 렌더링
  if (viewState === "auth") {
    if (authState === "login") {
      return (
        <div className="min-h-screen relative z-10">
          <LoginPage
            onBack={handleBackToMain}
            onLogin={handleLogin}
            onNavigateToSignup={() => setAuthState("signup")}
            onForgotPassword={() => console.log("비밀번호 찾기")}
          />
        </div>
      );
    }

    if (authState === "signup") {
      return (
        <div className="min-h-screen relative z-10">
          <SignupPage
            onBack={() => setAuthState("login")}
            onSignup={handleSignup}
            onNavigateToLogin={() => setAuthState("login")}
          />
        </div>
      );
    }
  }

  // SNS 상세 페이지
  if (viewState === "sns-detail" && selectedSNSPost) {
    return (
      <div className="min-h-screen relative z-10">
        <div className="max-w-md mx-auto px-4 pt-4 pb-20">
          <SNSDetailPage
            post={selectedSNSPost}
            onBack={() => {
              setViewState("main");
              setSelectedSNSPost(null);
            }}
          />
        </div>
      </div>
    );
  }

  // 주식 뉴스 상세 페이지
  if (viewState === "stock-news" && selectedStockNews) {
    return (
      <div className="min-h-screen relative z-10">
        <div className="max-w-md mx-auto px-4 pt-4 pb-20">
          <div className="space-y-4">
            {/* 뒤로가기 헤더 */}
            <div className="flex items-center justify-between">
              <button
                onClick={handleBackToMain}
                className="flex items-center space-x-2 px-3 py-1.5 glass-subtle rounded-lg hover:glass transition-all"
              >
                <ArrowLeft size={16} />
                <span className="text-sm">홈으로 돌아가기</span>
              </button>
              
              <div className="text-sm text-foreground/70">
                {selectedStockNews.news.length}개 뉴스
              </div>
            </div>

            {/* 주식 정보 */}
            <div className="glass-card rounded-xl p-4">
              <h2 className="text-lg font-bold text-center">{selectedStockNews.symbol} 관련 뉴스</h2>
            </div>

            {/* 뉴스 리스트 */}
            <div className="space-y-3">
              {selectedStockNews.news.map((news, index) => (
                <article
                  key={index}
                  className="glass-card p-4 rounded-xl cursor-pointer hover:glass transition-all group"
                  onClick={() => window.open(news.url, '_blank')}
                >
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-foreground/90">{news.source}</span>
                      <span className="text-xs text-foreground/50">
                        {formatTimestamp(news.published_at)}
                      </span>
                    </div>
                    
                    <h3 className="font-medium line-clamp-2 group-hover:text-primary transition-colors leading-tight">
                      {news.title}
                    </h3>
                    
                    <p className="text-sm text-foreground/70 line-clamp-3 leading-snug">
                      {news.description}
                    </p>
                    
                    <div className="flex items-center justify-between pt-2 border-t border-white/10">
                      <span className="text-xs text-primary font-medium">{news.symbol}</span>
                      <span className="text-xs text-foreground/50">자세히 보기 →</span>
                    </div>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // 뉴스 상세 페이지
  if (viewState === "news-detail" && selectedNewsItem) {
    return (
      <div className="min-h-screen relative z-10">
        <div className="max-w-md mx-auto px-4 pt-4 pb-20">
          <NewsDetailPage
            newsItem={selectedNewsItem}
            onBack={() => {
              setViewState("main");
              setSelectedNewsItem(null);
            }}
          />
        </div>
      </div>
    );
  }

  // 사용자 프로필 페이지
  if (viewState === "profile" && isLoggedIn) {
    return (
      <div className="min-h-screen relative z-10">
        <div className="max-w-md mx-auto">
          <div className="flex items-center justify-between p-3">
            <button
              onClick={handleBackToMain}
              className="p-2 rounded-lg glass hover:glass-strong transition-all"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="m15 18-6-6 6-6"/>
              </svg>
            </button>
            <h1 className="text-lg font-bold">프로필</h1>
            <div className="w-10" />
          </div>
          
          <div className="px-4 pb-8">
            <UserProfile
              user={user}
              onLogout={handleLogout}
              onEditProfile={() => console.log("프로필 편집")}
            />
          </div>
        </div>
      </div>
    );
  }

  const renderHeader = () => {
    if (activeTab !== "home") return null;

    return (
      <div className="flex items-center justify-between mb-3">
        <div>
          <h1 className="text-2xl font-bold bg-gradient-to-r from-white to-white/80 bg-clip-text text-transparent">
            W.E.I
          </h1>
          <p className="text-xs text-foreground/70">
            {isLoggedIn ? `${user.name}님, 안녕하세요` : "Wise & Easy Investment"}
          </p>
        </div>
        <div className="flex items-center space-x-2">
          {isLoggedIn ? (
            // 로그인된 사용자용 헤더
            <>
              <button
                onClick={handleNotificationClick}
                className="relative w-8 h-8 rounded-full glass flex items-center justify-center hover:glass-strong transition-all"
              >
                <Bell size={16} />
                {/* 알림 배지 */}
                <div className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-red-500 rounded-full shadow-lg"></div>
              </button>
              <button
                onClick={handleUserIconClick}
                className="w-8 h-8 rounded-full glass flex items-center justify-center hover:glass-strong transition-all"
              >
                <User size={16} />
              </button>
            </>
          ) : (
            // 게스트용 헤더
            <button
              onClick={handleLoginClick}
              className="flex items-center space-x-2 px-3 py-1.5 glass-strong rounded-lg hover:bg-white/10 transition-all"
            >
              <LogIn size={16} />
              <span className="text-xs font-medium">로그인</span>
            </button>
          )}
        </div>
      </div>
    );
  };

  const renderContent = () => {
    switch (activeTab) {
      case "home":
        return (
          <div className="space-y-4 relative z-10">
            {renderHeader()}

            {/* 실시간 급변동 배너 - WEI 타이틀 바로 밑 */}
            <StockBanner onStockNewsClick={handleStockNewsClick} />

            {/* 시장 이벤트 캘린더 - 스톡 배너 바로 밑 */}
            <EventCalendar />

            {/* SNS 피드 */}
            <SocialFeed isLoggedIn={isLoggedIn} onPostClick={handleSNSPostClick} />

            {/* 뉴스 리스트 */}
            <NewsList onViewAll={() => setActiveTab("news")} />
          </div>
        );

      case "markets":
        return (
          <div className="space-y-6 relative z-10">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl glass">
                  <TrendingUp size={24} className="text-primary" />
                </div>
                <h1 className="text-2xl font-bold">시장 & 재무</h1>
              </div>
            </div>
            
            <MarketPage />
          </div>
        );

      case "sns":
        return (
          <div className="space-y-6 relative z-10">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl glass">
                  <MessageSquare size={24} className="text-primary" />
                </div>
                <h1 className="text-2xl font-bold">SNS 피드</h1>
              </div>
            </div>
            
            <SNSPage 
              isLoggedIn={isLoggedIn} 
              onLoginPrompt={handleLoginClick}
              onPostClick={handleSNSPostClick}
            />
          </div>
        );

      case "news":
        return (
          <div className="space-y-6 relative z-10">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl glass">
                  <Newspaper size={24} className="text-primary" />
                </div>
                <h1 className="text-2xl font-bold">뉴스</h1>
              </div>
            </div>
            
            <NewsPage isLoggedIn={isLoggedIn} onLoginPrompt={handleLoginClick} onNewsClick={handleNewsClick} />
          </div>
        );

      case "ai":
        return (
          <div className="space-y-6 relative z-10">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl glass">
                  <Bot size={24} className="text-primary" />
                </div>
                <h1 className="text-2xl font-bold">AI 분석</h1>
              </div>
              {!isLoggedIn && (
                <button
                  onClick={handleLoginClick}
                  className="px-3 py-1 glass rounded-lg text-sm hover:glass-strong transition-all"
                >
                  로그인
                </button>
              )}
            </div>
            
            <AIAnalysis isLoggedIn={isLoggedIn} onLoginPrompt={handleLoginClick} />
          </div>
        );

      case "economy":
        return (
          <div className="space-y-6 relative z-10">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl glass">
                  <BarChart3 size={24} className="text-primary" />
                </div>
                <h1 className="text-2xl font-bold">경제 지표</h1>
              </div>
            </div>
            
            <EconomicDashboard isLoggedIn={true} onLoginPrompt={handleLoginClick} />
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen relative">
      <div className="max-w-md mx-auto relative z-10">
        {/* 메인 콘텐츠 - 상단 패딩 줄임 */}
        <div className="px-4 pt-4 pb-20">
          {renderContent()}
        </div>

        {/* 하단 네비게이션 */}
        <BottomNavigation activeTab={activeTab} onTabChange={setActiveTab} />

        {/* 알림 시스템 (로그인한 사용자만) */}
        {isLoggedIn && (
          <NotificationSystem
            isVisible={showNotifications}
            onClose={() => setShowNotifications(false)}
          />
        )}
      </div>
    </div>
  );
}