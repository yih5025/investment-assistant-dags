import { useState } from "react";
import { BottomNavigation } from "./components/BottomNavigation";
import { StockBanner } from "./components/StockBanner";
import { EventCalendar } from "./components/EventCalendar";
import { SocialFeed } from "./components/SocialFeed";
import { NewsList } from "./components/NewsList";
import { IntegratedMarket } from "./components/IntegratedMarket";
import { NewsPage } from "./components/NewsPage";
import { AIAnalysis } from "./components/AIAnalysis";
import { EconomicDashboard } from "./components/EconomicDashboard";
import { LoginPage } from "./components/auth/LoginPage";
import { SignupPage } from "./components/auth/SignupPage";
import { UserProfile } from "./components/user/UserProfile";
import { NotificationSystem } from "./components/notifications/NotificationSystem";
import { TrendingUp, Newspaper, Bot, BarChart3, Bell, User, LogIn } from "lucide-react";

type AuthState = "guest" | "login" | "signup" | "authenticated";
type ViewState = "main" | "auth" | "profile";

export default function App() {
  const [activeTab, setActiveTab] = useState("home");
  const [authState, setAuthState] = useState<AuthState>("guest"); // 게스트 모드로 시작
  const [viewState, setViewState] = useState<ViewState>("main");
  const [showNotifications, setShowNotifications] = useState(false);
  
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
    if (!isLoggedIn) {
      setAuthState("guest");
    }
  };

  // 인증 페이지 렌더링
  if (viewState === "auth") {
    if (authState === "login") {
      return (
        <LoginPage
          onBack={handleBackToMain}
          onLogin={handleLogin}
          onNavigateToSignup={() => setAuthState("signup")}
          onForgotPassword={() => console.log("비밀번호 찾기")}
        />
      );
    }

    if (authState === "signup") {
      return (
        <SignupPage
          onBack={() => setAuthState("login")}
          onSignup={handleSignup}
          onNavigateToLogin={() => setAuthState("login")}
        />
      );
    }
  }

  // 사용자 프로필 페이지
  if (viewState === "profile" && isLoggedIn) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-md mx-auto">
          <div className="flex items-center justify-between p-4">
            <button
              onClick={handleBackToMain}
              className="p-2 rounded-lg glass hover:bg-white/10 transition-colors"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="m15 18-6-6 6-6"/>
              </svg>
            </button>
            <h1 className="text-xl font-bold">프로필</h1>
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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">W.E.I</h1>
          <p className="text-sm text-foreground/70">
            {isLoggedIn ? `안녕하세요, ${user.name}님!` : "Wise & Easy Investment"}
          </p>
        </div>
        <div className="flex items-center space-x-2">
          {isLoggedIn ? (
            // 로그인된 사용자용 헤더
            <>
              <button
                onClick={handleNotificationClick}
                className="relative w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center hover:bg-primary/30 transition-colors"
              >
                <Bell size={20} />
                {/* 알림 배지 */}
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-red-500 rounded-full"></div>
              </button>
              <button
                onClick={handleUserIconClick}
                className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center hover:bg-primary/30 transition-colors"
              >
                <User size={20} />
              </button>
            </>
          ) : (
            // 게스트용 헤더
            <button
              onClick={handleLoginClick}
              className="flex items-center space-x-2 px-4 py-2 glass-strong rounded-xl hover:bg-primary/20 transition-colors"
            >
              <LogIn size={18} />
              <span className="text-sm font-medium">로그인</span>
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
          <div className="space-y-6">
            {renderHeader()}

            {/* 로그인 유도 배너 (게스트용) */}
            {!isLoggedIn && (
              <div className="glass-card rounded-2xl p-4 border border-primary/30">
                <div className="text-center">
                  <h3 className="font-semibold mb-2">🚀 더 많은 기능을 경험해보세요!</h3>
                  <p className="text-sm text-foreground/70 mb-3">
                    로그인하면 맞춤형 투자 분석, 관심 종목 추적, 실시간 알림 등 더 많은 기능을 이용할 수 있어요.
                  </p>
                  <button
                    onClick={handleLoginClick}
                    className="px-4 py-2 bg-primary/20 text-primary rounded-xl text-sm font-medium hover:bg-primary/30 transition-colors"
                  >
                    로그인하고 시작하기
                  </button>
                </div>
              </div>
            )}

            {/* 주식 배너 */}
            <StockBanner />

            {/* 시장 이벤트 캘린더 */}
            <EventCalendar />

            {/* 소셜 피드 */}
            <SocialFeed isLoggedIn={isLoggedIn} />

            {/* 뉴스 리스트 */}
            <NewsList />
          </div>
        );

      case "markets":
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <TrendingUp size={24} className="text-primary" />
                <h1 className="text-2xl font-bold">시장 & 재무</h1>
              </div>
              {!isLoggedIn && (
                <button
                  onClick={handleLoginClick}
                  className="px-3 py-1 glass rounded-lg text-sm hover:bg-primary/20 transition-colors"
                >
                  로그인
                </button>
              )}
            </div>
            
            <IntegratedMarket isLoggedIn={isLoggedIn} onLoginPrompt={handleLoginClick} />
          </div>
        );

      case "news":
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <Newspaper size={24} className="text-primary" />
                <h1 className="text-2xl font-bold">뉴스</h1>
              </div>
              {!isLoggedIn && (
                <button
                  onClick={handleLoginClick}
                  className="px-3 py-1 glass rounded-lg text-sm hover:bg-primary/20 transition-colors"
                >
                  로그인
                </button>
              )}
            </div>
            
            <NewsPage isLoggedIn={isLoggedIn} onLoginPrompt={handleLoginClick} />
          </div>
        );

      case "ai":
        return (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <Bot size={24} className="text-primary" />
                <h1 className="text-2xl font-bold">AI 분석</h1>
              </div>
              {!isLoggedIn && (
                <button
                  onClick={handleLoginClick}
                  className="px-3 py-1 glass rounded-lg text-sm hover:bg-primary/20 transition-colors"
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
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <BarChart3 size={24} className="text-primary" />
                <h1 className="text-2xl font-bold">경제 지표</h1>
              </div>
              {!isLoggedIn && (
                <button
                  onClick={handleLoginClick}
                  className="px-3 py-1 glass rounded-lg text-sm hover:bg-primary/20 transition-colors"
                >
                  로그인
                </button>
              )}
            </div>
            
            <EconomicDashboard isLoggedIn={isLoggedIn} onLoginPrompt={handleLoginClick} />
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-md mx-auto relative">
        {/* 메인 콘텐츠 */}
        <div className="px-4 pt-8 pb-20">
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