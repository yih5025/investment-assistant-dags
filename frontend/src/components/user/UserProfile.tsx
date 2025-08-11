import { useState } from "react";
import { 
  User, 
  Settings, 
  Bell, 
  Shield, 
  CreditCard, 
  HelpCircle, 
  LogOut,
  ChevronRight,
  Edit3,
  Camera,
  Award,
  TrendingUp
} from "lucide-react";

interface UserProfileProps {
  user: {
    name: string;
    email: string;
    phone: string;
    profileImage?: string;
    memberSince: string;
    totalReturn: number;
    portfolioValue: number;
  };
  onLogout: () => void;
  onEditProfile: () => void;
}

export function UserProfile({ user, onLogout, onEditProfile }: UserProfileProps) {
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

  const menuItems = [
    {
      icon: Edit3,
      title: "프로필 편집",
      description: "개인정보 수정",
      action: onEditProfile,
      color: "text-blue-400"
    },
    {
      icon: Bell,
      title: "알림 설정",
      description: "푸시 알림 관리",
      action: () => console.log("알림 설정"),
      color: "text-green-400"
    },
    {
      icon: Shield,
      title: "보안 설정",
      description: "비밀번호 및 생체인증",
      action: () => console.log("보안 설정"),
      color: "text-purple-400"
    },
    {
      icon: CreditCard,
      title: "결제 정보",
      description: "결제 수단 관리",
      action: () => console.log("결제 정보"),
      color: "text-yellow-400"
    },
    {
      icon: HelpCircle,
      title: "고객 지원",
      description: "FAQ 및 문의하기",
      action: () => console.log("고객 지원"),
      color: "text-orange-400"
    }
  ];

  const handleLogout = () => {
    setShowLogoutConfirm(false);
    onLogout();
  };

  return (
    <div className="space-y-6">
      {/* 프로필 헤더 */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center space-x-4 mb-4">
          <div className="relative">
            <div className="w-20 h-20 rounded-2xl bg-primary/20 flex items-center justify-center overflow-hidden">
              {user.profileImage ? (
                <img src={user.profileImage} alt="Profile" className="w-full h-full object-cover" />
              ) : (
                <User size={32} className="text-primary" />
              )}
            </div>
            <button className="absolute -bottom-1 -right-1 w-6 h-6 bg-primary rounded-full flex items-center justify-center">
              <Camera size={14} className="text-white" />
            </button>
          </div>
          
          <div className="flex-1">
            <h2 className="text-xl font-bold">{user.name}</h2>
            <p className="text-foreground/70">{user.email}</p>
            <div className="flex items-center space-x-2 mt-1">
              <Award size={14} className="text-yellow-400" />
              <span className="text-sm text-foreground/60">
                {user.memberSince} 부터 함께하고 있어요
              </span>
            </div>
          </div>
        </div>

        {/* 투자 성과 요약 */}
        <div className="grid grid-cols-2 gap-4 pt-4 border-t border-white/10">
          <div className="text-center">
            <div className="text-lg font-semibold text-green-400">
              +{user.totalReturn.toFixed(1)}%
            </div>
            <div className="text-sm text-foreground/70">총 수익률</div>
          </div>
          <div className="text-center">
            <div className="text-lg font-semibold">
              ${user.portfolioValue.toLocaleString()}
            </div>
            <div className="text-sm text-foreground/70">포트폴리오 가치</div>
          </div>
        </div>
      </div>

      {/* 빠른 액션 */}
      <div className="grid grid-cols-3 gap-3">
        <button className="glass-card rounded-2xl p-4 text-center hover:scale-[1.02] transition-transform">
          <TrendingUp className="w-6 h-6 mx-auto mb-2 text-green-400" />
          <span className="text-sm">투자 분석</span>
        </button>
        <button className="glass-card rounded-2xl p-4 text-center hover:scale-[1.02] transition-transform">
          <Bell className="w-6 h-6 mx-auto mb-2 text-blue-400" />
          <span className="text-sm">알림</span>
        </button>
        <button className="glass-card rounded-2xl p-4 text-center hover:scale-[1.02] transition-transform">
          <Settings className="w-6 h-6 mx-auto mb-2 text-purple-400" />
          <span className="text-sm">설정</span>
        </button>
      </div>

      {/* 메뉴 리스트 */}
      <div className="glass-card rounded-2xl p-4">
        <h3 className="font-semibold mb-4">계정 관리</h3>
        <div className="space-y-1">
          {menuItems.map((item, index) => {
            const Icon = item.icon;
            return (
              <button
                key={index}
                onClick={item.action}
                className="w-full flex items-center space-x-3 p-3 rounded-xl hover:bg-white/10 transition-colors"
              >
                <Icon size={20} className={item.color} />
                <div className="flex-1 text-left">
                  <div className="font-medium">{item.title}</div>
                  <div className="text-sm text-foreground/60">{item.description}</div>
                </div>
                <ChevronRight size={16} className="text-foreground/40" />
              </button>
            );
          })}
        </div>
      </div>

      {/* 로그아웃 */}
      <div className="glass-card rounded-2xl p-4">
        <button
          onClick={() => setShowLogoutConfirm(true)}
          className="w-full flex items-center space-x-3 p-3 rounded-xl hover:bg-red-500/10 transition-colors text-red-400"
        >
          <LogOut size={20} />
          <span className="flex-1 text-left font-medium">로그아웃</span>
          <ChevronRight size={16} className="text-foreground/40" />
        </button>
      </div>

      {/* 로그아웃 확인 모달 */}
      {showLogoutConfirm && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="glass-strong rounded-2xl p-6 w-full max-w-sm">
            <h3 className="text-lg font-semibold mb-2">로그아웃</h3>
            <p className="text-foreground/70 mb-6">
              정말 로그아웃 하시겠습니까?
            </p>
            <div className="flex space-x-3">
              <button
                onClick={() => setShowLogoutConfirm(false)}
                className="flex-1 glass rounded-xl p-3 font-medium transition-colors hover:bg-white/10"
              >
                취소
              </button>
              <button
                onClick={handleLogout}
                className="flex-1 bg-red-500/20 text-red-400 rounded-xl p-3 font-medium transition-colors hover:bg-red-500/30"
              >
                로그아웃
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}