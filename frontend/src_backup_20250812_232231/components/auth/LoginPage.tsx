import { useState } from "react";
import { Eye, EyeOff, Mail, Lock, ArrowLeft, Fingerprint } from "lucide-react";

interface LoginPageProps {
  onBack: () => void;
  onLogin: (email: string, password: string) => void;
  onNavigateToSignup: () => void;
  onForgotPassword: () => void;
}

export function LoginPage({ onBack, onLogin, onNavigateToSignup, onForgotPassword }: LoginPageProps) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [rememberMe, setRememberMe] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    
    // 로그인 시뮬레이션
    setTimeout(() => {
      onLogin(email, password);
      setIsLoading(false);
    }, 1500);
  };

  const handleBiometricLogin = () => {
    // 생체 인증 시뮬레이션
    setIsLoading(true);
    setTimeout(() => {
      onLogin("biometric", "biometric");
      setIsLoading(false);
    }, 1000);
  };

  return (
    <div className="min-h-screen flex flex-col">
      {/* 헤더 */}
      <div className="flex items-center space-x-3 p-4">
        <button
          onClick={onBack}
          className="p-2 rounded-lg glass hover:bg-white/10 transition-colors"
        >
          <ArrowLeft size={20} />
        </button>
        <h1 className="text-xl font-bold">로그인</h1>
      </div>

      <div className="flex-1 px-4 pb-8">
        {/* 로고 및 환영 메시지 */}
        <div className="text-center mb-8">
          <div className="w-20 h-20 mx-auto mb-4 rounded-2xl bg-primary/20 flex items-center justify-center">
            <span className="text-2xl font-bold text-primary">W.E.I</span>
          </div>
          <h2 className="text-2xl font-bold mb-2">다시 오신 것을 환영합니다</h2>
          <p className="text-foreground/70">현명한 투자의 시작</p>
        </div>

        {/* 로그인 폼 */}
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* 이메일 입력 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">이메일</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Mail size={20} className="text-foreground/60" />
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="이메일을 입력하세요"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
            </div>
          </div>

          {/* 비밀번호 입력 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">비밀번호</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Lock size={20} className="text-foreground/60" />
              <input
                type={showPassword ? "text" : "password"}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="비밀번호를 입력하세요"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="text-foreground/60 hover:text-foreground transition-colors"
              >
                {showPassword ? <EyeOff size={20} /> : <Eye size={20} />}
              </button>
            </div>
          </div>

          {/* 옵션 */}
          <div className="flex items-center justify-between">
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={(e) => setRememberMe(e.target.checked)}
                className="sr-only"
              />
              <div className={`w-4 h-4 rounded border transition-all ${
                rememberMe 
                  ? "bg-primary border-primary" 
                  : "border-foreground/30"
              }`}>
                {rememberMe && (
                  <div className="w-full h-full flex items-center justify-center">
                    <div className="w-2 h-2 bg-white rounded-sm"></div>
                  </div>
                )}
              </div>
              <span className="text-sm">로그인 상태 유지</span>
            </label>
            
            <button
              type="button"
              onClick={onForgotPassword}
              className="text-sm text-primary hover:text-primary/80 transition-colors"
            >
              비밀번호 찾기
            </button>
          </div>

          {/* 로그인 버튼 */}
          <button
            type="submit"
            disabled={isLoading}
            className="w-full glass-strong rounded-2xl p-4 font-medium transition-all hover:scale-[1.02] disabled:opacity-50 disabled:cursor-not-allowed bg-primary/20 text-primary"
          >
            {isLoading ? (
              <div className="flex items-center justify-center space-x-2">
                <div className="w-4 h-4 border-2 border-primary border-t-transparent rounded-full animate-spin"></div>
                <span>로그인 중...</span>
              </div>
            ) : (
              "로그인"
            )}
          </button>
        </form>

        {/* 생체 인증 */}
        <div className="mt-6">
          <div className="flex items-center space-x-4 mb-4">
            <div className="flex-1 h-px bg-foreground/20"></div>
            <span className="text-sm text-foreground/60">또는</span>
            <div className="flex-1 h-px bg-foreground/20"></div>
          </div>

          <button
            onClick={handleBiometricLogin}
            disabled={isLoading}
            className="w-full glass-card rounded-2xl p-4 flex items-center justify-center space-x-2 transition-all hover:scale-[1.02] disabled:opacity-50"
          >
            <Fingerprint size={20} className="text-primary" />
            <span>생체 인증으로 로그인</span>
          </button>
        </div>

        {/* 회원가입 링크 */}
        <div className="text-center mt-6">
          <span className="text-foreground/70">계정이 없으신가요? </span>
          <button
            onClick={onNavigateToSignup}
            className="text-primary hover:text-primary/80 font-medium transition-colors"
          >
            회원가입
          </button>
        </div>
      </div>
    </div>
  );
}