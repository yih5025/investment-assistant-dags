import { useState } from "react";
import { Eye, EyeOff, Mail, Lock, User, ArrowLeft, Phone, CheckCircle } from "lucide-react";

interface SignupPageProps {
  onBack: () => void;
  onSignup: (userData: any) => void;
  onNavigateToLogin: () => void;
}

export function SignupPage({ onBack, onSignup, onNavigateToLogin }: SignupPageProps) {
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    phone: "",
    password: "",
    confirmPassword: ""
  });
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [agreedToTerms, setAgreedToTerms] = useState(false);
  const [agreedToPrivacy, setAgreedToPrivacy] = useState(false);
  const [agreedToMarketing, setAgreedToMarketing] = useState(false);

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const validateForm = () => {
    if (formData.password !== formData.confirmPassword) {
      alert("비밀번호가 일치하지 않습니다.");
      return false;
    }
    if (!agreedToTerms || !agreedToPrivacy) {
      alert("필수 약관에 동의해주세요.");
      return false;
    }
    return true;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateForm()) return;
    
    setIsLoading(true);
    
    // 회원가입 시뮬레이션
    setTimeout(() => {
      onSignup(formData);
      setIsLoading(false);
    }, 1500);
  };

  const passwordStrength = (password: string) => {
    let strength = 0;
    if (password.length >= 8) strength++;
    if (/[A-Z]/.test(password)) strength++;
    if (/[0-9]/.test(password)) strength++;
    if (/[^A-Za-z0-9]/.test(password)) strength++;
    return strength;
  };

  const getPasswordStrengthText = (strength: number) => {
    switch (strength) {
      case 0:
      case 1:
        return { text: "약함", color: "text-red-400" };
      case 2:
        return { text: "보통", color: "text-yellow-400" };
      case 3:
      case 4:
        return { text: "강함", color: "text-green-400" };
      default:
        return { text: "", color: "" };
    }
  };

  const strengthInfo = getPasswordStrengthText(passwordStrength(formData.password));

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
        <h1 className="text-xl font-bold">회원가입</h1>
      </div>

      <div className="flex-1 px-4 pb-8">
        {/* 환영 메시지 */}
        <div className="text-center mb-6">
          <div className="w-16 h-16 mx-auto mb-3 rounded-2xl bg-primary/20 flex items-center justify-center">
            <span className="text-lg font-bold text-primary">W.E.I</span>
          </div>
          <h2 className="text-xl font-bold mb-1">투자의 새로운 시작</h2>
          <p className="text-sm text-foreground/70">간편하게 가입하고 똑똑한 투자를 시작하세요</p>
        </div>

        {/* 회원가입 폼 */}
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* 이름 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">이름</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <User size={20} className="text-foreground/60" />
              <input
                type="text"
                value={formData.name}
                onChange={(e) => handleInputChange("name", e.target.value)}
                placeholder="이름을 입력하세요"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
            </div>
          </div>

          {/* 이메일 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">이메일</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Mail size={20} className="text-foreground/60" />
              <input
                type="email"
                value={formData.email}
                onChange={(e) => handleInputChange("email", e.target.value)}
                placeholder="이메일을 입력하세요"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
            </div>
          </div>

          {/* 전화번호 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">전화번호</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Phone size={20} className="text-foreground/60" />
              <input
                type="tel"
                value={formData.phone}
                onChange={(e) => handleInputChange("phone", e.target.value)}
                placeholder="010-1234-5678"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
            </div>
          </div>

          {/* 비밀번호 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">비밀번호</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Lock size={20} className="text-foreground/60" />
              <input
                type={showPassword ? "text" : "password"}
                value={formData.password}
                onChange={(e) => handleInputChange("password", e.target.value)}
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
            {formData.password && (
              <div className="mt-2 flex items-center space-x-2">
                <span className="text-xs">비밀번호 강도:</span>
                <span className={`text-xs font-medium ${strengthInfo.color}`}>
                  {strengthInfo.text}
                </span>
                <div className="flex-1 h-1 bg-foreground/20 rounded-full overflow-hidden">
                  <div 
                    className={`h-full transition-all ${
                      passwordStrength(formData.password) === 1 ? 'bg-red-400 w-1/4' :
                      passwordStrength(formData.password) === 2 ? 'bg-yellow-400 w-2/4' :
                      passwordStrength(formData.password) >= 3 ? 'bg-green-400 w-full' : 'w-0'
                    }`}
                  />
                </div>
              </div>
            )}
          </div>

          {/* 비밀번호 확인 */}
          <div className="glass-card rounded-2xl p-4">
            <label className="block text-sm font-medium mb-2">비밀번호 확인</label>
            <div className="glass rounded-xl p-3 flex items-center space-x-3">
              <Lock size={20} className="text-foreground/60" />
              <input
                type={showConfirmPassword ? "text" : "password"}
                value={formData.confirmPassword}
                onChange={(e) => handleInputChange("confirmPassword", e.target.value)}
                placeholder="비밀번호를 다시 입력하세요"
                className="flex-1 bg-transparent outline-none placeholder-foreground/50"
                required
              />
              <button
                type="button"
                onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                className="text-foreground/60 hover:text-foreground transition-colors"
              >
                {showConfirmPassword ? <EyeOff size={20} /> : <Eye size={20} />}
              </button>
            </div>
            {formData.confirmPassword && (
              <div className="mt-2 flex items-center space-x-1">
                {formData.password === formData.confirmPassword ? (
                  <>
                    <CheckCircle size={14} className="text-green-400" />
                    <span className="text-xs text-green-400">비밀번호가 일치합니다</span>
                  </>
                ) : (
                  <span className="text-xs text-red-400">비밀번호가 일치하지 않습니다</span>
                )}
              </div>
            )}
          </div>

          {/* 약관 동의 */}
          <div className="glass-card rounded-2xl p-4 space-y-3">
            <h3 className="font-medium">약관 동의</h3>
            
            <label className="flex items-start space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={agreedToTerms}
                onChange={(e) => setAgreedToTerms(e.target.checked)}
                className="sr-only"
              />
              <div className={`w-4 h-4 rounded border mt-0.5 flex-shrink-0 transition-all ${
                agreedToTerms 
                  ? "bg-primary border-primary" 
                  : "border-foreground/30"
              }`}>
                {agreedToTerms && (
                  <div className="w-full h-full flex items-center justify-center">
                    <CheckCircle size={12} className="text-white" />
                  </div>
                )}
              </div>
              <div>
                <span className="text-sm">
                  <span className="text-red-400">[필수]</span> 서비스 이용약관에 동의합니다
                </span>
                <button type="button" className="block text-xs text-primary hover:text-primary/80">
                  자세히 보기
                </button>
              </div>
            </label>

            <label className="flex items-start space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={agreedToPrivacy}
                onChange={(e) => setAgreedToPrivacy(e.target.checked)}
                className="sr-only"
              />
              <div className={`w-4 h-4 rounded border mt-0.5 flex-shrink-0 transition-all ${
                agreedToPrivacy 
                  ? "bg-primary border-primary" 
                  : "border-foreground/30"
              }`}>
                {agreedToPrivacy && (
                  <div className="w-full h-full flex items-center justify-center">
                    <CheckCircle size={12} className="text-white" />
                  </div>
                )}
              </div>
              <div>
                <span className="text-sm">
                  <span className="text-red-400">[필수]</span> 개인정보 처리방침에 동의합니다
                </span>
                <button type="button" className="block text-xs text-primary hover:text-primary/80">
                  자세히 보기
                </button>
              </div>
            </label>

            <label className="flex items-start space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={agreedToMarketing}
                onChange={(e) => setAgreedToMarketing(e.target.checked)}
                className="sr-only"
              />
              <div className={`w-4 h-4 rounded border mt-0.5 flex-shrink-0 transition-all ${
                agreedToMarketing 
                  ? "bg-primary border-primary" 
                  : "border-foreground/30"
              }`}>
                {agreedToMarketing && (
                  <div className="w-full h-full flex items-center justify-center">
                    <CheckCircle size={12} className="text-white" />
                  </div>
                )}
              </div>
              <div>
                <span className="text-sm">
                  [선택] 마케팅 정보 수신에 동의합니다
                </span>
                <p className="text-xs text-foreground/60">투자 정보, 이벤트 소식을 받아보실 수 있습니다</p>
              </div>
            </label>
          </div>

          {/* 회원가입 버튼 */}
          <button
            type="submit"
            disabled={isLoading || !agreedToTerms || !agreedToPrivacy}
            className="w-full glass-strong rounded-2xl p-4 font-medium transition-all hover:scale-[1.02] disabled:opacity-50 disabled:cursor-not-allowed bg-primary/20 text-primary"
          >
            {isLoading ? (
              <div className="flex items-center justify-center space-x-2">
                <div className="w-4 h-4 border-2 border-primary border-t-transparent rounded-full animate-spin"></div>
                <span>가입 중...</span>
              </div>
            ) : (
              "회원가입"
            )}
          </button>
        </form>

        {/* 로그인 링크 */}
        <div className="text-center mt-6">
          <span className="text-foreground/70">이미 계정이 있으신가요? </span>
          <button
            onClick={onNavigateToLogin}
            className="text-primary hover:text-primary/80 font-medium transition-colors"
          >
            로그인
          </button>
        </div>
      </div>
    </div>
  );
}