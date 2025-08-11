import React from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { 
  Home,
  TrendingUp,
  Bitcoin,
  Newspaper,
  BarChart3,
  User,
  Settings,
  X,
  ChevronRight,
  LogOut
} from 'lucide-react';
import Button from '../ui/button';
import { useAuth } from '../../hooks/useAuth';
import { cn } from '../../utils/helpers';

interface SidebarProps {
  onClose?: () => void;
  className?: string;
}

interface NavItem {
  title: string;
  href: string;
  icon: React.ElementType;
  badge?: string | number;
  children?: NavItem[];
}

const navItems: NavItem[] = [
  {
    title: '대시보드',
    href: '/',
    icon: Home,
  },
  {
    title: '주식',
    href: '/stocks',
    icon: TrendingUp,
    children: [
      { title: '주식 목록', href: '/stocks', icon: TrendingUp },
      { title: '관심 종목', href: '/stocks/watchlist', icon: TrendingUp },
    ],
  },
  {
    title: '암호화폐',
    href: '/crypto',
    icon: Bitcoin,
    children: [
      { title: '암호화폐 목록', href: '/crypto', icon: Bitcoin },
      { title: '포트폴리오', href: '/crypto/portfolio', icon: Bitcoin },
    ],
  },
  {
    title: '뉴스',
    href: '/news',
    icon: Newspaper,
    badge: 'NEW',
  },
  {
    title: '경제지표',
    href: '/economic',
    icon: BarChart3,
  },
];

export const Sidebar: React.FC<SidebarProps> = ({
  onClose,
  className
}) => {
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [expandedItems, setExpandedItems] = React.useState<string[]>([]);

  const toggleExpanded = (href: string) => {
    setExpandedItems(prev =>
      prev.includes(href)
        ? prev.filter(item => item !== href)
        : [...prev, href]
    );
  };

  const isActive = (href: string) => {
    if (href === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(href);
  };

  const handleNavClick = () => {
    // 모바일에서 네비게이션 클릭 시 사이드바 닫기
    if (onClose) {
      onClose();
    }
  };

  const handleLogout = async () => {
    try {
      await logout();
      if (onClose) onClose();
      navigate('/login');
    } catch (error) {
      console.error('로그아웃 실패:', error);
    }
  };

  const renderNavItem = (item: NavItem, level: number = 0) => {
    const Icon = item.icon;
    const hasChildren = item.children && item.children.length > 0;
    const isExpanded = expandedItems.includes(item.href);
    const active = isActive(item.href);

    return (
      <div key={item.href}>
        <div className="relative">
          {hasChildren ? (
            <button
              onClick={() => toggleExpanded(item.href)}
              className={cn(
                'w-full flex items-center space-x-3 px-4 py-3 rounded-xl text-left transition-all duration-300',
                'hover:bg-white/10 lg:hover:bg-muted',
                level > 0 && 'ml-4',
                active && 'bg-white/20 text-white lg:bg-primary/10 lg:text-primary',
                !active && 'text-white/70 hover:text-white lg:text-foreground'
              )}
            >
              <Icon className="h-5 w-5 flex-shrink-0" />
              <span className="flex-1 font-medium">{item.title}</span>
              {item.badge && (
                <span className="px-2 py-1 text-xs bg-gradient-to-r from-cyan-500 to-purple-500 text-white rounded-full font-bold lg:bg-primary lg:text-primary-foreground">
                  {item.badge}
                </span>
              )}
              <ChevronRight 
                className={cn(
                  'h-4 w-4 transition-transform duration-300',
                  isExpanded && 'transform rotate-90'
                )} 
              />
            </button>
          ) : (
            <Link
              to={item.href}
              onClick={handleNavClick}
              className={cn(
                'flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-300',
                'hover:bg-white/10 lg:hover:bg-muted',
                level > 0 && 'ml-4',
                active && 'bg-white/20 text-white scale-105 lg:bg-primary/10 lg:text-primary lg:scale-100',
                !active && 'text-white/70 hover:text-white lg:text-foreground'
              )}
            >
              <Icon className="h-5 w-5 flex-shrink-0" />
              <span className="flex-1 font-medium">{item.title}</span>
              {item.badge && (
                <span className="px-2 py-1 text-xs bg-gradient-to-r from-cyan-500 to-purple-500 text-white rounded-full font-bold lg:bg-primary lg:text-primary-foreground">
                  {item.badge}
                </span>
              )}
            </Link>
          )}
        </div>

        {/* 하위 메뉴 */}
        {hasChildren && isExpanded && (
          <div className="mt-2 space-y-1">
            {item.children!.map(child => renderNavItem(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={cn(
      'h-full flex flex-col',
      // 모바일 스타일
      'bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 backdrop-blur-xl border-r border-white/10',
      // 데스크톱 스타일
      'lg:bg-background lg:border-border',
      className
    )}>
      {/* 헤더 */}
      <div className="flex items-center justify-between p-4 lg:p-6 border-b border-white/10 lg:border-border">
        <div className="flex items-center space-x-3">
          {/* 모바일: 사용자 아바타, 데스크톱: 로고 */}
          <div className="lg:hidden">
            <div className="text-2xl">👤</div>
          </div>
          <div className="hidden lg:flex items-center justify-center h-10 w-10 rounded-xl bg-gradient-to-br from-primary to-accent">
            <TrendingUp className="h-6 w-6 text-white" />
          </div>
          <div>
            {/* 모바일: 사용자 정보, 데스크톱: 앱 이름 */}
            <h2 className="font-bold text-white lg:text-foreground lg:text-xl">
              <span className="lg:hidden">{user?.fullName || '사용자'}</span>
              <span className="hidden lg:inline">W.E.I</span>
            </h2>
            <p className="text-sm text-white/60 lg:text-muted-foreground">
              <span className="lg:hidden">{user?.email}</span>
              <span className="hidden lg:inline">Wise & Easy Investment</span>
            </p>
          </div>
        </div>
        
        {/* 닫기 버튼 (모바일만) */}
        <Button
          variant="glass"
          size="sm"
          onClick={onClose}
          className="text-white/70 hover:text-white hover:bg-white/10 lg:hidden"
        >
          <X className="h-5 w-5" />
        </Button>
      </div>

      {/* 메인 네비게이션 */}
      <nav className="flex-1 p-4 space-y-2 overflow-y-auto">
        {navItems.map(item => renderNavItem(item))}
      </nav>

      {/* 하단 액션들 */}
      <div className="p-4 border-t border-white/10 lg:border-border space-y-2">
        {/* 모바일: 프로필/설정/로그아웃, 데스크톱: 사용자 정보 + 액션들 */}
        
        {/* 데스크톱 사용자 정보 */}
        <div className="hidden lg:flex items-center space-x-3 mb-4">
          <div className="h-10 w-10 bg-gradient-to-br from-primary to-accent rounded-full flex items-center justify-center">
            <User className="h-5 w-5 text-white" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-medium truncate">{user?.fullName || '사용자'}</p>
            <p className="text-sm text-muted-foreground truncate">{user?.email}</p>
          </div>
        </div>
        
        {/* 프로필 링크 */}
        <Link
          to="/profile"
          onClick={handleNavClick}
          className={cn(
            'flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-300',
            'text-white/70 hover:text-white hover:bg-white/10',
            'lg:text-foreground lg:hover:bg-muted lg:px-3 lg:py-2 lg:text-sm'
          )}
        >
          <User className="h-5 w-5 lg:h-4 lg:w-4" />
          <span className="font-medium">프로필</span>
        </Link>
        
        {/* 설정 링크 */}
        <Link
          to="/settings"
          onClick={handleNavClick}
          className={cn(
            'flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-300',
            'text-white/70 hover:text-white hover:bg-white/10',
            'lg:text-foreground lg:hover:bg-muted lg:px-3 lg:py-2 lg:text-sm'
          )}
        >
          <Settings className="h-5 w-5 lg:h-4 lg:w-4" />
          <span className="font-medium">설정</span>
        </Link>
        
        {/* 로그아웃 버튼 */}
        <button
          onClick={handleLogout}
          className={cn(
            'w-full flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-300',
            'text-red-300 hover:text-red-200 hover:bg-red-500/20',
            'lg:text-destructive lg:hover:bg-destructive/10 lg:px-3 lg:py-2 lg:text-sm'
          )}
        >
          <LogOut className="h-5 w-5 lg:h-4 lg:w-4" />
          <span className="font-medium">로그아웃</span>
        </button>
      </div>
    </div>
  );
};