import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  Home,
  TrendingUp,
  Bitcoin,
  Newspaper,
  BarChart3
} from 'lucide-react';
import { cn } from '../../utils/helpers';

interface NavItem {
  title: string;
  href: string;
  icon: React.ElementType;
  badge?: string;
}

const navItems: NavItem[] = [
  {
    title: '홈',
    href: '/',
    icon: Home,
  },
  {
    title: '주식',
    href: '/stocks',
    icon: TrendingUp,
  },
  {
    title: '암호화폐',
    href: '/crypto',
    icon: Bitcoin,
  },
  {
    title: '뉴스',
    href: '/news',
    icon: Newspaper,
    badge: 'HOT',
  },
  {
    title: '경제지표',
    href: '/economic',
    icon: BarChart3,
  },
];

export const MobileBottomNav: React.FC = () => {
  const location = useLocation();

  const isActive = (href: string) => {
    if (href === '/') {
      return location.pathname === '/';
    }
    return location.pathname.startsWith(href);
  };

  return (
    <nav className="fixed bottom-0 left-0 right-0 z-40 bg-black/95 backdrop-blur-xl border-t border-white/10">
      <div className="flex items-center justify-around py-2">
        {navItems.map((item) => {
          const Icon = item.icon;
          const active = isActive(item.href);
          
          return (
            <Link
              key={item.href}
              to={item.href}
              className={cn(
                'flex flex-col items-center justify-center py-2 px-3 rounded-xl transition-all duration-300 relative',
                'hover:bg-white/10',
                active 
                  ? 'text-cyan-400 bg-white/10 scale-110' 
                  : 'text-white/70 hover:text-white'
              )}
            >
              <div className="relative">
                <Icon className={cn(
                  'h-5 w-5 mb-1',
                  active && 'drop-shadow-[0_0_8px_rgba(34,211,238,0.6)]'
                )} />
                {item.badge && (
                  <div className="absolute -top-2 -right-2 bg-gradient-to-r from-red-500 to-pink-500 text-white text-xs px-1.5 py-0.5 rounded-full font-bold animate-pulse">
                    {item.badge}
                  </div>
                )}
              </div>
              <span className={cn(
                'text-xs font-medium',
                active && 'font-bold'
              )}>
                {item.title}
              </span>
              
              {/* 활성 상태 인디케이터 */}
              {active && (
                <div className="absolute -top-0.5 left-1/2 transform -translate-x-1/2 w-1 h-1 bg-gradient-to-r from-cyan-400 to-purple-400 rounded-full animate-pulse" />
              )}
            </Link>
          );
        })}
      </div>
      
      {/* 홈 인디케이터 (iPhone 스타일) */}
      <div className="flex justify-center pb-1">
        <div className="w-32 h-1 bg-white/20 rounded-full" />
      </div>
    </nav>
  );
};