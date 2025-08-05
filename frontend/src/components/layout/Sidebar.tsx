import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  Home,
  TrendingUp,
  Bitcoin,
  Newspaper,
  BarChart3,
  User,
  Settings,
  ChevronLeft,
  ChevronRight
} from 'lucide-react';
import { Button } from '../ui/button';
import { cn } from '../../utils/helpers';

interface SidebarProps {
  collapsed?: boolean;
  onToggle?: () => void;
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

const bottomNavItems: NavItem[] = [
  {
    title: '프로필',
    href: '/profile',
    icon: User,
  },
  {
    title: '설정',
    href: '/settings',
    icon: Settings,
  },
];

export const Sidebar: React.FC<SidebarProps> = ({
  collapsed = false,
  onToggle,
  className
}) => {
  const location = useLocation();
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
                'w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-left transition-colors',
                'hover:bg-muted',
                level > 0 && 'ml-4',
                active && 'bg-primary/10 text-primary',
                collapsed && 'justify-center px-2'
              )}
            >
              <Icon className={cn('h-5 w-5 flex-shrink-0')} />
              {!collapsed && (
                <>
                  <span className="flex-1">{item.title}</span>
                  {item.badge && (
                    <span className="px-2 py-1 text-xs bg-primary text-primary-foreground rounded-full">
                      {item.badge}
                    </span>
                  )}
                  <ChevronRight 
                    className={cn(
                      'h-4 w-4 transition-transform',
                      isExpanded && 'transform rotate-90'
                    )} 
                  />
                </>
              )}
            </button>
          ) : (
            <Link
              to={item.href}
              className={cn(
                'flex items-center space-x-3 px-3 py-2 rounded-lg transition-colors',
                'hover:bg-muted',
                level > 0 && 'ml-4',
                active && 'bg-primary/10 text-primary',
                collapsed && 'justify-center px-2'
              )}
            >
              <Icon className={cn('h-5 w-5 flex-shrink-0')} />
              {!collapsed && (
                <>
                  <span className="flex-1">{item.title}</span>
                  {item.badge && (
                    <span className="px-2 py-1 text-xs bg-primary text-primary-foreground rounded-full">
                      {item.badge}
                    </span>
                  )}
                </>
              )}
            </Link>
          )}
        </div>

        {/* 하위 메뉴 */}
        {hasChildren && isExpanded && !collapsed && (
          <div className="mt-1 space-y-1">
            {item.children!.map(child => renderNavItem(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <aside className={cn(
      'bg-background border-r transition-all duration-300 flex flex-col',
      collapsed ? 'w-16' : 'w-64',
      className
    )}>
      {/* 헤더 */}
      <div className="p-4 border-b">
        <div className="flex items-center justify-between">
          {!collapsed && (
            <h2 className="font-semibold text-lg">메뉴</h2>
          )}
          {onToggle && (
            <Button
              variant="ghost"
              size="icon"
              onClick={onToggle}
              className="ml-auto"
            >
              {collapsed ? (
                <ChevronRight className="h-4 w-4" />
              ) : (
                <ChevronLeft className="h-4 w-4" />
              )}
            </Button>
          )}
        </div>
      </div>

      {/* 메인 네비게이션 */}
      <nav className="flex-1 p-4 space-y-2 overflow-y-auto">
        {navItems.map(item => renderNavItem(item))}
      </nav>

      {/* 하단 네비게이션 */}
      <div className="p-4 border-t space-y-2">
        {bottomNavItems.map(item => renderNavItem(item))}
      </div>

      {/* 콜랩스 툴팁 (축소 상태일 때) */}
      {collapsed && (
        <div className="p-2 text-center text-xs text-muted-foreground">
          <Button
            variant="ghost"
            size="sm"
            onClick={onToggle}
            className="w-full"
          >
            펼치기
          </Button>
        </div>
      )}
    </aside>
  );
};