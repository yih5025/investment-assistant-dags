import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { 
  Bell, 
  Settings, 
  User, 
  Menu,
  Search,
  TrendingUp
} from 'lucide-react';
import { Button } from '../ui/button';
import { SearchBox } from '../common/SearchBox';
import { useAuth } from '../../hooks/useAuth';
import { useDashboardStore } from '../../store/dashboardStore';
import { cn } from '../../utils/helpers';

interface HeaderProps {
  onMenuToggle?: () => void;
  className?: string;
}

export const Header: React.FC<HeaderProps> = ({
  onMenuToggle,
  className
}) => {
  const navigate = useNavigate();
  const { user } = useAuth();
  const { alerts } = useDashboardStore();
  
  const [showSearch, setShowSearch] = React.useState(false);
  const [showNotifications, setShowNotifications] = React.useState(false);

  const unreadAlerts = alerts.filter(alert => !alert.isRead);

  const handleSearch = (query: string) => {
    if (query.trim()) {
      navigate(`/search?q=${encodeURIComponent(query)}`);
      setShowSearch(false);
    }
  };

  return (
    <>
      {/* 모바일 헤더 - lg 미만에서만 보임 */}
      <header className={cn(
        'sticky top-0 z-40 backdrop-blur-xl bg-black/20 border-b border-white/10 lg:hidden',
        className
      )}>
        <div className="flex items-center justify-between p-4">
          {/* 왼쪽: 메뉴 + 로고 */}
          <div className="flex items-center space-x-3">
            <Button
              variant="ghost"
              size="icon"
              onClick={onMenuToggle}
              className="p-2 hover:bg-white/10 rounded-xl transition-all duration-300"
            >
              <Menu className="h-5 w-5 text-white" />
            </Button>
            <Link to="/" className="flex items-center space-x-2">
              <div>
                <h1 className="text-2xl font-black bg-gradient-to-r from-cyan-400 to-purple-400 bg-clip-text text-transparent">
                  W.E.I
                </h1>
                <p className="text-xs text-white/60 font-medium">Wise & Easy Investment</p>
              </div>
            </Link>
          </div>

          {/* 오른쪽: 액션 버튼들 */}
          <div className="flex items-center space-x-2">
            {/* 검색 버튼 */}
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setShowSearch(true)}
              className="p-3 hover:bg-white/10 rounded-xl transition-all duration-300 hover:scale-110"
            >
              <Search className="h-5 w-5 text-white" />
            </Button>

            {/* 알림 버튼 */}
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setShowNotifications(!showNotifications)}
              className="p-3 hover:bg-white/10 rounded-xl relative transition-all duration-300 hover:scale-110"
            >
              <Bell className="h-5 w-5 text-white" />
              {unreadAlerts.length > 0 && (
                <div className="absolute -top-1 -right-1 w-4 h-4 bg-gradient-to-r from-red-500 to-pink-500 rounded-full animate-pulse shadow-lg shadow-red-500/50 flex items-center justify-center">
                  <span className="text-white text-xs font-bold">
                    {unreadAlerts.length > 9 ? '9+' : unreadAlerts.length}
                  </span>
                </div>
              )}
            </Button>
          </div>
        </div>

        {/* 모바일 전체화면 검색 오버레이 */}
        {showSearch && (
          <div className="fixed inset-0 bg-black/95 backdrop-blur-xl z-50">
            <div className="p-4">
              <div className="flex items-center space-x-4 mb-6">
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => setShowSearch(false)}
                  className="text-white"
                >
                  ←
                </Button>
                <h2 className="text-white text-lg font-bold">검색</h2>
              </div>
              
              <SearchBox
                placeholder="주식, 암호화폐, 뉴스 검색..."
                onSearch={handleSearch}
                className="w-full mb-6"
                autoFocus
              />
              
              <div className="space-y-4">
                <div className="text-white/70 text-sm font-medium">최근 검색어</div>
                <div className="space-y-2">
                  <div className="p-3 bg-white/10 rounded-xl border border-white/20 text-white">
                    AAPL
                  </div>
                  <div className="p-3 bg-white/10 rounded-xl border border-white/20 text-white">
                    Bitcoin
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* 모바일 알림 드롭다운 */}
        {showNotifications && (
          <div className="absolute top-full left-0 right-0 bg-black/95 backdrop-blur-xl border-b border-white/10 z-40">
            <div className="p-4">
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-bold text-white">알림</h3>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowNotifications(false)}
                  className="text-white/70"
                >
                  ✕
                </Button>
              </div>
              
              <div className="space-y-3 max-h-64 overflow-y-auto">
                {alerts.length === 0 ? (
                  <div className="text-center text-white/60 py-8">
                    새로운 알림이 없습니다
                  </div>
                ) : (
                  alerts.slice(0, 5).map((alert) => (
                    <div
                      key={alert.id}
                      className="p-3 bg-white/10 rounded-xl border border-white/20"
                    >
                      <p className="font-medium text-white text-sm">
                        {alert.title}
                      </p>
                      <p className="text-white/70 text-xs mt-1">
                        {alert.message}
                      </p>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}
      </header>

      {/* 데스크톱 헤더 - lg 이상에서만 보임 */}
      <header className={cn(
        'sticky top-0 z-40 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 hidden lg:block',
        className
      )}>
        <div className="container flex h-16 items-center justify-between px-4">
          {/* 왼쪽: 로고 */}
          <Link to="/" className="flex items-center space-x-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-accent">
              <TrendingUp className="h-5 w-5 text-white" />
            </div>
            <div className="font-bold text-foreground">
              <span className="text-lg">W.E.I</span>
              <div className="text-xs text-muted-foreground">Wise & Easy Investment</div>
            </div>
          </Link>

          {/* 중앙: 검색 */}
          <div className="flex-1 max-w-md mx-4">
            <SearchBox
              placeholder="주식, 암호화폐, 뉴스 검색..."
              onSearch={handleSearch}
              className="w-full"
            />
          </div>

          {/* 오른쪽: 액션 버튼들 */}
          <div className="flex items-center space-x-2">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setShowNotifications(!showNotifications)}
              className="relative"
            >
              <Bell className="h-5 w-5" />
              {unreadAlerts.length > 0 && (
                <span className="absolute -top-1 -right-1 bg-destructive text-destructive-foreground text-xs rounded-full h-5 w-5 flex items-center justify-center">
                  {unreadAlerts.length > 99 ? '99+' : unreadAlerts.length}
                </span>
              )}
            </Button>

            <Button
              variant="ghost"
              size="icon"
              onClick={() => navigate('/settings')}
            >
              <Settings className="h-5 w-5" />
            </Button>

            <Button
              variant="ghost"
              size="icon"
              onClick={() => navigate('/profile')}
            >
              <User className="h-5 w-5" />
            </Button>
          </div>
        </div>

        {/* 데스크톱 알림 드롭다운 */}
        {showNotifications && (
          <div className="absolute right-4 top-full mt-2 w-80 bg-background border rounded-lg shadow-lg z-50">
            <div className="p-4 border-b">
              <h3 className="font-semibold">알림</h3>
            </div>
            <div className="max-h-96 overflow-y-auto">
              {alerts.length === 0 ? (
                <div className="p-4 text-center text-muted-foreground">
                  새로운 알림이 없습니다
                </div>
              ) : (
                alerts.slice(0, 10).map((alert) => (
                  <div
                    key={alert.id}
                    className="p-4 border-b last:border-b-0 hover:bg-muted/50"
                  >
                    <p className="font-medium text-sm">{alert.title}</p>
                    <p className="text-sm text-muted-foreground">{alert.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>
        )}
      </header>

      {/* 알림 오버레이 클릭 시 닫기 */}
      {(showNotifications || showSearch) && (
        <div
          className="fixed inset-0 z-30"
          onClick={() => {
            setShowNotifications(false);
            setShowSearch(false);
          }}
        />
      )}
    </>
  );
};