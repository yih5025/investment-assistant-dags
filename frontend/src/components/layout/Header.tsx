import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { 
  Bell, 
  Settings, 
  User, 
  LogOut, 
  Moon, 
  Sun,
  Menu,
  TrendingUp
} from 'lucide-react';
import { Button } from '../ui/button';
import { SearchBox } from '../common/SearchBox';
import { useAuth } from '../../hooks/useAuth';
import { useDashboardStore } from '../../store/dashboardStore';
import { cn } from '../../utils/helpers';

interface HeaderProps {
  onMenuToggle?: () => void;
  showMenu?: boolean;
  className?: string;
}

export const Header: React.FC<HeaderProps> = ({
  onMenuToggle,
  showMenu = true,
  className
}) => {
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const { alerts } = useDashboardStore();
  
  const [isDarkMode, setIsDarkMode] = React.useState(false);
  const [showUserMenu, setShowUserMenu] = React.useState(false);
  const [showNotifications, setShowNotifications] = React.useState(false);

  const unreadAlerts = alerts.filter(alert => !alert.isRead);

  const handleSearch = (query: string) => {
    if (query.trim()) {
      navigate(`/search?q=${encodeURIComponent(query)}`);
    }
  };

  const handleLogout = async () => {
    try {
      await logout();
      navigate('/login');
    } catch (error) {
      console.error('로그아웃 실패:', error);
    }
  };

  const toggleDarkMode = () => {
    setIsDarkMode(!isDarkMode);
    document.documentElement.classList.toggle('dark');
  };

  return (
    <header className={cn(
      'sticky top-0 z-40 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60',
      className
    )}>
      <div className="container flex h-16 items-center justify-between px-4">
        {/* 왼쪽: 로고 및 메뉴 */}
        <div className="flex items-center space-x-4">
          {showMenu && (
            <Button
              variant="ghost"
              size="icon"
              onClick={onMenuToggle}
              className="md:hidden"
            >
              <Menu className="h-5 w-5" />
            </Button>
          )}
          
          <Link to="/" className="flex items-center space-x-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-accent">
              <TrendingUp className="h-5 w-5 text-white" />
            </div>
            <div className="hidden font-bold text-foreground sm:block">
              <span className="text-lg">I.A</span>
              <div className="text-xs text-muted-foreground">Investment Assistant</div>
            </div>
          </Link>
        </div>

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
          {/* 다크모드 토글 */}
          <Button
            variant="ghost"
            size="icon"
            onClick={toggleDarkMode}
            className="hidden sm:flex"
          >
            {isDarkMode ? (
              <Sun className="h-5 w-5" />
            ) : (
              <Moon className="h-5 w-5" />
            )}
          </Button>

          {/* 알림 */}
          <div className="relative">
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

            {/* 알림 드롭다운 */}
            {showNotifications && (
              <div className="absolute right-0 mt-2 w-80 bg-background border rounded-lg shadow-lg z-50">
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
                        className={cn(
                          'p-4 border-b last:border-b-0 hover:bg-muted/50',
                          !alert.isRead && 'bg-muted/30'
                        )}
                      >
                        <div className="flex items-start space-x-3">
                          <div className={cn(
                            'w-2 h-2 rounded-full mt-2',
                            alert.severity === 'error' && 'bg-destructive',
                            alert.severity === 'warning' && 'bg-orange-500',
                            alert.severity === 'success' && 'bg-green-500',
                            alert.severity === 'info' && 'bg-blue-500'
                          )} />
                          <div className="flex-1 min-w-0">
                            <p className="font-medium text-sm">
                              {alert.title}
                            </p>
                            <p className="text-sm text-muted-foreground">
                              {alert.message}
                            </p>
                            <p className="text-xs text-muted-foreground mt-1">
                              {new Date(alert.timestamp).toLocaleString()}
                            </p>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
                {alerts.length > 0 && (
                  <div className="p-2 border-t">
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      className="w-full"
                      onClick={() => navigate('/notifications')}
                    >
                      모든 알림 보기
                    </Button>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* 설정 */}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => navigate('/settings')}
            className="hidden sm:flex"
          >
            <Settings className="h-5 w-5" />
          </Button>

          {/* 사용자 메뉴 */}
          <div className="relative">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setShowUserMenu(!showUserMenu)}
              className="relative"
            >
              <User className="h-5 w-5" />
            </Button>

            {/* 사용자 드롭다운 */}
            {showUserMenu && (
              <div className="absolute right-0 mt-2 w-48 bg-background border rounded-lg shadow-lg z-50">
                <div className="p-4 border-b">
                  <p className="font-medium">{user?.fullName || user?.username}</p>
                  <p className="text-sm text-muted-foreground">{user?.email}</p>
                </div>
                <div className="p-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start"
                    onClick={() => {
                      navigate('/profile');
                      setShowUserMenu(false);
                    }}
                  >
                    <User className="h-4 w-4 mr-2" />
                    프로필
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start"
                    onClick={() => {
                      navigate('/settings');
                      setShowUserMenu(false);
                    }}
                  >
                    <Settings className="h-4 w-4 mr-2" />
                    설정
                  </Button>
                  <div className="border-t my-2" />
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start text-destructive hover:text-destructive"
                    onClick={() => {
                      setShowUserMenu(false);
                      handleLogout();
                    }}
                  >
                    <LogOut className="h-4 w-4 mr-2" />
                    로그아웃
                  </Button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* 모바일에서 알림/사용자 메뉴 오버레이 클릭 시 닫기 */}
      {(showNotifications || showUserMenu) && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => {
            setShowNotifications(false);
            setShowUserMenu(false);
          }}
        />
      )}
    </header>
  );
};