import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Header } from './Header';
import { Sidebar } from './Sidebar';
import { ErrorBoundary } from '../common/ErrorBoundary';
import { cn } from '../../utils/helpers';

interface LayoutProps {
  children?: React.ReactNode;
  className?: string;
}

export const Layout: React.FC<LayoutProps> = ({ children, className }) => {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const toggleSidebar = () => {
    setSidebarCollapsed(!sidebarCollapsed);
  };

  const toggleMobileMenu = () => {
    setMobileMenuOpen(!mobileMenuOpen);
  };

  return (
    <div className={cn('min-h-screen bg-background', className)}>
      {/* 헤더 */}
      <Header onMenuToggle={toggleMobileMenu} />
      
      <div className="flex">
        {/* 사이드바 - 데스크톱 */}
        <div className="hidden md:block">
          <Sidebar
            collapsed={sidebarCollapsed}
            onToggle={toggleSidebar}
          />
        </div>

        {/* 사이드바 - 모바일 오버레이 */}
        {mobileMenuOpen && (
          <>
            <div
              className="fixed inset-0 bg-black/50 z-40 md:hidden"
              onClick={() => setMobileMenuOpen(false)}
            />
            <div className="fixed inset-y-0 left-0 z-50 md:hidden">
              <Sidebar
                collapsed={false}
                onToggle={() => setMobileMenuOpen(false)}
              />
            </div>
          </>
        )}

        {/* 메인 콘텐츠 영역 */}
        <main className={cn(
          'flex-1 transition-all duration-300',
          'min-h-[calc(100vh-4rem)]', // 헤더 높이(4rem) 제외
        )}>
          <ErrorBoundary>
            {children || <Outlet />}
          </ErrorBoundary>
        </main>
      </div>
    </div>
  );
};

// 인증이 필요한 레이아웃
export const AuthenticatedLayout: React.FC<LayoutProps> = (props) => {
  return <Layout {...props} />;
};

// 인증이 필요없는 레이아웃 (로그인, 회원가입 등)
export const PublicLayout: React.FC<LayoutProps> = ({ children, className }) => {
  return (
    <div className={cn('min-h-screen bg-background', className)}>
      <ErrorBoundary>
        {children}
      </ErrorBoundary>
    </div>
  );
};

// 풀스크린 레이아웃 (프레젠테이션 모드 등)
export const FullscreenLayout: React.FC<LayoutProps> = ({ children, className }) => {
  return (
    <div className={cn('min-h-screen bg-background', className)}>
      <ErrorBoundary>
        {children}
      </ErrorBoundary>
    </div>
  );
};