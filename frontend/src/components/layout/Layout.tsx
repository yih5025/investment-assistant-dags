import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Header } from './Header';
import { Sidebar } from './Sidebar';
import { MobileBottomNav } from './MobileBottomNav';
import { ErrorBoundary } from '../common/ErrorBoundary';
import { cn } from '../../utils/helpers';

interface LayoutProps {
  children?: React.ReactNode;
  className?: string;
}

export const Layout: React.FC<LayoutProps> = ({ children, className }) => {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const toggleMobileMenu = () => {
    setMobileMenuOpen(!mobileMenuOpen);
  };

  return (
    <div className={cn('min-h-screen bg-background', className)}>
      {/* 
        헤더: 모든 화면에서 보이지만 스타일이 다름
        - 모바일: 간단한 로고 + 메뉴 버튼
        - 데스크톱: 풀 헤더 (검색바 포함)
      */}
      <Header 
        onMenuToggle={toggleMobileMenu}
      />

      <div className="relative">
        {/* 
          데스크톱 사이드바: lg(1024px) 이상에서만 보임
          - fixed 포지션으로 화면 좌측 고정
        */}
        <div className="hidden lg:fixed lg:inset-y-0 lg:z-50 lg:flex lg:w-72 lg:flex-col">
          <Sidebar />
        </div>

        {/* 
          모바일 사이드바 오버레이: lg 미만에서만 동작
          - 어두운 배경 + 슬라이드 사이드바
        */}
        {mobileMenuOpen && (
          <>
            <div
              className="fixed inset-0 bg-black/80 z-40 lg:hidden"
              onClick={() => setMobileMenuOpen(false)}
            />
            <div className="fixed inset-y-0 left-0 z-50 w-full max-w-xs bg-background lg:hidden">
              <Sidebar 
                onClose={() => setMobileMenuOpen(false)}
              />
            </div>
          </>
        )}

        {/* 
          메인 콘텐츠 영역: 반응형 여백 적용
          - 모바일: 하단 네비게이션 공간 확보 (pb-16)
          - 데스크톱: 사이드바 공간 확보 (lg:pl-72)
        */}
        <div className={cn(
          'pb-16 lg:pb-0',  // 모바일: 하단 여백 / 데스크톱: 하단 여백 없음
          'lg:pl-72'        // 데스크톱: 사이드바만큼 왼쪽 여백
        )}>
          <main className="min-h-screen">
            <ErrorBoundary>
              {children || <Outlet />}
            </ErrorBoundary>
          </main>
        </div>
      </div>

      {/* 
        모바일 하단 네비게이션: lg 미만에서만 보임
        - fixed 포지션으로 화면 하단 고정
      */}
      <div className="lg:hidden">
        <MobileBottomNav />
      </div>
    </div>
  );
};

// 다른 레이아웃들도 동일한 방식
export const AuthenticatedLayout: React.FC<LayoutProps> = (props) => {
  return <Layout {...props} />;
};

export const PublicLayout: React.FC<LayoutProps> = ({ children, className }) => {
  return (
    <div className={cn('min-h-screen bg-background', className)}>
      <ErrorBoundary>
        {children}
      </ErrorBoundary>
    </div>
  );
};