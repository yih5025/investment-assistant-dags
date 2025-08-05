import React from 'react';
import { createBrowserRouter, Navigate } from 'react-router-dom';
import { AuthenticatedLayout, PublicLayout } from './components/layout/Layout';
import { useAuth } from './hooks/useAuth';

// 페이지 컴포넌트들 (Lazy Loading)
const Home = React.lazy(() => import('./pages/Home'));
const Dashboard = React.lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const Login = React.lazy(() => import('./pages/Auth/Login').then(m => ({ default: m.Login })));
const Register = React.lazy(() => import('./pages/Auth/Register').then(m => ({ default: m.Register })));
const StocksList = React.lazy(() => import('./pages/Stocks/StocksList').then(m => ({ default: m.StocksList })));
const StockDetail = React.lazy(() => import('./pages/Stocks/StockDetail').then(m => ({ default: m.StockDetail })));
const CryptoList = React.lazy(() => import('./pages/Crypto/CryptoList').then(m => ({ default: m.CryptoList })));
const NewsList = React.lazy(() => import('./pages/News/NewsList').then(m => ({ default: m.NewsList })));
const NewsDetail = React.lazy(() => import('./pages/News/NewsDetail').then(m => ({ default: m.NewsDetail })));
const EconomicData = React.lazy(() => import('./pages/Economic/EconomicData').then(m => ({ default: m.EconomicData })));

// 로딩 컴포넌트
const LoadingFallback = () => (
  <div className="flex items-center justify-center min-h-[400px]">
    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
  </div>
);

// 보호된 라우트 컴포넌트 (개인화 기능에만 사용)
const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return <LoadingFallback />;
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
};

// 공개 라우트 컴포넌트 (로그인된 사용자는 홈으로 리다이렉트) ㄴㅇ
const PublicRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return <LoadingFallback />;
  }

  if (isAuthenticated) {
    return <Navigate to="/" replace />; // 홈으로 리다이렉트
  }

  return <>{children}</>;
};

// 라우터 설정
export const router = createBrowserRouter([
  // 메인 레이아웃 - 공개 접근 가능 (로그인 선택적)
  {
    path: '/',
    element: <AuthenticatedLayout />,
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <Home />
          </React.Suspense>
        ),
      },
      // dashboard 경로를 홈으로 리다이렉트 (기존 URL 호환성 유지)
      {
        path: 'dashboard',
        element: <Navigate to="/" replace />,
      },
    ],
  },

  // 주식 관련 라우트 - 공개 접근 가능
  {
    path: '/stocks',
    element: <AuthenticatedLayout />,
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <StocksList />
          </React.Suspense>
        ),
      },
      {
        path: ':symbol',
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <StockDetail />
          </React.Suspense>
        ),
      },
    ],
  },

  // 암호화폐 관련 라우트 - 공개 접근 가능
  {
    path: '/crypto',
    element: <AuthenticatedLayout />,
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <CryptoList />
          </React.Suspense>
        ),
      },
      {
        path: ':symbol',
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <CryptoList />
          </React.Suspense>
        ),
      },
    ],
  },

  // 뉴스 관련 라우트 - 공개 접근 가능
  {
    path: '/news',
    element: <AuthenticatedLayout />,
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <NewsList />
          </React.Suspense>
        ),
      },
      {
        path: ':id',
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <NewsDetail />
          </React.Suspense>
        ),
      },
    ],
  },

  // 경제지표 라우트 - 공개 접근 가능
  {
    path: '/economic',
    element: <AuthenticatedLayout />,
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <EconomicData />
          </React.Suspense>
        ),
      },
    ],
  },

  // 개인화 기능들 - 로그인 필수
  {
    path: '/portfolio',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: (
          <div className="p-6">
            <div className="bg-card rounded-lg p-8 text-center">
              <h1 className="text-2xl font-bold mb-4">개인 포트폴리오</h1>
              <p className="text-muted-foreground mb-4">
                회원님의 투자 포트폴리오를 관리할 수 있습니다.
              </p>
              <div className="text-sm text-muted-foreground">
                🚧 개발 진행 중...
              </div>
            </div>
          </div>
        ),
      },
    ],
  },

  {
    path: '/watchlist',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: (
          <div className="p-6">
            <div className="bg-card rounded-lg p-8 text-center">
              <h1 className="text-2xl font-bold mb-4">관심 종목</h1>
              <p className="text-muted-foreground mb-4">
                관심 있는 주식과 암호화폐를 저장하고 추적할 수 있습니다.
              </p>
              <div className="text-sm text-muted-foreground">
                🚧 개발 진행 중...
              </div>
            </div>
          </div>
        ),
      },
    ],
  },

  {
    path: '/settings',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: (
          <div className="p-6">
            <div className="bg-card rounded-lg p-8 text-center">
              <h1 className="text-2xl font-bold mb-4">설정</h1>
              <p className="text-muted-foreground mb-4">
                개인화 설정과 알림 설정을 관리할 수 있습니다.
              </p>
              <div className="text-sm text-muted-foreground">
                🚧 개발 진행 중...
              </div>
            </div>
          </div>
        ),
      },
    ],
  },

  {
    path: '/notifications',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: (
          <div className="p-6">
            <div className="bg-card rounded-lg p-8 text-center">
              <h1 className="text-2xl font-bold mb-4">알림</h1>
              <p className="text-muted-foreground mb-4">
                가격 알림과 뉴스 알림을 확인할 수 있습니다.
              </p>
              <div className="text-sm text-muted-foreground">
                🚧 개발 진행 중...
              </div>
            </div>
          </div>
        ),
      },
    ],
  },

  // 인증 관련 페이지들
  {
    path: '/login',
    element: (
      <PublicLayout>
        <PublicRoute>
          <React.Suspense fallback={<LoadingFallback />}>
            <Login />
          </React.Suspense>
        </PublicRoute>
      </PublicLayout>
    ),
  },
  {
    path: '/register',
    element: (
      <PublicLayout>
        <PublicRoute>
          <React.Suspense fallback={<LoadingFallback />}>
            <Register />
          </React.Suspense>
        </PublicRoute>
      </PublicLayout>
    ),
  },

  // 404 페이지
  {
    path: '*',
    element: (
      <PublicLayout>
        <div className="min-h-screen flex items-center justify-center">
          <div className="text-center">
            <h1 className="text-4xl font-bold text-muted-foreground mb-4">404</h1>
            <p className="text-muted-foreground mb-8">페이지를 찾을 수 없습니다.</p>
            <a 
              href="/" 
              className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors"
            >
              홈으로 돌아가기
            </a>
          </div>
        </div>
      </PublicLayout>
    ),
  },
]);