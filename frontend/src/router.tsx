import React from 'react';
import { createBrowserRouter, Navigate } from 'react-router-dom';
import { AuthenticatedLayout, PublicLayout } from './components/layout/Layout';
import { useAuth } from './hooks/useAuth';

// 페이지 컴포넌트들 (Lazy Loading)
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

// 보호된 라우트 컴포넌트
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

// 공개 라우트 컴포넌트 (로그인된 사용자는 대시보드로 리다이렉트)
const PublicRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return <LoadingFallback />;
  }

  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};

// 라우터 설정
export const router = createBrowserRouter([
  // 공개 라우트 (인증 불필요)
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

  // 보호된 라우트 (인증 필요)
  {
    path: '/',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <Dashboard />
          </React.Suspense>
        ),
      },
      {
        path: 'dashboard',
        element: <Navigate to="/" replace />,
      },
    ],
  },

  // 주식 관련 라우트
  {
    path: '/stocks',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
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
      {
        path: 'watchlist',
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <StocksList />
          </React.Suspense>
        ),
      },
    ],
  },

  // 암호화폐 관련 라우트
  {
    path: '/crypto',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
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
      {
        path: 'portfolio',
        element: (
          <React.Suspense fallback={<LoadingFallback />}>
            <CryptoList />
          </React.Suspense>
        ),
      },
    ],
  },

  // 뉴스 관련 라우트
  {
    path: '/news',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
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

  // 경제지표 라우트
  {
    path: '/economic',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
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

  // 사용자 관련 라우트
  {
    path: '/profile',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: <div className="p-6">프로필 페이지 (개발 예정)</div>,
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
        element: <div className="p-6">설정 페이지 (개발 예정)</div>,
      },
    ],
  },

  // 검색 라우트
  {
    path: '/search',
    element: (
      <ProtectedRoute>
        <AuthenticatedLayout />
      </ProtectedRoute>
    ),
    children: [
      {
        index: true,
        element: <div className="p-6">검색 결과 페이지 (개발 예정)</div>,
      },
    ],
  },

  // 알림 라우트
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
        element: <div className="p-6">알림 페이지 (개발 예정)</div>,
      },
    ],
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