import React from 'react';
import { createBrowserRouter, Navigate } from 'react-router-dom';
import { AuthenticatedLayout, PublicLayout } from './components/layout/Layout';
import { useAuth } from './hooks/useAuth';

// 페이지 컴포넌트들 (Lazy Loading) - K8s 환경에서 안전한 방식
const Home = React.lazy(() => import('./pages/Home'));

// 임시 페이지 컴포넌트 (아직 생성되지 않은 페이지들용)
const TempPage: React.FC<{ title: string; description: string }> = ({ title, description }) => (
  <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 flex items-center justify-center">
    <div className="max-w-md mx-auto p-8">
      <div className="relative">
        <div className="absolute inset-0 bg-gradient-to-br from-blue-600/20 via-purple-600/20 to-pink-600/20 rounded-3xl"></div>
        <div className="relative backdrop-blur-xl bg-white/10 border border-white/20 rounded-3xl p-8 text-center shadow-2xl">
          <div className="text-6xl mb-4">🚧</div>
          <h1 className="text-2xl font-bold text-white mb-4">{title}</h1>
          <p className="text-white/70 mb-6">{description}</p>
          <div className="text-sm text-white/50">개발 진행 중...</div>
          <button 
            onClick={() => window.history.back()}
            className="mt-6 px-6 py-3 bg-gradient-to-r from-cyan-500 to-purple-500 text-white rounded-xl font-bold hover:scale-105 transition-all duration-300"
          >
            뒤로 가기
          </button>
        </div>
      </div>
    </div>
  </div>
);

// 각 페이지별 임시 컴포넌트들
const Dashboard = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="대시보드" description="투자 현황을 한눈에 볼 수 있는 대시보드입니다." /> 
}));

const Login = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="로그인" description="W.E.I 계정으로 로그인하세요." /> 
}));

const Register = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="회원가입" description="W.E.I에 가입하여 맞춤형 투자 정보를 받아보세요." /> 
}));

const StocksList = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="주식 목록" description="실시간 주식 정보와 분석을 확인하세요." /> 
}));

const StockDetail = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="주식 상세" description="선택한 주식의 상세 정보와 차트를 확인하세요." /> 
}));

const CryptoList = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="암호화폐" description="실시간 암호화폐 시세와 트렌드를 확인하세요." /> 
}));

const NewsList = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="뉴스" description="투자에 도움되는 최신 뉴스를 확인하세요." /> 
}));

const NewsDetail = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="뉴스 상세" description="선택한 뉴스의 전체 내용을 확인하세요." /> 
}));

const EconomicData = React.lazy(() => Promise.resolve({ 
  default: () => <TempPage title="경제 지표" description="주요 경제 지표와 그래프를 확인하세요." /> 
}));

// 로딩 컴포넌트
const LoadingFallback = () => (
  <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 flex items-center justify-center">
    <div className="text-center">
      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-cyan-400 mx-auto mb-4"></div>
      <div className="text-white/70">로딩 중...</div>
    </div>
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

// 공개 라우트 컴포넌트 (로그인된 사용자는 홈으로 리다이렉트)
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
      {
        path: 'dashboard',
        element: <Navigate to="/" replace />,
      },
    ],
  },

  // 주식 관련 라우트
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

  // 암호화폐 관련 라우트
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
    ],
  },

  // 뉴스 관련 라우트
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

  // 경제지표 라우트
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
        element: <TempPage title="포트폴리오" description="개인 투자 포트폴리오를 관리하세요." />,
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
        element: <TempPage title="관심 종목" description="관심 있는 주식과 암호화폐를 추적하세요." />,
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
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 flex items-center justify-center">
        <div className="text-center">
          <div className="text-8xl mb-6">🔍</div>
          <h1 className="text-4xl font-bold text-white mb-4">404</h1>
          <p className="text-white/70 mb-8">페이지를 찾을 수 없습니다.</p>
          <a 
            href="/" 
            className="inline-flex items-center px-6 py-3 bg-gradient-to-r from-cyan-500 to-purple-500 text-white rounded-xl font-bold hover:scale-105 transition-all duration-300 shadow-2xl"
          >
            홈으로 돌아가기
          </a>
        </div>
      </div>
    ),
  },
]);