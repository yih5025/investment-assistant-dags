import { Suspense, useEffect } from 'react';
import { RouterProvider } from 'react-router-dom';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { LoadingScreen } from './components/common/LoadingSpinner';
import { useAuth } from './hooks/useAuth';
import { router } from './router';

function App() {
  const { checkAuth } = useAuth();

  useEffect(() => {
    // 앱 시작 시 인증 상태 확인
    checkAuth();
  }, [checkAuth]);

  return (
    <ErrorBoundary
      fallback={
        <div className="min-h-screen flex items-center justify-center bg-background">
          <div className="text-center p-8">
            <h1 className="text-2xl font-bold text-destructive mb-4">앱 오류</h1>
            <p className="text-muted-foreground mb-4">
              예상치 못한 오류가 발생했습니다.
            </p>
            <button 
              onClick={() => window.location.reload()}
              className="px-4 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors"
            >
              새로고침
            </button>
          </div>
        </div>
      }
    >
      <Suspense 
        fallback={
          <div className="min-h-screen flex items-center justify-center bg-background">
            <LoadingScreen message="Investment Assistant 로딩 중..." />
          </div>
        }
      >
        <RouterProvider router={router} />
      </Suspense>
    </ErrorBoundary>
  );
}

export default App;