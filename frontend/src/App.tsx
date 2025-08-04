import { useEffect } from 'react';
import { RouterProvider } from 'react-router-dom';
import { router } from './router';
import { useAuth } from './hooks/useAuth';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { LoadingScreen } from './components/common/LoadingSpinner';

function App() {
  const { checkAuth, loading } = useAuth();

  // 앱 초기화 시 인증 상태 확인
  useEffect(() => {
    checkAuth();
  }, [checkAuth]);

  // 초기 로딩 중
  if (loading) {
    return <LoadingScreen message="앱을 초기화하는 중..." />;
  }

  return (
    <ErrorBoundary>
      <div className="App">
        <RouterProvider router={router} />
      </div>
    </ErrorBoundary>
  );
}

export default App;