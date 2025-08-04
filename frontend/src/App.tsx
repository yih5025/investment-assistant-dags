function App() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center">
      <div className="text-center p-8 bg-white rounded-lg shadow-lg max-w-2xl">
        <h1 className="text-4xl font-bold text-gray-800 mb-4">💰 Investment Assistant</h1>
        <p className="text-lg text-gray-600 mb-8">React 프론트엔드가 성공적으로 실행되었습니다!</p>
        
        <div className="bg-gray-50 p-6 rounded-lg">
          <h2 className="text-2xl font-semibold text-gray-700 mb-4">🚀 개발 진행 상황</h2>
          <div className="text-left space-y-2">
            <div className="flex items-center">
              <span className="text-green-500 mr-2">✅</span>
              <span>기본 React 앱 실행</span>
            </div>
            <div className="flex items-center">
              <span className="text-yellow-500 mr-2">⏳</span>
              <span>API 연동</span>
            </div>
            <div className="flex items-center">
              <span className="text-gray-400 mr-2">❌</span>
              <span className="line-through text-gray-400">사용자 인증 (제거됨)</span>
            </div>
            <div className="flex items-center">
              <span className="text-yellow-500 mr-2">⏳</span>
              <span>대시보드 구현</span>
            </div>
            <div className="flex items-center">
              <span className="text-yellow-500 mr-2">⏳</span>
              <span>주식/암호화폐 데이터</span>
            </div>
          </div>
        </div>
        
        <div className="mt-6 text-sm text-gray-500 space-y-1">
          <p>Kubernetes Pod에서 실행 중 | Port: 30333</p>
          <p>🎯 간단한 구조로 재설계 완료</p>
        </div>
      </div>
    </div>
  );
}

export default App;