// src/App.tsx
import React from 'react';
import { BrowserRouter, RouterProvider } from 'react-router-dom';
import Layout from './components/layout/Layout';
import { router } from './router';
import { ErrorBoundary }   from './components/common/ErrorBoundary';
import './styles/globals.css';
import './App.css';

function App() {
  return (
    <ErrorBoundary>
      <BrowserRouter>
        <div className="app">
          {/* 배경 효과 레이어 */}
          <div className="app-background">
            <div className="gradient-orb orb-1"></div>
            <div className="gradient-orb orb-2"></div>
            <div className="gradient-orb orb-3"></div>
          </div>
          
          {/* 메인 콘텐츠 */}
          <Layout>
            <RouterProvider router={router} />
          </Layout>
        </div>
      </BrowserRouter>
    </ErrorBoundary>
  );
}

export default App;