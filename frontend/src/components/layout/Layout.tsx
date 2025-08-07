// src/components/layout/Layout.tsx
import React from 'react';
import Header from './Header';
import MobileBottomNav from './MobileBottomNav';
import './Layout.css';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  return (
    <div className="layout">
      <Header />
      <main className="main-content">
        {children}
      </main>
      <MobileBottomNav />
    </div>
  );
};

export default Layout;