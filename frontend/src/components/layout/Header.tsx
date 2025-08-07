// src/components/layout/Header.tsx
import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import SearchBox from '../common/SearchBox';
import './Header.css';

const Header: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const getPageTitle = () => {
    switch (location.pathname) {
      case '/':
        return 'W.E.I';
      case '/stocks':
        return '주식';
      case '/crypto':
        return '암호화폐';
      case '/news':
        return '뉴스';
      case '/economic':
        return '경제 지표';
      default:
        return 'W.E.I';
    }
  };

  const showSearch = ['/stocks', '/crypto', '/news'].includes(location.pathname);

  return (
    <header className="header glass">
      <div className="header-content">
        {/* 뒤로가기/홈 버튼 */}
        <div className="header-left">
          {location.pathname !== '/' ? (
            <button 
              className="back-button btn-glass"
              onClick={() => navigate(-1)}
              aria-label="뒤로가기"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
                <path 
                  d="M15 18L9 12L15 6" 
                  stroke="currentColor" 
                  strokeWidth="2" 
                  strokeLinecap="round" 
                  strokeLinejoin="round"
                />
              </svg>
            </button>
          ) : (
            <div className="logo">
              <span className="logo-icon">💰</span>
            </div>
          )}
        </div>

        {/* 페이지 제목 */}
        <div className="header-center">
          <h1 className="page-title">{getPageTitle()}</h1>
        </div>

        {/* 검색/메뉴 버튼 */}
        <div className="header-right">
          {showSearch ? (
            <button 
              className="search-toggle btn-glass"
              onClick={() => {/* 검색 토글 로직 */}}
              aria-label="검색"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
                <circle cx="11" cy="11" r="8" stroke="currentColor" strokeWidth="2"/>
                <path d="21 21L16.65 16.65" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              </svg>
            </button>
          ) : (
            <button 
              className="menu-button btn-glass"
              onClick={() => {/* 메뉴 토글 로직 */}}
              aria-label="메뉴"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
                <path d="M3 12H21" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                <path d="M3 6H21" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                <path d="M3 18H21" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              </svg>
            </button>
          )}
        </div>
      </div>

      {/* 검색박스 (조건부 렌더링) */}
      {showSearch && (
        <div className="search-container">
          <SearchBox placeholder={`${getPageTitle()} 검색...`} />
        </div>
      )}
    </header>
  );
};

export default Header;