// src/pages/Home.tsx - Liquid Glass 스타일 홈페이지
import React from 'react';
import './Home.css';

interface StockTicker {
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
}

interface NewsItem {
  id: string;
  title: string;
  summary: string;
  timestamp: string;
  source: string;
  url: string;
}

interface SocialPost {
  id: string;
  author: string;
  content: string;
  timestamp: string;
  platform: 'x' | 'truth-social';
  url: string;
}

const Home: React.FC = () => {
  // 더미 데이터 (실제로는 API에서 가져옴)
  const topGainers: StockTicker[] = [
    { symbol: 'AAPL', price: 175.23, change: 5.34, changePercent: 3.14 },
    { symbol: 'TSLA', price: 234.56, change: 12.45, changePercent: 5.61 },
    { symbol: 'NVDA', price: 456.78, change: 23.12, changePercent: 5.33 },
  ];

  const topLosers: StockTicker[] = [
    { symbol: 'META', price: 298.45, change: -8.23, changePercent: -2.68 },
    { symbol: 'GOOGL', price: 134.67, change: -3.45, changePercent: -2.50 },
    { symbol: 'AMZN', price: 142.89, change: -4.12, changePercent: -2.81 },
  ];

  const mostActive: StockTicker[] = [
    { symbol: 'SPY', price: 445.12, change: 1.23, changePercent: 0.28 },
    { symbol: 'QQQ', price: 378.45, change: 2.34, changePercent: 0.62 },
    { symbol: 'IWM', price: 201.56, change: -0.89, changePercent: -0.44 },
  ];

  const newsItems: NewsItem[] = [
    {
      id: '1',
      title: 'Federal Reserve Announces Interest Rate Decision',
      summary: 'The Fed maintains current rates amid economic uncertainty...',
      timestamp: '2 hours ago',
      source: 'Reuters',
      url: '#'
    },
    {
      id: '2',
      title: 'Tech Stocks Rally on AI Optimism',
      summary: 'Major tech companies see gains following AI breakthrough announcements...',
      timestamp: '4 hours ago',
      source: 'CNBC',
      url: '#'
    }
  ];

  const socialPosts: SocialPost[] = [
    {
      id: '1',
      author: '@elonmusk',
      content: 'Tesla production numbers looking great this quarter! 🚀',
      timestamp: '1 hour ago',
      platform: 'x',
      url: '#'
    },
    {
      id: '2',
      author: '@realDonaldTrump',
      content: 'Market is doing phenomenally well! America First policies working!',
      timestamp: '3 hours ago',
      platform: 'truth-social',
      url: '#'
    }
  ];

  const formatChange = (change: number, changePercent: number) => {
    const sign = change >= 0 ? '+' : '';
    return `${sign}$${change.toFixed(2)} (${sign}${changePercent.toFixed(2)}%)`;
  };

  const getChangeClass = (change: number) => {
    if (change > 0) return 'text-up';
    if (change < 0) return 'text-down';
    return 'text-neutral';
  };

  return (
    <div className="home-container">
      {/* 주식 티커 슬라이더 */}
      <div className="ticker-section">
        <div className="ticker-category">
          <h3 className="ticker-title">📈 상승 종목</h3>
          <div className="ticker-slider">
            <div className="ticker-content gaining">
              {[...topGainers, ...topGainers].map((stock, index) => (
                <div key={`gain-${index}`} className="ticker-item glass-up">
                  <div className="ticker-symbol">{stock.symbol}</div>
                  <div className="ticker-price">${stock.price.toFixed(2)}</div>
                  <div className={`ticker-change ${getChangeClass(stock.change)}`}>
                    {formatChange(stock.change, stock.changePercent)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="ticker-category">
          <h3 className="ticker-title">📊 거래량 TOP</h3>
          <div className="ticker-slider">
            <div className="ticker-content active">
              {[...mostActive, ...mostActive].map((stock, index) => (
                <div key={`active-${index}`} className="ticker-item glass-card">
                  <div className="ticker-symbol">{stock.symbol}</div>
                  <div className="ticker-price">${stock.price.toFixed(2)}</div>
                  <div className={`ticker-change ${getChangeClass(stock.change)}`}>
                    {formatChange(stock.change, stock.changePercent)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="ticker-category">
          <h3 className="ticker-title">📉 하락 종목</h3>
          <div className="ticker-slider">
            <div className="ticker-content losing">
              {[...topLosers, ...topLosers].map((stock, index) => (
                <div key={`lose-${index}`} className="ticker-item glass-down">
                  <div className="ticker-symbol">{stock.symbol}</div>
                  <div className="ticker-price">${stock.price.toFixed(2)}</div>
                  <div className={`ticker-change ${getChangeClass(stock.change)}`}>
                    {formatChange(stock.change, stock.changePercent)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* 소셜 미디어 피드 */}
      <div className="social-section">
        <h2 className="section-title">💬 영향력 있는 인물들</h2>
        <div className="social-posts">
          {socialPosts.map((post) => (
            <div key={post.id} className="social-post glass-card">
              <div className="post-header">
                <div className="post-author">
                  <span className={`platform-icon ${post.platform}`}>
                    {post.platform === 'x' ? '𝕏' : '🇺🇸'}
                  </span>
                  {post.author}
                </div>
                <div className="post-timestamp">{post.timestamp}</div>
              </div>
              <div className="post-content">{post.content}</div>
              <button className="post-link btn-glass">
                원문 보기 →
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* 실시간 뉴스 */}
      <div className="news-section">
        <h2 className="section-title">📰 실시간 뉴스</h2>
        <div className="news-list">
          {newsItems.map((news) => (
            <div key={news.id} className="news-item glass-card">
              <div className="news-header">
                <div className="news-source">{news.source}</div>
                <div className="news-timestamp">{news.timestamp}</div>
              </div>
              <h3 className="news-title">{news.title}</h3>
              <p className="news-summary">{news.summary}</p>
              <button className="news-link btn-glass">
                자세히 보기 →
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* 하단 여백 (모바일 네비게이션용) */}
      <div className="bottom-spacer"></div>
    </div>
  );
};

export default Home;