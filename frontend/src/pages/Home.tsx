import React, { useState, useEffect } from 'react';

// 타입 정의들 (그대로 유지)
interface CalendarEvent {
  company: string;
  event: string;
  type: 'earnings' | 'announcement' | 'meeting';
  time: string;
  impact: 'high' | 'medium' | 'low';
}

interface Stock {
  symbol: string;
  price: string;
  change: string;
  name: string;
  logo: string;
}

interface StockBanner {
  title: string;
  type: 'gainers' | 'active' | 'losers';
  gradient: string;
  stocks: Stock[];
}

interface SocialFeed {
  id: number;
  username: string;
  displayName: string;
  platform: 'Truth Social' | 'X';
  avatar: string;
  verified: boolean;
  time: string;
  content: string;
  engagement: string;
  impact: 'high' | 'medium' | 'low';
}

interface NewsItem {
  id: number;
  title: string;
  source: string;
  time: string;
  category: 'economy' | 'tech' | 'earnings' | 'crypto';
  thumbnail: string;
  impact: 'high' | 'medium' | 'low';
  trending: boolean;
}

type CalendarEvents = {
  [key: string]: CalendarEvent[];
};

const Home: React.FC = () => {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [currentBannerIndex, setCurrentBannerIndex] = useState(0);

  // 데이터들 (그대로 유지)
  const calendarEvents: CalendarEvents = {
    "2024-01-15": [
      { company: "AAPL", event: "Q4 실적발표", type: "earnings", time: "16:30", impact: "high" },
      { company: "TSLA", event: "사이버트럭 업데이트", type: "announcement", time: "14:00", impact: "medium" }
    ],
    "2024-01-18": [
      { company: "MSFT", event: "Q4 실적발표", type: "earnings", time: "16:30", impact: "high" },
      { company: "NFLX", event: "구독자 수 발표", type: "announcement", time: "15:00", impact: "medium" }
    ],
    "2024-01-22": [
      { company: "GOOGL", event: "Q4 실적발표", type: "earnings", time: "16:30", impact: "high" },
      { company: "AMZN", event: "주주총회", type: "meeting", time: "10:00", impact: "low" },
      { company: "META", event: "AI 제품 발표", type: "announcement", time: "13:00", impact: "high" }
    ],
    "2024-01-25": [
      { company: "NVDA", event: "GPU 신제품 발표", type: "announcement", time: "11:00", impact: "high" }
    ]
  };

  const stockBanners: StockBanner[] = [
    {
      title: "🚀 급상승 주식",
      type: "gainers",
      gradient: "from-emerald-400 via-green-500 to-emerald-600",
      stocks: [
        { symbol: "NVDA", price: "$875.42", change: "+8.7%", name: "NVIDIA", logo: "🟢" },
        { symbol: "AAPL", price: "$185.25", change: "+5.2%", name: "Apple", logo: "🍎" },
        { symbol: "TSLA", price: "$248.73", change: "+4.8%", name: "Tesla", logo: "🔋" },
        { symbol: "MSFT", price: "$378.91", change: "+3.1%", name: "Microsoft", logo: "💻" }
      ]
    },
    {
      title: "⚡ 활발한 거래",
      type: "active",
      gradient: "from-blue-400 via-cyan-500 to-blue-600",
      stocks: [
        { symbol: "AMC", price: "$4.86", change: "+12.3%", name: "AMC Entertainment", logo: "🎬" },
        { symbol: "GME", price: "$16.42", change: "-2.1%", name: "GameStop", logo: "🎮" },
        { symbol: "PLTR", price: "$18.73", change: "+6.7%", name: "Palantir", logo: "👁️" },
        { symbol: "SOFI", price: "$7.91", change: "+3.4%", name: "SoFi", logo: "🏦" }
      ]
    },
    {
      title: "📉 급하락 주식",
      type: "losers",
      gradient: "from-red-400 via-rose-500 to-red-600",
      stocks: [
        { symbol: "NFLX", price: "$425.18", change: "-6.8%", name: "Netflix", logo: "📺" },
        { symbol: "META", price: "$358.42", change: "-4.2%", name: "Meta", logo: "👥" },
        { symbol: "SNAP", price: "$10.76", change: "-8.9%", name: "Snap", logo: "👻" },
        { symbol: "UBER", price: "$56.23", change: "-3.7%", name: "Uber", logo: "🚗" }
      ]
    }
  ];

  const socialFeeds: SocialFeed[] = [
    {
      id: 1,
      username: "realDonaldTrump",
      displayName: "Donald J. Trump",
      platform: "Truth Social",
      avatar: "🟠",
      verified: true,
      time: "2시간 전",
      content: "The Stock Market is doing GREAT under our policies! American workers and investors are winning bigly! 🇺🇸📈",
      engagement: "2.1K",
      impact: "high"
    },
    {
      id: 2,
      username: "elonmusk",
      displayName: "Elon Musk",
      platform: "X",
      avatar: "⚡",
      verified: true,
      time: "4시간 전",
      content: "Tesla production numbers looking good this quarter. Sustainable transport future is accelerating! 🚗⚡ Mars next! 🚀",
      engagement: "15.7K",
      impact: "high"
    }
  ];

  const liveNews: NewsItem[] = [
    {
      id: 1,
      title: "Fed 금리 동결 결정, 시장 반응 긍정적으로 급등",
      source: "Reuters",
      time: "10분 전",
      category: "economy",
      thumbnail: "📈",
      impact: "high",
      trending: true
    },
    {
      id: 2,
      title: "NVIDIA, AI 칩 수요 급증으로 매출 전망 50% 상향 조정",
      source: "Bloomberg",
      time: "23분 전",
      category: "tech",
      thumbnail: "🔥",
      impact: "high",
      trending: true
    },
    {
      id: 3,
      title: "삼성전자, 3분기 영업이익 예상치 200% 상회 깜짝 실적",
      source: "Yonhap",
      time: "1시간 전",
      category: "earnings",
      thumbnail: "💎",
      impact: "medium",
      trending: false
    },
    {
      id: 4,
      title: "비트코인 6만달러 돌파, 기관 투자자 대거 유입",
      source: "CoinDesk",
      time: "2시간 전",
      category: "crypto",
      thumbnail: "₿",
      impact: "high",
      trending: true
    }
  ];

  const renderCalendar = () => {
    const year = currentDate.getFullYear();
    const month = currentDate.getMonth();
    const firstDay = new Date(year, month, 1).getDay();
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    
    const monthNames = [
      "1월", "2월", "3월", "4월", "5월", "6월",
      "7월", "8월", "9월", "10월", "11월", "12월"
    ];

    const calendar = [];
    
    // 빈 날짜들 추가
    for (let i = 0; i < firstDay; i++) {
      calendar.push(<div key={`empty-${i}`} className="p-2"></div>);
    }
    
    // 실제 날짜들 추가
    for (let day = 1; day <= daysInMonth; day++) {
      const dateStr = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
      const hasEvents = calendarEvents[dateStr];
      const hasHighImpact = hasEvents?.some(event => event.impact === "high");
      
      calendar.push(
        <div
          key={day}
          className={`p-2 text-center text-sm cursor-pointer rounded-xl relative transition-all duration-300 hover:scale-105 ${
            hasEvents ? 
              hasHighImpact ? 
                "bg-gradient-to-br from-purple-400 to-pink-500 text-white shadow-lg shadow-purple-500/30" :
                "bg-gradient-to-br from-blue-400 to-cyan-500 text-white shadow-lg shadow-blue-500/30"
              : "hover:bg-gradient-to-br hover:from-gray-100 hover:to-gray-200"
          } ${selectedDate === dateStr ? "ring-4 ring-yellow-400 ring-opacity-50 scale-110" : ""}`}
          onClick={() => setSelectedDate(selectedDate === dateStr ? null : dateStr)}
        >
          <div className="font-bold">{day}</div>
          {hasEvents && (
            <div className={`absolute -top-1 -right-1 w-3 h-3 rounded-full animate-pulse ${
              hasHighImpact ? "bg-yellow-400 shadow-lg shadow-yellow-400/50" : "bg-emerald-400 shadow-lg shadow-emerald-400/50"
            }`}></div>
          )}
        </div>
      );
    }

    return (
      <div className="relative">
        <div className="absolute inset-0 bg-gradient-to-br from-blue-600/20 via-purple-600/20 to-pink-600/20 rounded-3xl"></div>
        <div className="relative backdrop-blur-xl bg-white/10 border border-white/20 rounded-3xl p-6 shadow-2xl">
          <div className="flex items-center justify-between mb-6">
            <button
              onClick={() => setCurrentDate(new Date(year, month - 1, 1))}
              className="p-3 hover:bg-white/20 rounded-xl transition-all duration-300 hover:scale-110"
            >
              ←
            </button>
            <div className="text-center">
              <h2 className="text-xl font-bold text-white mb-1">
                {year}년 {monthNames[month]}
              </h2>
              <div className="text-sm text-white/70">기업 이벤트 캘린더</div>
            </div>
            <button
              onClick={() => setCurrentDate(new Date(year, month + 1, 1))}
              className="p-3 hover:bg-white/20 rounded-xl transition-all duration-300 hover:scale-110"
            >
              →
            </button>
          </div>
          
          <div className="grid grid-cols-7 gap-2 mb-4">
            {["일", "월", "화", "수", "목", "금", "토"].map(day => (
              <div key={day} className="p-2 text-center text-sm font-bold text-white/80">
                {day}
              </div>
            ))}
          </div>
          
          <div className="grid grid-cols-7 gap-2">
            {calendar}
          </div>
          
          {selectedDate && calendarEvents[selectedDate] && (
            <div className="mt-6 p-4 bg-black/20 backdrop-blur-xl rounded-2xl border border-white/10">
              <h3 className="font-bold text-white mb-3 flex items-center">
                📅 {selectedDate} 기업 일정
              </h3>
              <div className="space-y-3">
                {calendarEvents[selectedDate].map((event, idx) => (
                  <div key={idx} className="flex items-center justify-between p-3 bg-white/10 rounded-xl border border-white/20">
                    <div className="flex items-center space-x-3">
                      <div className={`w-3 h-3 rounded-full ${
                        event.impact === "high" ? "bg-red-500 animate-pulse" :
                        event.impact === "medium" ? "bg-yellow-500" : "bg-green-500"
                      }`}></div>
                      <div>
                        <span className="font-bold text-white">{event.company}</span>
                        <div className="text-sm text-white/70">{event.event}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-white font-medium">{event.time}</div>
                      <div className={`text-xs px-2 py-1 rounded-full ${
                        event.impact === "high" ? "bg-red-500/30 text-red-200" :
                        event.impact === "medium" ? "bg-yellow-500/30 text-yellow-200" :
                        "bg-green-500/30 text-green-200"
                      }`}>
                        {event.impact === "high" ? "고영향" : event.impact === "medium" ? "중영향" : "저영향"}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };
  
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentBannerIndex((prev) => (prev + 1) % stockBanners.length);
    }, 4000);
    return () => clearInterval(interval);
  }, [stockBanners.length]);

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      {/* 애니메이션 배경 */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-purple-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-cyan-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse"></div>
        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 w-80 h-80 bg-pink-500 rounded-full mix-blend-multiply filter blur-xl opacity-10 animate-pulse"></div>
      </div>

      {/* 메인 콘텐츠 */}
      <div className="relative">
        {/* 모바일: 중앙 정렬 컨테이너 / 데스크톱: 전체 너비 */}
        <div className="max-w-md mx-auto lg:max-w-none lg:mx-0 p-4 lg:p-8 space-y-8">
          {/* 캘린더 섹션 */}
          <section>
            <div className="flex items-center space-x-3 mb-4">
              <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-500 rounded-xl">
                📅
              </div>
              <h2 className="text-xl font-bold text-white">투자 캘린더</h2>
              <span className="animate-pulse">⭐</span>
            </div>
            {renderCalendar()}
          </section>

          {/* 실시간 주식 배너 */}
          <section>
            <div className="flex items-center space-x-3 mb-4">
              <div className="p-2 bg-gradient-to-r from-green-500 to-emerald-500 rounded-xl">
                📈
              </div>
              <h2 className="text-xl font-bold text-white">실시간 주식</h2>
              <span className="animate-bounce">⚡</span>
            </div>
            
            <div className="relative overflow-hidden rounded-3xl">
              <div
                className="flex transition-transform duration-700 ease-in-out"
                style={{ transform: `translateX(-${currentBannerIndex * 100}%)` }}
              >
                {stockBanners.map((banner, index) => (
                  <div
                    key={index}
                    className={`w-full flex-shrink-0 bg-gradient-to-br ${banner.gradient} p-6 rounded-3xl relative overflow-hidden`}
                  >
                    <div className="absolute inset-0 bg-gradient-to-r from-white/20 to-transparent opacity-50"></div>
                    
                    <div className="relative z-10">
                      <div className="flex items-center space-x-3 mb-4">
                        <div className="p-2 bg-black/20 rounded-xl backdrop-blur-sm">
                          📊
                        </div>
                        <h3 className="font-bold text-lg text-white">{banner.title}</h3>
                      </div>
                      <div className="space-y-3">
                        {banner.stocks.slice(0, 3).map((stock, idx) => (
                          <div key={idx} className="flex justify-between items-center p-3 bg-black/20 backdrop-blur-sm rounded-2xl border border-white/20">
                            <div className="flex items-center space-x-3">
                              <span className="text-2xl">{stock.logo}</span>
                              <div>
                                <span className="font-bold text-white">{stock.symbol}</span>
                                <div className="text-xs text-white/70">{stock.name}</div>
                              </div>
                            </div>
                            <div className="text-right">
                              <div className="font-bold text-white">{stock.price}</div>
                              <div className={`text-sm font-medium ${
                                stock.change.startsWith("+") ? "text-green-200" : "text-red-200"
                              }`}>
                                {stock.change}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              
              <div className="flex justify-center space-x-3 mt-4">
                {stockBanners.map((_, index) => (
                  <button
                    key={index}
                    className={`w-3 h-3 rounded-full transition-all duration-300 ${
                      index === currentBannerIndex 
                        ? "bg-gradient-to-r from-cyan-400 to-purple-400 scale-125 shadow-lg shadow-purple-500/50" 
                        : "bg-white/30 hover:bg-white/50"
                    }`}
                    onClick={() => setCurrentBannerIndex(index)}
                  />
                ))}
              </div>
            </div>
          </section>

          {/* 소셜 피드 */}
          <section>
            <div className="flex items-center space-x-3 mb-4">
              <div className="p-2 bg-gradient-to-r from-orange-500 to-red-500 rounded-xl">
                🗣️
              </div>
              <h2 className="text-xl font-bold text-white">시장 영향력 피드</h2>
              <div className="px-3 py-1 bg-red-500/20 rounded-full border border-red-500/30">
                <span className="text-xs text-red-300 font-bold">LIVE</span>
              </div>
            </div>
            
            <div className="space-y-4">
              {socialFeeds.map((feed) => (
                <div key={feed.id} className="relative group">
                  <div className="absolute -inset-0.5 bg-gradient-to-r from-pink-600 to-purple-600 rounded-3xl blur opacity-30 group-hover:opacity-60 transition duration-500"></div>
                  
                  <div className="relative backdrop-blur-xl bg-white/10 border border-white/20 rounded-3xl p-5 hover:bg-white/15 transition-all duration-300">
                    <div className="flex items-start space-x-4">
                      <div className="relative">
                        <div className="text-3xl">{feed.avatar}</div>
                        {feed.verified && (
                          <div className="absolute -bottom-1 -right-1 w-5 h-5 bg-blue-500 rounded-full flex items-center justify-center">
                            <span className="text-white text-xs">✓</span>
                          </div>
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center space-x-2 mb-2">
                          <span className="font-bold text-white">{feed.displayName}</span>
                          <span className="text-sm text-white/60">@{feed.username}</span>
                          <span className={`text-xs px-2 py-1 rounded-full font-bold ${
                            feed.platform === "Truth Social" 
                              ? "bg-orange-500/20 text-orange-300 border border-orange-500/30" 
                              : "bg-blue-500/20 text-blue-300 border border-blue-500/30"
                          }`}>
                            {feed.platform}
                          </span>
                        </div>
                        <p className="text-white/90 mb-3 leading-relaxed">{feed.content}</p>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-4">
                            <span className="text-sm text-white/60">{feed.time}</span>
                            <div className="flex items-center space-x-1">
                              <span className="text-sm text-white/60">{feed.engagement}</span>
                              <span className="text-red-400">❤️</span>
                            </div>
                          </div>
                          <button className="p-2 bg-white/10 hover:bg-white/20 rounded-xl transition-all duration-300 hover:scale-110 group">
                            🔗
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* 실시간 뉴스 */}
          <section>
            <div className="flex items-center space-x-3 mb-4">
              <div className="p-2 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-xl">
                📰
              </div>
              <h2 className="text-xl font-bold text-white">실시간 뉴스</h2>
              <div className="px-3 py-1 bg-green-500/20 rounded-full border border-green-500/30 animate-pulse">
                <span className="text-xs text-green-300 font-bold">HOT</span>
              </div>
            </div>
            
            <div className="space-y-4">
              {liveNews.map((news) => (
                <div key={news.id} className="relative group cursor-pointer">
                  {news.trending && (
                    <div className="absolute -inset-0.5 bg-gradient-to-r from-yellow-600 to-orange-600 rounded-3xl blur opacity-30 group-hover:opacity-60 transition duration-500"></div>
                  )}
                  
                  <div className="relative backdrop-blur-xl bg-white/10 border border-white/20 rounded-3xl p-5 hover:bg-white/15 transition-all duration-300 hover:scale-105">
                    <div className="flex items-start space-x-4">
                      <div className="relative">
                        <div className="text-3xl">{news.thumbnail}</div>
                        {news.trending && (
                          <div className="absolute -top-2 -right-2 w-4 h-4 bg-yellow-400 rounded-full animate-ping"></div>
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <h3 className="font-bold text-white mb-2 line-clamp-2 group-hover:text-cyan-400 transition-colors">
                          {news.title}
                        </h3>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-3">
                            <span className="text-sm text-white/60">{news.source}</span>
                            <span className="text-white/40">•</span>
                            <span className="text-sm text-white/60">{news.time}</span>
                          </div>
                          <div className="flex items-center space-x-2">
                            <span className={`text-xs px-3 py-1 rounded-full font-bold border ${
                              news.category === "economy" ? "bg-blue-500/20 text-blue-300 border-blue-500/30" :
                              news.category === "tech" ? "bg-purple-500/20 text-purple-300 border-purple-500/30" :
                              news.category === "earnings" ? "bg-green-500/20 text-green-300 border-green-500/30" :
                              "bg-orange-500/20 text-orange-300 border-orange-500/30"
                            }`}>
                              {news.category === "economy" ? "경제" :
                               news.category === "tech" ? "기술" :
                               news.category === "earnings" ? "실적" : 
                               news.category === "crypto" ? "크립토" : "원자재"}
                            </span>
                            <span className="text-white/60 group-hover:text-cyan-400 transition-colors">↗</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <div className="text-center py-6">
            <button className="px-8 py-4 bg-gradient-to-r from-cyan-500 via-purple-500 to-pink-500 text-white rounded-2xl font-bold hover:scale-105 transition-all duration-300 shadow-2xl shadow-purple-500/30 hover:shadow-purple-500/50">
              <span className="flex items-center space-x-2">
                <span>더 많은 뉴스 보기</span>
                <span>↗</span>
              </span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Home;