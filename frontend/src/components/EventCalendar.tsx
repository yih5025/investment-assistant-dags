import { useState } from "react";
import { Calendar, TrendingUp, Building2, ChevronLeft, ChevronRight, ArrowLeft, ExternalLink } from "lucide-react";
import { Badge } from "./ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";

interface EarningsEvent {
  symbol: string;
  company: string;
  date: string;
  time: string;
  type: "earnings" | "guidance" | "conference";
  importance: "high" | "medium" | "low";
  expectedEPS?: string;
  previousEPS?: string;
  consensusEPS?: string;
}

interface CompanyNewsItem {
  symbol: string;
  report_date: string;
  category: string;
  article_id: number;
  headline: string;
  image?: string;
  related: string;
  source: string;
  summary: string;
  url: string;
  published_at: string;
  fetched_at?: string;
}

// 현재 달의 실제 기업 일정 데이터
const mockEarningsData: EarningsEvent[] = [
  // 1월 29일 (수요일)
  {
    symbol: "AAPL",
    company: "Apple Inc",
    date: "2025-01-29",
    time: "16:30",
    type: "earnings",
    importance: "high",
    expectedEPS: "$2.35",
    previousEPS: "$2.18",
    consensusEPS: "$2.35"
  },
  {
    symbol: "MSFT",
    company: "Microsoft Corp",
    date: "2025-01-29",
    time: "16:00",
    type: "earnings", 
    importance: "high",
    expectedEPS: "$3.12",
    previousEPS: "$2.99",
    consensusEPS: "$3.10"
  },
  // 1월 30일 (목요일)
  {
    symbol: "TSLA",
    company: "Tesla Inc",
    date: "2025-01-30",
    time: "17:00",
    type: "earnings",
    importance: "high",
    expectedEPS: "$0.73",
    previousEPS: "$0.71",
    consensusEPS: "$0.75"
  },
  {
    symbol: "AMZN",
    company: "Amazon.com Inc",
    date: "2025-01-30",
    time: "16:30",
    type: "earnings",
    importance: "high",
    expectedEPS: "$1.28",
    previousEPS: "$0.98",
    consensusEPS: "$1.30"
  },
  // 1월 31일 (금요일)
  {
    symbol: "GOOGL",
    company: "Alphabet Inc",
    date: "2025-01-31",
    time: "16:00",
    type: "earnings",
    importance: "high",
    expectedEPS: "$1.85",
    previousEPS: "$1.64",
    consensusEPS: "$1.87"
  },
  {
    symbol: "META",
    company: "Meta Platforms",
    date: "2025-01-31",
    time: "16:30",
    type: "earnings",
    importance: "high",
    expectedEPS: "$5.94",
    previousEPS: "$5.33",
    consensusEPS: "$5.95"
  },
  // 2월 3일 (월요일)
  {
    symbol: "NVDA",
    company: "NVIDIA Corp",
    date: "2025-02-03",
    time: "17:00",
    type: "guidance",
    importance: "high",
  },
  {
    symbol: "AMD",
    company: "Advanced Micro Devices",
    date: "2025-02-03",
    time: "16:30",
    type: "earnings",
    importance: "medium",
    expectedEPS: "$0.92",
    previousEPS: "$0.70",
    consensusEPS: "$0.90"
  },
  // 2월 4일 (화요일)
  {
    symbol: "UBER",
    company: "Uber Technologies",
    date: "2025-02-04",
    time: "16:00",
    type: "earnings",
    importance: "medium",
    expectedEPS: "$0.41",
    previousEPS: "$0.37",
    consensusEPS: "$0.43"
  },
  {
    symbol: "SNAP",
    company: "Snap Inc",
    date: "2025-02-04",
    time: "16:30",
    type: "earnings",
    importance: "medium",
    expectedEPS: "$0.08",
    previousEPS: "$0.02",
    consensusEPS: "$0.09"
  }
];

// 각 기업별 관련 뉴스 (company 타입)
const companyRelatedNews: { [key: string]: CompanyNewsItem[] } = {
  "AAPL": [
    {
      symbol: "AAPL",
      report_date: "2025-01-31",
      category: "company",
      article_id: 136033181,
      headline: "Nvidia, Apple Partner Sets Up After 217% Surge; Custom Chip King Eyes AI Boom",
      image: "https://images.unsplash.com/photo-1592179900824-cb2cb6ff2c80?w=200&h=120&fit=crop",
      related: "AAPL",
      source: "Yahoo",
      summary: "Arm Holdings has tripled since it went public in 2023. Shares are setting up in a deep cup with handle base with a buy point of 168.31.",
      url: "https://finnhub.io/api/news?id=49dc77db4f3a8808d746694e2b37ad4d875a4fb786599a1850183bec187bff28",
      published_at: "2025-01-22 16:32:11",
      fetched_at: "2025-01-29 00:01:26.872055"
    },
    {
      symbol: "AAPL",
      report_date: "2025-01-31",
      category: "company",
      article_id: 136085823,
      headline: "Too Big To Win: Why America's Favorite Stocks Could Ruin Your Retirement",
      image: "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?w=200&h=120&fit=crop",
      related: "AAPL",
      source: "SeekingAlpha",
      summary: "Market timing, mega-cap tech reliance, AI-driven disruptions, and historical trends impact investments. Read here for more analysis.",
      url: "https://finnhub.io/api/news?id=173941737652ffa9dfb0fae808030424e6a6a340af6a402ddfe2789ead9a183a",
      published_at: "2025-01-25 07:30:00",
      fetched_at: "2025-01-31 00:01:33.646352"
    },
    {
      symbol: "AAPL",
      report_date: "2025-01-31",
      category: "company",
      article_id: 136068618,
      headline: "Franklin DynaTech Fund Q2 2025 Commentary",
      image: "https://images.unsplash.com/photo-1560472354-b33ff0c44a43?w=200&h=120&fit=crop",
      related: "AAPL",
      source: "SeekingAlpha",
      summary: "Global equities collectively rose during 2025's second quarter (2Q25) after bouncing back from their April lows. Read more here.",
      url: "https://finnhub.io/api/news?id=1d140ef197dc20fb9e983f614e4725518fafdf030ce6c69d8a2b9e0e0e5aed94",
      published_at: "2025-01-24 13:19:00",
      fetched_at: "2025-01-31 00:01:33.900008"
    }
  ],
  "TSLA": [
    {
      symbol: "TSLA",
      report_date: "2025-01-30",
      category: "company",
      article_id: 136067580,
      headline: "Tesla Stock Surges as Q4 Delivery Numbers Beat Expectations",
      image: "https://images.unsplash.com/photo-1571019613454-1cb2f99b2d8b?w=200&h=120&fit=crop",
      related: "TSLA",
      source: "MarketWatch",
      summary: "Tesla's record Q4 deliveries of 484,507 vehicles have analysts optimistic about earnings results, despite ongoing margin concerns in a competitive EV market.",
      url: "https://marketwatch.com/tesla-q4-delivery-earnings",
      published_at: "2025-01-28 09:45:15"
    },
    {
      symbol: "TSLA",
      report_date: "2025-01-30", 
      category: "company",
      article_id: 136067581,
      headline: "Tesla Energy Storage Business Shows Strong Growth Potential",
      related: "TSLA",
      source: "TechCrunch",
      summary: "Energy storage deployments reached 11.0 GWh in Q4, representing 152% growth year-over-year and providing diversification beyond automotive revenue.",
      url: "https://techcrunch.com/tesla-energy-storage-growth",
      published_at: "2025-01-28 07:20:30"
    }
  ],
  "MSFT": [
    {
      symbol: "MSFT",
      report_date: "2025-01-29",
      category: "company",
      article_id: 136067582,
      headline: "Microsoft Azure Growth Expected to Drive Strong Q2 Results",
      image: "https://images.unsplash.com/photo-1633419461186-7d40a38105ec?w=200&h=120&fit=crop",
      related: "MSFT",
      source: "CNBC",
      summary: "Cloud computing revenue growth and AI services adoption expected to boost Microsoft's Q2 earnings above consensus estimates, with Azure leading the charge.",
      url: "https://cnbc.com/microsoft-azure-q2-earnings",
      published_at: "2025-01-28 11:30:45"
    }
  ]
};

export function EventCalendar() {
  const [currentDate] = useState(new Date());
  const [selectedEvent, setSelectedEvent] = useState<EarningsEvent | null>(null);
  const [showNewsDetail, setShowNewsDetail] = useState(false);

  // 현재 달의 날짜 계산
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();
  const today = new Date();
  
  // 달의 첫날과 마지막날
  const firstDay = new Date(year, month, 1);
  const lastDay = new Date(year, month + 1, 0);
  
  // 달력 시작일 (이전 달의 마지막 주 포함)
  const startDate = new Date(firstDay);
  startDate.setDate(startDate.getDate() - firstDay.getDay());
  
  // 달력에 표시할 날짜들 생성
  const calendarDays = [];
  const currentCalendarDate = new Date(startDate);
  
  for (let i = 0; i < 42; i++) { // 6주 × 7일
    calendarDays.push(new Date(currentCalendarDate));
    currentCalendarDate.setDate(currentCalendarDate.getDate() + 1);
  }

  // 특정 날짜의 이벤트 가져오기
  const getEventsForDate = (date: Date) => {
    const dateString = date.toISOString().split('T')[0];
    return mockEarningsData.filter(event => event.date === dateString);
  };

  // 이번 주 이벤트 가져오기
  const getThisWeekEvents = () => {
    const startOfWeek = new Date(today);
    startOfWeek.setDate(today.getDate() - today.getDay());
    const endOfWeek = new Date(startOfWeek);
    endOfWeek.setDate(startOfWeek.getDate() + 6);
    
    return mockEarningsData.filter(event => {
      const eventDate = new Date(event.date);
      return eventDate >= startOfWeek && eventDate <= endOfWeek;
    });
  };

  // 이번 달 이벤트 가져오기
  const getThisMonthEvents = () => {
    return mockEarningsData.filter(event => {
      const eventDate = new Date(event.date);
      return eventDate.getMonth() === month && eventDate.getFullYear() === year;
    });
  };

  // 이벤트 클릭 처리
  const handleEventClick = (event: EarningsEvent) => {
    setSelectedEvent(event);
    setShowNewsDetail(true);
  };

  // 중요도별 색상
  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case "high": return "bg-red-500/20 text-red-400 border-red-500/30";
      case "medium": return "bg-yellow-500/20 text-yellow-400 border-yellow-500/30";
      case "low": return "bg-gray-500/20 text-gray-400 border-gray-500/30";
      default: return "bg-blue-500/20 text-blue-400 border-blue-500/30";
    }
  };

  // 이벤트 타입별 아이콘
  const getEventIcon = (type: string) => {
    switch (type) {
      case "earnings": return <TrendingUp size={12} />;
      case "guidance": return <Building2 size={12} />;
      case "conference": return <Calendar size={12} />;
      default: return <Calendar size={12} />;
    }
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / 3600000);
    
    if (diffHours < 1) {
      return "방금 전";
    } else if (diffHours < 24) {
      return `${diffHours}시간 전`;
    } else {
      return date.toLocaleDateString("ko-KR", { 
        month: "short", 
        day: "numeric"
      });
    }
  };

  // 뉴스 상세 페이지
  if (showNewsDetail && selectedEvent) {
    const relatedNews = companyRelatedNews[selectedEvent.symbol] || [];
    
    return (
      <div className="glass-card rounded-xl p-4">
        {/* 뒤로가기 헤더 */}
        <div className="flex items-center justify-between mb-4">
          <button
            onClick={() => setShowNewsDetail(false)}
            className="flex items-center space-x-2 px-3 py-1.5 glass-subtle rounded-lg hover:glass transition-all"
          >
            <ArrowLeft size={16} />
            <span className="text-sm">이벤트로 돌아가기</span>
          </button>
          
          <div className="text-sm text-foreground/70">
            {relatedNews.length}개 뉴스
          </div>
        </div>

        {/* 이벤트 정보 */}
        <div className="glass-subtle rounded-lg p-4 mb-4">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-lg font-bold">{selectedEvent.company}</h2>
            <Badge className={getImportanceColor(selectedEvent.importance)}>
              {selectedEvent.type === 'earnings' ? '실적발표' : selectedEvent.type === 'guidance' ? '가이던스' : '컨퍼런스'}
            </Badge>
          </div>
          <div className="text-sm text-foreground/70">
            {new Date(selectedEvent.date).toLocaleDateString('ko-KR')} {selectedEvent.time} • {selectedEvent.symbol}
          </div>
        </div>

        {/* 관련 뉴스 리스트 */}
        <div className="space-y-3">
          {relatedNews.length > 0 ? (
            relatedNews.map((news, index) => (
              <article
                key={index}
                className="glass-subtle p-4 rounded-lg cursor-pointer hover:glass transition-all group"
                onClick={() => window.open(news.url, '_blank')}
              >
                <div className="flex gap-3">
                  {news.image && (
                    <div className="flex-shrink-0">
                      <img
                        src={news.image}
                        alt=""
                        className="w-20 h-16 object-cover rounded-lg"
                        loading="lazy"
                      />
                    </div>
                  )}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground/90">{news.source}</span>
                      <span className="text-xs text-foreground/50">
                        {formatTimestamp(news.published_at)}
                      </span>
                    </div>
                    
                    <h3 className="font-medium mb-2 line-clamp-2 group-hover:text-primary transition-colors leading-tight">
                      {news.headline}
                    </h3>
                    
                    <p className="text-sm text-foreground/70 line-clamp-2 mb-3 leading-snug">
                      {news.summary}
                    </p>
                    
                    <div className="flex items-center justify-between">
                      <Badge variant="outline" className="text-xs">{news.symbol}</Badge>
                      <ExternalLink size={14} className="text-foreground/40 group-hover:text-primary transition-colors" />
                    </div>
                  </div>
                </div>
              </article>
            ))
          ) : (
            <div className="glass-subtle p-8 rounded-lg text-center">
              <Calendar size={48} className="mx-auto mb-4 text-foreground/30" />
              <p className="text-foreground/70">관련 뉴스가 없습니다</p>
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="glass-card rounded-xl p-4">
      {/* 헤더 */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center space-x-2">
          <Calendar size={20} className="text-primary" />
          <h3 className="font-medium">시장 이벤트</h3>
        </div>
        
        <div className="flex items-center space-x-2">
          <button className="p-1 glass-subtle rounded hover:glass transition-all">
            <ChevronLeft size={16} />
          </button>
          <span className="text-sm font-medium min-w-[100px] text-center">
            {currentDate.toLocaleDateString('ko-KR', { year: 'numeric', month: 'long' })}
          </span>
          <button className="p-1 glass-subtle rounded hover:glass transition-all">
            <ChevronRight size={16} />
          </button>
        </div>
      </div>

      {/* 탭 네비게이션 */}
      <Tabs defaultValue="calendar" className="w-full">
        <TabsList className="grid w-full grid-cols-3 glass-card">
          <TabsTrigger value="calendar">캘린더</TabsTrigger>
          <TabsTrigger value="thisweek">이번주</TabsTrigger>
          <TabsTrigger value="thismonth">이번달</TabsTrigger>
        </TabsList>
        
        <TabsContent value="calendar" className="mt-4">
          {/* 요일 헤더 */}
          <div className="grid grid-cols-7 gap-1 mb-2">
            {['일', '월', '화', '수', '목', '금', '토'].map((day, index) => (
              <div key={index} className="text-center text-xs font-medium text-foreground/70 py-2">
                {day}
              </div>
            ))}
          </div>

          {/* 달력 그리드 */}
          <div className="calendar-grid mb-4">
            {calendarDays.map((date, index) => {
              const isCurrentMonth = date.getMonth() === month;
              const isToday = date.toDateString() === today.toDateString();
              const events = getEventsForDate(date);
              
              return (
                <div
                  key={index}
                  className={`calendar-day relative ${
                    isCurrentMonth ? 'text-foreground' : 'text-foreground/30'
                  } ${
                    isToday ? 'bg-primary/20 text-primary' : ''
                  } ${
                    events.length > 0 ? 'has-event' : ''
                  }`}
                >
                  <span className="text-xs">{date.getDate()}</span>
                  
                  {/* 이벤트 인디케이터 */}
                  {events.length > 0 && (
                    <div className="absolute bottom-1 left-1/2 transform -translate-x-1/2">
                      <div className="flex space-x-0.5">
                        {events.slice(0, 3).map((_, eventIndex) => (
                          <div
                            key={eventIndex}
                            className="w-1 h-1 bg-primary rounded-full"
                          />
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </TabsContent>
        
        <TabsContent value="thisweek" className="mt-4">
          <div className="space-y-3">
            {getThisWeekEvents().map((event, index) => (
              <div
                key={index}
                className="glass-subtle p-3 rounded-lg cursor-pointer hover:glass transition-all group"
                onClick={() => handleEventClick(event)}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className={`p-2 rounded ${getImportanceColor(event.importance)} flex-shrink-0`}>
                      {getEventIcon(event.type)}
                    </div>
                    
                    <div>
                      <div className="font-medium text-sm group-hover:text-primary transition-colors">
                        {event.symbol} - {event.company}
                      </div>
                      <div className="text-xs text-foreground/60">
                        {new Date(event.date).toLocaleDateString('ko-KR', { 
                          month: 'short', 
                          day: 'numeric' 
                        })} {event.time}
                        {event.expectedEPS && (
                          <span className="ml-2">예상 EPS: {event.expectedEPS}</span>
                        )}
                      </div>
                    </div>
                  </div>
                  
                  <ChevronRight size={16} className="text-foreground/40 group-hover:text-primary transition-colors" />
                </div>
              </div>
            ))}
          </div>
        </TabsContent>
        
        <TabsContent value="thismonth" className="mt-4">
          <div className="space-y-3">
            {getThisMonthEvents().map((event, index) => (
              <div
                key={index}
                className="glass-subtle p-3 rounded-lg cursor-pointer hover:glass transition-all group"
                onClick={() => handleEventClick(event)}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className={`p-2 rounded ${getImportanceColor(event.importance)} flex-shrink-0`}>
                      {getEventIcon(event.type)}
                    </div>
                    
                    <div>
                      <div className="font-medium text-sm group-hover:text-primary transition-colors">
                        {event.symbol} - {event.company}
                      </div>
                      <div className="text-xs text-foreground/60">
                        {new Date(event.date).toLocaleDateString('ko-KR', { 
                          month: 'short', 
                          day: 'numeric' 
                        })} {event.time}
                        {event.expectedEPS && (
                          <span className="ml-2">예상 EPS: {event.expectedEPS}</span>
                        )}
                      </div>
                    </div>
                  </div>
                  
                  <ChevronRight size={16} className="text-foreground/40 group-hover:text-primary transition-colors" />
                </div>
              </div>
            ))}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}