import { useState } from "react";
import { Calendar, Building, TrendingUp, Clock, ChevronRight, ChevronLeft, Grid } from "lucide-react";

interface CalendarEvent {
  id: string;
  title: string;
  company: string;
  symbol: string;
  date: string;
  time: string;
  type: "earnings" | "economic" | "event";
  importance: "high" | "medium" | "low";
  description: string;
  dayOfMonth?: number;
}

export function EventCalendar() {
  const [selectedView, setSelectedView] = useState<"week" | "month" | "calendar">("week");
  const [selectedEvent, setSelectedEvent] = useState<CalendarEvent | null>(null);
  const [currentMonth, setCurrentMonth] = useState(new Date());

  const events: CalendarEvent[] = [
    {
      id: "1",
      title: "실적 발표",
      company: "Apple Inc.",
      symbol: "AAPL",
      date: "오늘",
      time: "16:30",
      type: "earnings",
      importance: "high",
      description: "Q4 2024 실적 발표 및 컨퍼런스 콜",
      dayOfMonth: 15
    },
    {
      id: "2",
      title: "연준 FOMC 회의",
      company: "연방준비제도",
      symbol: "FED",
      date: "오늘",
      time: "20:00",
      type: "economic",
      importance: "high",
      description: "기준금리 발표 및 정책 방향 제시",
      dayOfMonth: 15
    },
    {
      id: "3",
      title: "실적 발표",
      company: "Tesla Inc.",
      symbol: "TSLA",
      date: "내일",
      time: "17:00",
      type: "earnings",
      importance: "high",
      description: "Q4 2024 실적 발표",
      dayOfMonth: 16
    },
    {
      id: "4",
      title: "신제품 발표",
      company: "NVIDIA Corp.",
      symbol: "NVDA",
      date: "이번 주",
      time: "11:00",
      type: "event",
      importance: "medium",
      description: "새로운 AI 칩 아키텍처 공개",
      dayOfMonth: 18
    },
    {
      id: "5",
      title: "고용통계 발표",
      company: "미국 노동부",
      symbol: "JOBS",
      date: "이번 주",
      time: "22:30",
      type: "economic",
      importance: "high",
      description: "12월 비농업 고용 변화 발표",
      dayOfMonth: 19
    },
    {
      id: "6",
      title: "실적 발표",
      company: "Microsoft Corp.",
      symbol: "MSFT",
      date: "이번 달",
      time: "16:00",
      type: "earnings",
      importance: "high",
      description: "Q4 2024 실적 발표",
      dayOfMonth: 25
    },
    {
      id: "7",
      title: "제품 발표회",
      company: "Meta Platforms",
      symbol: "META",
      date: "이번 달",
      time: "14:00",
      type: "event",
      importance: "medium",
      description: "새로운 VR 헤드셋 공개",
      dayOfMonth: 28
    }
  ];

  const getTypeIcon = (type: string) => {
    switch (type) {
      case "earnings":
        return <TrendingUp size={14} className="text-green-400" />;
      case "economic":
        return <Building size={14} className="text-blue-400" />;
      case "event":
        return <Calendar size={14} className="text-purple-400" />;
      default:
        return <Clock size={14} className="text-gray-400" />;
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case "earnings":
        return "실적";
      case "economic":
        return "경제";
      case "event":
        return "이벤트";
      default:
        return "";
    }
  };

  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case "high":
        return "border-l-red-400";
      case "medium":
        return "border-l-yellow-400";
      case "low":
        return "border-l-green-400";
      default:
        return "border-l-gray-400";
    }
  };

  const getImportanceLabel = (importance: string) => {
    switch (importance) {
      case "high":
        return "🔥 중요";
      case "medium":
        return "⚠️ 보통";
      case "low":
        return "📝 참고";
      default:
        return "";
    }
  };

  const getFilteredEvents = () => {
    switch (selectedView) {
      case "week":
        return events.filter(event => event.date === "오늘" || event.date === "내일" || event.date === "이번 주");
      case "month":
        return events.filter(event => event.date === "이번 달" || event.date === "오늘" || event.date === "내일" || event.date === "이번 주");
      case "calendar":
        return events;
      default:
        return events;
    }
  };

  const generateCalendarDays = () => {
    const year = currentMonth.getFullYear();
    const month = currentMonth.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    const daysInMonth = lastDay.getDate();
    const startingDayOfWeek = firstDay.getDay();

    const days = [];
    
    // 이전 달의 빈 날들
    for (let i = 0; i < startingDayOfWeek; i++) {
      days.push(null);
    }
    
    // 이번 달의 날들
    for (let day = 1; day <= daysInMonth; day++) {
      const hasEvent = events.some(event => event.dayOfMonth === day);
      days.push({ day, hasEvent });
    }

    return days;
  };

  const monthNames = ["1월", "2월", "3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월"];
  const dayNames = ["일", "월", "화", "수", "목", "금", "토"];

  const navigateMonth = (direction: 'prev' | 'next') => {
    setCurrentMonth(prev => {
      const newDate = new Date(prev);
      if (direction === 'prev') {
        newDate.setMonth(prev.getMonth() - 1);
      } else {
        newDate.setMonth(prev.getMonth() + 1);
      }
      return newDate;
    });
  };

  if (selectedEvent) {
    return (
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center space-x-3 mb-4">
          <button
            onClick={() => setSelectedEvent(null)}
            className="p-2 rounded-lg glass hover:glass-strong transition-all"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="m15 18-6-6 6-6"/>
            </svg>
          </button>
          <h3 className="font-semibold">이벤트 상세</h3>
        </div>

        <div className={`glass rounded-xl p-4 border-l-4 ${getImportanceColor(selectedEvent.importance)}`}>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center space-x-2">
              {getTypeIcon(selectedEvent.type)}
              <span className="text-sm font-medium">{getTypeLabel(selectedEvent.type)}</span>
              <span className="text-xs px-2 py-1 glass rounded-md">{selectedEvent.symbol}</span>
            </div>
            <span className="text-xs">{getImportanceLabel(selectedEvent.importance)}</span>
          </div>

          <h4 className="font-semibold mb-1">{selectedEvent.title}</h4>
          <p className="text-sm text-foreground/80 mb-2">{selectedEvent.company}</p>
          
          <div className="flex items-center space-x-4 text-sm text-foreground/70 mb-3">
            <div className="flex items-center space-x-1">
              <Calendar size={12} />
              <span>{selectedEvent.date}</span>
            </div>
            <div className="flex items-center space-x-1">
              <Clock size={12} />
              <span>{selectedEvent.time}</span>
            </div>
          </div>

          <div className="glass-subtle rounded-lg p-3">
            <p className="text-sm text-foreground/80 leading-relaxed">
              {selectedEvent.description}
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="glass-card rounded-2xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-semibold flex items-center">
          <Calendar className="mr-2" size={18} />
          📅 시장 이벤트
        </h3>
        <div className="flex space-x-1">
          <button
            onClick={() => setSelectedView("week")}
            className={`px-3 py-1 text-xs rounded-lg transition-all ${
              selectedView === "week"
                ? "glass text-primary"
                : "text-foreground/60 hover:glass-subtle"
            }`}
          >
            이번 주
          </button>
          <button
            onClick={() => setSelectedView("month")}
            className={`px-3 py-1 text-xs rounded-lg transition-all ${
              selectedView === "month"
                ? "glass text-primary"
                : "text-foreground/60 hover:glass-subtle"
            }`}
          >
            이번 달
          </button>
          <button
            onClick={() => setSelectedView("calendar")}
            className={`px-3 py-1 text-xs rounded-lg transition-all flex items-center space-x-1 ${
              selectedView === "calendar"
                ? "glass text-primary"
                : "text-foreground/60 hover:glass-subtle"
            }`}
          >
            <Grid size={12} />
            <span>캘린더</span>
          </button>
        </div>
      </div>

      {selectedView === "calendar" ? (
        <div className="space-y-4">
          {/* 캘린더 헤더 */}
          <div className="flex items-center justify-between">
            <button
              onClick={() => navigateMonth('prev')}
              className="p-2 rounded-lg glass hover:glass-strong transition-all"
            >
              <ChevronLeft size={16} />
            </button>
            <h4 className="font-semibold">
              {currentMonth.getFullYear()}년 {monthNames[currentMonth.getMonth()]}
            </h4>
            <button
              onClick={() => navigateMonth('next')}
              className="p-2 rounded-lg glass hover:glass-strong transition-all"
            >
              <ChevronRight size={16} />
            </button>
          </div>

          {/* 요일 헤더 */}
          <div className="grid grid-cols-7 gap-1 mb-2">
            {dayNames.map(day => (
              <div key={day} className="text-center text-xs font-medium text-foreground/70 py-2">
                {day}
              </div>
            ))}
          </div>

          {/* 캘린더 그리드 */}
          <div className="calendar-grid">
            {generateCalendarDays().map((dayInfo, index) => (
              <div
                key={index}
                className={`calendar-day ${dayInfo?.hasEvent ? 'has-event' : ''} ${!dayInfo ? 'opacity-0' : ''}`}
                onClick={() => {
                  if (dayInfo?.hasEvent) {
                    const dayEvent = events.find(event => event.dayOfMonth === dayInfo.day);
                    if (dayEvent) setSelectedEvent(dayEvent);
                  }
                }}
              >
                {dayInfo && (
                  <span className="text-sm font-medium">{dayInfo.day}</span>
                )}
              </div>
            ))}
          </div>

          <div className="glass-subtle rounded-lg p-3">
            <p className="text-xs text-foreground/70 text-center">
              💡 파란색 점이 있는 날짜를 클릭하면 이벤트 상세를 볼 수 있어요
            </p>
          </div>
        </div>
      ) : (
        <div className="space-y-2">
          {getFilteredEvents().map((event) => (
            <button
              key={event.id}
              onClick={() => setSelectedEvent(event)}
              className={`w-full glass rounded-xl p-3 border-l-4 ${getImportanceColor(event.importance)} hover:glass-strong transition-all text-left`}
            >
              <div className="flex items-center justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-1">
                    {getTypeIcon(event.type)}
                    <span className="text-xs text-foreground/60 glass-subtle px-2 py-0.5 rounded-md">
                      {getTypeLabel(event.type)}
                    </span>
                    <span className="text-xs font-medium px-2 py-0.5 bg-primary/20 text-primary rounded-md">
                      {event.symbol}
                    </span>
                  </div>
                  
                  <h4 className="font-medium text-sm mb-1">{event.title}</h4>
                  <p className="text-xs text-foreground/70">{event.company}</p>
                  
                  <div className="flex items-center space-x-3 mt-2 text-xs text-foreground/60">
                    <div className="flex items-center space-x-1">
                      <Calendar size={10} />
                      <span>{event.date}</span>
                    </div>
                    <div className="flex items-center space-x-1">
                      <Clock size={10} />
                      <span>{event.time}</span>
                    </div>
                    <span>{getImportanceLabel(event.importance)}</span>
                  </div>
                </div>
                
                <ChevronRight size={16} className="text-foreground/40" />
              </div>
            </button>
          ))}
        </div>
      )}

      {getFilteredEvents().length === 0 && selectedView !== "calendar" && (
        <div className="text-center py-8 text-foreground/60">
          <Calendar size={48} className="mx-auto mb-4 opacity-50" />
          <p>선택한 기간에 예정된 이벤트가 없습니다</p>
        </div>
      )}
    </div>
  );
}