import { useState } from "react";
import { ChevronLeft, ChevronRight, Building, Calendar, TrendingUp, DollarSign } from "lucide-react";

interface Event {
  id: string;
  date: string;
  symbol: string;
  title: string;
  description: string;
  type: "earnings" | "economic" | "event" | "meeting";
  importance: "high" | "medium" | "low";
  time?: string;
}

export function EventCalendar() {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);

  const events: Event[] = [
    {
      id: "1",
      date: "2025-01-15",
      symbol: "AAPL",
      title: "Apple Inc. Q1 2025 실적 발표",
      description: "애플의 2025년 1분기 실적을 발표합니다. 아이폰 15 시리즈 판매량과 서비스 부문 성장률이 주목됩니다.",
      type: "earnings",
      importance: "high",
      time: "16:30"
    },
    {
      id: "2",
      date: "2025-01-16",
      symbol: "CPI",
      title: "미국 소비자물가지수 발표",
      description: "12월 CPI 데이터 발표. 연준의 다음 금리 결정에 중요한 영향을 미칠 예정입니다.",
      type: "economic",
      importance: "high",
      time: "22:30"
    },
    {
      id: "3",
      date: "2025-01-17",
      symbol: "TSLA",
      title: "Tesla 배터리 데이 이벤트",
      description: "테슬라의 새로운 배터리 기술과 에너지 저장 솔루션을 발표하는 행사입니다.",
      type: "event",
      importance: "high",
      time: "11:00"
    },
    {
      id: "4",
      date: "2025-01-18",
      symbol: "FOMC",
      title: "연준 FOMC 회의록 공개",
      description: "12월 FOMC 회의의 상세 논의 내용이 공개됩니다. 향후 통화정책 방향성을 파악할 수 있습니다.",
      type: "economic",
      importance: "high",
      time: "04:00"
    },
    {
      id: "5",
      date: "2025-01-20",
      symbol: "MSFT",
      title: "Microsoft 주주총회",
      description: "마이크로소프트의 연례 주주총회가 개최됩니다. AI 전략과 클라우드 사업 계획을 발표할 예정입니다.",
      type: "meeting",
      importance: "medium",
      time: "02:00"
    },
    {
      id: "6",
      date: "2025-01-22",
      symbol: "NVDA",
      title: "NVIDIA AI 컨퍼런스 키노트",
      description: "젠슨 황 CEO가 차세대 AI 칩과 데이터센터 솔루션을 공개하는 기조연설을 진행합니다.",
      type: "event",
      importance: "high",
      time: "03:00"
    },
    {
      id: "7",
      date: "2025-01-23",
      symbol: "GDP",
      title: "미국 GDP 예비치 발표",
      description: "4분기 GDP 성장률 예비치가 발표됩니다. 경기 둔화 우려 속에서 주목받고 있습니다.",
      type: "economic",
      importance: "medium",
      time: "22:30"
    },
    {
      id: "8",
      date: "2025-01-24",
      symbol: "META",
      title: "Meta 실적 발표",
      description: "메타의 4분기 실적과 메타버스 부문의 진전 상황이 공개됩니다.",
      type: "earnings",
      importance: "high",
      time: "17:00"
    },
  ];

  const daysInMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0).getDate();
  const firstDayOfMonth = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1).getDay();

  const nextMonth = () => {
    setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() + 1));
  };

  const prevMonth = () => {
    setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() - 1));
  };

  const getEventForDate = (day: number) => {
    const dateStr = `${currentDate.getFullYear()}-${String(currentDate.getMonth() + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    return events.filter(event => event.date === dateStr);
  };

  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case "high": return "bg-red-500";
      case "medium": return "bg-yellow-500";
      case "low": return "bg-green-500";
      default: return "bg-gray-500";
    }
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case "earnings":
        return <TrendingUp size={14} className="text-green-400" />;
      case "economic":
        return <DollarSign size={14} className="text-blue-400" />;
      case "event":
        return <Building size={14} className="text-purple-400" />;
      case "meeting":
        return <Calendar size={14} className="text-orange-400" />;
      default:
        return <Calendar size={14} className="text-gray-400" />;
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case "earnings": return "실적발표";
      case "economic": return "경제지표";
      case "event": return "기업행사";
      case "meeting": return "주주총회";
      default: return "일정";
    }
  };

  return (
    <div className="glass-card rounded-2xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold flex items-center">
          <Calendar className="mr-2" size={20} />
          시장 이벤트 캘린더
        </h2>
        <div className="flex items-center space-x-2">
          <button onClick={prevMonth} className="p-1 rounded-lg hover:bg-white/10">
            <ChevronLeft size={20} />
          </button>
          <span className="text-sm font-medium min-w-24 text-center">
            {currentDate.getFullYear()}.{String(currentDate.getMonth() + 1).padStart(2, '0')}
          </span>
          <button onClick={nextMonth} className="p-1 rounded-lg hover:bg-white/10">
            <ChevronRight size={20} />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-7 gap-1 mb-2">
        {["일", "월", "화", "수", "목", "금", "토"].map((day) => (
          <div key={day} className="text-center text-xs text-foreground/60 py-2">
            {day}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-7 gap-1">
        {Array.from({ length: firstDayOfMonth }, (_, i) => (
          <div key={`empty-${i}`} className="h-16" />
        ))}
        
        {Array.from({ length: daysInMonth }, (_, i) => {
          const day = i + 1;
          const dayEvents = getEventForDate(day);
          
          return (
            <button
              key={day}
              className="h-16 flex flex-col items-center justify-start rounded-lg hover:bg-white/10 relative p-1 transition-colors"
              onClick={() => dayEvents.length > 0 && setSelectedEvent(dayEvents[0])}
            >
              <span className="text-sm mb-1">{day}</span>
              {dayEvents.length > 0 && (
                <div className="w-full space-y-0.5">
                  {dayEvents.slice(0, 2).map((event) => (
                    <div
                      key={event.id}
                      className={`text-xs px-1 py-0.5 rounded text-center truncate ${
                        event.type === 'earnings' ? 'bg-green-500/20 text-green-300' :
                        event.type === 'economic' ? 'bg-blue-500/20 text-blue-300' :
                        event.type === 'event' ? 'bg-purple-500/20 text-purple-300' :
                        'bg-orange-500/20 text-orange-300'
                      }`}
                      title={event.title}
                    >
                      {event.symbol}
                    </div>
                  ))}
                  {dayEvents.length > 2 && (
                    <div className="text-xs text-center text-foreground/60">
                      +{dayEvents.length - 2}
                    </div>
                  )}
                </div>
              )}
            </button>
          );
        })}
      </div>

      {selectedEvent && (
        <div className="mt-4 p-4 glass rounded-xl">
          <div className="flex items-start justify-between mb-3">
            <div className="flex items-center space-x-2 mb-2">
              {getTypeIcon(selectedEvent.type)}
              <span className="text-sm font-medium text-primary">{selectedEvent.symbol}</span>
              <div className={`w-2 h-2 rounded-full ${getImportanceColor(selectedEvent.importance)}`} />
              <span className="text-xs text-foreground/60">{getTypeLabel(selectedEvent.type)}</span>
            </div>
            <button 
              onClick={() => setSelectedEvent(null)}
              className="text-foreground/50 hover:text-foreground"
            >
              ✕
            </button>
          </div>

          <h3 className="font-medium mb-2">{selectedEvent.title}</h3>
          <p className="text-sm text-foreground/70 mb-3 leading-relaxed">
            {selectedEvent.description}
          </p>
          
          <div className="flex items-center justify-between text-xs text-foreground/60">
            <span>{selectedEvent.date}</span>
            {selectedEvent.time && (
              <span className="bg-white/10 px-2 py-1 rounded">
                {selectedEvent.time} (KST)
              </span>
            )}
          </div>
        </div>
      )}

      {/* 범례 */}
      <div className="mt-4 p-3 glass rounded-xl">
        <h4 className="text-sm font-medium mb-2">이벤트 유형</h4>
        <div className="grid grid-cols-2 gap-2 text-xs">
          <div className="flex items-center space-x-2">
            <TrendingUp size={12} className="text-green-400" />
            <span>실적발표</span>
          </div>
          <div className="flex items-center space-x-2">
            <DollarSign size={12} className="text-blue-400" />
            <span>경제지표</span>
          </div>
          <div className="flex items-center space-x-2">
            <Building size={12} className="text-purple-400" />
            <span>기업행사</span>
          </div>
          <div className="flex items-center space-x-2">
            <Calendar size={12} className="text-orange-400" />
            <span>주주총회</span>
          </div>
        </div>
      </div>
    </div>
  );
}