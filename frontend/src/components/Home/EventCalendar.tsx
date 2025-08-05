import { useState } from "react";
import { Calendar as CalendarIcon, ChevronLeft, ChevronRight, Building2, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface CalendarEvent {
  id: string;
  date: Date;
  title: string;
  company: string;
  type: 'earnings' | 'meeting' | 'announcement' | 'dividend';
  importance: 'high' | 'medium' | 'low';
  time?: string;
}

const EventCalendar = () => {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedEvent, setSelectedEvent] = useState<CalendarEvent | null>(null);

  // Mock events data
  const events: CalendarEvent[] = [
    {
      id: '1',
      date: new Date(2024, 11, 15),
      title: 'Q4 실적 발표',
      company: 'Apple Inc.',
      type: 'earnings',
      importance: 'high',
      time: '16:30'
    },
    {
      id: '2',
      date: new Date(2024, 11, 16),
      title: 'Fed 금리 결정',
      company: 'Federal Reserve',
      type: 'meeting',
      importance: 'high',
      time: '21:00'
    },
    {
      id: '3',
      date: new Date(2024, 11, 17),
      title: '배당금 지급일',
      company: 'Microsoft',
      type: 'dividend',
      importance: 'medium',
      time: '09:00'
    },
    {
      id: '4',
      date: new Date(2024, 11, 18),
      title: '신제품 발표회',
      company: 'Tesla',
      type: 'announcement',
      importance: 'high',
      time: '02:00'
    }
  ];

  const getTypeColor = (type: string) => {
    switch (type) {
      case 'earnings':
        return 'bg-primary text-primary-foreground';
      case 'meeting':
        return 'bg-destructive text-destructive-foreground';
      case 'announcement':
        return 'bg-accent text-accent-foreground';
      case 'dividend':
        return 'bg-success text-success-foreground';
      default:
        return 'bg-muted text-muted-foreground';
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case 'earnings':
        return '실적';
      case 'meeting':
        return '회의';
      case 'announcement':
        return '발표';
      case 'dividend':
        return '배당';
      default:
        return '기타';
    }
  };

  const getImportanceColor = (importance: string) => {
    switch (importance) {
      case 'high':
        return 'border-destructive bg-destructive/10';
      case 'medium':
        return 'border-warning bg-warning/10';
      case 'low':
        return 'border-muted bg-muted/10';
      default:
        return 'border-muted';
    }
  };

  const formatDate = (date: Date) => {
    return new Intl.DateTimeFormat('ko-KR', {
      month: 'long',
      day: 'numeric',
      weekday: 'short'
    }).format(date);
  };

  const navigateMonth = (direction: 'prev' | 'next') => {
    setCurrentDate(prev => {
      const newDate = new Date(prev);
      if (direction === 'prev') {
        newDate.setMonth(newDate.getMonth() - 1);
      } else {
        newDate.setMonth(newDate.getMonth() + 1);
      }
      return newDate;
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <CalendarIcon className="h-5 w-5 text-primary" />
          <h2 className="text-lg font-semibold">기업 이벤트 캘린더</h2>
        </div>
        <div className="flex items-center space-x-2">
          <Button variant="outline" size="sm" onClick={() => navigateMonth('prev')}>
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <span className="font-medium text-sm">
            {currentDate.getFullYear()}년 {currentDate.getMonth() + 1}월
          </span>
          <Button variant="outline" size="sm" onClick={() => navigateMonth('next')}>
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="grid gap-3">
        {events.map((event) => (
          <Card 
            key={event.id}
            className={`p-4 cursor-pointer transition-all hover:shadow-md ${getImportanceColor(event.importance)}`}
            onClick={() => setSelectedEvent(event)}
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="flex items-center space-x-2 mb-2">
                  <Badge className={getTypeColor(event.type)}>
                    {getTypeLabel(event.type)}
                  </Badge>
                  {event.importance === 'high' && (
                    <Badge variant="destructive" className="text-xs">
                      중요
                    </Badge>
                  )}
                </div>
                <h3 className="font-semibold text-foreground mb-1">
                  {event.title}
                </h3>
                <div className="flex items-center space-x-1 text-sm text-muted-foreground">
                  <Building2 className="h-3 w-3" />
                  <span>{event.company}</span>
                </div>
              </div>
              <div className="text-right text-sm">
                <div className="font-medium text-foreground">
                  {formatDate(event.date)}
                </div>
                {event.time && (
                  <div className="flex items-center justify-end space-x-1 text-muted-foreground mt-1">
                    <Clock className="h-3 w-3" />
                    <span>{event.time}</span>
                  </div>
                )}
              </div>
            </div>
          </Card>
        ))}
      </div>

      {selectedEvent && (
        <Card className="p-4 bg-primary/5 border-primary">
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-foreground">이벤트 상세 정보</h3>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => setSelectedEvent(null)}
              >
                ✕
              </Button>
            </div>
            <div className="space-y-2">
              <div>
                <span className="text-sm font-medium">제목: </span>
                <span className="text-sm">{selectedEvent.title}</span>
              </div>
              <div>
                <span className="text-sm font-medium">회사: </span>
                <span className="text-sm">{selectedEvent.company}</span>
              </div>
              <div>
                <span className="text-sm font-medium">일시: </span>
                <span className="text-sm">
                  {formatDate(selectedEvent.date)} {selectedEvent.time}
                </span>
              </div>
              <div className="pt-2">
                <Button variant="outline" size="sm" className="w-full">
                  관련 뉴스 보기
                </Button>
              </div>
            </div>
          </div>
        </Card>
      )}
    </div>
  );
};

export default EventCalendar;