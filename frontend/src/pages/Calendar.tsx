import Header from "@/components/Layout/Header";
import { Calendar as CalendarComponent } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Building2, TrendingUp, Calendar as CalendarIcon, DollarSign } from "lucide-react";
import { useState } from "react";

const eventData = {
  "2025-01-15": [
    { type: "earnings", company: "Apple", icon: "🍎", time: "16:30" },
    { type: "meeting", company: "Tesla", icon: "🚗", time: "10:00" }
  ],
  "2025-01-16": [
    { type: "dividend", company: "Microsoft", icon: "💰", time: "09:00" }
  ],
  "2025-01-17": [
    { type: "product", company: "Google", icon: "🔍", time: "11:00" },
    { type: "earnings", company: "Netflix", icon: "📺", time: "17:00" }
  ]
};

const eventTypes = {
  earnings: { label: "실적발표", color: "bg-success text-success-foreground" },
  meeting: { label: "주주총회", color: "bg-primary text-primary-foreground" },
  dividend: { label: "배당금", color: "bg-warning text-warning-foreground" },
  product: { label: "제품발표", color: "bg-accent text-accent-foreground" }
};

const Calendar = () => {
  const [selectedDate, setSelectedDate] = useState<Date | undefined>(new Date());
  const selectedDateStr = selectedDate?.toISOString().split('T')[0];
  const dayEvents = selectedDateStr ? eventData[selectedDateStr] || [] : [];

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-4 space-y-6">
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold mb-2">기업 이벤트 캘린더</h1>
          <p className="text-muted-foreground">중요한 기업 일정을 한눈에 확인하세요</p>
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <Card className="p-4">
            <CardHeader className="pb-4">
              <CardTitle className="flex items-center gap-2">
                <CalendarIcon className="h-5 w-5" />
                이벤트 캘린더
              </CardTitle>
            </CardHeader>
            <CardContent>
              <CalendarComponent
                mode="single"
                selected={selectedDate}
                onSelect={setSelectedDate}
                className="rounded-md border w-full"
                modifiers={{
                  hasEvents: (date) => {
                    const dateStr = date.toISOString().split('T')[0];
                    return dateStr in eventData;
                  }
                }}
                modifiersStyles={{
                  hasEvents: {
                    backgroundColor: 'hsl(var(--primary))',
                    color: 'hsl(var(--primary-foreground))',
                    fontWeight: 'bold'
                  }
                }}
              />
            </CardContent>
          </Card>

          <Card className="p-4">
            <CardHeader className="pb-4">
              <CardTitle className="flex items-center gap-2">
                <Building2 className="h-5 w-5" />
                {selectedDate ? `${selectedDate.getMonth() + 1}월 ${selectedDate.getDate()}일 일정` : "일정 선택"}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {dayEvents.length > 0 ? (
                <div className="space-y-3">
                  {dayEvents.map((event, index) => (
                    <div key={index} className="flex items-center justify-between p-3 rounded-lg border bg-card">
                      <div className="flex items-center gap-3">
                        <span className="text-2xl">{event.icon}</span>
                        <div>
                          <p className="font-medium">{event.company}</p>
                          <p className="text-sm text-muted-foreground">{event.time}</p>
                        </div>
                      </div>
                      <Badge className={eventTypes[event.type].color}>
                        {eventTypes[event.type].label}
                      </Badge>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <CalendarIcon className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>선택한 날짜에 일정이 없습니다</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {Object.entries(eventTypes).map(([key, type]) => (
            <Card key={key} className="p-4 text-center">
              <div className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${type.color}`}>
                {type.label}
              </div>
              <p className="text-xs text-muted-foreground mt-2">
                {key === 'earnings' && '분기별 실적 발표'}
                {key === 'meeting' && '주주총회 및 이사회'}
                {key === 'dividend' && '배당금 지급일'}
                {key === 'product' && '신제품 발표회'}
              </p>
            </Card>
          ))}
        </div>
      </main>
    </div>
  );
};

export default Calendar;