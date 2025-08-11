import { useState, useEffect } from "react";
import { Bell, X, TrendingUp, TrendingDown, AlertCircle, DollarSign, Calendar } from "lucide-react";

interface Notification {
  id: string;
  title: string;
  message: string;
  type: "price_alert" | "news" | "event" | "system";
  timestamp: Date;
  read: boolean;
  data?: any;
}

interface NotificationSystemProps {
  isVisible: boolean;
  onClose: () => void;
}

export function NotificationSystem({ isVisible, onClose }: NotificationSystemProps) {
  const [notifications, setNotifications] = useState<Notification[]>([
    {
      id: "1",
      title: "AAPL 급등 알림",
      message: "Apple 주가가 설정한 목표가 $190를 돌파했습니다!",
      type: "price_alert",
      timestamp: new Date(Date.now() - 5 * 60 * 1000),
      read: false,
      data: { symbol: "AAPL", price: 192.45, change: 3.2 }
    },
    {
      id: "2",
      title: "연준 금리 결정",
      message: "FOMC 회의 결과가 발표되었습니다. 기준금리 0.25%p 인하",
      type: "news",
      timestamp: new Date(Date.now() - 1 * 60 * 60 * 1000),
      read: false
    },
    {
      id: "3",
      title: "Tesla 실적 발표 예정",
      message: "내일 오후 4시 30분 Tesla Q4 실적이 발표됩니다.",
      type: "event",
      timestamp: new Date(Date.now() - 2 * 60 * 60 * 1000),
      read: true
    },
    {
      id: "4",
      title: "시스템 업데이트",
      message: "새로운 기능이 추가되었습니다. 확인해보세요!",
      type: "system",
      timestamp: new Date(Date.now() - 6 * 60 * 60 * 1000),
      read: true
    }
  ]);

  const [filter, setFilter] = useState<"all" | "unread">("all");

  // 실시간 알림 시뮬레이션
  useEffect(() => {
    if (!isVisible) return;

    const interval = setInterval(() => {
      // 랜덤하게 새 알림 생성 (10% 확률)
      if (Math.random() < 0.1) {
        const mockNotifications = [
          {
            id: Date.now().toString(),
            title: "BTC 변동 알림",
            message: "비트코인이 $98,000을 돌파했습니다!",
            type: "price_alert" as const,
            timestamp: new Date(),
            read: false,
            data: { symbol: "BTC", price: 98456, change: 2.8 }
          },
          {
            id: Date.now().toString(),
            title: "뉴스 업데이트",
            message: "미국 증시 관련 중요 뉴스가 업데이트되었습니다.",
            type: "news" as const,
            timestamp: new Date(),
            read: false
          }
        ];

        const newNotification = mockNotifications[Math.floor(Math.random() * mockNotifications.length)];
        setNotifications(prev => [newNotification, ...prev]);
      }
    }, 30000); // 30초마다 체크

    return () => clearInterval(interval);
  }, [isVisible]);

  const markAsRead = (id: string) => {
    setNotifications(prev =>
      prev.map(notif =>
        notif.id === id ? { ...notif, read: true } : notif
      )
    );
  };

  const deleteNotification = (id: string) => {
    setNotifications(prev => prev.filter(notif => notif.id !== id));
  };

  const markAllAsRead = () => {
    setNotifications(prev =>
      prev.map(notif => ({ ...notif, read: true }))
    );
  };

  const clearAll = () => {
    setNotifications([]);
  };

  const getIcon = (type: string) => {
    switch (type) {
      case "price_alert":
        return <TrendingUp size={20} className="text-green-400" />;
      case "news":
        return <AlertCircle size={20} className="text-blue-400" />;
      case "event":
        return <Calendar size={20} className="text-purple-400" />;
      case "system":
        return <DollarSign size={20} className="text-orange-400" />;
      default:
        return <Bell size={20} className="text-gray-400" />;
    }
  };

  const formatTime = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const minutes = Math.floor(diff / (1000 * 60));
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));

    if (minutes < 60) {
      return `${minutes}분 전`;
    } else if (hours < 24) {
      return `${hours}시간 전`;
    } else {
      return `${days}일 전`;
    }
  };

  const filteredNotifications = filter === "unread" 
    ? notifications.filter(n => !n.read)
    : notifications;

  const unreadCount = notifications.filter(n => !n.read).length;

  if (!isVisible) return null;

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-start justify-center pt-8 z-50 p-4">
      <div className="glass-strong rounded-2xl w-full max-w-md max-h-[80vh] overflow-hidden">
        {/* 헤더 */}
        <div className="p-4 border-b border-white/10">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center space-x-2">
              <Bell size={20} className="text-primary" />
              <h2 className="text-lg font-semibold">알림</h2>
              {unreadCount > 0 && (
                <span className="bg-red-500 text-white text-xs px-2 py-1 rounded-full">
                  {unreadCount}
                </span>
              )}
            </div>
            <button
              onClick={onClose}
              className="p-1 rounded-lg hover:bg-white/10 transition-colors"
            >
              <X size={20} />
            </button>
          </div>

          {/* 필터 및 액션 */}
          <div className="flex items-center justify-between">
            <div className="flex space-x-2">
              <button
                onClick={() => setFilter("all")}
                className={`px-3 py-1 text-sm rounded-lg transition-colors ${
                  filter === "all" 
                    ? "bg-primary/20 text-primary" 
                    : "hover:bg-white/10"
                }`}
              >
                전체
              </button>
              <button
                onClick={() => setFilter("unread")}
                className={`px-3 py-1 text-sm rounded-lg transition-colors ${
                  filter === "unread" 
                    ? "bg-primary/20 text-primary" 
                    : "hover:bg-white/10"
                }`}
              >
                읽지않음
              </button>
            </div>

            <div className="flex space-x-2">
              {unreadCount > 0 && (
                <button
                  onClick={markAllAsRead}
                  className="text-xs text-primary hover:text-primary/80"
                >
                  모두읽음
                </button>
              )}
              <button
                onClick={clearAll}
                className="text-xs text-red-400 hover:text-red-300"
              >
                전체삭제
              </button>
            </div>
          </div>
        </div>

        {/* 알림 리스트 */}
        <div className="overflow-y-auto max-h-96">
          {filteredNotifications.length === 0 ? (
            <div className="p-8 text-center text-foreground/60">
              <Bell size={48} className="mx-auto mb-4 opacity-50" />
              <p>알림이 없습니다</p>
            </div>
          ) : (
            <div className="space-y-1 p-2">
              {filteredNotifications.map((notification) => (
                <div
                  key={notification.id}
                  className={`glass rounded-xl p-3 transition-all hover:bg-white/10 cursor-pointer ${
                    !notification.read ? "bg-primary/5 border-l-4 border-l-primary" : ""
                  }`}
                  onClick={() => markAsRead(notification.id)}
                >
                  <div className="flex items-start space-x-3">
                    <div className="flex-shrink-0 mt-1">
                      {getIcon(notification.type)}
                    </div>
                    
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between">
                        <h4 className={`text-sm font-medium truncate ${
                          !notification.read ? "text-primary" : ""
                        }`}>
                          {notification.title}
                        </h4>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteNotification(notification.id);
                          }}
                          className="text-foreground/40 hover:text-red-400 ml-2"
                        >
                          <X size={14} />
                        </button>
                      </div>
                      
                      <p className="text-sm text-foreground/70 mt-1 leading-relaxed">
                        {notification.message}
                      </p>
                      
                      <div className="flex items-center justify-between mt-2">
                        <span className="text-xs text-foreground/50">
                          {formatTime(notification.timestamp)}
                        </span>
                        
                        {notification.data && (
                          <div className="text-xs">
                            <span className="text-foreground/60">{notification.data.symbol}</span>
                            <span className={`ml-2 ${
                              notification.data.change >= 0 ? "text-green-400" : "text-red-400"
                            }`}>
                              ${notification.data.price} ({notification.data.change >= 0 ? "+" : ""}{notification.data.change}%)
                            </span>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}