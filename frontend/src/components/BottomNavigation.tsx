import { Home, TrendingUp, Newspaper, Bot, BarChart3 } from "lucide-react";

interface BottomNavigationProps {
  activeTab: string;
  onTabChange: (tab: string) => void;
}

export function BottomNavigation({ activeTab, onTabChange }: BottomNavigationProps) {
  const tabs = [
    { id: "home", label: "홈", icon: Home },
    { id: "markets", label: "시장", icon: TrendingUp },
    { id: "news", label: "뉴스", icon: Newspaper },
    { id: "ai", label: "AI", icon: Bot },
    { id: "economy", label: "경제", icon: BarChart3 },
  ];

  return (
    <div className="fixed bottom-0 left-0 right-0 z-40">
      <div className="max-w-md mx-auto">
        <div className="glass-strong backdrop-blur-lg border-t border-white/20 px-4 py-2">
          <div className="flex justify-around items-center">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              
              return (
                <button
                  key={tab.id}
                  onClick={() => onTabChange(tab.id)}
                  className={`flex flex-col items-center space-y-1 px-3 py-2 rounded-lg transition-all duration-200 ${
                    isActive 
                      ? "bg-primary/20 text-primary scale-105" 
                      : "text-foreground/60 hover:text-foreground hover:bg-white/10"
                  }`}
                >
                  <Icon size={20} />
                  <span className="text-xs font-medium">{tab.label}</span>
                </button>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}