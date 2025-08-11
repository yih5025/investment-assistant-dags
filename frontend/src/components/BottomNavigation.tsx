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
        <div className="glass-strong backdrop-blur-xl border-t border-white/30 px-2 py-2 m-2 rounded-2xl">
          <div className="flex justify-around items-center">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              
              return (
                <button
                  key={tab.id}
                  onClick={() => onTabChange(tab.id)}
                  className={`flex flex-col items-center space-y-1 px-3 py-2 rounded-xl transition-all duration-300 ${
                    isActive 
                      ? "glass text-primary scale-105 shadow-lg" 
                      : "text-foreground/70 hover:text-foreground hover:glass-subtle hover:scale-105"
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