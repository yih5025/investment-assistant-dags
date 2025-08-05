import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { 
  Widget, 
  DashboardLayout, 
  MarketOverview, 
  Portfolio, 
  Alert 
} from '../types/dashboard';
import { STORAGE_KEYS, UPDATE_INTERVALS } from '../utils/constants';
import { generateId } from '../utils/helpers';

interface DashboardStore {
  // State
  currentLayout: DashboardLayout | null;
  layouts: DashboardLayout[];
  marketOverview: MarketOverview | null;
  portfolios: Portfolio[];
  alerts: Alert[];
  widgets: Widget[];
  isEditing: boolean;
  loading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  updateInterval: number;

  // Actions - Layout
  setCurrentLayout: (layout: DashboardLayout) => void;
  addLayout: (layout: DashboardLayout) => void;
  updateLayout: (layoutId: string, updates: Partial<DashboardLayout>) => void;
  deleteLayout: (layoutId: string) => void;
  duplicateLayout: (layoutId: string, newName: string) => void;

  // Actions - Widgets
  addWidget: (widget: Omit<Widget, 'id'>) => void;
  updateWidget: (widgetId: string, updates: Partial<Widget>) => void;
  removeWidget: (widgetId: string) => void;
  moveWidget: (widgetId: string, position: Widget['position']) => void;
  toggleWidgetVisibility: (widgetId: string) => void;
  resetWidgets: () => void;

  // Actions - Data
  setMarketOverview: (data: MarketOverview) => void;
  updateMarketData: (updates: Partial<MarketOverview>) => void;
  setPortfolios: (portfolios: Portfolio[]) => void;
  updatePortfolio: (portfolioId: string, updates: Partial<Portfolio>) => void;
  
  // Actions - Alerts
  addAlert: (alert: Omit<Alert, 'id'>) => void;
  updateAlert: (alertId: string, updates: Partial<Alert>) => void;
  removeAlert: (alertId: string) => void;
  markAlertAsRead: (alertId: string) => void;
  markAllAlertsAsRead: () => void;
  clearAlerts: () => void;

  // Actions - UI
  setEditing: (editing: boolean) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setUpdateInterval: (interval: number) => void;
  refreshData: () => void;
}

// 기본 레이아웃
const createDefaultLayout = (): DashboardLayout => ({
  id: 'default',
  name: '기본 레이아웃',
  isDefault: true,
  widgets: [
    {
      id: generateId(),
      type: 'market_overview',
      title: '시장 개요',
      position: { x: 0, y: 0, width: 12, height: 4 },
      isVisible: true,
    },
    {
      id: generateId(),
      type: 'stock_watchlist',
      title: '관심 주식',
      position: { x: 0, y: 4, width: 6, height: 6 },
      isVisible: true,
    },
    {
      id: generateId(),
      type: 'crypto_prices',
      title: '암호화폐 가격',
      position: { x: 6, y: 4, width: 6, height: 6 },
      isVisible: true,
    },
    {
      id: generateId(),
      type: 'recent_news',
      title: '최근 뉴스',
      position: { x: 0, y: 10, width: 8, height: 6 },
      isVisible: true,
    },
    {
      id: generateId(),
      type: 'economic_calendar',
      title: '경제 캘린더',
      position: { x: 8, y: 10, width: 4, height: 6 },
      isVisible: true,
    },
  ],
});

export const useDashboardStore = create<DashboardStore>()(
  persist(
    (set, get) => ({
      // Initial state
      currentLayout: null,
      layouts: [createDefaultLayout()],
      marketOverview: null,
      portfolios: [],
      alerts: [],
      widgets: [],
      isEditing: false,
      loading: false,
      error: null,
      lastUpdated: null,
      updateInterval: UPDATE_INTERVALS.NORMAL,

      // Layout actions
      setCurrentLayout: (layout) => {
        set({ 
          currentLayout: layout,
          widgets: layout.widgets,
        });
      },

      addLayout: (layout) => {
        const layouts = get().layouts;
        set({ 
          layouts: [...layouts, layout],
        });
      },

      updateLayout: (layoutId, updates) => {
        const layouts = get().layouts;
        const updatedLayouts = layouts.map(layout =>
          layout.id === layoutId ? { ...layout, ...updates } : layout
        );
        
        set({ layouts: updatedLayouts });
        
        // 현재 레이아웃이 업데이트된 경우
        const currentLayout = get().currentLayout;
        if (currentLayout?.id === layoutId) {
          const updatedLayout = updatedLayouts.find(l => l.id === layoutId);
          if (updatedLayout) {
            set({ 
              currentLayout: updatedLayout,
              widgets: updatedLayout.widgets,
            });
          }
        }
      },

      deleteLayout: (layoutId) => {
        const layouts = get().layouts.filter(l => l.id !== layoutId);
        set({ layouts });
        
        // 삭제된 레이아웃이 현재 레이아웃인 경우 기본 레이아웃으로 변경
        const currentLayout = get().currentLayout;
        if (currentLayout?.id === layoutId) {
          const defaultLayout = layouts.find(l => l.isDefault) || layouts[0];
          if (defaultLayout) {
            set({ 
              currentLayout: defaultLayout,
              widgets: defaultLayout.widgets,
            });
          }
        }
      },

      duplicateLayout: (layoutId, newName) => {
        const layouts = get().layouts;
        const layoutToDuplicate = layouts.find(l => l.id === layoutId);
        
        if (layoutToDuplicate) {
          const newLayout: DashboardLayout = {
            ...layoutToDuplicate,
            id: generateId(),
            name: newName,
            isDefault: false,
            widgets: layoutToDuplicate.widgets.map(widget => ({
              ...widget,
              id: generateId(),
            })),
          };
          
          set({ layouts: [...layouts, newLayout] });
        }
      },

      // Widget actions
      addWidget: (widget) => {
        const newWidget = { ...widget, id: generateId() };
        const widgets = [...get().widgets, newWidget];
        
        set({ widgets });
        
        // 현재 레이아웃에도 추가
        const currentLayout = get().currentLayout;
        if (currentLayout) {
          get().updateLayout(currentLayout.id, {
            widgets,
          });
        }
      },

      updateWidget: (widgetId, updates) => {
        const widgets = get().widgets.map(widget =>
          widget.id === widgetId ? { ...widget, ...updates } : widget
        );
        
        set({ widgets });
        
        // 현재 레이아웃 업데이트
        const currentLayout = get().currentLayout;
        if (currentLayout) {
          get().updateLayout(currentLayout.id, { widgets });
        }
      },

      removeWidget: (widgetId) => {
        const widgets = get().widgets.filter(w => w.id !== widgetId);
        
        set({ widgets });
        
        // 현재 레이아웃 업데이트
        const currentLayout = get().currentLayout;
        if (currentLayout) {
          get().updateLayout(currentLayout.id, { widgets });
        }
      },

      moveWidget: (widgetId, position) => {
        get().updateWidget(widgetId, { position });
      },

      toggleWidgetVisibility: (widgetId) => {
        const widget = get().widgets.find(w => w.id === widgetId);
        if (widget) {
          get().updateWidget(widgetId, { isVisible: !widget.isVisible });
        }
      },

      resetWidgets: () => {
        const defaultLayout = createDefaultLayout();
        set({ 
          widgets: defaultLayout.widgets,
          currentLayout: defaultLayout,
        });
      },

      // Data actions
      setMarketOverview: (data) => {
        set({ 
          marketOverview: data,
          lastUpdated: new Date(),
        });
      },

      updateMarketData: (updates) => {
        const current = get().marketOverview;
        if (current) {
          set({ 
            marketOverview: { ...current, ...updates },
            lastUpdated: new Date(),
          });
        }
      },

      setPortfolios: (portfolios) => {
        set({ portfolios });
      },

      updatePortfolio: (portfolioId, updates) => {
        const portfolios = get().portfolios.map(portfolio =>
          portfolio.id === portfolioId ? { ...portfolio, ...updates } : portfolio
        );
        set({ portfolios });
      },

      // Alert actions
      addAlert: (alert) => {
        const newAlert = { ...alert, id: generateId() };
        const alerts = [newAlert, ...get().alerts];
        set({ alerts });
      },

      updateAlert: (alertId, updates) => {
        const alerts = get().alerts.map(alert =>
          alert.id === alertId ? { ...alert, ...updates } : alert
        );
        set({ alerts });
      },

      removeAlert: (alertId) => {
        const alerts = get().alerts.filter(a => a.id !== alertId);
        set({ alerts });
      },

      markAlertAsRead: (alertId) => {
        get().updateAlert(alertId, { isRead: true });
      },

      markAllAlertsAsRead: () => {
        const alerts = get().alerts.map(alert => ({ ...alert, isRead: true }));
        set({ alerts });
      },

      clearAlerts: () => {
        set({ alerts: [] });
      },

      // UI actions
      setEditing: (editing) => {
        set({ isEditing: editing });
      },

      setLoading: (loading) => {
        set({ loading });
      },

      setError: (error) => {
        set({ error });
      },

      setUpdateInterval: (interval) => {
        set({ updateInterval: interval });
      },

      refreshData: () => {
        set({ lastUpdated: new Date() });
      },
    }),
    {
      name: STORAGE_KEYS.DASHBOARD_LAYOUT,
      partialize: (state) => ({
        currentLayout: state.currentLayout,
        layouts: state.layouts,
        widgets: state.widgets,
        updateInterval: state.updateInterval,
      }),
    }
  )
);