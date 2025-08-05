import Header from "@/components/Layout/Header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, TrendingDown, DollarSign, BarChart3, Info, AlertCircle } from "lucide-react";

const economicData = {
  cpi: {
    current: 3.2,
    previous: 3.4,
    trend: "down",
    description: "소비자물가지수(CPI)는 일반 소비자가 구매하는 상품과 서비스의 가격 변동을 측정하는 지표입니다.",
    importance: "인플레이션을 측정하는 핵심 지표로, 연준의 금리 정책 결정에 큰 영향을 미칩니다."
  },
  inflation: {
    current: 3.1,
    previous: 3.7,
    trend: "down",
    description: "인플레이션율은 전년 동기 대비 물가 상승률을 나타내며, 경제의 건전성을 보여주는 중요한 지표입니다.",
    importance: "적정 수준의 인플레이션(2% 내외)은 경제 성장을 의미하지만, 과도한 인플레이션은 경제에 부담이 됩니다."
  },
  fedRate: {
    current: 5.25,
    previous: 5.00,
    trend: "up",
    description: "연방기금금리는 미국 연방준비제도이사회가 설정하는 기준금리로, 경제 정책의 핵심 도구입니다.",
    importance: "금리 변동은 주식, 채권, 부동산 등 모든 자산 가격에 직접적인 영향을 미치는 가장 중요한 지표입니다."
  },
  treasuryYield: {
    current: 4.8,
    previous: 4.6,
    trend: "up",
    description: "10년 만기 미국 국채 수익률은 장기 금리의 기준이 되며, 경제 전망을 반영하는 중요한 지표입니다.",
    importance: "주식 시장의 밸류에이션과 부동산 시장에 큰 영향을 미치며, 안전자산으로서의 매력도를 나타냅니다."
  }
};

const getTrendIcon = (trend) => {
  return trend === "up" ? <TrendingUp className="h-4 w-4 text-destructive" /> : <TrendingDown className="h-4 w-4 text-success" />;
};

const getTrendColor = (trend) => {
  return trend === "up" ? "text-destructive" : "text-success";
};

const Economics = () => {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-4 space-y-6">
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold mb-2">경제 지표 대시보드</h1>
          <p className="text-muted-foreground">핵심 경제 지표를 통해 시장 동향을 파악하세요</p>
        </div>

        <Card className="p-4">
          <CardHeader className="pb-4">
            <CardTitle className="flex items-center gap-2">
              <Info className="h-5 w-5" />
              경제 지표란?
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground mb-4">
              경제 지표는 국가 경제의 현재 상태와 미래 전망을 수치로 나타낸 데이터입니다. 
              투자자들은 이러한 지표를 통해 시장의 방향성을 예측하고 투자 결정을 내립니다.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="p-3 bg-muted/50 rounded-lg">
                <h4 className="font-semibold mb-1">📈 선행 지표</h4>
                <p className="text-muted-foreground">미래 경제 상황을 예측 (주식지수, 금리)</p>
              </div>
              <div className="p-3 bg-muted/50 rounded-lg">
                <h4 className="font-semibold mb-1">📊 동행 지표</h4>
                <p className="text-muted-foreground">현재 경제 상황을 반영 (GDP, 물가)</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {Object.entries(economicData).map(([key, data]) => (
            <Card key={key} className="p-6">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <div className="flex items-center justify-center w-10 h-10 rounded-full bg-primary/10">
                    {key === 'cpi' && <BarChart3 className="h-5 w-5" />}
                    {key === 'inflation' && <TrendingUp className="h-5 w-5" />}
                    {key === 'fedRate' && <DollarSign className="h-5 w-5" />}
                    {key === 'treasuryYield' && <BarChart3 className="h-5 w-5" />}
                  </div>
                  <h3 className="font-semibold">
                    {key === 'cpi' && 'CPI (소비자물가지수)'}
                    {key === 'inflation' && '인플레이션율'}
                    {key === 'fedRate' && '연방기금금리'}
                    {key === 'treasuryYield' && '10년 국채 수익률'}
                  </h3>
                </div>
                <div className="text-right">
                  <p className="text-2xl font-bold">{data.current}%</p>
                  <div className={`flex items-center gap-1 ${getTrendColor(data.trend)}`}>
                    {getTrendIcon(data.trend)}
                    <span className="text-sm">
                      {data.trend === 'up' ? '+' : ''}{(data.current - data.previous).toFixed(1)}%p
                    </span>
                  </div>
                </div>
              </div>
              
              <div className="space-y-3">
                <div>
                  <p className="text-sm font-medium mb-1">📝 설명</p>
                  <p className="text-sm text-muted-foreground">{data.description}</p>
                </div>
                <div>
                  <p className="text-sm font-medium mb-1">💡 투자 중요성</p>
                  <p className="text-sm text-muted-foreground">{data.importance}</p>
                </div>
              </div>
            </Card>
          ))}
        </div>

        <Card className="p-6">
          <CardHeader className="pb-4">
            <CardTitle className="flex items-center gap-2">
              <AlertCircle className="h-5 w-5" />
              지표 간 상관관계
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Tabs defaultValue="relationships" className="w-full">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="relationships">상관관계</TabsTrigger>
                <TabsTrigger value="impact">투자 영향</TabsTrigger>
              </TabsList>
              
              <TabsContent value="relationships" className="space-y-4">
                <div className="grid gap-4">
                  <div className="p-4 border rounded-lg">
                    <h4 className="font-semibold mb-2">📊 CPI ↔ 연방기금금리</h4>
                    <p className="text-sm text-muted-foreground">
                      물가가 오르면 연준은 인플레이션을 억제하기 위해 금리를 인상합니다. 
                      반대로 물가가 안정되면 경기 부양을 위해 금리를 인하할 수 있습니다.
                    </p>
                  </div>
                  <div className="p-4 border rounded-lg">
                    <h4 className="font-semibold mb-2">💰 연방기금금리 ↔ 국채 수익률</h4>
                    <p className="text-sm text-muted-foreground">
                      연방기금금리가 오르면 국채 수익률도 함께 상승하는 경향이 있습니다. 
                      단기금리와 장기금리는 서로 영향을 주고받습니다.
                    </p>
                  </div>
                </div>
              </TabsContent>
              
              <TabsContent value="impact" className="space-y-4">
                <div className="grid gap-4">
                  <div className="p-4 border rounded-lg">
                    <h4 className="font-semibold mb-2">📈 주식 시장에 미치는 영향</h4>
                    <p className="text-sm text-muted-foreground">
                      금리 인상은 일반적으로 주식 시장에 부정적이며, 특히 성장주에 큰 영향을 미칩니다. 
                      반면 금리 인하는 주식 시장에 긍정적으로 작용합니다.
                    </p>
                  </div>
                  <div className="p-4 border rounded-lg">
                    <h4 className="font-semibold mb-2">🏠 부동산 시장에 미치는 영향</h4>
                    <p className="text-sm text-muted-foreground">
                      금리가 오르면 대출 비용이 증가해 부동산 수요가 감소하고, 
                      금리가 내리면 대출이 용이해져 부동산 시장이 활성화됩니다.
                    </p>
                  </div>
                </div>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      </main>
    </div>
  );
};

export default Economics;