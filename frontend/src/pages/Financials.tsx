import Header from "@/components/Layout/Header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Search, Building2, TrendingUp, TrendingDown, AlertTriangle, CheckCircle, Info } from "lucide-react";
import { useState } from "react";

const financialData = {
  AAPL: {
    company: "Apple Inc.",
    revenue: 394328000000,
    netIncome: 99803000000,
    totalAssets: 352755000000,
    totalDebt: 123930000000,
    cashAndEquivalents: 29965000000,
    analysis: {
      profitability: "excellent",
      liquidity: "good",
      leverage: "moderate",
      overall: "strong"
    }
  },
  TSLA: {
    company: "Tesla Inc.",
    revenue: 96773000000,
    netIncome: 15000000000,
    totalAssets: 106618000000,
    totalDebt: 9548000000,
    cashAndEquivalents: 29094000000,
    analysis: {
      profitability: "good",
      liquidity: "excellent",
      leverage: "low",
      overall: "strong"
    }
  }
};

const explanations = {
  revenue: "매출액은 기업이 제품이나 서비스 판매로 얻은 총 수익입니다. 기업의 규모와 성장성을 나타내는 핵심 지표입니다.",
  netIncome: "순이익은 모든 비용과 세금을 차감한 후 남은 최종 이익입니다. 기업의 실제 수익성을 보여줍니다.",
  totalAssets: "총자산은 기업이 소유한 모든 자산의 가치입니다. 기업의 규모와 자원을 나타냅니다.",
  totalDebt: "총부채는 기업이 갚아야 할 모든 빚의 총액입니다. 재무 리스크를 평가하는 중요한 지표입니다.",
  cashAndEquivalents: "현금 및 현금성 자산은 즉시 사용 가능한 유동성 자금입니다. 기업의 단기 지급능력을 나타냅니다."
};

const getStatusColor = (status) => {
  switch (status) {
    case "excellent": return "text-success";
    case "good": return "text-primary";
    case "moderate": return "text-warning";
    case "poor": return "text-destructive";
    default: return "text-muted-foreground";
  }
};

const getStatusIcon = (status) => {
  switch (status) {
    case "excellent": case "good": return <CheckCircle className="h-4 w-4" />;
    case "moderate": return <AlertTriangle className="h-4 w-4" />;
    case "poor": return <TrendingDown className="h-4 w-4" />;
    default: return <Info className="h-4 w-4" />;
  }
};

const Financials = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedCompany, setSelectedCompany] = useState("AAPL");

  const data = financialData[selectedCompany];

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-4 space-y-6">
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold mb-2">기업 재무제표 분석</h1>
          <p className="text-muted-foreground">재무제표를 쉽게 이해하고 기업의 재정 상태를 분석해보세요</p>
        </div>

        <Card className="p-4">
          <CardHeader className="pb-4">
            <CardTitle className="flex items-center gap-2">
              <Info className="h-5 w-5" />
              재무제표란?
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground mb-4">
              재무제표는 기업의 재정 상태와 경영 성과를 숫자로 보여주는 보고서입니다. 
              손익계산서, 재무상태표, 현금흐름표로 구성되며, 투자 결정에 필수적인 정보를 제공합니다.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div className="p-3 bg-muted/50 rounded-lg">
                <h4 className="font-semibold mb-1">손익계산서</h4>
                <p className="text-muted-foreground">매출, 비용, 순이익 등 수익성 정보</p>
              </div>
              <div className="p-3 bg-muted/50 rounded-lg">
                <h4 className="font-semibold mb-1">재무상태표</h4>
                <p className="text-muted-foreground">자산, 부채, 자본 등 재정 상태</p>
              </div>
              <div className="p-3 bg-muted/50 rounded-lg">
                <h4 className="font-semibold mb-1">현금흐름표</h4>
                <p className="text-muted-foreground">현금의 유입과 유출 현황</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="p-4">
          <CardContent className="space-y-4">
            <div className="relative">
              <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="기업명 또는 종목코드 검색 (예: AAPL, Tesla)"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
              />
            </div>

            <div className="flex gap-2">
              <Button
                variant={selectedCompany === "AAPL" ? "default" : "outline"}
                onClick={() => setSelectedCompany("AAPL")}
                size="sm"
              >
                Apple (AAPL)
              </Button>
              <Button
                variant={selectedCompany === "TSLA" ? "default" : "outline"}
                onClick={() => setSelectedCompany("TSLA")}
                size="sm"
              >
                Tesla (TSLA)
              </Button>
            </div>
          </CardContent>
        </Card>

        {data && (
          <div className="space-y-6">
            <Card className="p-6">
              <CardHeader className="pb-4">
                <CardTitle className="flex items-center gap-2">
                  <Building2 className="h-5 w-5" />
                  {data.company} 재무 데이터
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Tabs defaultValue="financials" className="w-full">
                  <TabsList className="grid w-full grid-cols-2">
                    <TabsTrigger value="financials">재무 데이터</TabsTrigger>
                    <TabsTrigger value="analysis">재정 상태 분석</TabsTrigger>
                  </TabsList>
                  
                  <TabsContent value="financials" className="space-y-4">
                    <div className="grid gap-4">
                      {[
                        { key: "revenue", label: "매출액", value: data.revenue },
                        { key: "netIncome", label: "순이익", value: data.netIncome },
                        { key: "totalAssets", label: "총자산", value: data.totalAssets },
                        { key: "totalDebt", label: "총부채", value: data.totalDebt },
                        { key: "cashAndEquivalents", label: "현금 및 현금성자산", value: data.cashAndEquivalents }
                      ].map((item) => (
                        <Card key={item.key} className="p-4">
                          <div className="flex justify-between items-start mb-2">
                            <h4 className="font-semibold">{item.label}</h4>
                            <p className="text-xl font-bold">
                              ${(item.value / 1000000000).toFixed(1)}B
                            </p>
                          </div>
                          <p className="text-sm text-muted-foreground">{explanations[item.key]}</p>
                        </Card>
                      ))}
                    </div>
                  </TabsContent>
                  
                  <TabsContent value="analysis" className="space-y-4">
                    <div className="grid gap-4">
                      <Card className="p-4">
                        <h4 className="font-semibold mb-3">재정 상태 종합 분석</h4>
                        <div className="space-y-3">
                          <div className="flex items-center justify-between">
                            <span>수익성</span>
                            <div className={`flex items-center gap-2 ${getStatusColor(data.analysis.profitability)}`}>
                              {getStatusIcon(data.analysis.profitability)}
                              <span className="capitalize">{data.analysis.profitability}</span>
                            </div>
                          </div>
                          <div className="flex items-center justify-between">
                            <span>유동성</span>
                            <div className={`flex items-center gap-2 ${getStatusColor(data.analysis.liquidity)}`}>
                              {getStatusIcon(data.analysis.liquidity)}
                              <span className="capitalize">{data.analysis.liquidity}</span>
                            </div>
                          </div>
                          <div className="flex items-center justify-between">
                            <span>레버리지</span>
                            <div className={`flex items-center gap-2 ${getStatusColor(data.analysis.leverage)}`}>
                              {getStatusIcon(data.analysis.leverage)}
                              <span className="capitalize">{data.analysis.leverage}</span>
                            </div>
                          </div>
                        </div>
                        <div className="mt-4 p-3 bg-muted/50 rounded-lg">
                          <div className="flex items-center gap-2 mb-2">
                            <div className={`flex items-center gap-1 ${getStatusColor(data.analysis.overall)}`}>
                              {getStatusIcon(data.analysis.overall)}
                              <span className="font-semibold">종합 평가: {data.analysis.overall}</span>
                            </div>
                          </div>
                          <p className="text-sm text-muted-foreground">
                            {selectedCompany === "AAPL" 
                              ? "Apple은 높은 수익성과 안정적인 현금 흐름을 바탕으로 매우 건전한 재정 상태를 유지하고 있습니다."
                              : "Tesla는 급속한 성장과 함께 강력한 현금 보유로 건전한 재정 구조를 보여주고 있습니다."
                            }
                          </p>
                        </div>
                      </Card>
                    </div>
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>
          </div>
        )}
      </main>
    </div>
  );
};

export default Financials;