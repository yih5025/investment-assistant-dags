import Header from "@/components/Layout/Header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Search, TrendingUp, TrendingDown, BarChart3, Bitcoin, DollarSign } from "lucide-react";
import { useState } from "react";

const stockData = [
  { symbol: "AAPL", name: "Apple Inc.", price: 185.25, change: 2.45, changePercent: 1.34, type: "stock" },
  { symbol: "TSLA", name: "Tesla Inc.", price: 248.50, change: -5.23, changePercent: -2.06, type: "stock" },
  { symbol: "MSFT", name: "Microsoft Corp.", price: 378.90, change: 8.12, changePercent: 2.19, type: "stock" },
  { symbol: "GOOGL", name: "Alphabet Inc.", price: 138.45, change: 1.85, changePercent: 1.35, type: "stock" },
  { symbol: "NVDA", name: "NVIDIA Corp.", price: 875.60, change: 15.30, changePercent: 1.78, type: "stock" },
];

const cryptoData = [
  { symbol: "BTC", name: "Bitcoin", price: 42350, change: 1250, changePercent: 3.04, type: "crypto" },
  { symbol: "ETH", name: "Ethereum", price: 2580, change: -45, changePercent: -1.71, type: "crypto" },
  { symbol: "BNB", name: "Binance Coin", price: 315, change: 8.5, changePercent: 2.77, type: "crypto" },
  { symbol: "SOL", name: "Solana", price: 98.5, change: 4.2, changePercent: 4.46, type: "crypto" },
];

const Markets = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [activeTab, setActiveTab] = useState("stocks");

  const allAssets = [...stockData, ...cryptoData];
  const filteredAssets = allAssets.filter(asset =>
    asset.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
    asset.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const currentData = activeTab === "stocks" ? stockData : cryptoData;

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-4 space-y-6">
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold mb-2">실시간 시장 데이터</h1>
          <p className="text-muted-foreground">주식과 암호화폐 가격을 실시간으로 확인하세요</p>
        </div>

        <Card className="p-4">
          <CardContent className="space-y-4">
            <div className="relative">
              <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="종목명 또는 심볼 검색 (예: AAPL, BTC)"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
              />
            </div>

            <div className="flex gap-2">
              <Button
                variant={activeTab === "stocks" ? "default" : "outline"}
                onClick={() => setActiveTab("stocks")}
                className="flex items-center gap-2"
              >
                <BarChart3 className="h-4 w-4" />
                주식
              </Button>
              <Button
                variant={activeTab === "crypto" ? "default" : "outline"}
                onClick={() => setActiveTab("crypto")}
                className="flex items-center gap-2"
              >
                <Bitcoin className="h-4 w-4" />
                암호화폐
              </Button>
            </div>
          </CardContent>
        </Card>

        <div className="grid gap-4">
          {(searchTerm ? filteredAssets : currentData).map((asset) => (
            <Card key={asset.symbol} className="p-4 hover:shadow-md transition-shadow cursor-pointer"
                  onClick={() => setSelectedAsset(asset)}>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="flex items-center justify-center w-10 h-10 rounded-full bg-primary/10">
                    {asset.type === "crypto" ? <Bitcoin className="h-5 w-5" /> : <DollarSign className="h-5 w-5" />}
                  </div>
                  <div>
                    <p className="font-semibold">{asset.symbol}</p>
                    <p className="text-sm text-muted-foreground">{asset.name}</p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="font-bold text-lg">
                    ${asset.price.toLocaleString(undefined, { minimumFractionDigits: asset.type === "crypto" ? 0 : 2 })}
                  </p>
                  <div className="flex items-center gap-1">
                    {asset.change >= 0 ? (
                      <TrendingUp className="h-4 w-4 text-success" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-destructive" />
                    )}
                    <span className={asset.change >= 0 ? "text-success" : "text-destructive"}>
                      {asset.change >= 0 ? "+" : ""}{asset.changePercent.toFixed(2)}%
                    </span>
                  </div>
                </div>
              </div>
            </Card>
          ))}
        </div>

        {selectedAsset && (
          <Card className="p-6">
            <CardHeader className="pb-4">
              <CardTitle className="flex items-center justify-between">
                <span>{selectedAsset.symbol} 상세 정보</span>
                <Button variant="outline" size="sm" onClick={() => setSelectedAsset(null)}>
                  닫기
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-sm text-muted-foreground">현재가</p>
                  <p className="text-2xl font-bold">${selectedAsset.price.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">변동</p>
                  <p className={`text-xl font-semibold ${selectedAsset.change >= 0 ? "text-success" : "text-destructive"}`}>
                    {selectedAsset.change >= 0 ? "+" : ""}{selectedAsset.change} ({selectedAsset.changePercent.toFixed(2)}%)
                  </p>
                </div>
              </div>
              
              <div className="bg-muted/50 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground mb-2">📊 차트 및 상세 분석</p>
                <p className="text-sm">상세한 차트와 기술적 분석이 곧 제공됩니다.</p>
              </div>
            </CardContent>
          </Card>
        )}
      </main>
    </div>
  );
};

export default Markets;