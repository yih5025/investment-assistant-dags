import { Clock, ExternalLink, TrendingUp, Building2 } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

interface NewsItem {
  id: string;
  title: string;
  summary: string;
  source: string;
  timestamp: string;
  category: 'stock' | 'crypto' | 'economy' | 'tech';
  impact: 'high' | 'medium' | 'low';
  url: string;
  companies?: string[];
}

const NewsList = () => {
  // Mock news data
  const newsItems: NewsItem[] = [
    {
      id: '1',
      title: 'Apple Q4 실적 발표, 예상치 상회하며 주가 급등',
      summary: 'Apple이 4분기 실적에서 아이폰 매출과 서비스 부문에서 강력한 성장을 보였습니다. 매출은 전년 동기 대비 8% 증가한 1,195억 달러를 기록했습니다.',
      source: 'Reuters',
      timestamp: '30분 전',
      category: 'stock',
      impact: 'high',
      url: 'https://example.com/news/1',
      companies: ['AAPL']
    },
    {
      id: '2',
      title: 'Fed 금리 동결 결정, 시장은 안도감 표시',
      summary: '연방준비제도이사회가 기준금리를 현 수준으로 유지하기로 결정했습니다. 인플레이션 둔화 신호가 나타나면서 시장에서는 긍정적인 반응을 보이고 있습니다.',
      source: 'Bloomberg',
      timestamp: '1시간 전',
      category: 'economy',
      impact: 'high',
      url: 'https://example.com/news/2'
    },
    {
      id: '3',
      title: '비트코인 45,000달러 돌파, 기관 투자 증가 영향',
      summary: '비트코인이 45,000달러를 넘어서며 한 달 만에 최고치를 기록했습니다. 대형 기관들의 지속적인 투자와 ETF 승인 기대감이 상승 요인으로 작용했습니다.',
      source: 'CoinDesk',
      timestamp: '2시간 전',
      category: 'crypto',
      impact: 'medium',
      url: 'https://example.com/news/3'
    },
    {
      id: '4',
      title: 'Tesla 새로운 배터리 기술 발표, 주가 5% 상승',
      summary: 'Tesla가 차세대 4680 배터리 셀의 대량 생산 계획을 발표했습니다. 새로운 기술로 배터리 수명이 20% 향상되고 비용은 15% 절감될 것으로 예상됩니다.',
      source: 'TechCrunch',
      timestamp: '3시간 전',
      category: 'tech',
      impact: 'medium',
      url: 'https://example.com/news/4',
      companies: ['TSLA']
    },
    {
      id: '5',
      title: '중국 경제지표 개선, 아시아 증시 전반 상승',
      summary: '중국의 12월 제조업 PMI가 50.1을 기록하며 6개월 만에 확장 국면에 진입했습니다. 이에 따라 아시아 증시 전반이 상승세를 보이고 있습니다.',
      source: 'Financial Times',
      timestamp: '4시간 전',
      category: 'economy',
      impact: 'medium',
      url: 'https://example.com/news/5'
    }
  ];

  const getCategoryColor = (category: string) => {
    switch (category) {
      case 'stock':
        return 'bg-primary text-primary-foreground';
      case 'crypto':
        return 'bg-accent text-accent-foreground';
      case 'economy':
        return 'bg-destructive text-destructive-foreground';
      case 'tech':
        return 'bg-success text-success-foreground';
      default:
        return 'bg-muted text-muted-foreground';
    }
  };

  const getCategoryLabel = (category: string) => {
    switch (category) {
      case 'stock':
        return '주식';
      case 'crypto':
        return '암호화폐';
      case 'economy':
        return '경제';
      case 'tech':
        return '기술';
      default:
        return '기타';
    }
  };

  const getImpactIcon = (impact: string) => {
    switch (impact) {
      case 'high':
        return <TrendingUp className="h-3 w-3 text-destructive" />;
      case 'medium':
        return <TrendingUp className="h-3 w-3 text-warning" />;
      case 'low':
        return <TrendingUp className="h-3 w-3 text-muted-foreground" />;
      default:
        return null;
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <TrendingUp className="h-5 w-5 text-primary" />
          <h2 className="text-lg font-semibold">실시간 뉴스</h2>
          <Badge variant="secondary" className="text-xs">Live</Badge>
        </div>
        <Button variant="outline" size="sm">
          전체 보기
        </Button>
      </div>

      <div className="space-y-3">
        {newsItems.map((item) => (
          <Card key={item.id} className="p-4 hover:shadow-md transition-all cursor-pointer group">
            <div className="space-y-3">
              {/* Header */}
              <div className="flex items-start justify-between">
                <div className="flex items-center space-x-2">
                  <Badge className={getCategoryColor(item.category)}>
                    {getCategoryLabel(item.category)}
                  </Badge>
                  {getImpactIcon(item.impact)}
                  {item.companies && (
                    <div className="flex items-center space-x-1">
                      <Building2 className="h-3 w-3 text-muted-foreground" />
                      <span className="text-xs text-muted-foreground">
                        {item.companies.join(', ')}
                      </span>
                    </div>
                  )}
                </div>
                <div className="flex items-center space-x-1 text-xs text-muted-foreground">
                  <Clock className="h-3 w-3" />
                  <span>{item.timestamp}</span>
                </div>
              </div>

              {/* Content */}
              <div>
                <h3 className="font-semibold text-foreground mb-2 group-hover:text-primary transition-colors">
                  {item.title}
                </h3>
                <p className="text-sm text-muted-foreground leading-relaxed line-clamp-2">
                  {item.summary}
                </p>
              </div>

              {/* Footer */}
              <div className="flex items-center justify-between pt-2 border-t">
                <span className="text-xs font-medium text-muted-foreground">
                  {item.source}
                </span>
                <Button 
                  variant="ghost" 
                  size="sm"
                  onClick={() => window.open(item.url, '_blank')}
                  className="opacity-0 group-hover:opacity-100 transition-opacity"
                >
                  <ExternalLink className="h-3 w-3 mr-1" />
                  <span className="text-xs">자세히 보기</span>
                </Button>
              </div>
            </div>
          </Card>
        ))}
      </div>

      <div className="text-center">
        <Button variant="outline" className="w-full">
          더 많은 뉴스 보기
        </Button>
      </div>
    </div>
  );
};

export default NewsList;