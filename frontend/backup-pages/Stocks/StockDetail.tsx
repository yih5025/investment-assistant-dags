import React from 'react';
import { useParams } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';

export const StockDetail: React.FC = () => {
  const { symbol } = useParams<{ symbol: string }>();

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold">{symbol?.toUpperCase()}</h1>
        <p className="text-muted-foreground">주식 상세 정보</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>상세 정보</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            주식 상세 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};