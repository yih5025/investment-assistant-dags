import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';

export const EconomicData: React.FC = () => {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold">경제지표</h1>
        <p className="text-muted-foreground">주요 경제지표와 분석</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>경제지표</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            경제지표 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};