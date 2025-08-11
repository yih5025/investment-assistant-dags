import React from 'react';
import { useParams } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';

export const NewsDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold">뉴스 상세</h1>
        <p className="text-muted-foreground">뉴스 ID: {id}</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>뉴스 내용</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            뉴스 상세 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};