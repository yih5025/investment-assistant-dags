import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { SearchBox } from '../../components/common/SearchBox';

export const NewsList: React.FC = () => {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold">뉴스</h1>
        <p className="text-muted-foreground">최신 금융 뉴스와 분석</p>
      </div>

      <SearchBox
        placeholder="뉴스 검색..."
        className="max-w-md"
      />

      <Card>
        <CardHeader>
          <CardTitle>최신 뉴스</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            뉴스 목록 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};