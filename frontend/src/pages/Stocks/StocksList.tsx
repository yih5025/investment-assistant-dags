import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { SearchBox } from '../../components/common/SearchBox';
import { Plus } from 'lucide-react';

export const StocksList: React.FC = () => {
  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">주식</h1>
          <p className="text-muted-foreground">실시간 주식 정보와 분석</p>
        </div>
        <Button>
          <Plus className="h-4 w-4 mr-2" />
          관심 종목 추가
        </Button>
      </div>

      <div className="flex items-center space-x-4">
        <SearchBox
          placeholder="주식 검색..."
          className="flex-1 max-w-md"
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>주요 종목</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            주식 데이터 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};