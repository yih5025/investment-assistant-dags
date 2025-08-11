import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { SearchBox } from '../../components/common/SearchBox';

export const CryptoList: React.FC = () => {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold">암호화폐</h1>
        <p className="text-muted-foreground">실시간 암호화폐 시세와 분석</p>
      </div>

      <SearchBox
        placeholder="암호화폐 검색..."
        className="max-w-md"
      />

      <Card>
        <CardHeader>
          <CardTitle>주요 암호화폐</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-12 text-muted-foreground">
            암호화폐 데이터 컴포넌트 (개발 예정)
          </div>
        </CardContent>
      </Card>
    </div>
  );
};