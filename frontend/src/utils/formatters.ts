import { format } from 'date-fns';
import { ko } from 'date-fns/locale';

// 숫자 포맷팅
export const formatNumber = (value: number, decimals: number = 2): string => {
  return new Intl.NumberFormat('ko-KR', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
};

// 통화 포맷팅
export const formatCurrency = (
  value: number, 
  currency: 'KRW' | 'USD' | 'BTC' | 'ETH' = 'KRW'
): string => {
  switch (currency) {
    case 'KRW':
      return `₩${formatNumber(value, 0)}`;
    case 'USD':
      return `$${formatNumber(value, 2)}`;
    case 'BTC':
      return `₿${formatNumber(value, 8)}`;
    case 'ETH':
      return `Ξ${formatNumber(value, 6)}`;
    default:
      return formatNumber(value);
  }
};

// 퍼센트 포맷팅
export const formatPercent = (value: number, decimals: number = 2): string => {
  return `${value >= 0 ? '+' : ''}${formatNumber(value, decimals)}%`;
};

// 거래량 포맷팅 (K, M, B 단위)
export const formatVolume = (value: number): string => {
  if (value >= 1e9) {
    return `${formatNumber(value / 1e9, 1)}B`;
  } else if (value >= 1e6) {
    return `${formatNumber(value / 1e6, 1)}M`;
  } else if (value >= 1e3) {
    return `${formatNumber(value / 1e3, 1)}K`;
  }
  return formatNumber(value, 0);
};

// 시가총액 포맷팅
export const formatMarketCap = (value: number): string => {
  if (value >= 1e12) {
    return `${formatNumber(value / 1e12, 2)}T`;
  } else if (value >= 1e9) {
    return `${formatNumber(value / 1e9, 2)}B`;
  } else if (value >= 1e6) {
    return `${formatNumber(value / 1e6, 2)}M`;
  }
  return formatCurrency(value);
};

// 날짜 포맷팅
export const formatDate = (
  date: string | Date, 
  formatString: string = 'yyyy-MM-dd'
): string => {
  const dateObj = typeof date === 'string' ? new Date(date) : date;
  return format(dateObj, formatString, { locale: ko });
};

// 상대 시간 포맷팅 (예: "5분 전", "1시간 전")
export const formatRelativeTime = (date: string | Date): string => {
  const dateObj = typeof date === 'string' ? new Date(date) : date;
  const now = new Date();
  const diffInSeconds = Math.floor((now.getTime() - dateObj.getTime()) / 1000);

  if (diffInSeconds < 60) {
    return '방금 전';
  } else if (diffInSeconds < 3600) {
    const minutes = Math.floor(diffInSeconds / 60);
    return `${minutes}분 전`;
  } else if (diffInSeconds < 86400) {
    const hours = Math.floor(diffInSeconds / 3600);
    return `${hours}시간 전`;
  } else if (diffInSeconds < 2592000) {
    const days = Math.floor(diffInSeconds / 86400);
    return `${days}일 전`;
  } else {
    return formatDate(dateObj, 'yyyy-MM-dd');
  }
};

// 가격 변동에 따른 색상 클래스 반환
export const getPriceChangeClass = (change: number): string => {
  if (change > 0) return 'price-up';
  if (change < 0) return 'price-down';
  return 'price-neutral';
};

// 가격 변동 방향 아이콘
export const getPriceChangeIcon = (change: number): string => {
  if (change > 0) return '▲';
  if (change < 0) return '▼';
  return '—';
};

// 심볼 정규화 (대문자 변환)
export const normalizeSymbol = (symbol: string): string => {
  return symbol.toUpperCase().trim();
};

// 텍스트 줄임표 처리
export const truncateText = (text: string, maxLength: number): string => {
  if (text.length <= maxLength) return text;
  return `${text.substring(0, maxLength)}...`;
};