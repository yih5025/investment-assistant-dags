# yahoo_direct_test.py
import requests
import json
import time
from datetime import datetime

def test_yahoo_finance_direct():
    """Yahoo Finance API를 직접 호출해서 테스트"""
    print("=" * 80)
    print("Yahoo Finance 직접 API 테스트")
    print("=" * 80)
    
    test_symbols = ['SPY', 'QQQ', 'VTI', 'IWM', 'GLD']
    
    for symbol in test_symbols:
        print(f"\n🔍 {symbol} 테스트:")
        print("-" * 50)
        
        try:
            # Yahoo Finance Query API (v8)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart']:
                result = data['chart']['result'][0]
                meta = result.get('meta', {})
                
                print(f"심볼: {meta.get('symbol')}")
                print(f"거래소: {meta.get('exchangeName')}")
                print(f"통화: {meta.get('currency')}")
                print(f"현재가: {meta.get('regularMarketPrice')}")
                print(f"이전 종가: {meta.get('previousClose')}")
                print(f"52주 최고: {meta.get('fiftyTwoWeekHigh')}")
                print(f"52주 최저: {meta.get('fiftyTwoWeekLow')}")
                
                # 전체 메타 데이터 키 출력
                print(f"사용 가능한 메타 필드: {list(meta.keys())}")
                
        except Exception as e:
            print(f"❌ {symbol} 오류: {e}")
        
        time.sleep(0.5)  # API 제한 방지

def test_yahoo_quote_summary():
    """Yahoo Finance Quote Summary API 테스트 (더 자세한 정보)"""
    print("\n" + "=" * 80)
    print("Yahoo Finance Quote Summary API 테스트")
    print("=" * 80)
    
    test_symbols = ['SPY', 'QQQ']
    
    for symbol in test_symbols:
        print(f"\n🔍 {symbol} 상세 정보:")
        print("-" * 50)
        
        try:
            # Quote Summary API
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
            params = {
                'modules': 'summaryDetail,fundProfile,topHoldings,majorHoldersBreakdown'
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'quoteSummary' in data and 'result' in data['quoteSummary']:
                result = data['quoteSummary']['result'][0]
                
                # Summary Detail
                if 'summaryDetail' in result:
                    summary = result['summaryDetail']
                    print("기본 정보:")
                    for key, value in summary.items():
                        if isinstance(value, dict) and 'raw' in value:
                            print(f"  {key}: {value['raw']}")
                        else:
                            print(f"  {key}: {value}")
                
                # Fund Profile (ETF 전용)
                if 'fundProfile' in result:
                    fund_profile = result['fundProfile']
                    print("\nETF 펀드 정보:")
                    for key, value in fund_profile.items():
                        if isinstance(value, dict) and 'raw' in value:
                            print(f"  {key}: {value['raw']}")
                        else:
                            print(f"  {key}: {value}")
                
                # Top Holdings (ETF 구성종목)
                if 'topHoldings' in result:
                    holdings = result['topHoldings']
                    print("\nETF 구성종목:")
                    if 'holdings' in holdings:
                        for i, holding in enumerate(holdings['holdings'][:5]):
                            print(f"  {i+1}. {holding}")
                    print(f"  구성종목 관련 필드: {list(holdings.keys())}")
                
                # Major Holders
                if 'majorHoldersBreakdown' in result:
                    major_holders = result['majorHoldersBreakdown']
                    print("\n주요 보유자:")
                    for key, value in major_holders.items():
                        if isinstance(value, dict) and 'raw' in value:
                            print(f"  {key}: {value['raw']}")
                        else:
                            print(f"  {key}: {value}")
                
                # 사용 가능한 모든 모듈 출력
                print(f"\n사용 가능한 모듈: {list(result.keys())}")
                
        except Exception as e:
            print(f"❌ {symbol} 오류: {e}")
        
        time.sleep(1)  # API 제한 방지

def test_yahoo_screener_etf():
    """Yahoo Finance Screener로 ETF 목록 가져오기"""
    print("\n" + "=" * 80)
    print("Yahoo Finance Screener ETF 목록 테스트")
    print("=" * 80)
    
    try:
        # Yahoo Finance Screener API
        url = "https://query1.finance.yahoo.com/v1/finance/screener"
        
        # ETF 스크리너 쿼리
        payload = {
            "size": 25,
            "offset": 0,
            "sortField": "intradaymarketcap",
            "sortType": "DESC",
            "quoteType": "ETF",
            "query": {
                "operator": "AND",
                "operands": [
                    {
                        "operator": "eq",
                        "operands": ["quoteType", "ETF"]
                    }
                ]
            },
            "userId": "",
            "userIdType": "guid"
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        if 'finance' in data and 'result' in data['finance']:
            result = data['finance']['result'][0]
            quotes = result.get('quotes', [])
            
            print(f"발견된 ETF: {len(quotes)}개")
            print("\n상위 10개 ETF:")
            
            for i, quote in enumerate(quotes[:10]):
                symbol = quote.get('symbol', 'N/A')
                name = quote.get('longName', quote.get('shortName', 'N/A'))
                market_cap = quote.get('marketCap', 'N/A')
                
                print(f"  {i+1:2d}. {symbol:6s} - {name[:50]}")
                print(f"      시가총액: {market_cap}")
            
            # 첫 번째 ETF의 모든 필드 출력
            if quotes:
                print(f"\n첫 번째 ETF ({quotes[0].get('symbol')})의 모든 필드:")
                for key, value in quotes[0].items():
                    print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"❌ ETF 스크리너 오류: {e}")

def save_yahoo_sample_data():
    """Yahoo Finance 샘플 데이터를 JSON으로 저장"""
    print("\n" + "=" * 80)
    print("Yahoo Finance 샘플 데이터 저장")
    print("=" * 80)
    
    symbol = 'SPY'
    
    try:
        # 1. Chart 데이터
        chart_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        chart_response = requests.get(chart_url, headers=headers, timeout=10)
        chart_data = chart_response.json()
        
        with open(f'{symbol}_yahoo_chart.json', 'w', encoding='utf-8') as f:
            json.dump(chart_data, f, indent=2, ensure_ascii=False)
        print(f"✅ {symbol} Chart 데이터를 {symbol}_yahoo_chart.json으로 저장")
        
        # 2. Quote Summary 데이터
        summary_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        summary_params = {
            'modules': 'summaryDetail,fundProfile,topHoldings,majorHoldersBreakdown,fundOwnership'
        }
        
        summary_response = requests.get(summary_url, params=summary_params, headers=headers, timeout=10)
        summary_data = summary_response.json()
        
        with open(f'{symbol}_yahoo_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        print(f"✅ {symbol} Summary 데이터를 {symbol}_yahoo_summary.json으로 저장")
        
    except Exception as e:
        print(f"❌ 샘플 데이터 저장 실패: {e}")

def main():
    """메인 테스트 실행"""
    print("Yahoo Finance 직접 API 테스트 시작")
    print(f"테스트 시간: {datetime.now()}")
    
    # 1. 기본 Chart API 테스트
    test_yahoo_finance_direct()
    
    # 2. Quote Summary API 테스트
    test_yahoo_quote_summary()
    
    # 3. ETF 스크리너 테스트
    test_yahoo_screener_etf()
    
    # 4. 샘플 데이터 저장
    save_yahoo_sample_data()
    
    print("\n" + "=" * 80)
    print("Yahoo Finance 직접 API 테스트 완료!")
    print("생성된 파일들:")
    print("  - SPY_yahoo_chart.json")
    print("  - SPY_yahoo_summary.json")
    print("=" * 80)

if __name__ == "__main__":
    main()