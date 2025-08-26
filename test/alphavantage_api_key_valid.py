#!/usr/bin/env python3
"""
Alpha Vantage API 키 검증 스크립트
각 API 키의 상태와 남은 할당량을 확인합니다.
"""

import requests
import json
import time
from datetime import datetime

# API 키 설정 (Airflow Variable에서 가져온 값으로 교체)
API_KEYS = {
    "API_KEY_3": "DT91GNLNF095FO9W",
    "API_KEY_4": "0HBO6MJM3FXVS4CI"
}

def test_api_key(key_name, api_key):
    """단일 API 키 테스트"""
    print(f"\n🔍 {key_name} 테스트 중...")
    
    # 간단한 테스트 호출
    test_url = "https://www.alphavantage.co/query"
    params = {
        'function': 'OVERVIEW',
        'symbol': 'AAPL',  # 확실히 존재하는 심볼
        'apikey': api_key
    }
    
    try:
        response = requests.get(test_url, params=params, timeout=10)
        response.raise_for_status()
        
        # 응답 내용 분석
        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"❌ {key_name}: JSON 파싱 실패")
            print(f"   응답 내용: {response.text[:200]}...")
            return False
            
        # 응답 상태 확인
        if 'Note' in data and 'API call frequency' in data['Note']:
            print(f"⚠️ {key_name}: API 호출 제한 도달")
            print(f"   메시지: {data['Note']}")
            return False
            
        elif 'Error Message' in data:
            print(f"❌ {key_name}: API 오류")
            print(f"   에러: {data['Error Message']}")
            return False
            
        elif 'Information' in data:
            print(f"⚠️ {key_name}: 정보 메시지")
            print(f"   메시지: {data['Information']}")
            return False
            
        elif data.get('Symbol') == 'AAPL' and data.get('Name'):
            print(f"✅ {key_name}: 정상 동작")
            print(f"   회사명: {data.get('Name')}")
            print(f"   섹터: {data.get('Sector', 'N/A')}")
            return True
            
        else:
            print(f"❓ {key_name}: 예상치 못한 응답")
            print(f"   응답: {json.dumps(data, indent=2)[:300]}...")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ {key_name}: 네트워크 오류 - {str(e)}")
        return False
    except Exception as e:
        print(f"❌ {key_name}: 알 수 없는 오류 - {str(e)}")
        return False

def main():
    print("🔑 Alpha Vantage API 키 검증 시작")
    print(f"⏰ 테스트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    results = {}
    
    for key_name, api_key in API_KEYS.items():
        if not api_key or api_key == f"YOUR_ALPHA_VANTAGE_API_KEY_{key_name.split('_')[-1]}_HERE":
            print(f"\n❌ {key_name}: API 키가 설정되지 않음")
            results[key_name] = False
            continue
            
        results[key_name] = test_api_key(key_name, api_key)
        
        # API Rate Limit 고려하여 딜레이
        if key_name != list(API_KEYS.keys())[-1]:  # 마지막이 아니면
            print("   ⏳ 5초 대기...")
            time.sleep(5)
    
    # 결과 요약
    print("\n" + "=" * 50)
    print("📊 검증 결과 요약:")
    
    working_keys = 0
    for key_name, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"   {status_icon} {key_name}: {'정상' if status else '문제'}")
        if status:
            working_keys += 1
    
    print(f"\n🎯 사용 가능한 API 키: {working_keys}/{len(API_KEYS)}개")
    
    if working_keys == 0:
        print("\n⚠️ 모든 API 키에 문제가 있습니다!")
        print("   1. API 키가 올바르게 설정되었는지 확인")
        print("   2. 일일 할당량(25회)이 소진되었는지 확인")
        print("   3. 새로운 API 키 발급 고려")
    elif working_keys < len(API_KEYS):
        print(f"\n⚠️ {len(API_KEYS) - working_keys}개 API 키에 문제가 있습니다!")
        print("   문제가 있는 키를 교체하거나 내일 다시 시도하세요.")
    else:
        print("\n🎉 모든 API 키가 정상 동작합니다!")

if __name__ == "__main__":
    main()