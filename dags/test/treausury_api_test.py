#!/usr/bin/env python3
"""
Treasury Yield API 테스트 스크립트
Alpha Vantage Treasury Yield API가 정상 동작하는지 확인

사용법:
python test_treasury_yield_api.py YOUR_API_KEY
"""

import requests
import json
import sys
from datetime import datetime

def test_treasury_yield_api(api_key):
    """Treasury Yield API 전체 테스트"""
    
    print("🔍 Treasury Yield API 테스트 시작")
    print("=" * 60)
    print(f"🔑 API 키: {api_key[:8]}..." if len(api_key) > 8 else api_key)
    print(f"🕐 테스트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 테스트할 조합들 (DAG와 동일)
    test_combinations = [
        ('monthly', '2year'),
        ('monthly', '10year'), 
        ('monthly', '30year'),
    ]
    
    base_url = "https://www.alphavantage.co/query"
    
    for i, (interval, maturity) in enumerate(test_combinations, 1):
        print(f"📊 테스트 {i}/3: {interval} {maturity}")
        print("-" * 40)
        
        # API 파라미터 구성
        params = {
            'function': 'TREASURY_YIELD',
            'interval': interval,
            'maturity': maturity,
            'datatype': 'json',
            'apikey': api_key
        }
        
        # URL 출력 (수동 확인용)
        url_with_params = f"{base_url}?" + "&".join([f"{k}={v}" for k, v in params.items()])
        print(f"🌐 요청 URL: {url_with_params}")
        
        try:
            # API 호출
            print("🚀 API 호출 중...")
            response = requests.get(base_url, params=params, timeout=30)
            
            # HTTP 상태 확인
            print(f"📡 HTTP 상태: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ HTTP 오류: {response.status_code}")
                print(f"응답 내용: {response.text[:200]}")
                continue
            
            # 응답 내용 출력 (처음 500자)
            response_text = response.text
            print(f"📄 응답 길이: {len(response_text)} 문자")
            print(f"📄 응답 내용 (처음 500자):")
            print(response_text[:500])
            print()
            
            # 특정 키워드 체크
            if 'Error Message' in response_text:
                print("❌ API 오류 메시지 발견:")
                print(response_text)
                continue
                
            if 'Thank you for using Alpha Vantage' in response_text:
                print("⚠️ API 한도 초과 메시지 발견")
                continue
                
            if 'API call frequency' in response_text:
                print("⚠️ API 호출 빈도 제한 메시지 발견")
                continue
            
            # JSON 파싱 시도
            try:
                data = response.json()
                print(f"✅ JSON 파싱 성공")
                print(f"📋 JSON 최상위 키들: {list(data.keys())}")
                
                # 데이터 구조 분석
                if 'data' in data:
                    records = data['data']
                    print(f"📊 'data' 필드 발견: {len(records)}개 레코드")
                    if records:
                        print(f"📝 첫 번째 레코드: {records[0]}")
                        print(f"📝 마지막 레코드: {records[-1]}")
                    else:
                        print("⚠️ 'data' 필드는 있지만 비어있음")
                else:
                    print("❌ 'data' 필드 없음")
                    print("🔍 전체 응답 구조:")
                    print(json.dumps(data, indent=2)[:1000])
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {e}")
                print("📄 원시 응답:")
                print(response_text[:1000])
                
        except requests.exceptions.RequestException as e:
            print(f"❌ 네트워크 오류: {e}")
            
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            
        print("\n" + "=" * 60 + "\n")

def test_single_api_call(api_key, interval='monthly', maturity='10year'):
    """단일 API 호출 상세 테스트"""
    
    print(f"🎯 단일 호출 테스트: {interval} {maturity}")
    print("=" * 60)
    
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'TREASURY_YIELD',
        'interval': interval,
        'maturity': maturity,
        'datatype': 'json',
        'apikey': api_key
    }
    
    print(f"🌐 완전한 URL:")
    full_url = url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
    print(full_url)
    print()
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        print(f"📡 상태 코드: {response.status_code}")
        print(f"📡 응답 헤더:")
        for key, value in response.headers.items():
            print(f"   {key}: {value}")
        print()
        
        print(f"📄 전체 응답:")
        print(response.text)
        print()
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"📋 JSON 구조 (예쁘게 출력):")
                print(json.dumps(data, indent=2))
            except:
                print("❌ JSON 파싱 불가")
        
    except Exception as e:
        print(f"❌ 오류: {e}")

def main():
    """메인 함수"""
    
    if len(sys.argv) != 2:
        print("사용법: python test_treasury_yield_api.py YOUR_API_KEY")
        print()
        print("예시:")
        print("python test_treasury_yield_api.py demo")
        print("python test_treasury_yield_api.py your_actual_api_key_here")
        sys.exit(1)
    
    api_key = sys.argv[1]
    
    if not api_key or api_key.lower() in ['none', 'null', '']:
        print("❌ 유효한 API 키를 입력해주세요")
        sys.exit(1)
    
    print("🚀 Treasury Yield API 종합 테스트")
    print("=" * 60)
    print()
    
    # 1. 전체 조합 테스트 (DAG와 동일)
    test_treasury_yield_api(api_key)
    
    # 2. 추가 질문
    try:
        choice = input("상세 단일 테스트를 진행하시겠습니까? (y/n): ").lower()
        if choice in ['y', 'yes']:
            print()
            test_single_api_call(api_key)
    except KeyboardInterrupt:
        print("\n테스트 종료")

if __name__ == "__main__":
    main()