#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime

# X API 설정
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAALeN0wEAAAAAeOv4ceQY%2B%2FziMshO7JeXxEnZG%2BM%3D2i0w1jJAHkU0OD1ry6WuNnLEC3TvgJZbH5Bg36vgGr5jRAoK8i'
ELON_MUSK_USER_ID = '44196397'  # @elonmusk의 USER_ID

def get_elon_recent_tweets():
    """일론 머스크의 최신 트윗 10개 가져오기"""
    
    print("🚀 일론 머스크 최신 트윗 10개 수집 시작...")
    
    # API 엔드포인트
    url = f"https://api.twitter.com/2/users/{ELON_MUSK_USER_ID}/tweets"
    
    # 요청 파라미터 (최대 100개까지 가능)
    params = {
        "max_results": 10,  # 10개 가져오기 (1~100 사이 조정 가능)
        
        # 투자 분석에 필요한 트윗 필드
        "tweet.fields": ",".join([
            "created_at",           # 작성 시간
            "text",                 # 트윗 내용  
            "public_metrics",       # 좋아요, 리트윗 수
            "context_annotations",  # 자동 태그 (Tesla, SpaceX 등)
            "entities",             # 해시태그, 멘션, URL
            "lang",                 # 언어
            "source"                # 트윗 출처
        ]),
        
        # 확장 정보
        "expansions": "author_id",
        
        # 사용자 정보
        "user.fields": ",".join([
            "name",
            "username", 
            "verified",
            "public_metrics"
        ])
    }
    
    # 헤더
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "User-Agent": "InvestmentAssistant-Test/1.0"
    }
    
    try:
        print("📡 X API 호출 중...")
        
        # API 요청
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        print(f"📊 응답 코드: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ API 요청 실패: {response.status_code}")
            print(f"에러 응답: {response.text}")
            return None
        
        # JSON 파싱
        data = response.json()
        
        # 응답 데이터 확인
        if 'data' not in data or not data['data']:
            print("⚠️ 최근 트윗이 없습니다")
            return None
        
        print("✅ 트윗 수집 성공!")
        return data
        
    except requests.exceptions.Timeout:
        print("❌ API 요청 타임아웃 (30초 초과)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 네트워크 오류: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파싱 실패: {e}")
        return None
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        return None

def analyze_tweet_data(data):
    """트윗 데이터 분석 및 출력"""
    
    if not data or 'data' not in data:
        print("❌ 분석할 데이터가 없습니다")
        return
    
    tweets = data['data']  # 모든 트윗들
    includes = data.get('includes', {})
    users = includes.get('users', [])
    
    print("\n" + "="*60)
    print(f"📱 일론 머스크 최신 트윗 {len(tweets)}개 분석")
    print("="*60)
    
    # 사용자 정보 (includes에서)
    if users:
        user = users[0]
        print(f"👤 작성자 정보:")
        print(f"   이름: {user.get('name', 'N/A')}")
        print(f"   사용자명: @{user.get('username', 'N/A')}")
        print(f"   인증: {'✅ 인증됨' if user.get('verified') else '❌ 미인증'}")
        
        # 팔로워 정보
        metrics = user.get('public_metrics', {})
        if metrics:
            print(f"   팔로워: {metrics.get('followers_count', 0):,}명")
            print(f"   팔로잉: {metrics.get('following_count', 0):,}명")
    
    # 각 트윗 분석
    investment_tweets = []
    total_engagement = 0
    
    for i, tweet in enumerate(tweets, 1):
        print(f"\n📝 트윗 #{i}")
        print("-" * 40)
        
        # 기본 정보
        print(f"🆔 ID: {tweet['id']}")
        print(f"📅 작성: {tweet['created_at']}")
        print(f"🌐 언어: {tweet.get('lang', 'N/A')}")
        
        # 트윗 내용 (길면 줄임)
        text = tweet['text']
        display_text = text if len(text) <= 100 else text[:100] + "..."
        print(f"💬 내용: {display_text}")
        
        # 참여도 지표
        metrics = tweet.get('public_metrics', {})
        if metrics:
            likes = metrics.get('like_count', 0)
            retweets = metrics.get('retweet_count', 0)
            replies = metrics.get('reply_count', 0)
            quotes = metrics.get('quote_count', 0)
            
            engagement = likes + retweets + replies + quotes
            total_engagement += engagement
            
            print(f"📊 참여도: ❤️{likes:,} 🔄{retweets:,} 💬{replies:,} 📝{quotes:,} (총 {engagement:,})")
        
        # 투자 관련성 분석
        text_lower = text.lower()
        investment_keywords = [
            'tesla', 'tsla', 'spacex', 'stock', 'market', 'crypto', 'bitcoin', 
            'economy', 'price', 'share', 'investor', 'ipo', 'earnings', 'doge'
        ]
        
        found_keywords = [kw for kw in investment_keywords if kw in text_lower]
        
        if found_keywords:
            print(f"💰 투자키워드: {', '.join(found_keywords)}")
            investment_tweets.append({
                'tweet_id': tweet['id'],
                'keywords': found_keywords,
                'engagement': engagement,
                'text': text
            })
    
    # 전체 통계
    print(f"\n" + "="*60)
    print(f"📊 전체 통계")
    print("="*60)
    print(f"총 트윗: {len(tweets)}개")
    print(f"투자 관련 트윗: {len(investment_tweets)}개 ({len(investment_tweets)/len(tweets)*100:.1f}%)")
    print(f"총 참여도: {total_engagement:,}")
    print(f"평균 참여도: {total_engagement//len(tweets):,}/트윗")
    
    # 투자 관련도 높은 트윗 TOP 3
    if investment_tweets:
        print(f"\n🎯 투자 관련 인기 트윗 TOP 3:")
        top_investment = sorted(investment_tweets, key=lambda x: x['engagement'], reverse=True)[:3]
        
        for i, tweet in enumerate(top_investment, 1):
            text_preview = tweet['text'][:80] + "..." if len(tweet['text']) > 80 else tweet['text']
            print(f"{i}. 참여도 {tweet['engagement']:,} | 키워드: {', '.join(tweet['keywords'])}")
            print(f"   {text_preview}")
    
    # 시장 영향 예상도
    high_impact_count = len([t for t in investment_tweets if t['engagement'] > 50000])
    
    print(f"\n🚨 시장 영향 예상:")
    if high_impact_count > 0:
        print(f"   높은 영향 트윗: {high_impact_count}개")
        print(f"   🚨 시장 모니터링 필요")
    elif len(investment_tweets) > 0:
        print(f"   중간 영향 트윗: {len(investment_tweets)}개")
        print(f"   ⚠️ 관심 필요")
    else:
        print(f"   ℹ️ 일반 트윗 - 낮은 영향")

def main():
    """메인 실행 함수"""
    
    print("🎯 일론 머스크 트윗 10개 수집 테스트")
    print("="*50)
    
    # 현재 시간
    print(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 트윗 수집
    data = get_elon_recent_tweets()
    
    if data:
        # 원본 JSON도 출력 (디버깅용)
        print(f"\n📄 원본 JSON 응답:")
        print("-" * 40)
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        # 상세 분석
        analyze_tweet_data(data)
        
        print(f"\n✅ 테스트 완료!")
    else:
        print(f"\n❌ 테스트 실패 - 트윗을 가져올 수 없습니다")

if __name__ == "__main__":
    main()