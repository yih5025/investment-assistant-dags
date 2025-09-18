from datetime import datetime, timedelta
import requests
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,  # 테스트용이므로 재시도 없음
}

def test_max_results_limits(**context):
    """다양한 max_results 값으로 테스트"""
    
    # 일론 머스크 user_id (하드코딩)
    username = "elonmusk"
    user_id = "44196397"
    
    # 테스트할 max_results 값들
    test_values = [50, 100]
    
    print(f"🧪 X API max_results 한도 테스트 시작")
    print(f"📋 대상: {username} (user_id: {user_id})")
    print(f"🔢 테스트 값들: {test_values}")
    print("=" * 60)
    
    # Token 1 사용
    try:
        bearer_token = Variable.get('X_API_BEARER_TOKEN_1')
        print(f"🔑 사용 토큰: X_API_BEARER_TOKEN_1")
    except Exception as e:
        print(f"❌ 토큰 조회 실패: {e}")
        return
    
    results = {}
    
    for max_result in test_values:
        print(f"\n🔍 max_results={max_result} 테스트 중...")
        
        try:
            url = f"https://api.twitter.com/2/users/{user_id}/tweets"
            
            params = {
                "max_results": max_result,
                "tweet.fields": "created_at,text,public_metrics,context_annotations,entities,lang",
                "expansions": "author_id",
                "user.fields": "name,username,verified,public_metrics"
            }
            
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": "InvestmentAssistant-Test/1.0"
            }
            
            print(f"   📡 API 호출: max_results={max_result}")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            # 응답 상태 확인
            print(f"   📊 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # 실제 반환된 트윗 수 확인
                actual_count = len(data.get('data', []))
                print(f"   ✅ 성공: 요청 {max_result}개 → 실제 {actual_count}개 수신")
                
                # 첫 번째와 마지막 트윗 정보
                if actual_count > 0:
                    tweets = data['data']
                    first_tweet = tweets[0]
                    last_tweet = tweets[-1] if len(tweets) > 1 else tweets[0]
                    
                    print(f"   📝 첫 번째 트윗: {first_tweet['created_at']} - {first_tweet['text'][:50]}...")
                    if len(tweets) > 1:
                        print(f"   📝 마지막 트윗: {last_tweet['created_at']} - {last_tweet['text'][:50]}...")
                    
                    # 사용자 정보
                    if 'includes' in data and 'users' in data['includes']:
                        user_info = data['includes']['users'][0]
                        print(f"   👤 사용자: {user_info['name']} (@{user_info['username']})")
                        print(f"   📈 팔로워: {user_info['public_metrics']['followers_count']:,}명")
                
                results[max_result] = {
                    'status': 'success',
                    'requested': max_result,
                    'actual': actual_count,
                    'response_size': len(response.text)
                }
                
            elif response.status_code == 429:
                print(f"   ⚠️  Rate Limit 도달: {response.status_code}")
                print(f"   📄 응답: {response.text[:200]}...")
                
                results[max_result] = {
                    'status': 'rate_limited',
                    'error': response.text
                }
                
                # Rate Limit 도달 시 테스트 중단
                print(f"   🛑 Rate Limit으로 인해 테스트 중단")
                break
                
            else:
                print(f"   ❌ 실패: {response.status_code}")
                print(f"   📄 응답: {response.text[:200]}...")
                
                results[max_result] = {
                    'status': 'error',
                    'status_code': response.status_code,
                    'error': response.text[:200]
                }
            
            # 각 테스트 간 1초 대기 (과도한 호출 방지)
            if max_result != test_values[-1]:  # 마지막이 아니면
                print(f"   ⏳ 1초 대기...")
                import time
                time.sleep(1)
                
        except Exception as e:
            print(f"   ❌ 예외 발생: {str(e)}")
            results[max_result] = {
                'status': 'exception',
                'error': str(e)
            }
    
    # 최종 결과 요약
    print(f"\n" + "=" * 60)
    print(f"📊 테스트 결과 요약:")
    print(f"=" * 60)
    
    for max_result, result in results.items():
        status = result['status']
        if status == 'success':
            print(f"✅ max_results={max_result}: 성공 ({result['actual']}개 수신)")
        elif status == 'rate_limited':
            print(f"⚠️  max_results={max_result}: Rate Limit 도달")
        elif status == 'error':
            print(f"❌ max_results={max_result}: 에러 ({result['status_code']})")
        else:
            print(f"❌ max_results={max_result}: 예외 발생")
    
    # 권장사항 제시
    successful_values = [k for k, v in results.items() if v['status'] == 'success']
    if successful_values:
        max_successful = max(successful_values)
        print(f"\n💡 권장사항:")
        print(f"   🎯 안전한 최대값: max_results={max_successful}")
        print(f"   📈 실제 수집 가능: 최대 {results[max_successful]['actual']}개 트윗")
    else:
        print(f"\n⚠️  모든 테스트 실패 - API 토큰이나 설정 확인 필요")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='test_results', value=results)
    
    return results

def analyze_test_results(**context):
    """테스트 결과 분석"""
    
    results = context['ti'].xcom_pull(key='test_results') or {}
    
    if not results:
        print("📭 테스트 결과가 없습니다")
        return
    
    print(f"\n🔬 상세 분석:")
    print(f"=" * 60)
    
    # API 호출 통계
    total_calls = len(results)
    successful_calls = len([r for r in results.values() if r['status'] == 'success'])
    
    print(f"📊 API 호출 통계:")
    print(f"   전체 호출: {total_calls}회")
    print(f"   성공 호출: {successful_calls}회")
    print(f"   성공률: {(successful_calls/total_calls*100):.1f}%")
    
    # Rate Limit 분석
    rate_limited = any(r['status'] == 'rate_limited' for r in results.values())
    if rate_limited:
        print(f"\n⚠️  Rate Limit 분석:")
        print(f"   - Free Tier 제한이 예상보다 엄격할 수 있음")
        print(f"   - 연속 호출 시 제한 발생")
        print(f"   - 실제 운영에서는 더 긴 딜레이 필요")
    
    # 최적값 권장
    successful_results = {k: v for k, v in results.items() if v['status'] == 'success'}
    if successful_results:
        optimal_value = max(successful_results.keys())
        print(f"\n🎯 운영 권장사항:")
        print(f"   최적 max_results: {optimal_value}")
        print(f"   예상 수집량: {successful_results[optimal_value]['actual']}개/회")
        print(f"   월간 예상량: 약 {successful_results[optimal_value]['actual'] * 30}개")

# DAG 정의
with DAG(
    dag_id='test_x_api_max_results_limits',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    description='X API max_results 한도 테스트 (Token 1 사용)',
    tags=['test', 'x_api', 'max_results', 'token_1'],
) as dag:
    
    # max_results 한도 테스트
    test_limits = PythonOperator(
        task_id='test_max_results_limits',
        python_callable=test_max_results_limits,
    )
    
    # 결과 분석
    analyze_results = PythonOperator(
        task_id='analyze_test_results',
        python_callable=analyze_test_results,
    )
    
    # Task 의존성
    test_limits >> analyze_results