#!/bin/bash
# 수정된 Truth Social 데이터 구조 분석

echo "🔬 Truth Social 데이터 구조 분석 (수정됨)"
echo "=================================================="

# 계정 정보
read -p "Truth Social 사용자명: " TS_USERNAME
read -s -p "Truth Social 비밀번호: " TS_PASSWORD
echo ""

echo "📊 1. 트럼프 포스트 데이터 구조 분석"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'

echo '🔍 트럼프 포스트 원본 JSON (처음 몇 줄):'
echo '========================================'
truthbrush statuses realDonaldTrump | head -20

echo ''
echo '📋 JSON 구조 분석:'
echo '=================='
truthbrush statuses realDonaldTrump | python3 -c \"
import json, sys, re

try:
    # 첫 번째 줄만 읽어서 JSON 파싱 시도
    first_line = sys.stdin.readline().strip()
    if first_line:
        data = json.loads(first_line)
        
        print('=== 데이터 타입 ===')
        print(f'타입: {type(data).__name__}')
        
        if isinstance(data, dict):
            print('=== 포스트 필드 목록 ===')
            for key, value in data.items():
                if isinstance(value, dict):
                    print(f'📁 {key}: dict ({len(value)} keys)')
                elif isinstance(value, list):
                    print(f'📋 {key}: list ({len(value)} items)')
                else:
                    print(f'📄 {key}: {type(value).__name__} = {str(value)[:80]}')
            
            print('')
            print('=== 핵심 필드 상세 ===')
            print(f'ID: {data.get(\"id\", \"N/A\")}')
            print(f'내용 길이: {len(data.get(\"content\", \"\"))} 문자')
            print(f'작성일: {data.get(\"created_at\", \"N/A\")}')
            print(f'좋아요: {data.get(\"favourites_count\", 0)}')
            print(f'리블로그: {data.get(\"reblogs_count\", 0)}')
            print(f'댓글: {data.get(\"replies_count\", 0)}')
            
            # account 정보
            account = data.get('account', {})
            if account:
                print('')
                print('=== 계정 정보 ===')
                print(f'표시명: {account.get(\"display_name\", \"N/A\")}')
                print(f'사용자명: {account.get(\"acct\", \"N/A\")}')
                print(f'팔로워: {account.get(\"followers_count\", 0)}')
            
            # 내용 샘플 (HTML 태그 제거)
            content = data.get('content', '')
            if content:
                clean_content = re.sub(r'<[^>]+>', '', content)
                print('')
                print('=== 내용 샘플 ===')
                print(f'{clean_content[:200]}...' if len(clean_content) > 200 else clean_content)
                
        elif isinstance(data, list):
            print(f'배열 길이: {len(data)}')
            if len(data) > 0:
                print('첫 번째 항목 분석:')
                first_item = data[0]
                for key in first_item.keys() if isinstance(first_item, dict) else []:
                    print(f'  {key}: {type(first_item[key]).__name__}')
        else:
            print(f'예상치 못한 데이터 타입: {type(data)}')
            print(f'값: {str(data)[:200]}')
    else:
        print('데이터가 비어있습니다')
        
except json.JSONDecodeError as e:
    print(f'JSON 파싱 오류: {e}')
    print('원본 데이터가 JSON 형식이 아닐 수 있습니다')
except Exception as e:
    print(f'분석 오류: {e}')
\"
"

echo ""
echo "⏳ 5초 대기 (Rate limiting 방지)..."
sleep 5

echo ""
echo "📊 2. 백악관 포스트 데이터 구조 분석"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'

echo '🔍 백악관 포스트 원본 JSON (처음 몇 줄):'
echo '========================================'
truthbrush statuses WhiteHouse | head -10

echo ''
echo '📋 백악관 포스트 필드 분석:'
echo '=========================='
truthbrush statuses WhiteHouse | python3 -c \"
import json, sys

try:
    first_line = sys.stdin.readline().strip()
    if first_line:
        data = json.loads(first_line)
        
        if isinstance(data, dict):
            print('백악관 포스트 구조:')
            essential_fields = ['id', 'content', 'created_at', 'account', 'favourites_count', 'reblogs_count']
            
            for field in essential_fields:
                if field in data:
                    value = data[field]
                    if field == 'account':
                        display_name = value.get('display_name', 'N/A') if isinstance(value, dict) else 'N/A'
                        print(f'  ✅ {field}: {display_name}')
                    elif field == 'content':
                        import re
                        clean = re.sub(r'<[^>]+>', '', str(value))
                        print(f'  ✅ {field}: {len(clean)} 문자')
                    else:
                        print(f'  ✅ {field}: {value}')
                else:
                    print(f'  ❌ {field}: 없음')
        else:
            print(f'예상치 못한 데이터 타입: {type(data)}')
    else:
        print('백악관 데이터가 비어있습니다')
        
except Exception as e:
    print(f'백악관 데이터 분석 오류: {e}')
\"
"

echo ""
echo "⏳ 5초 대기..."
sleep 5

echo ""
echo "📊 3. 트렌딩 포스트 구조 분석"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'

echo '🔥 트렌딩 포스트 원본:'
echo '==================='
truthbrush trends | head -10

echo ''
echo '📊 트렌딩 포스트 분석:'
echo '==================='
truthbrush trends | python3 -c \"
import json, sys

try:
    first_line = sys.stdin.readline().strip()
    if first_line:
        data = json.loads(first_line)
        print('트렌딩 포스트 기본 정보:')
        print(f'데이터 타입: {type(data).__name__}')
        
        if isinstance(data, dict):
            print('주요 필드:')
            for key, value in data.items():
                print(f'  {key}: {type(value).__name__}')
        elif isinstance(data, list):
            print(f'트렌딩 포스트 개수: {len(data)}')
            if len(data) > 0:
                print('첫 번째 트렌딩 포스트 구조:')
                item = data[0]
                if isinstance(item, dict):
                    for k, v in item.items():
                        print(f'  {k}: {type(v).__name__}')
    else:
        print('트렌딩 데이터 없음')
        
except Exception as e:
    print(f'트렌딩 분석 오류: {e}')
\"
"

echo ""
echo "⏳ 5초 대기..."
sleep 5

echo ""
echo "🏷️ 4. 해시태그 구조 분석"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'

echo '🏷️ 해시태그 원본:'
echo '================'
truthbrush tags | head -10

echo ''
echo '📊 해시태그 분석:'
echo '================'
truthbrush tags | python3 -c \"
import json, sys

try:
    first_line = sys.stdin.readline().strip()
    if first_line:
        data = json.loads(first_line)
        
        if isinstance(data, list):
            print(f'해시태그 개수: {len(data)}')
            
            if len(data) > 0:
                tag = data[0]
                print('해시태그 구조:')
                for key, value in tag.items():
                    if key == 'history' and isinstance(value, list):
                        print(f'  {key}: list with {len(value)} history items')
                        if len(value) > 0:
                            hist = value[0]
                            print(f'    사용량: {hist.get(\"uses\", 0)}')
                            print(f'    계정수: {hist.get(\"accounts\", 0)}')
                    else:
                        print(f'  {key}: {value}')
                
                print('')
                print('상위 5개 해시태그:')
                for i, tag in enumerate(data[:5], 1):
                    tag_name = tag.get('name', 'Unknown')
                    history = tag.get('history', [])
                    uses = history[0].get('uses', 0) if history else 0
                    print(f'  {i}. #{tag_name} ({uses} uses)')
        else:
            print(f'예상치 못한 해시태그 데이터 타입: {type(data)}')
    else:
        print('해시태그 데이터 없음')
        
except Exception as e:
    print(f'해시태그 분석 오류: {e}')
\"
"