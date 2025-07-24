#!/bin/bash
# 수정된 Truthbrush 테스트 스크립트

echo "🔍 수정된 Truthbrush 테스트 (Cloudflare 회피 + 올바른 명령어)"
echo "=================================================="

# Truth Social 계정 정보
read -p "Truth Social 사용자명: " TS_USERNAME
read -s -p "Truth Social 비밀번호: " TS_PASSWORD
echo ""

echo "🧪 1. 기본 연결 테스트 (트렌딩 해시태그)"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
echo '📋 트렌딩 해시태그 수집 중...'
truthbrush tags | head -10
"

echo "⏳ 30초 대기 (Rate limiting 회피)..."
sleep 5

echo ""
echo "🧪 2. 트럼프 포스트 테스트 (올바른 옵션)"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
echo '📱 트럼프 포스트 수집 중 (옵션 없이)...'
truthbrush statuses realDonaldTrump | head -20
"

echo "⏳ 30초 대기..."
sleep 5

echo ""
echo "🧪 3. 트렌딩 포스트 테스트 (올바른 옵션)"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
echo '🔥 트렌딩 포스트 수집 중 (옵션 없이)...'
truthbrush trends | head -20
"

echo "⏳ 30초 대기..."
sleep 5

echo ""
echo "🧪 4. 검색 테스트 (단일 키워드)"
echo "───────────────────────────────────────────────"

kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
bash -c "
export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
echo '🔍 economy 검색 중...'
truthbrush search --searchtype statuses economy | head -10
"

echo ""
echo "📊 테스트 완료!"