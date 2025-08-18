#!/bin/bash
# Truth Social 투자 관련 계정 분석

echo "🎯 Truth Social 투자 영향력 계정 분석"
echo "=================================================="

# 계정 정보 입력
read -p "Truth Social 사용자명: " TS_USERNAME
read -s -p "Truth Social 비밀번호: " TS_PASSWORD
echo ""

# # 분석할 계정들 (Truth Social에서 확인 가능한 계정들)
# declare -A ACCOUNTS=(
#     ["realDonaldTrump"]="도널드 트럼프 - 정책/경제 발언"
#     ["DonaldJTrumpJr"]="도널드 트럼프 주니어 - 정치/경제"
#     ["EricTrump"]="에릭 트럼프 - 트럼프 조직"
#     ["KashPatel"]="캐시 파텔 - 정치 분석가"
#     ["DevinNunes"]="데빈 누네스 - Truth Social CEO"
#     ["TulsiGabbard"]="털시 가바드 - 정치인"
#     ["vivekgramaswamy"]="비벡 라마스와미 - 기업가/정치인"
#     ["ElonMuskTruth"]="일론 머스크 (Truth Social 계정)"
#     ["RobertKennedyJr"]="로버트 케네디 주니어"
#     ["TuckerCarlson"]="터커 칼슨 - 언론인"
# )

# echo "🔍 계정별 존재 여부 및 활동 상태 확인"
# echo "───────────────────────────────────────────────"

# for account in "${!ACCOUNTS[@]}"; do
#     description="${ACCOUNTS[$account]}"
#     echo ""
#     echo "📱 @$account ($description)"
#     echo "   검색 중..."
    
#     # 계정 존재 확인
#     result=$(kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
#     bash -c "
#     export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
#     export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
#     timeout 15 truthbrush search --searchtype accounts '$account' 2>/dev/null | head -5
#     " 2>/dev/null)
    
#     if [[ -n "$result" && "$result" != *"error"* ]]; then
#         echo "   ✅ 계정 존재 확인"
        
#         # 최신 포스트 확인
#         post_result=$(kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
#         bash -c "
#         export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
#         export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
#         timeout 15 truthbrush statuses '$account' --count 2 2>/dev/null | head -10
#         " 2>/dev/null)
        
#         if [[ -n "$post_result" && "$post_result" != *"error"* ]]; then
#             echo "   📊 최근 활동: 있음"
            
#             # 포스트 수 및 날짜 추출 시도
#             post_count=$(echo "$post_result" | grep -o '"id"' | wc -l)
#             echo "   📈 수집된 포스트: ${post_count}개"
#         else
#             echo "   ⚠️ 포스트 수집 실패 또는 비공개"
#         fi
#     else
#         echo "   ❌ 계정 없음 또는 접근 불가"
#     fi
    
#     sleep 2  # Rate limiting 방지
# done

# echo ""
# echo "🔥 트렌딩 데이터 분석"
# echo "───────────────────────────────────────────────"

# echo "📊 트렌딩 해시태그:"
# kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
# bash -c "
# export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
# export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
# timeout 20 truthbrush tags | head -20
# " 2>/dev/null || echo "트렌딩 해시태그 수집 실패"

# echo ""
# echo "🔥 트렌딩 포스트 샘플:"
# kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
# bash -c "
# export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
# export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
# timeout 20 truthbrush trends | head -30
# " 2>/dev/null || echo "트렌딩 포스트 수집 실패"

echo ""
echo "💡 투자 관련 키워드 검색"
echo "───────────────────────────────────────────────"

INVESTMENT_KEYWORDS=("economy" "inflation" "fed" "stock" "market" "dollar" "bitcoin" "crypto")

for keyword in "${INVESTMENT_KEYWORDS[@]}"; do
    echo "🔍 '$keyword' 관련 포스트:"
    
    result=$(kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- \
    bash -c "
    export TRUTHSOCIAL_USERNAME='$TS_USERNAME'
    export TRUTHSOCIAL_PASSWORD='$TS_PASSWORD'
    timeout 15 truthbrush search --searchtype statuses '$keyword' | head -5
    " 2>/dev/null)
    
    if [[ -n "$result" && "$result" != *"error"* ]]; then
        post_count=$(echo "$result" | grep -o '"id"' | wc -l)
        echo "   📊 관련 포스트: ${post_count}개 발견"
    else
        echo "   ❌ 검색 실패 또는 결과 없음"
    fi
    
    sleep 1
done

# echo ""
# echo "📋 분석 완료!"
# echo "=================================================="
# echo ""
# echo "📊 다음 단계 권장사항:"
# echo "1. 위 결과에서 활성화된 계정들을 확인"
# echo "2. 투자 관련 키워드가 많은 계정들 우선 선택"
# echo "3. 트렌딩 데이터의 품질 평가"
# echo "4. 수집 전략 및 DAG 스케줄 결정"