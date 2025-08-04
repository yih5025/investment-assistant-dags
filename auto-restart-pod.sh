#!/bin/bash

# 🚀 Investment Service 자동 재시작 스크립트
# Git push 후 이 스크립트를 실행하면 자동으로 pod를 재시작합니다.
#
# 사용법:
#   ./auto-restart-pod.sh        # FastAPI 재시작 (기본값)
#   ./auto-restart-pod.sh api    # FastAPI 재시작
#   ./auto-restart-pod.sh frontend  # 프론트엔드 재시작

set -e

# 설정 (사용법: ./auto-restart-pod.sh [api|frontend])
NAMESPACE="investment-assistant"
SERVICE_TYPE=${1:-"api"}  # default: api

if [ "$SERVICE_TYPE" = "frontend" ]; then
    DEPLOYMENT_NAME="investment-frontend"
    API_ENDPOINT="http://localhost:30333"
    SERVICE_NAME="프론트엔드"
else
    DEPLOYMENT_NAME="investment-api"
    API_ENDPOINT="http://localhost:30888/api/v1/"
    SERVICE_NAME="FastAPI"
fi

CHECK_INTERVAL=5
MAX_WAIT_TIME=300

# 색깔 출력 함수
print_info() {
    echo -e "\033[34mℹ️  $1\033[0m"
}

print_success() {
    echo -e "\033[32m✅ $1\033[0m"
}

print_error() {
    echo -e "\033[31m❌ $1\033[0m"
}

print_warning() {
    echo -e "\033[33m⚠️  $1\033[0m"
}

# 현재 시간 출력
current_time() {
    date '+%Y-%m-%d %H:%M:%S'
}

print_info "🚀 Investment $SERVICE_NAME 자동 재시작 시작 [$(current_time)]"

# 1. 현재 Pod 상태 확인
print_info "📋 현재 Pod 상태 확인..."
kubectl get pods -n $NAMESPACE | grep $DEPLOYMENT_NAME || {
    print_error "Pod를 찾을 수 없습니다."
    exit 1
}

# 2. Git 최신 상태 확인 (선택사항)
if [ -d ".git" ]; then
    print_info "🔄 Git 최신 상태 확인..."
    git fetch origin main
    LOCAL_COMMIT=$(git rev-parse HEAD)
    REMOTE_COMMIT=$(git rev-parse origin/main)
    
    if [ "$LOCAL_COMMIT" = "$REMOTE_COMMIT" ]; then
        print_success "Git이 최신 상태입니다."
    else
        print_warning "로컬과 원격이 다릅니다. 최신 코드를 pull하세요."
        echo "  Local:  $LOCAL_COMMIT"
        echo "  Remote: $REMOTE_COMMIT"
    fi
fi

# 3. 현재 서비스 응답 확인
print_info "🌐 현재 $SERVICE_NAME 응답 확인..."
if [ "$SERVICE_TYPE" = "frontend" ]; then
    CURRENT_RESPONSE=$(curl -s -I $API_ENDPOINT | head -n1 2>/dev/null || echo "서비스 응답 없음")
else
    CURRENT_RESPONSE=$(curl -s $API_ENDPOINT | jq -r .message 2>/dev/null || echo "API 응답 없음")
fi
echo "  현재 응답: $CURRENT_RESPONSE"

# 4. Pod 재시작 실행
print_info "🔄 Pod 재시작 실행..."
kubectl rollout restart deployment/$DEPLOYMENT_NAME -n $NAMESPACE

# 5. 재시작 완료 대기
print_info "⏳ 재시작 완료 대기 중..."
kubectl rollout status deployment/$DEPLOYMENT_NAME -n $NAMESPACE --timeout=${MAX_WAIT_TIME}s

if [ $? -eq 0 ]; then
    print_success "Pod 재시작 완료!"
else
    print_error "Pod 재시작 실패 또는 시간 초과"
    exit 1
fi

# 6. 새로운 Pod 확인
print_info "📋 새로운 Pod 상태 확인..."
kubectl get pods -n $NAMESPACE | grep $DEPLOYMENT_NAME

# 7. 서비스 응답 재확인 (최대 60초 대기)
print_info "🔍 $SERVICE_NAME 응답 재확인 중..."
COUNTER=0
MAX_API_WAIT=12  # 12 * 5초 = 60초

while [ $COUNTER -lt $MAX_API_WAIT ]; do
    sleep $CHECK_INTERVAL
    
    if [ "$SERVICE_TYPE" = "frontend" ]; then
        NEW_RESPONSE=$(curl -s -I $API_ENDPOINT | head -n1 2>/dev/null || echo "")
    else
        NEW_RESPONSE=$(curl -s $API_ENDPOINT 2>/dev/null || echo "")
        if [ ! -z "$NEW_RESPONSE" ]; then
            NEW_RESPONSE=$(echo "$NEW_RESPONSE" | jq -r .message 2>/dev/null || echo "파싱 실패")
        fi
    fi
    
    if [ ! -z "$NEW_RESPONSE" ] && [ "$NEW_RESPONSE" != "파싱 실패" ]; then
        if [ "$NEW_RESPONSE" != "$CURRENT_RESPONSE" ]; then
            print_success "🎉 새로운 코드가 성공적으로 적용되었습니다!"
            echo "  이전: $CURRENT_RESPONSE"
            echo "  현재: $NEW_RESPONSE"
            break
        else
            print_info "아직 이전 코드가 실행 중입니다... (${COUNTER}/${MAX_API_WAIT})"
        fi
    else
        print_warning "$SERVICE_NAME 응답이 없습니다... (${COUNTER}/${MAX_API_WAIT})"
    fi
    
    COUNTER=$((COUNTER + 1))
done

if [ $COUNTER -eq $MAX_API_WAIT ]; then
    print_warning "$SERVICE_NAME 응답 확인 시간이 초과되었지만 Pod는 정상적으로 재시작되었습니다."
    print_info "수동으로 확인해주세요: curl $API_ENDPOINT"
fi

print_success "🎯 자동 재시작 프로세스 완료! [$(current_time)]"