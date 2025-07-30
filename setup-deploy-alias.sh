#!/bin/bash

# 🚀 Deploy 별칭 자동 설정 스크립트

echo "🎯 Deploy 별칭 설정을 시작합니다..."

# 현재 디렉토리 저장
CURRENT_DIR=$(pwd)

# 별칭을 .bashrc에 추가
echo "" >> ~/.bashrc
echo "# 🚀 Investment API 자동 배포 별칭" >> ~/.bashrc
echo "alias deploy='echo \"🚀 자동 배포 시작...\"; git add . && git commit -m \"update \$(date +\"%Y-%m-%d %H:%M:%S\")\" && git push origin main && cd \"$CURRENT_DIR\" && ./auto-restart-pod.sh && echo \"✅ 배포 완료!\"'" >> ~/.bashrc

# 현재 셸에도 적용
alias deploy='echo "🚀 자동 배포 시작..."; git add . && git commit -m "update $(date +"%Y-%m-%d %H:%M:%S")" && git push origin main && cd "'$CURRENT_DIR'" && ./auto-restart-pod.sh && echo "✅ 배포 완료!"'

echo ""
echo "✅ Deploy 별칭이 설정되었습니다!"
echo ""
echo "🎯 사용법:"
echo "   1. 코드를 수정한 후"
echo "   2. 터미널에서 'deploy' 입력하고 엔터"
echo "   3. 끝! 🎉"
echo ""
echo "💡 이제 다음 명령어만 입력하면 됩니다:"
echo "   deploy"
echo ""
echo "📝 참고: 새 터미널에서는 'source ~/.bashrc' 실행 후 사용하세요."
echo ""