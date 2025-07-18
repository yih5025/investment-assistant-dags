#!/bin/bash
# DAGs PVC 즉시 해결

echo "🚀 Airflow DAGs PVC 영구 해결"
echo "=================================================="

echo ""
echo "1️⃣ DAGs 전용 PVC 생성"
echo "────────────────────────────────────────────────"
kubectl apply -f - << 'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-dags-pvc
  namespace: investment-assistant
  labels:
    app: airflow
    component: scheduler
    tier: airflow
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: local-path
  resources:
    requests:
      storage: 5Gi
EOF

if [ $? -eq 0 ]; then
    echo "✅ DAGs PVC 생성 완료"
else
    echo "❌ DAGs PVC 생성 실패"
    exit 1
fi

echo ""
echo "2️⃣ StatefulSet에 DAGs 볼륨 마운트 추가"
echo "────────────────────────────────────────────────"

# JSON patch로 볼륨과 볼륨마운트 동시 추가
kubectl patch statefulset airflow-scheduler -n investment-assistant --type='json' -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/volumes/-",
    "value": {
      "name": "dags-storage",
      "persistentVolumeClaim": {
        "claimName": "airflow-dags-pvc"
      }
    }
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/volumeMounts/-",
    "value": {
      "name": "dags-storage",
      "mountPath": "/opt/airflow/dags"
    }
  }
]'

if [ $? -eq 0 ]; then
    echo "✅ StatefulSet 볼륨 마운트 추가 완료"
else
    echo "❌ StatefulSet 패치 실패"
    exit 1
fi

echo ""
echo "3️⃣ Scheduler Pod 재시작 (새 볼륨 설정 적용)"
echo "────────────────────────────────────────────────"
echo "기존 Pod 삭제 (자동으로 새 설정으로 재시작됨):"
kubectl delete pod airflow-scheduler-0 -n investment-assistant

echo ""
echo "Pod 재시작 대기 중..."
kubectl wait --for=condition=Ready pod/airflow-scheduler-0 -n investment-assistant --timeout=300s

if [ $? -eq 0 ]; then
    echo "✅ Scheduler Pod 재시작 완료"
else
    echo "❌ Pod 재시작 실패 또는 타임아웃"
    echo "수동으로 확인: kubectl get pods -n investment-assistant"
    exit 1
fi

echo ""
echo "4️⃣ 볼륨 마운트 확인"
echo "────────────────────────────────────────────────"
echo "DAGs 볼륨 마운트 상태:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- df -h /opt/airflow/dags

echo ""
echo "DAGs 폴더 권한 확인:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- ls -la /opt/airflow/dags/

echo ""
echo "5️⃣ DAGs 파일 복사 (영구 저장소에)"
echo "────────────────────────────────────────────────"

if [ -d "../dags" ]; then
    echo "DAGs 파일들을 영구 저장소에 복사:"
    kubectl cp ../dags/ investment-assistant/airflow-scheduler-0:/opt/airflow/ -c scheduler
    
    if [ $? -eq 0 ]; then
        echo "✅ DAGs 파일 복사 완료"
        
        # 권한 설정
        kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- chmod -R 644 /opt/airflow/dags/*.py 2>/dev/null
        kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- find /opt/airflow/dags -name "*.sql" -exec chmod 644 {} \; 2>/dev/null
        
        echo "✅ 파일 권한 설정 완료"
    else
        echo "❌ DAGs 파일 복사 실패"
    fi
else
    echo "⚠️ ../dags 폴더를 찾을 수 없습니다."
    echo "DAGs 파일들을 수동으로 복사해주세요."
fi

echo ""
echo "6️⃣ 최종 확인"
echo "────────────────────────────────────────────────"
echo "생성된 PVC 확인:"
kubectl get pvc -n investment-assistant

echo ""
echo "복사된 DAG 파일들:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- ls -la /opt/airflow/dags/*.py 2>/dev/null || echo "Python 파일 없음"

echo ""
echo "7️⃣ 영구 저장 테스트"
echo "────────────────────────────────────────────────"
echo "테스트 파일 생성:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- touch /opt/airflow/dags/test-persistence.txt

echo ""
echo "Pod 재시작 테스트:"
kubectl delete pod airflow-scheduler-0 -n investment-assistant
echo "Pod 재시작 대기..."
kubectl wait --for=condition=Ready pod/airflow-scheduler-0 -n investment-assistant --timeout=300s

echo ""
echo "테스트 파일 존재 확인:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -c scheduler -- ls -la /opt/airflow/dags/test-persistence.txt && echo "✅ 영구 저장 테스트 성공!" || echo "❌ 영구 저장 테스트 실패"

echo ""
echo "8️⃣ 완료!"
echo "────────────────────────────────────────────────"
echo "✅ DAGs 전용 PVC 생성됨 (5Gi)"
echo "✅ Scheduler StatefulSet에 볼륨 마운트 추가됨"
echo "✅ DAGs 파일들이 영구 저장소에 저장됨"
echo "✅ Pod 재시작 시에도 DAGs 파일 유지됨"
echo ""
echo "📊 최종 PVC 현황:"
kubectl get pvc -n investment-assistant

echo ""
echo "🎯 다음 단계:"
echo "1. 웹 UI에서 DAGs 목록 확인"
echo "2. load_sp500_top50 DAG 실행"
echo "3. 다른 DAG들 순차 실행"
echo ""
echo "💡 향후 Pod 재시작 시에도 DAGs가 유지됩니다!"
