#!/bin/bash
mkdir -p git-sync-info
cd git-sync-info

echo "📊 Current StatefulSet..."
kubectl get statefulset airflow-scheduler -n investment-assistant -o yaml > current-statefulset.yaml

echo "🔑 Current Secret..."
kubectl get secret airflow-secret -n investment-assistant -o yaml > current-secret.yaml

echo "📋 Current ConfigMap..."
kubectl get configmap airflow-config -n investment-assistant -o yaml > current-configmap.yaml

echo "🌐 Current Services..."
kubectl get svc -n investment-assistant -o yaml > current-services.yaml

echo "💾 Current PVCs..."
kubectl get pvc -n investment-assistant -o yaml > current-pvcs.yaml

echo "📁 DAG Structure..."
find /home/spark/investment-assistant-project/investment-service/ -name "*.py" -o -name "*.sql" > files-structure.txt

echo "✅ 정보 수집 완료! git-sync-info/ 폴더 확인"
ls -la