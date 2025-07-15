#!/bin/bash
# 즉시 실행: DAG 배포 + Secret 활용 스크립트

echo "🚀 DAG 배포 및 Secret 활용 - 원스텝 스크립트"
echo "=================================================="

echo ""
echo "1️⃣ 현재 Airflow DAG 폴더 상태 확인"
echo "────────────────────────────────────────────────"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  ls -la /opt/airflow/dags/

echo ""
echo "2️⃣ DAG 파일들 배포"
echo "────────────────────────────────────────────────"
echo "dags 폴더 복사 중..."
kubectl cp /home/spark/investment-assistant-project/investment-service/dags/ \
  investment-assistant/airflow-scheduler-0:/opt/airflow/

echo "sql 폴더 복사 중..."
kubectl cp /home/spark/investment-assistant-project/investment-service/dags/sql/ \
  investment-assistant/airflow-scheduler-0:/opt/airflow/sql/

echo "권한 설정 중..."
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  chown -R airflow:airflow /opt/airflow/dags/ /opt/airflow/sql/

echo ""
echo "3️⃣ 배포 결과 확인"
echo "────────────────────────────────────────────────"
echo "DAG 파일들:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  ls -la /opt/airflow/dags/

echo ""
echo "SQL 파일들:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  ls -la /opt/airflow/sql/

echo ""
echo "4️⃣ Secret 값들을 Airflow Variables로 설정"
echo "────────────────────────────────────────────────"

echo "NewsAPI 키 설정:"
NEWSAPI_KEY=$(kubectl get secret api-keys -n investment-assistant -o jsonpath='{.data.newsapi-key}' | base64 -d)
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set NEWSAPI_API_KEY "$NEWSAPI_KEY"

echo "Finnhub 키 설정:"
FINNHUB_KEY=$(kubectl get secret api-keys -n investment-assistant -o jsonpath='{.data.finnhub-key}' | base64 -d)
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set FINNHUB_API_KEY "$FINNHUB_KEY"

echo "Alpha Vantage 키 설정:"
ALPHAVANTAGE_KEY=$(kubectl get secret api-keys -n investment-assistant -o jsonpath='{.data.alphavantage-key}' | base64 -d)
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set ALPHA_VANTAGE_API_KEY "$ALPHAVANTAGE_KEY"

echo "FRED 키 설정:"
FRED_KEY=$(kubectl get secret api-keys -n investment-assistant -o jsonpath='{.data.fred-key}' | base64 -d)
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set FRED_API_KEY "$FRED_KEY"

echo "FMP 키 설정:"
FMP_KEY=$(kubectl get secret api-keys -n investment-assistant -o jsonpath='{.data.fmp-key}' | base64 -d)
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set FMP_API_KEY "$FMP_KEY"

echo "Balance Sheet Offset 초기화:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables set BALANCE_SHEET_OFFSET "0"

echo ""
echo "5️⃣ Variables 설정 확인"
echo "────────────────────────────────────────────────"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow variables list

echo ""
echo "6️⃣ PostgreSQL 연결 설정"
echo "────────────────────────────────────────────────"

echo "기존 연결 확인:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow connections list | grep postgres || echo "PostgreSQL 연결 없음"

echo ""
echo "PostgreSQL 연결 추가:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgresql.investment-assistant.svc.cluster.local \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema investment_db \
    --conn-port 5432

echo ""
echo "7️⃣ 연결 테스트"
echo "────────────────────────────────────────────────"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow connections test postgres_default

echo ""
echo "8️⃣ DAG 목록 확인"
echo "────────────────────────────────────────────────"
echo "Airflow에서 인식된 DAG들:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow dags list

echo ""
echo "9️⃣ 첫 번째 DAG 실행 (SP500 기초 데이터)"
echo "────────────────────────────────────────────────"
echo "SP500 테이블 및 데이터 생성 DAG 실행:"
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow dags trigger load_sp500_top50

echo ""
echo "DAG 실행 상태 확인:"
sleep 5
kubectl exec -it airflow-scheduler-0 -n investment-assistant -- \
  airflow dags state load_sp500_top50 $(date +%Y-%m-%d)

echo ""
echo "🔟 Airflow 웹 UI 접속 준비"
echo "────────────────────────────────────────────────"
echo "포트포워딩 시작 (백그라운드):"
kubectl port-forward svc/airflow-webserver 8080:8080 -n investment-assistant &

echo ""
echo "웹 UI 접속 정보:"
echo "URL: http://localhost:8080"
echo "ID: admin"
echo "PW: airflow123"

echo ""
echo "🎉 완료!"
echo "────────────────────────────────────────────────"
echo "✅ DAG 파일들 배포됨"
echo "✅ Secret에서 API 키들 Variables로 설정됨"
echo "✅ PostgreSQL 연결 설정됨"
echo "✅ 첫 번째 DAG 실행됨"
echo "✅ 웹 UI 접속 가능"

echo ""
echo "�� 다음 단계"
echo "────────────────────────────────────────────────"
echo "1. 웹 브라우저에서 http://localhost:8080 접속"
echo "2. admin/airflow123로 로그인"
echo "3. DAGs 탭에서 모든 DAG 목록 확인"
echo "4. load_sp500_top50 DAG 실행 결과 확인"
echo "5. 다른 DAG들 순차 실행"

echo ""
echo "🔍 문제 발생 시 확인 명령어"
echo "────────────────────────────────────────────────"
echo "DAG 파일 확인:"
echo "kubectl exec -it airflow-scheduler-0 -n investment-assistant -- ls -la /opt/airflow/dags/"
echo ""
echo "Variables 확인:"
echo "kubectl exec -it airflow-scheduler-0 -n investment-assistant -- airflow variables list"
echo ""
echo "Connection 확인:"
echo "kubectl exec -it airflow-scheduler-0 -n investment-assistant -- airflow connections list"
echo ""
echo "DAG 상태 확인:"
echo "kubectl exec -it airflow-scheduler-0 -n investment-assistant -- airflow dags list"
