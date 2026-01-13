"""
Airflow 로그 자동 정리 DAG
7일 이상 된 로그 파일을 매일 자동으로 삭제합니다.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cleanup_old_logs',
    default_args=default_args,
    description='오래된 Airflow 로그 자동 정리 (7일 이상)',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시 실행
    catchup=False,
    tags=['maintenance', 'cleanup'],
)

# 7일 이상 된 로그 파일 삭제
cleanup_logs = BashOperator(
    task_id='cleanup_old_logs',
    bash_command='''
    echo "=== 오래된 로그 정리 시작 ==="
    
    # 로그 디렉토리 경로
    LOG_DIR="${AIRFLOW_HOME}/logs"
    
    # 7일 이상 된 로그 파일 삭제
    if [ -d "$LOG_DIR" ]; then
        echo "로그 디렉토리: $LOG_DIR"
        
        # 삭제 전 개수 확인
        OLD_FILES=$(find "$LOG_DIR" -type f -mtime +7 2>/dev/null | wc -l)
        echo "삭제 대상 파일: $OLD_FILES 개"
        
        # 7일 이상 된 파일 삭제
        find "$LOG_DIR" -type f -mtime +7 -delete 2>/dev/null
        
        # 빈 디렉토리 정리
        find "$LOG_DIR" -type d -empty -delete 2>/dev/null
        
        # 정리 후 디스크 용량
        echo "=== 디스크 사용량 ==="
        df -h "$LOG_DIR" | tail -n 1
        
        echo "✅ 로그 정리 완료"
    else
        echo "⚠️ 로그 디렉토리를 찾을 수 없습니다"
    fi
    ''',
    dag=dag,
)

# Task만 정의 (의존성 없음)
cleanup_logs
