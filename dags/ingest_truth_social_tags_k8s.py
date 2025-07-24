from datetime import datetime, timedelta, date
import subprocess
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_truth_social_tags.sql"), encoding="utf-8") as f:
    UPSERT_TAGS_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def run_truthbrush_command(command_args):
    """Truthbrush 명령어 실행"""
    username = Variable.get('TRUTHSOCIAL_USERNAME')
    password = Variable.get('TRUTHSOCIAL_PASSWORD')
    try:
        cmd = ['truthbrush', '--username', username, '--password', password] + command_args
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            return result.stdout
        else:
            raise Exception(f"Truthbrush 실행 실패: {result.stderr}")
    except subprocess.TimeoutExpired:
        raise Exception("Truthbrush 명령어 타임아웃")

def parse_tag_data(raw_tag):
    """해시태그 데이터 간단 파싱"""
    history = raw_tag.get('history', [])
    
    # 필수 데이터 검증
    if not raw_tag.get('name') or not history:
        raise ValueError("필수 데이터 누락")
    
    # 7일간 사용량 추출
    day_uses = []
    for i in range(7):
        if i < len(history):
            day_uses.append(int(history[i].get('uses', 0)))
        else:
            day_uses.append(0)
    
    return {
        'name': raw_tag['name'],
        'collected_date': date.today(),
        'url': raw_tag.get('url'),
        'total_uses': day_uses[0],
        'total_accounts': int(history[0].get('accounts', 0)) if history else 0,
        'recent_statuses_count': raw_tag.get('recent_statuses_count', 0),
        'history_data': json.dumps(history),
        'day_0_uses': day_uses[0],
        'day_1_uses': day_uses[1],
        'day_2_uses': day_uses[2],
        'day_3_uses': day_uses[3],
        'day_4_uses': day_uses[4],
        'day_5_uses': day_uses[5],
        'day_6_uses': day_uses[6],
        'trend_score': day_uses[0],  # 단순히 오늘 사용량
        'growth_rate': 0,  # 계산 생략
        'weekly_average': sum(day_uses) / 7,
        'tag_category': 'general',  # 기본값
        'market_relevance': 0  # 기본값
    }

def fetch_trending_tags(**context):
    """트렌딩 해시태그 수집"""
    print("🏷️ 트렌딩 해시태그 수집 중...")
    
    output = run_truthbrush_command(['tags'])
    
    if not output.strip():
        raise Exception("빈 응답 받음")
    
    # JSON 파싱
    first_line = output.strip().split('\n')[0]
    tag_list = json.loads(first_line)
    
    if not isinstance(tag_list, list) or len(tag_list) == 0:
        raise Exception("유효한 해시태그 데이터 없음")
    
    # 데이터 파싱
    tags = []
    for tag_data in tag_list:
        try:
            processed_tag = parse_tag_data(tag_data)
            tags.append(processed_tag)
        except Exception as e:
            print(f"⚠️ 태그 파싱 실패: {tag_data.get('name', 'Unknown')} - {e}")
            continue
    
    if len(tags) == 0:
        raise Exception("파싱된 해시태그 없음")
    
    print(f"✅ 트렌딩 해시태그 {len(tags)}개 수집")
    context['ti'].xcom_push(key='trending_tags', value=tags)
    return len(tags)

def store_tags_to_db(**context):
    """트렌딩 해시태그를 DB에 저장"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    tags = context['ti'].xcom_pull(key='trending_tags')
    
    if not tags:
        raise Exception("저장할 해시태그 데이터 없음")
    
    success_count = 0
    for tag in tags:
        try:
            hook.run(UPSERT_TAGS_SQL, parameters=tag)
            success_count += 1
        except Exception as e:
            print(f"❌ 해시태그 저장 실패: {tag.get('name', 'Unknown')} - {e}")
            raise  # 하나라도 실패하면 전체 실패
    
    print(f"✅ 해시태그 저장 완료: {success_count}개")
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_truth_social_tags_k8s',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # 매일 자정
    catchup=False,
    description='Truth Social 트렌딩 해시태그 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['truth_social', 'hashtags', 'market_trends', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_truth_social_tags_table',
        postgres_conn_id='postgres_default',
        sql='create_truth_social_tags.sql',
    )
    
    # 트렌딩 해시태그 수집
    fetch_tags = PythonOperator(
        task_id='fetch_trending_tags',
        python_callable=fetch_trending_tags,
    )
    
    # DB 저장
    store_tags = PythonOperator(
        task_id='store_tags_to_db',
        python_callable=store_tags_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_tags >> store_tags