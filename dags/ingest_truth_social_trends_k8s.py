from datetime import datetime, timedelta
import subprocess
import json
import re
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_truth_social_trends.sql"), encoding="utf-8") as f:
    UPSERT_TRENDS_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def run_truthbrush_command(command_args):
    """Truthbrush 명령어 실행"""
    try:
        cmd = ['truthbrush'] + command_args
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            return result.stdout
        else:
            raise Exception(f"Truthbrush 실행 실패: {result.stderr}")
    except subprocess.TimeoutExpired:
        raise Exception("Truthbrush 명령어 타임아웃")

def clean_html_content(content):
    """HTML 태그 제거"""
    if not content:
        return ""
    return re.sub(r'<[^>]+>', '', content).strip()

def calculate_trend_score(post_data):
    """트렌딩 점수 계산"""
    replies = post_data.get('replies_count', 0)
    reblogs = post_data.get('reblogs_count', 0)
    favourites = post_data.get('favourites_count', 0)
    
    # 가중치 적용한 트렌딩 점수
    score = (favourites * 1.0) + (reblogs * 2.0) + (replies * 1.5)
    return round(score, 2)

def parse_trend_data(raw_post, rank):
    """트렌딩 포스트 데이터 파싱"""
    account = raw_post.get('account', {})
    content = raw_post.get('content', '')
    
    created_at = raw_post.get('created_at', '').replace('Z', '+00:00') if raw_post.get('created_at') else None
    
    return {
        'id': raw_post.get('id'),
        'created_at': created_at,
        'username': account.get('username', ''),
        'account_id': account.get('id'),
        'display_name': account.get('display_name'),
        'content': content,
        'clean_content': clean_html_content(content),
        'language': raw_post.get('language'),
        'replies_count': raw_post.get('replies_count', 0),
        'reblogs_count': raw_post.get('reblogs_count', 0),
        'favourites_count': raw_post.get('favourites_count', 0),
        'upvotes_count': raw_post.get('upvotes_count', 0),
        'downvotes_count': raw_post.get('downvotes_count', 0),
        'url': raw_post.get('url'),
        'uri': raw_post.get('uri'),
        'tags': json.dumps(raw_post.get('tags', [])),
        'mentions': json.dumps(raw_post.get('mentions', [])),
        'visibility': raw_post.get('visibility', 'public'),
        'sensitive': raw_post.get('sensitive', False),
        'in_reply_to_id': raw_post.get('in_reply_to_id'),
        'trend_rank': rank,
        'trend_score': calculate_trend_score(raw_post)
    }

def fetch_trending_posts(**context):
    """트렌딩 포스트 수집"""
    print("🔥 트렌딩 포스트 수집 중...")
    
    try:
        output = run_truthbrush_command(['trends'])
        
        trends = []
        lines = output.strip().split('\n')
        
        # 첫 번째 줄이 JSON 배열인지 확인
        if lines and lines[0].strip().startswith('['):
            try:
                trend_list = json.loads(lines[0])
                
                for rank, post_data in enumerate(trend_list[:20], 1):  # 상위 20개만
                    processed_trend = parse_trend_data(post_data, rank)
                    trends.append(processed_trend)
                    
            except json.JSONDecodeError:
                print("⚠️ 트렌딩 데이터 JSON 파싱 실패")
        else:
            # 라인별로 JSON 처리
            for rank, line in enumerate(lines[:20], 1):
                line = line.strip()
                if line and line.startswith('{'):
                    try:
                        post_data = json.loads(line)
                        processed_trend = parse_trend_data(post_data, rank)
                        trends.append(processed_trend)
                    except json.JSONDecodeError:
                        continue
        
        print(f"✅ 트렌딩 포스트 {len(trends)}개 수집")
        context['ti'].xcom_push(key='trending_posts', value=trends)
        return len(trends)
        
    except Exception as e:
        print(f"❌ 트렌딩 포스트 수집 실패: {e}")
        context['ti'].xcom_push(key='trending_posts', value=[])
        return 0

def store_trends_to_db(**context):
    """트렌딩 포스트를 DB에 저장"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    trends = context['ti'].xcom_pull(key='trending_posts') or []
    success_count = 0
    error_count = 0
    
    for trend in trends:
        try:
            hook.run(UPSERT_TRENDS_SQL, parameters=trend)
            success_count += 1
        except Exception as e:
            print(f"❌ 트렌딩 포스트 저장 실패: {trend.get('id', 'Unknown')} - {e}")
            error_count += 1
    
    print(f"✅ 트렌딩 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 통계 조회
    result = hook.get_first("SELECT COUNT(*) FROM truth_social_trends WHERE collected_at >= NOW() - INTERVAL '1 day'")
    total_trends = result[0] if result else 0
    print(f"📊 최근 24시간 트렌딩 포스트: {total_trends}개")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_truth_social_trends_k8s',
    default_args=default_args,
    schedule_interval='0 */12 * * *',  # 12시간마다
    catchup=False,
    description='Truth Social 트렌딩 포스트 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['truth_social', 'trends', 'market_sentiment', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_truth_social_trends_table',
        postgres_conn_id='postgres_default',
        sql='create_truth_social_trends.sql',
    )
    
    # 트렌딩 포스트 수집
    fetch_trends = PythonOperator(
        task_id='fetch_trending_posts',
        python_callable=fetch_trending_posts,
    )
    
    # DB 저장
    store_trends = PythonOperator(
        task_id='store_trends_to_db',
        python_callable=store_trends_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_trends >> store_trends