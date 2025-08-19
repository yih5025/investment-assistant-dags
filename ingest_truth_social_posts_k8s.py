from datetime import datetime, timedelta
import subprocess
import json
import re
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ✅ 모니터링 유틸리티 import
from monitoring_utils import create_monitor

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_truth_social_posts.sql"), encoding="utf-8") as f:
    UPSERT_POSTS_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def run_truthbrush_command(command_args):
    """Truthbrush 명령어 실행"""
    truth_social_username = Variable.get('TRUTHSOCIAL_USERNAME')
    truth_social_password = Variable.get('TRUTHSOCIAL_PASSWORD')
    
    # 기존 환경변수 복사하고 인증 정보 추가
    env = os.environ.copy()
    env['TRUTHSOCIAL_USERNAME'] = truth_social_username
    env['TRUTHSOCIAL_PASSWORD'] = truth_social_password
    
    try:
        cmd = ['truthbrush'] + command_args
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=120,
            env=env
        )
        
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

def classify_account(username):
    """계정 분류 및 시장 영향력 결정"""
    classification = {
        'realDonaldTrump': ('individual', 10),
        'WhiteHouse': ('government', 9),
        'DonaldJTrumpJr': ('individual', 7)
    }
    return classification.get(username, ('individual', 0))

def parse_post_data(raw_post, username):
    """JSON 데이터를 DB 스키마에 맞게 변환"""
    account = raw_post.get('account', {})
    content = raw_post.get('content', '')
    
    # 기본 정보
    created_at = raw_post.get('created_at', '').replace('Z', '+00:00') if raw_post.get('created_at') else None
    account_type, market_influence = classify_account(username)
    
    # 미디어 정보
    media_attachments = raw_post.get('media_attachments', [])
    tags = raw_post.get('tags', [])
    mentions = raw_post.get('mentions', [])
    
    # 카드 정보
    card = raw_post.get('card') or {}
    
    return {
        'id': raw_post.get('id'),
        'created_at': created_at,
        'username': username,
        'account_id': account.get('id'),
        'display_name': account.get('display_name'),
        'verified': account.get('verified', False),
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
        'has_media': len(media_attachments) > 0,
        'media_count': len(media_attachments),
        'media_attachments': json.dumps(media_attachments) if media_attachments else None,
        'tags': json.dumps(tags),
        'mentions': json.dumps(mentions),
        'has_tags': len(tags) > 0,
        'has_mentions': len(mentions) > 0,
        'card_url': card.get('url'),
        'card_title': card.get('title'),
        'card_description': card.get('description'),
        'card_image': card.get('image'),
        'visibility': raw_post.get('visibility', 'public'),
        'sensitive': raw_post.get('sensitive', False),
        'spoiler_text': raw_post.get('spoiler_text'),
        'in_reply_to_id': raw_post.get('in_reply_to_id'),
        'quote_id': raw_post.get('quote_id'),
        'account_type': account_type,
        'market_influence': market_influence
    }

def fetch_posts_for_account(username, **context):
    """✅ 모니터링이 적용된 특정 계정의 포스트 수집"""
    
    # 1. 모니터 생성
    monitor = create_monitor(context)
    
    try:
        print(f"🔍 {username} 포스트 수집 중...")
        
        # 1시간 전 이후 포스트만 수집
        one_hours_ago = (datetime.now() - timedelta(hours=1)).isoformat()
        
        # 2. API 호출을 모니터링과 함께
        with monitor.monitor_api_call():
            output = run_truthbrush_command([
                'statuses', username, 
                '--created-after', one_hours_ago,
                '--no-replies'
            ])
        
        # 3. 데이터 처리
        posts = []
        invalid_posts = 0
        latest_timestamp = None
        
        for line in output.strip().split('\n'):
            line = line.strip()
            if line and line.startswith('{'):
                try:
                    post_data = json.loads(line)
                    processed_post = parse_post_data(post_data, username)
                    posts.append(processed_post)
                    
                    # 최신 타임스탬프 추적
                    if processed_post.get('created_at'):
                        if not latest_timestamp or processed_post['created_at'] > latest_timestamp:
                            latest_timestamp = processed_post['created_at']
                            
                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON 파싱 실패: {line[:50]}... - {e}")
                    invalid_posts += 1
                    continue
        
        # 4. 처리 결과 모니터링에 기록
        total_lines = len([line for line in output.strip().split('\n') if line.strip()])
        monitor.log_data_processing(
            fetched=total_lines,
            processed=len(posts),
            invalid=invalid_posts
        )
        
        if len(posts) == 0:
            monitor.set_warning(f"{username}: 최근 1시간 내 새 포스트 없음")
        
        print(f"✅ {username}: {len(posts)}개 포스트 수집 완료")
        context['ti'].xcom_push(key=f'{username}_posts', value=posts)
        
        # 5. 모니터링 완료
        monitor.finalize_and_save(latest_timestamp)
        return len(posts)
        
    except Exception as e:
        # 6. 실패 시 에러 기록
        monitor.set_error(f"{username} 수집 실패: {str(e)}")
        monitor.finalize_and_save()
        
        print(f"❌ {username} 수집 실패: {e}")
        context['ti'].xcom_push(key=f'{username}_posts', value=[])
        raise

def store_posts_to_db(**context):
    """✅ 모니터링이 적용된 포스트 DB 저장"""
    
    # 1. 모니터 생성
    monitor = create_monitor(context)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        accounts = ['realDonaldTrump', 'WhiteHouse', 'DonaldJTrumpJr']
        total_posts = 0
        total_success = 0
        total_error = 0
        latest_timestamp = None
        
        # 2. 각 계정의 포스트 처리
        for username in accounts:
            posts = context['ti'].xcom_pull(key=f'{username}_posts') or []
            total_posts += len(posts)
            
            for post in posts:
                try:
                    hook.run(UPSERT_POSTS_SQL, parameters=post)
                    total_success += 1
                    
                    # 최신 타임스탬프 추적
                    if post.get('created_at'):
                        if not latest_timestamp or post['created_at'] > latest_timestamp:
                            latest_timestamp = post['created_at']
                            
                except Exception as e:
                    print(f"❌ {username} 포스트 저장 실패: {post.get('id', 'Unknown')} - {e}")
                    total_error += 1
        
        # 3. 저장 결과 모니터링에 기록
        monitor.log_data_processing(
            processed=total_posts,
            inserted=total_success,
            skipped=total_error
        )
        
        if total_error > 0:
            monitor.set_warning(f"{total_error}개 포스트 저장 실패")
        
        if total_success == 0 and total_posts > 0:
            monitor.set_error("모든 포스트 저장 실패")
        elif total_success == 0:
            monitor.set_warning("저장할 포스트 없음")
        
        print(f"✅ 저장 완료: {total_success}개 성공, {total_error}개 실패")
        
        # 4. 모니터링 완료
        monitor.finalize_and_save(latest_timestamp)
        return total_success
        
    except Exception as e:
        # 5. 실패 시 에러 기록
        monitor.set_error(f"DB 저장 실패: {str(e)}")
        monitor.finalize_and_save()
        raise

# ✅ 개별 계정 수집 함수들 (모니터링 적용)
def fetch_trump_posts(**context):
    """트럼프 포스트 수집 (모니터링 포함)"""
    return fetch_posts_for_account('realDonaldTrump', **context)

def fetch_whitehouse_posts(**context):
    """백악관 포스트 수집 (모니터링 포함)"""
    return fetch_posts_for_account('WhiteHouse', **context)

def fetch_jr_posts(**context):
    """도널드 트럼프 주니어 포스트 수집 (모니터링 포함)"""
    return fetch_posts_for_account('DonaldJTrumpJr', **context)

# DAG 정의
with DAG(
    dag_id='ingest_truth_social_posts_k8s',  # ✅ 새 이름
    default_args=default_args,
    schedule_interval='0 */1 * * *',  # 1시간마다
    catchup=False,
    description='트럼프, 백악관, DonaldJTrumpJr Truth Social 포스트 수집 (모니터링 포함)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['truth_social', 'posts', 'realtime', 'monitoring', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_truth_social_posts_table',
        postgres_conn_id='postgres_default',
        sql='create_truth_social_posts.sql',
    )
    
    # ✅ 각 계정별 포스트 수집 (모니터링 적용, 병렬 실행)
    fetch_trump = PythonOperator(
        task_id='fetch_trump_posts',
        python_callable=fetch_trump_posts,  # 모니터링 적용된 함수
    )
    
    fetch_whitehouse = PythonOperator(
        task_id='fetch_whitehouse_posts',
        python_callable=fetch_whitehouse_posts,  # 모니터링 적용된 함수
    )
    
    fetch_jr = PythonOperator(
        task_id='fetch_jr_posts',
        python_callable=fetch_jr_posts,  # 모니터링 적용된 함수
    )
    
    # ✅ DB 저장 (모니터링 적용)
    store_posts = PythonOperator(
        task_id='store_posts_to_db',
        python_callable=store_posts_to_db,  # 모니터링 적용된 함수
    )
    
    # Task 의존성
    create_table >> [fetch_trump, fetch_whitehouse, fetch_jr] >> store_posts