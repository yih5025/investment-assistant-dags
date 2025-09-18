from datetime import datetime, timedelta
import requests
import time
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 경로 설정s
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_x_user_profiles.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=2),
}

# 모든 사용자명 (순서대로 처리)
ALL_USERNAMES = [
    # Primary Token 계정들
    ('elonmusk', 'core_investors'),
    ('RayDalio', 'core_investors'),
    ('jimcramer', 'core_investors'),
    ('tim_cook', 'core_investors'),
    ('satyanadella', 'core_investors'),
    ('sundarpichai', 'core_investors'),
    ('SecYellen', 'core_investors'),
    ('VitalikButerin', 'core_investors'),
    
    # Secondary Token 계정들
    ('saylor', 'crypto'),
    ('brian_armstrong', 'crypto'),
    ('CoinbaseAssets', 'crypto'),
    ('jeffbezos', 'tech_ceo'),
    ('IBM', 'tech_ceo'),
    ('CathieDWood', 'institutional'),
    ('mcuban', 'institutional'),
    ('chamath', 'institutional'),
    ('CNBC', 'media'),
    ('business', 'media'),
    ('WSJ', 'media'),
    ('Tesla', 'corporate'),
    ('nvidia', 'corporate'),
    ('meta', 'corporate'),
    ('oracle', 'corporate'),
    
]

def get_next_username_to_process():
    """다음에 처리할 사용자명 확인"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 이미 수집된 사용자들 확인
    try:
        result = hook.get_records("SELECT username FROM x_user_profiles")
        completed_users = {row[0] for row in result} if result else set()
    except:
        completed_users = set()
    
    # 아직 수집되지 않은 첫 번째 사용자 찾기
    for username, category in ALL_USERNAMES:
        if username not in completed_users:
            return username, category
    
    return None, None

def fetch_single_user_id(**context):
    """한 번에 하나의 사용자 ID만 수집"""
    
    # 다음 처리할 사용자 확인
    username, category = get_next_username_to_process()
    
    if not username:
        print("🎉 모든 사용자 ID 수집 완료!")
        return "completed"
    
    print(f"🎯 처리 대상: {username} ({category})")
    
    # Bearer Token 가져오기
    bearer_token = Variable.get('X_API_BEARER_TOKEN_2')
    
    # API 호출
    url = f"https://api.twitter.com/2/users/by/username/{username}"
    
    params = {
        "user.fields": "id,name,username,verified,public_metrics"
    }
    
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "InvestmentAssistant-UserID/1.0"
    }
    
    try:
        print(f"🔍 {username} API 호출 중...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'data' not in data:
            print(f"❌ {username}: 사용자를 찾을 수 없음")
            return "not_found"
        
        user_data = data['data']
        
        # DB에 저장
        user_info = {
            'username': username,
            'user_id': user_data['id'],
            'display_name': user_data['name'],
            'category': category
        }
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run(UPSERT_SQL, parameters=user_info)
        
        print(f"✅ {username}: {user_data['name']} (ID: {user_data['id']}) - 저장 완료")
        
        # XCom에 결과 저장
        context['ti'].xcom_push(key='processed_user', value=user_info)
        
        return "success"
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"⏰ Rate Limit 도달 - 15분 후 재시도 예정")
            return "rate_limited"
        else:
            print(f"❌ {username} API 에러: {e}")
            return "api_error"
    except Exception as e:
        print(f"❌ {username} 처리 실패: {e}")
        return "error"

def check_completion_status(**context):
    """완료 상태 확인 및 통계 출력"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # 전체 수집 현황
        result = hook.get_first("SELECT COUNT(*) FROM x_user_profiles")
        total_collected = result[0] if result else 0
        
        remaining = len(ALL_USERNAMES) - total_collected
        
        print(f"📊 수집 현황: {total_collected}/{len(ALL_USERNAMES)} 완료")
        print(f"📋 남은 작업: {remaining}개")
        
        if remaining > 0:
            # 다음 처리 대상
            username, category = get_next_username_to_process()
            if username:
                print(f"🎯 다음 대상: {username} ({category})")
        
        # 카테고리별 통계
        result = hook.get_records("""
            SELECT category, COUNT(*) 
            FROM x_user_profiles 
            GROUP BY category 
            ORDER BY COUNT(*) DESC
        """)
        
        if result:
            print(f"📈 카테고리별 현황:")
            for row in result:
                print(f"   - {row[0]}: {row[1]}개")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    return total_collected

# DAG 정의
with DAG(
    dag_id='fetch_user_ids_batch_15min',
    default_args=default_args,
    schedule_interval='*/16 * * * *',  # 15분마다 실행
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지
    description='X API Rate Limit 대응 - 15분마다 1개씩 User ID 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'user_profiles', 'batch', 'rate_limit'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_user_profiles_table',
        postgres_conn_id='postgres_default',
        sql='create_x_user_profiles.sql',
    )
    
    # 한 개씩 사용자 ID 수집
    fetch_one = PythonOperator(
        task_id='fetch_single_user_id',
        python_callable=fetch_single_user_id,
    )
    
    # 완료 상태 확인
    check_status = PythonOperator(
        task_id='check_completion_status',
        python_callable=check_completion_status,
    )
    
    # Task 의존성
    create_table >> fetch_one >> check_status