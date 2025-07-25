from datetime import datetime, timedelta
import requests
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
with open(os.path.join(DAGS_SQL_DIR, "upsert_x_user_profiles.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Primary + Secondary DAG에서 사용하는 모든 사용자명
ALL_USERNAMES = {
    # Primary Token 계정들
    'elonmusk': {'category': 'core_investors'},
    'RayDalio': {'category': 'core_investors'},
    'jimcramer': {'category': 'core_investors'},
    'tim_cook': {'category': 'core_investors'},
    'satyanadella': {'category': 'core_investors'},
    'sundarpichai': {'category': 'core_investors'},
    'SecYellen': {'category': 'core_investors'},
    'VitalikButerin': {'category': 'core_investors'},
    
    # Secondary Token 계정들
    'saylor': {'category': 'crypto'},
    'brian_armstrong': {'category': 'crypto'},
    'CoinbaseAssets': {'category': 'crypto'},
    'jeffbezos': {'category': 'tech_ceo'},
    'IBM': {'category': 'tech_ceo'},
    'CathieDWood': {'category': 'institutional'},
    'mcuban': {'category': 'institutional'},
    'chamath': {'category': 'institutional'},
    'CNBC': {'category': 'media'},
    'business': {'category': 'media'},
    'WSJ': {'category': 'media'},
    'Tesla': {'category': 'corporate'},
    'nvidia': {'category': 'corporate'}
}

def fetch_user_id_from_api(username):
    """X API로 사용자명에서 user_id 조회"""
    bearer_token = Variable.get('X_API_BEARER_TOKEN_3')  # Secondary 토큰 사용
    
    url = f"https://api.twitter.com/2/users/by/username/{username}"
    
    params = {
        "user.fields": "id,name,username,verified,public_metrics"
    }
    
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "InvestmentAssistant-UserID/1.0"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    return data

def fetch_all_user_ids(**context):
    """모든 사용자명의 user_id 수집"""
    
    print(f"🎯 사용자 ID 수집 시작: {len(ALL_USERNAMES)}개 계정")
    
    collected_users = []
    success_count = 0
    error_count = 0
    
    for username, config in ALL_USERNAMES.items():
        try:
            print(f"🔍 {username} ({config['category']}) ID 조회 중...")
            
            # API 호출
            api_response = fetch_user_id_from_api(username)
            
            if 'data' not in api_response:
                print(f"❌ {username}: 사용자를 찾을 수 없음")
                error_count += 1
                continue
            
            user_data = api_response['data']
            
            # 데이터 처리
            user_info = {
                'username': username,
                'user_id': user_data['id'],
                'display_name': user_data['name'],
                'category': config['category']
            }
            
            collected_users.append(user_info)
            success_count += 1
            
            print(f"✅ {username}: {user_data['name']} (ID: {user_data['id']})")
            
        except Exception as e:
            print(f"❌ {username} 조회 실패: {e}")
            error_count += 1
            continue
    
    print(f"\n📊 수집 완료: {success_count}개 성공, {error_count}개 실패")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='collected_users', value=collected_users)
    
    return success_count

def store_user_profiles(**context):
    """수집된 사용자 정보를 DB에 저장"""
    
    # XCom에서 사용자 정보 가져오기
    collected_users = context['ti'].xcom_pull(key='collected_users') or []
    
    if not collected_users:
        print("ℹ️ 저장할 사용자 정보가 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"💾 DB 저장 시작: {len(collected_users)}개 사용자")
    
    for user_info in collected_users:
        try:
            hook.run(UPSERT_SQL, parameters=user_info)
            success_count += 1
            print(f"✅ {user_info['username']}: 저장 완료")
            
        except Exception as e:
            print(f"❌ {user_info['username']} 저장 실패: {e}")
            error_count += 1
            continue
    
    print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 저장된 데이터 확인
    try:
        result = hook.get_first("SELECT COUNT(*) FROM user_profiles")
        total_users = result[0] if result else 0
        print(f"📊 총 저장된 사용자: {total_users}개")
        
        # 카테고리별 통계
        result = hook.get_records("""
            SELECT category, COUNT(*) 
            FROM user_profiles 
            GROUP BY category 
            ORDER BY COUNT(*) DESC
        """)
        
        print(f"📈 카테고리별 통계:")
        for row in result:
            print(f"   - {row[0]}: {row[1]}개")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='fetch_user_ids_onetime',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    description='X API로 사용자명 → user_id 매핑 수집 (일회성)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'user_profiles', 'onetime', 'setup'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_user_profiles_table',
        postgres_conn_id='postgres_default',
        sql='create_x_user_profiles.sql',
    )
    
    # 사용자 ID 수집
    fetch_ids = PythonOperator(
        task_id='fetch_all_user_ids',
        python_callable=fetch_all_user_ids,
    )
    
    # DB 저장
    store_profiles = PythonOperator(
        task_id='store_user_profiles',
        python_callable=store_user_profiles,
    )
    
    # Task 의존성
    create_table >> fetch_ids >> store_profiles