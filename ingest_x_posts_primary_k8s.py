from datetime import datetime, timedelta
import requests
import json
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_x_posts.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,  # Rate Limit 에러 시 1회 재시도
    'retry_delay': timedelta(minutes=20),  # 20분 후 재시도 (15분 + 여유분)
}

# ===== DB 기반 계정 스케줄 설정 (user_id 제거) =====
PRIMARY_ACCOUNT_SCHEDULE = {
    # 초고영향 계정 (매일 or 격일)
    'elonmusk': {
        'frequency': 'daily',          # 매일 (30회/월)
        'max_results': 50,             # Free Tier 제한 고려
        'priority': 1
    },
    'RayDalio': {
        'frequency': 'every_2_days',   # 2일마다 (15회/월)
        'max_results': 50,
        'priority': 1
    },
    'jimcramer': {
        'frequency': 'every_2_days',   # 2일마다 (15회/월)
        'max_results': 50,
        'priority': 1
    },
    
    # 고영향 계정 (3일마다)
    'tim_cook': {
        'frequency': 'every_3_days',   # 3일마다 (10회/월)
        'max_results': 50,
        'priority': 2
    },
    'satyanadella': {
        'frequency': 'every_3_days',   # 3일마다 (10회/월)
        'max_results': 50,
        'priority': 2
    },
    
    # 중간영향 계정 (주 2회)
    'sundarpichai': {
        'frequency': 'twice_weekly',   # 주 2회 (8회/월)
        'max_results': 50,
        'priority': 3,
        'weekly_days': [1, 4]  # 화, 금
    },
    'SecYellen': {
        'frequency': 'twice_weekly',   # 주 2회 (8회/월)
        'max_results': 50,
        'priority': 3,
        'weekly_days': [0, 3]  # 월, 목
    },
    
    # 저영향 계정 (주 1회)
    'VitalikButerin': {
        'frequency': 'weekly',         # 주 1회 (4회/월)
        'max_results': 50,
        'priority': 4,
        'weekly_day': 6  # 일요일
    }
}

def get_user_id_from_db(username):
    """DB에서 username으로 user_id 조회"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        result = hook.get_first(
            "SELECT user_id FROM x_user_profiles WHERE username = %s",
            parameters=[username]
        )
        
        if result:
            user_id = result[0]
            print(f"✅ DB 조회 성공: {username} → {user_id}")
            return user_id
        else:
            print(f"❌ DB에서 {username}을 찾을 수 없습니다")
            return None
            
    except Exception as e:
        print(f"❌ DB 조회 실패: {username} - {e}")
        return None

def should_run_account_today_primary(username):
    """첫 번째 토큰 계정들의 오늘 실행 여부 판단"""
    config = PRIMARY_ACCOUNT_SCHEDULE.get(username)
    if not config:
        return False
    
    frequency = config['frequency']
    today = datetime.now()
    day_of_year = today.timetuple().tm_yday  # 1-365
    day_of_week = today.weekday()  # 0=월요일, 6=일요일
    
    if frequency == 'daily':
        return True
    elif frequency == 'every_2_days':
        # 계정별로 다른 시작점 (충돌 방지)
        offset = {'RayDalio': 0, 'jimcramer': 1}.get(username, 0)
        return (day_of_year + offset) % 2 == 0
    elif frequency == 'every_3_days':
        # 계정별로 다른 시작점
        offset = {'tim_cook': 0, 'satyanadella': 1}.get(username, 0)
        return (day_of_year + offset) % 3 == 0
    elif frequency == 'twice_weekly':
        # 계정별로 정확한 요일 할당
        assigned_days = config.get('weekly_days', [0, 3])
        return day_of_week in assigned_days
    elif frequency == 'weekly':
        # 계정별로 정확한 요일
        assigned_day = config.get('weekly_day', 6)
        return day_of_week == assigned_day
    
    return False

def get_todays_primary_accounts():
    """오늘 수집할 첫 번째 토큰 계정들 반환"""
    todays_accounts = []
    
    for username in PRIMARY_ACCOUNT_SCHEDULE.keys():
        if should_run_account_today_primary(username):
            todays_accounts.append(username)
    
    # 우선순위 순으로 정렬
    todays_accounts.sort(key=lambda x: PRIMARY_ACCOUNT_SCHEDULE[x]['priority'])
    
    return todays_accounts

def call_x_api_with_rate_limit(username, user_id, max_results=10):
    """start_time 없이 최신 트윗만 가져오기"""
    try:
        bearer_token = Variable.get('X_API_BEARER_TOKEN_4')
        
        url = f"https://api.twitter.com/2/users/{user_id}/tweets"
        
        # ✅ start_time 제거 - 최신 트윗들만 가져오기
        params = {
            "max_results": min(max_results, 10),  # Free Tier는 10개 제한
            "tweet.fields": "created_at,text,public_metrics,context_annotations,entities,lang,edit_history_tweet_ids",
            "expansions": "author_id",
            "user.fields": "name,username,verified,public_metrics"
        }
        
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "InvestmentAssistant-Primary/2.0"
        }
        
        print(f"🔍 API 호출 중: {username} (user_id: {user_id}) - 최신 트윗")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 400:
            print(f"❌ 400 Bad Request:")
            print(f"   Response: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ API 호출 성공: {username}")
        return data
        
    except Exception as e:
        print(f"❌ API 호출 실패: {username} - {e}")
        raise

def process_tweet_data_primary(tweet, user_info, source_account):
    """트윗 데이터 처리 (Primary Token용)"""
    
    # 기본 트윗 정보
    processed_data = {
        'tweet_id': tweet['id'],
        'author_id': tweet['author_id'],
        'text': tweet['text'],
        'created_at': tweet['created_at'].replace('Z', '+00:00'),
        'lang': tweet.get('lang', 'en'),
        'source_account': source_account,
        'account_category': 'core_investors',
        'collection_source': 'primary_token',
    }
    
    # 참여도 지표
    metrics = tweet.get('public_metrics', {})
    processed_data.update({
        'retweet_count': metrics.get('retweet_count', 0),
        'reply_count': metrics.get('reply_count', 0),
        'like_count': metrics.get('like_count', 0),
        'quote_count': metrics.get('quote_count', 0),
        'bookmark_count': metrics.get('bookmark_count', 0),
        'impression_count': metrics.get('impression_count', 0),
    })
    
    # 엔티티 정보 (JSON으로 저장)
    entities = tweet.get('entities', {})
    processed_data.update({
        'hashtags': json.dumps(entities.get('hashtags', [])) if entities.get('hashtags') else None,
        'mentions': json.dumps(entities.get('mentions', [])) if entities.get('mentions') else None,
        'urls': json.dumps(entities.get('urls', [])) if entities.get('urls') else None,
        'cashtags': json.dumps(entities.get('cashtags', [])) if entities.get('cashtags') else None,
        'annotations': json.dumps(entities.get('annotations', [])) if entities.get('annotations') else None,
    })
    
    # 컨텍스트 주석
    context_annotations = tweet.get('context_annotations', [])
    processed_data['context_annotations'] = json.dumps(context_annotations) if context_annotations else None
    
    # 사용자 정보
    if user_info:
        user_metrics = user_info.get('public_metrics', {})
        processed_data.update({
            'username': user_info.get('username', source_account),
            'display_name': user_info.get('name', ''),
            'user_verified': user_info.get('verified', False),
            'user_followers_count': user_metrics.get('followers_count', 0),
            'user_following_count': user_metrics.get('following_count', 0),
            'user_tweet_count': user_metrics.get('tweet_count', 0),
        })
    else:
        processed_data.update({
            'username': source_account,
            'display_name': '',
            'user_verified': False,
            'user_followers_count': 0,
            'user_following_count': 0,
            'user_tweet_count': 0,
        })
    
    # 편집 이력
    edit_history = tweet.get('edit_history_tweet_ids', [])
    processed_data['edit_history_tweet_ids'] = json.dumps(edit_history) if edit_history else None
    
    return processed_data

def fetch_primary_tweets_with_delay(**context):
    """방안 2: 15분 딜레이를 두고 순차적으로 계정 수집"""
    
    # 오늘 수집할 계정들 결정
    todays_accounts = get_todays_primary_accounts()
    
    if not todays_accounts:
        print("📅 [PRIMARY] 오늘은 수집할 계정이 없습니다")
        context['ti'].xcom_push(key='collected_tweets', value=[])
        context['ti'].xcom_push(key='api_calls_made', value=0)
        return 0
    
    print(f"🎯 [PRIMARY TOKEN] 오늘 수집 대상: {len(todays_accounts)}개 계정")
    for account in todays_accounts:
        config = PRIMARY_ACCOUNT_SCHEDULE[account]
        print(f"   - {account}: {config['frequency']} (우선순위 {config['priority']})")
    
    # Rate Limit 체크 (하루 17회 제한)
    if len(todays_accounts) > 17:
        print(f"⚠️ 경고: 오늘 수집 계정({len(todays_accounts)}개)이 일일 제한(17회)을 초과합니다")
        print(f"   처음 17개 계정만 수집합니다")
        todays_accounts = todays_accounts[:17]
    
    # 예상 소요 시간 계산
    estimated_time = (len(todays_accounts) - 1) * 15  # 15분 간격
    print(f"⏰ 예상 소요 시간: {estimated_time}분 (15분 간격 × {len(todays_accounts)-1}회 대기)")
    
    # 각 계정별 트윗 수집 (15분 딜레이 포함)
    all_tweets = []
    total_api_calls = 0
    successful_accounts = []
    failed_accounts = []
    
    for i, username in enumerate(todays_accounts):
        try:
            # 두 번째 계정부터 15분 대기 (Rate Limit 준수)
            if i > 0:
                wait_minutes = 15
                print(f"\n⏰ [RATE LIMIT] {wait_minutes}분 대기 중... (현재 {i+1}/{len(todays_accounts)})")
                print(f"   다음 계정: {username}")
                
                # 실제 환경에서는 15분, 테스트에서는 1분으로 조정 가능
                time.sleep(wait_minutes * 60)  # 15분 = 900초
                
                print(f"✅ 대기 완료! {username} 수집 시작")
            
            # DB에서 user_id 조회
            user_id = get_user_id_from_db(username)
            if not user_id:
                print(f"❌ {username}: DB에서 user_id를 찾을 수 없어 건너뜁니다")
                failed_accounts.append(f"{username} (user_id 없음)")
                continue
            
            config = PRIMARY_ACCOUNT_SCHEDULE[username]
            max_results = config['max_results']
            
            print(f"\n🔍 [{i+1}/{len(todays_accounts)}] {username} 트윗 수집 중...")
            print(f"   User ID: {user_id}")
            print(f"   최대 결과: {max_results}개")
            
            # API 호출
            api_response = call_x_api_with_rate_limit(username, user_id, max_results)
            total_api_calls += 1
            
            if 'data' not in api_response or not api_response['data']:
                print(f"⚠️ {username}: 최근 24시간 내 트윗 없음")
                successful_accounts.append(f"{username} (트윗 없음)")
                continue
            
            # 사용자 정보 추출
            user_info = {}
            if 'includes' in api_response and 'users' in api_response['includes']:
                user_info = api_response['includes']['users'][0]
            
            # 트윗 데이터 처리
            account_tweets = []
            for tweet in api_response['data']:
                processed_tweet = process_tweet_data_primary(tweet, user_info, username)
                account_tweets.append(processed_tweet)
            
            all_tweets.extend(account_tweets)
            successful_accounts.append(f"{username} ({len(account_tweets)}개)")
            print(f"✅ {username}: {len(account_tweets)}개 트윗 수집 완료")
            
        except Exception as e:
            print(f"❌ {username} 수집 실패: {e}")
            failed_accounts.append(f"{username} ({str(e)[:50]})")
            total_api_calls += 1  # 실패해도 API 호출은 차감
            continue
    
    # 최종 결과 요약
    print(f"\n📊 [PRIMARY TOKEN] 수집 완료:")
    print(f"   📱 총 트윗: {len(all_tweets)}개")
    print(f"   🔑 API 호출: {total_api_calls}회 / 17회 (일일 제한)")
    print(f"   ✅ 성공: {len(successful_accounts)}개 계정")
    if successful_accounts:
        for account in successful_accounts:
            print(f"      - {account}")
    
    if failed_accounts:
        print(f"   ❌ 실패: {len(failed_accounts)}개 계정")
        for account in failed_accounts:
            print(f"      - {account}")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='collected_tweets', value=all_tweets)
    context['ti'].xcom_push(key='api_calls_made', value=total_api_calls)
    context['ti'].xcom_push(key='successful_accounts', value=successful_accounts)
    context['ti'].xcom_push(key='failed_accounts', value=failed_accounts)
    
    return len(all_tweets)

def store_primary_tweets_to_db(**context):
    """수집된 핵심 계정 트윗을 DB에 저장"""
    
    # XCom에서 수집된 데이터 가져오기
    all_tweets = context['ti'].xcom_pull(key='collected_tweets') or []
    api_calls = context['ti'].xcom_pull(key='api_calls_made') or 0
    successful_accounts = context['ti'].xcom_pull(key='successful_accounts') or []
    failed_accounts = context['ti'].xcom_pull(key='failed_accounts') or []
    
    if not all_tweets:
        print("ℹ️ 저장할 트윗이 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"💾 [PRIMARY] DB 저장 시작: {len(all_tweets)}개 트윗")
    
    for tweet_data in all_tweets:
        try:
            hook.run(UPSERT_SQL, parameters=tweet_data)
            success_count += 1
            
            # 진행률 표시 (50개마다)
            if success_count % 50 == 0:
                print(f"📊 저장 진행률: {success_count}/{len(all_tweets)}")
                
        except Exception as e:
            print(f"❌ 트윗 저장 실패: {tweet_data.get('tweet_id', 'Unknown')} - {e}")
            error_count += 1
            continue
    
    print(f"✅ [PRIMARY] 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 통계 조회 및 최종 리포트
    try:
        # 오늘 수집된 primary 토큰 트윗
        result = hook.get_first("""
            SELECT COUNT(*) FROM x_posts 
            WHERE collected_at >= NOW() - INTERVAL '1 day'
            AND collection_source = 'primary_token'
        """)
        primary_today = result[0] if result else 0
        
        # 전체 트윗 수
        result = hook.get_first("SELECT COUNT(*) FROM x_posts")
        total_all = result[0] if result else 0
        
        print(f"\n📈 [최종 리포트]")
        print(f"   📊 오늘 Primary 토큰 수집: {primary_today}개")
        print(f"   📊 전체 저장된 트윗: {total_all}개")
        print(f"   🔥 오늘 API 호출: {api_calls}회 / 17회")
        print(f"   ✅ 성공 계정: {len(successful_accounts)}개")
        print(f"   ❌ 실패 계정: {len(failed_accounts)}개")
        
        # 남은 API 호출 수
        remaining_calls = 17 - api_calls
        print(f"   🔋 남은 API 호출: {remaining_calls}회")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_x_posts_primary_with_delay_k8s',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 매일 새벽 2시 실행 (8시간마다 → 1일 1회)
    catchup=False,
    description='X API Rate Limit 준수: 15분 딜레이 + DB 기반 user_id 조회 (방안 2)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'twitter', 'rate_limit_safe', 'db_based', 'delay_strategy', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_x_posts_table_primary',
        postgres_conn_id='postgres_default',
        sql='create_x_posts.sql',
    )
    
    # Rate Limit 준수하여 트윗 수집
    fetch_tweets = PythonOperator(
        task_id='fetch_primary_tweets_with_delay',
        python_callable=fetch_primary_tweets_with_delay,
    )
    
    # DB 저장
    store_tweets = PythonOperator(
        task_id='store_primary_tweets_to_db',
        python_callable=store_primary_tweets_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_tweets >> store_tweets