from datetime import datetime, timedelta
import requests
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
with open(os.path.join(DAGS_SQL_DIR, "upsert_x_posts.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 계정별 호출 스케줄 정의 (월 100회 최적 배분)
ACCOUNT_SCHEDULE = {
    # 초고영향 계정 (매일 or 격일)
    'elonmusk': {
        'user_id': '44196397',
        'frequency': 'daily',          # 매일 (30회/월)
        'max_results': 50,
        'priority': 1
    },
    'RayDalio': {
        'user_id': '2545181', 
        'frequency': 'every_2_days',   # 2일마다 (15회/월)
        'max_results': 50,
        'priority': 1
    },
    'jimcramer': {
        'user_id': '18973134',
        'frequency': 'every_2_days',   # 2일마다 (15회/월)
        'max_results': 50,
        'priority': 1
    },
    
    # 고영향 계정 (3일마다)
    'tim_cook': {
        'user_id': '1636590253',
        'frequency': 'every_3_days',   # 3일마다 (10회/월)
        'max_results': 50,
        'priority': 2
    },
    'satyanadella': {
        'user_id': '729315142',
        'frequency': 'every_3_days',   # 3일마다 (10회/월)
        'max_results': 50,
        'priority': 2
    },
    
    # 중간영향 계정 (주 2회)
    'sundarpichai': {
        'user_id': '16144047',
        'frequency': 'twice_weekly',   # 주 2회 (8회/월)
        'max_results': 50,
        'priority': 3
    },
    'SecYellen': {
        'user_id': '950837342094893062',
        'frequency': 'twice_weekly',   # 주 2회 (8회/월)
        'max_results': 50,
        'priority': 3
    },
    
    # 저영향 계정 (주 1회)
    'VitalikButerin': {
        'user_id': '295218901',
        'frequency': 'weekly',         # 주 1회 (4회/월)
        'max_results': 50,
        'priority': 4
    }
    # 총 호출: 30+15+15+10+10+8+8+4 = 100회/월
}

def should_run_account_today(username):
    """오늘 이 계정을 수집해야 하는지 판단"""
    config = ACCOUNT_SCHEDULE.get(username)
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
        # 계정별로 다른 요일 할당
        if username == 'sundarpichai':
            return day_of_week in [0, 3]  # 월, 목
        elif username == 'SecYellen':
            return day_of_week in [1, 4]  # 화, 금
    elif frequency == 'weekly':
        # 계정별로 다른 요일
        weekly_schedule = {
            'VitalikButerin': 6,  # 일요일
        }
        return day_of_week == weekly_schedule.get(username, 6)
    
    return False

def get_todays_accounts():
    """오늘 수집할 계정들 반환"""
    todays_accounts = []
    
    for username in ACCOUNT_SCHEDULE.keys():
        if should_run_account_today(username):
            todays_accounts.append(username)
    
    # 우선순위 순으로 정렬
    todays_accounts.sort(key=lambda x: ACCOUNT_SCHEDULE[x]['priority'])
    
    return todays_accounts

def call_x_api(username, user_id, max_results=50):
    """X API 호출"""
    bearer_token = Variable.get('X_API_BEARER_TOKEN')
    
    url = f"https://api.twitter.com/2/users/{user_id}/tweets"
    
    # 24시간 전부터 수집
    start_time = (datetime.utcnow() - timedelta(hours=24)).isoformat() + 'Z'
    
    params = {
        "max_results": min(max_results, 100),
        "start_time": start_time,
        "tweet.fields": "created_at,text,public_metrics,context_annotations,entities,lang,edit_history_tweet_ids",
        "expansions": "author_id",
        "user.fields": "name,username,verified,public_metrics"
    }
    
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "InvestmentAssistant/1.0"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    return data

def process_tweet_data(tweet, user_info, source_account):
    """트윗 데이터 처리"""
    
    # 기본 트윗 정보
    processed_data = {
        'tweet_id': tweet['id'],
        'author_id': tweet['author_id'],
        'text': tweet['text'],
        'created_at': tweet['created_at'].replace('Z', '+00:00'),
        'lang': tweet.get('lang', 'en'),
        'source_account': source_account,
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

def fetch_todays_tweets(**context):
    """오늘 수집 대상 계정들의 트윗 수집"""
    
    # 오늘 수집할 계정들 결정
    todays_accounts = get_todays_accounts()
    
    if not todays_accounts:
        print("📅 오늘은 수집할 계정이 없습니다")
        context['ti'].xcom_push(key='collected_tweets', value=[])
        return 0
    
    print(f"🎯 오늘 수집 대상: {len(todays_accounts)}개 계정")
    for account in todays_accounts:
        config = ACCOUNT_SCHEDULE[account]
        print(f"   - {account}: {config['frequency']} (우선순위 {config['priority']})")
    
    # 각 계정별 트윗 수집
    all_tweets = []
    total_api_calls = 0
    
    for username in todays_accounts:
        try:
            config = ACCOUNT_SCHEDULE[username]
            user_id = config['user_id']
            max_results = config['max_results']
            
            print(f"\n🔍 {username} 트윗 수집 중 (최대 {max_results}개)...")
            
            # API 호출
            api_response = call_x_api(username, user_id, max_results)
            total_api_calls += 1
            
            if 'data' not in api_response or not api_response['data']:
                print(f"⚠️ {username}: 최근 24시간 내 트윗 없음")
                continue
            
            # 사용자 정보 추출
            user_info = {}
            if 'includes' in api_response and 'users' in api_response['includes']:
                user_info = api_response['includes']['users'][0]
            
            # 트윗 데이터 처리
            account_tweets = []
            for tweet in api_response['data']:
                processed_tweet = process_tweet_data(tweet, user_info, username)
                account_tweets.append(processed_tweet)
            
            all_tweets.extend(account_tweets)
            print(f"✅ {username}: {len(account_tweets)}개 트윗 수집")
            
        except Exception as e:
            print(f"❌ {username} 수집 실패: {e}")
            total_api_calls += 1  # 실패해도 API 호출은 차감
            continue
    
    print(f"\n📊 수집 완료: {len(all_tweets)}개 트윗, {total_api_calls}회 API 호출")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='collected_tweets', value=all_tweets)
    context['ti'].xcom_push(key='api_calls_made', value=total_api_calls)
    
    return len(all_tweets)

def store_tweets_to_db(**context):
    """수집된 트윗을 DB에 저장"""
    
    # XCom에서 수집된 트윗 가져오기
    all_tweets = context['ti'].xcom_pull(key='collected_tweets') or []
    api_calls = context['ti'].xcom_pull(key='api_calls_made') or 0
    
    if not all_tweets:
        print("ℹ️ 저장할 트윗이 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"💾 DB 저장 시작: {len(all_tweets)}개 트윗")
    
    for tweet_data in all_tweets:
        try:
            hook.run(UPSERT_SQL, parameters=tweet_data)
            success_count += 1
            
            # 진행률 표시 (100개마다)
            if success_count % 100 == 0:
                print(f"📊 저장 진행률: {success_count}/{len(all_tweets)}")
                
        except Exception as e:
            print(f"❌ 트윗 저장 실패: {tweet_data.get('tweet_id', 'Unknown')} - {e}")
            error_count += 1
            continue
    
    print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 통계 조회
    try:
        result = hook.get_first("SELECT COUNT(*) FROM x_posts WHERE collected_at >= NOW() - INTERVAL '1 day'")
        total_today = result[0] if result else 0
        print(f"📊 오늘 수집된 총 트윗: {total_today}개")
        
        result = hook.get_first("SELECT COUNT(*) FROM x_posts")
        total_all = result[0] if result else 0
        print(f"📊 전체 저장된 트윗: {total_all}개")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    print(f"🔥 오늘 API 호출: {api_calls}회")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_x_posts_scheduled_k8s',
    default_args=default_args,
    schedule_interval='0 */8 * * *',  # 8시간마다 실행
    catchup=False,
    description='X API 날짜 기반 스케줄링 트윗 수집 (월 100회 최적화)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'twitter', 'scheduled', 'investment', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_x_posts_table',
        postgres_conn_id='postgres_default',
        sql='create_x_posts.sql',
    )
    
    # 오늘 대상 계정들의 트윗 수집
    fetch_tweets = PythonOperator(
        task_id='fetch_todays_tweets',
        python_callable=fetch_todays_tweets,
    )
    
    # DB 저장
    store_tweets = PythonOperator(
        task_id='store_tweets_to_db',
        python_callable=store_tweets_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_tweets >> store_tweets