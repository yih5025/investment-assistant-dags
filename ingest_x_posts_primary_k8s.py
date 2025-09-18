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
    'retries': 1,
    'retry_delay': timedelta(minutes=30),  # 30분 후 재시도
}

# ===== Primary 계정별 토큰 할당 및 중요도 설정 =====
PRIMARY_ACCOUNT_CONFIG = {
    # 최고 중요도 계정 (매일 실행)
    'elonmusk': {
        'frequency': 'daily',
        'max_results': 50,
        'priority': 1,
        'token': 'X_API_BEARER_TOKEN_2',  # 토큰 2 사용
        'wait_minutes': 30  # 30분 간격
    },
    'RayDalio': {
        'frequency': 'every_2_days',
        'max_results': 50,
        'priority': 1,
        'token': 'X_API_BEARER_TOKEN_4',  # 토큰 4 사용
        'wait_minutes': 30
    },
    
    # 고중요도 계정 (덜 빈번)
    'jimcramer': {
        'frequency': 'every_3_days',
        'max_results': 50,
        'priority': 2,
        'token': 'X_API_BEARER_TOKEN_2',
        'wait_minutes': 45  # 중요도 낮은 계정은 더 긴 간격
    },
    'tim_cook': {
        'frequency': 'every_3_days',
        'max_results': 50,
        'priority': 2,
        'token': 'X_API_BEARER_TOKEN_4',
        'wait_minutes': 45
    },
    
    # 중간 중요도 계정
    'oracle': {
        'frequency': 'twice_weekly',
        'max_results': 50,  # 덜 중요한 계정은 적게
        'priority': 3,
        'token': 'X_API_BEARER_TOKEN_2',
        'weekly_days': [1, 4],  # 화, 금
        'wait_minutes': 60  # 1시간 간격
    },
    'sundarpichai': {
        'frequency': 'twice_weekly',
        'max_results': 50,
        'priority': 3,
        'token': 'X_API_BEARER_TOKEN_4',
        'weekly_days': [0, 3],  # 월, 목
        'wait_minutes': 60
    },
    
    # 낮은 중요도 계정
    'BitCoin': {
        'frequency': 'weekly',
        'max_results': 50,
        'priority': 4,
        'token': 'X_API_BEARER_TOKEN_2',
        'weekly_day': 6,  # 일요일
        'wait_minutes': 90  # 1.5시간 간격
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
    """Primary 계정들의 오늘 실행 여부 판단"""
    config = PRIMARY_ACCOUNT_CONFIG.get(username)
    if not config:
        return False
    
    frequency = config['frequency']
    today = datetime.now()
    day_of_year = today.timetuple().tm_yday
    day_of_week = today.weekday()
    
    if frequency == 'daily':
        return True
    elif frequency == 'every_2_days':
        # RayDalio는 홀수일에 실행
        return day_of_year % 2 == 1
    elif frequency == 'every_3_days':
        # 계정별로 다른 시작점
        offset = {'jimcramer': 0, 'tim_cook': 1}.get(username, 0)
        return (day_of_year + offset) % 3 == 0
    elif frequency == 'twice_weekly':
        assigned_days = config.get('weekly_days', [0, 3])
        return day_of_week in assigned_days
    elif frequency == 'weekly':
        assigned_day = config.get('weekly_day', 6)
        return day_of_week == assigned_day
    
    return False

def get_todays_primary_accounts():
    """오늘 수집할 Primary 계정들 반환 (우선순위별 정렬)"""
    todays_accounts = []
    
    for username in PRIMARY_ACCOUNT_CONFIG.keys():
        if should_run_account_today_primary(username):
            todays_accounts.append(username)
    
    # 우선순위 순으로 정렬
    todays_accounts.sort(key=lambda x: PRIMARY_ACCOUNT_CONFIG[x]['priority'])
    
    return todays_accounts

def call_x_api_with_flexible_token(username, user_id, max_results, token_key):
    """지정된 토큰으로 API 호출"""
    try:
        bearer_token = Variable.get(token_key)
        
        url = f"https://api.twitter.com/2/users/{user_id}/tweets"
        
        params = {
            "max_results": min(max_results, 50),  # 테스트 결과 기반 최대 50개
            "tweet.fields": "created_at,text,public_metrics,context_annotations,entities,lang,edit_history_tweet_ids",
            "expansions": "author_id",
            "user.fields": "name,username,verified,public_metrics"
        }
        
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": f"InvestmentAssistant-Primary-{token_key[-1]}/2.0"
        }
        
        print(f"🔑 사용 토큰: {token_key}")
        print(f"🔍 API 호출 중: {username} (user_id: {user_id}) - 최신 {max_results}개 트윗")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 400:
            print(f"❌ 400 Bad Request: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ API 호출 성공: {username} ({token_key})")
        return data
        
    except Exception as e:
        print(f"❌ API 호출 실패: {username} ({token_key}) - {e}")
        raise

def process_tweet_data_primary(tweet, user_info, source_account):
    """트윗 데이터 처리"""
    
    processed_data = {
        'tweet_id': tweet['id'],
        'author_id': tweet['author_id'],
        'text': tweet['text'],
        'created_at': tweet['created_at'].replace('Z', '+00:00'),
        'lang': tweet.get('lang', 'en'),
        'source_account': source_account,
        'account_category': 'core_investors',
        'collection_source': 'primary_token_improved',
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
    
    # 엔티티 정보
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

def fetch_primary_tweets_improved(**context):
    """개선된 Primary 트윗 수집 (30분+ 딜레이, 토큰 분산)"""
    
    # 오늘 수집할 계정들 결정
    todays_accounts = get_todays_primary_accounts()
    
    if not todays_accounts:
        print("📅 [PRIMARY] 오늘은 수집할 계정이 없습니다")
        context['ti'].xcom_push(key='collected_tweets', value=[])
        context['ti'].xcom_push(key='api_calls_made', value=0)
        return 0
    
    print(f"🎯 [PRIMARY IMPROVED] 오늘 수집 대상: {len(todays_accounts)}개 계정")
    
    # 토큰별 계정 그룹화 미리보기
    token_groups = {}
    total_wait_time = 0
    for i, username in enumerate(todays_accounts):
        config = PRIMARY_ACCOUNT_CONFIG[username]
        token = config['token']
        wait_min = config['wait_minutes']
        
        if token not in token_groups:
            token_groups[token] = []
        token_groups[token].append(username)
        
        print(f"   - {username}: {config['frequency']} ({token}, {wait_min}분 간격, 우선순위 {config['priority']})")
        if i > 0:
            total_wait_time += wait_min
    
    print(f"📊 토큰별 분산: {dict((k, len(v)) for k, v in token_groups.items())}")
    print(f"⏰ 예상 총 소요 시간: {total_wait_time}분")
    
    # 월간 API 사용량 체크 (70회 제한)
    if len(todays_accounts) > 10:  # 하루 최대 10회 호출로 제한
        print(f"⚠️ 경고: 일일 호출 제한 적용 - 10개 계정으로 제한")
        todays_accounts = todays_accounts[:10]
    
    # 각 계정별 트윗 수집
    all_tweets = []
    total_api_calls = 0
    successful_accounts = []
    failed_accounts = []
    token_usage = {}
    
    for i, username in enumerate(todays_accounts):
        try:
            # 두 번째 계정부터 대기
            if i > 0:
                prev_config = PRIMARY_ACCOUNT_CONFIG[todays_accounts[i-1]]
                wait_minutes = prev_config['wait_minutes']
                
                print(f"\n⏰ [RATE LIMIT] {wait_minutes}분 대기 중... ({i+1}/{len(todays_accounts)})")
                print(f"   다음 계정: {username}")
                
                # 실제 환경에서는 해당 시간 대기
                time.sleep(wait_minutes * 60)
                
                print(f"✅ 대기 완료! {username} 수집 시작")
            
            # DB에서 user_id 조회
            user_id = get_user_id_from_db(username)
            if not user_id:
                print(f"❌ {username}: DB에서 user_id를 찾을 수 없어 건너뜁니다")
                failed_accounts.append(f"{username} (user_id 없음)")
                continue
            
            config = PRIMARY_ACCOUNT_CONFIG[username]
            max_results = config['max_results']
            token_key = config['token']
            
            print(f"\n🔍 [{i+1}/{len(todays_accounts)}] {username} 수집 중...")
            print(f"   User ID: {user_id}")
            print(f"   최대 결과: {max_results}개")
            print(f"   우선순위: {config['priority']}")
            
            # API 호출
            api_response = call_x_api_with_flexible_token(username, user_id, max_results, token_key)
            total_api_calls += 1
            
            # 토큰 사용량 추적
            if token_key not in token_usage:
                token_usage[token_key] = 0
            token_usage[token_key] += 1
            
            if 'data' not in api_response or not api_response['data']:
                print(f"⚠️ {username}: 최근 트윗 없음")
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
            total_api_calls += 1
            continue
    
    # 최종 결과 요약
    print(f"\n📊 [PRIMARY IMPROVED] 수집 완료:")
    print(f"   📱 총 트윗: {len(all_tweets)}개")
    print(f"   🔑 총 API 호출: {total_api_calls}회")
    print(f"   ✅ 성공: {len(successful_accounts)}개 계정")
    
    if successful_accounts:
        for account in successful_accounts:
            print(f"      - {account}")
    
    if failed_accounts:
        print(f"   ❌ 실패: {len(failed_accounts)}개 계정")
        for account in failed_accounts:
            print(f"      - {account}")
    
    print(f"📈 토큰별 사용량:")
    for token, count in token_usage.items():
        print(f"   - {token}: {count}회 사용")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='collected_tweets', value=all_tweets)
    context['ti'].xcom_push(key='api_calls_made', value=total_api_calls)
    context['ti'].xcom_push(key='token_usage', value=token_usage)
    context['ti'].xcom_push(key='successful_accounts', value=successful_accounts)
    context['ti'].xcom_push(key='failed_accounts', value=failed_accounts)
    
    return len(all_tweets)

def store_primary_tweets_to_db(**context):
    """수집된 트윗을 DB에 저장"""
    
    all_tweets = context['ti'].xcom_pull(key='collected_tweets') or []
    api_calls = context['ti'].xcom_pull(key='api_calls_made') or 0
    token_usage = context['ti'].xcom_pull(key='token_usage') or {}
    successful_accounts = context['ti'].xcom_pull(key='successful_accounts') or []
    failed_accounts = context['ti'].xcom_pull(key='failed_accounts') or []
    
    if not all_tweets:
        print("ℹ️ 저장할 트윗이 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"💾 [PRIMARY IMPROVED] DB 저장 시작: {len(all_tweets)}개 트윗")
    
    for tweet_data in all_tweets:
        try:
            hook.run(UPSERT_SQL, parameters=tweet_data)
            success_count += 1
            
            if success_count % 25 == 0:
                print(f"📊 저장 진행률: {success_count}/{len(all_tweets)}")
                
        except Exception as e:
            print(f"❌ 트윗 저장 실패: {tweet_data.get('tweet_id', 'Unknown')} - {e}")
            error_count += 1
            continue
    
    print(f"✅ [PRIMARY IMPROVED] 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 최종 통계
    try:
        result = hook.get_first("""
            SELECT COUNT(*) FROM x_posts 
            WHERE collected_at >= NOW() - INTERVAL '1 day'
            AND collection_source = 'primary_token_improved'
        """)
        primary_today = result[0] if result else 0
        
        print(f"\n📈 [최종 리포트]")
        print(f"   📊 오늘 Primary 개선 버전 수집: {primary_today}개")
        print(f"   🔥 오늘 총 API 호출: {api_calls}회")
        print(f"   ✅ 성공 계정: {len(successful_accounts)}개")
        print(f"   ❌ 실패 계정: {len(failed_accounts)}개")
        
        print(f"\n📈 토큰별 사용량:")
        for token, count in token_usage.items():
            print(f"   - {token}: {count}회 사용")
        
        # 월간 사용량 추정
        monthly_estimated = api_calls * 30
        print(f"   📊 월간 예상 사용량: {monthly_estimated}회 (70회 제한 대비)")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_x_posts_primary_k8s',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # 매일 새벽 1시 실행 (Secondary와 시간 분리)
    catchup=False,
    description='X API Primary 개선: 30분+ 딜레이, 토큰 분산, 월 70회 제한',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'twitter', 'primary_improved', 'token_distributed', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_x_posts_table_primary_improved',
        postgres_conn_id='postgres_default',
        sql='create_x_posts.sql',
    )
    
    # 개선된 트윗 수집
    fetch_tweets = PythonOperator(
        task_id='fetch_primary_tweets_improved',
        python_callable=fetch_primary_tweets_improved,
    )
    
    # DB 저장
    store_tweets = PythonOperator(
        task_id='store_primary_tweets_to_db',
        python_callable=store_primary_tweets_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_tweets >> store_tweets