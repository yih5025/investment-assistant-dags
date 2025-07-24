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
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# ===== 두 번째 토큰용 확장 계정들 (월 100회 배분) =====
SECONDARY_ACCOUNT_SCHEDULE = {
    # === 1. 암호화폐 생태계 (35회/월) ===
    'saylor': {  # Michael Saylor (MicroStrategy)
        'user_id': '244647486',
        'frequency': 'daily',              # 30회/월
        'max_results': 50,
        'priority': 1,
        'category': 'crypto'
    },
    'brian_armstrong': {  # Coinbase CEO
        'user_id': '9224862',
        'frequency': 'twice_weekly',       # 8회/월 (화, 금)
        'max_results': 50,
        'priority': 2,
        'category': 'crypto'
    },
    'CoinbaseAssets': {  # Coinbase 공식
        'user_id': '1087818612',
        'frequency': 'weekly',             # 4회/월 (일요일)
        'max_results': 30,
        'priority': 3,
        'category': 'crypto'
    },
    
    # === 2. 추가 빅테크 CEO들 (25회/월) ===
    'jeffbezos': {  # Amazon 창립자
        'user_id': '12071242',
        'frequency': 'every_2_days',       # 15회/월
        'max_results': 50,
        'priority': 1,
        'category': 'tech_ceo'
    },
    'sundarpichai': {  # Google CEO (중복 방지 - 다른 스케줄)
        'user_id': '16144047',
        'frequency': 'twice_weekly',       # 8회/월 (월, 목)
        'max_results': 50,
        'priority': 2,
        'category': 'tech_ceo'
    },
    'IBM': {  # IBM 공식 계정
        'user_id': '18994444',
        'frequency': 'weekly',             # 4회/월 (토요일)
        'max_results': 30,
        'priority': 3,
        'category': 'tech_ceo'
    },
    
    # === 3. 투자 기관 & 인플루언서들 (20회/월) ===
    'CathieDWood': {  # ARK Invest
        'user_id': '2899617799',
        'frequency': 'twice_weekly',       # 8회/월 (수, 토)
        'max_results': 50,
        'priority': 2,
        'category': 'institutional'
    },
    'mcuban': {  # Mark Cuban
        'user_id': '15164565',
        'frequency': 'twice_weekly',       # 8회/월 (화, 금)
        'max_results': 50,
        'priority': 2,
        'category': 'institutional'
    },
    'chamath': {  # Chamath Palihapitiya
        'user_id': '3291691',
        'frequency': 'weekly',             # 4회/월 (일요일)
        'max_results': 40,
        'priority': 3,
        'category': 'institutional'
    },
    
    # === 4. 금융 미디어 (15회/월) ===
    'CNBC': {
        'user_id': '20402945',
        'frequency': 'twice_weekly',       # 8회/월 (월, 목)
        'max_results': 40,
        'priority': 2,
        'category': 'media'
    },
    'business': {  # Bloomberg
        'user_id': '34713362',
        'frequency': 'weekly',             # 4회/월 (화요일)
        'max_results': 30,
        'priority': 3,
        'category': 'media'
    },
    'WSJ': {  # Wall Street Journal
        'user_id': '3108351',
        'frequency': 'weekly',             # 4회/월 (수요일)
        'max_results': 30,
        'priority': 3,
        'category': 'media'
    },
    
    # === 5. 기업 공식 계정들 (5회/월) ===
    'Tesla': {
        'user_id': '13298072',
        'frequency': 'weekly',             # 4회/월 (금요일)
        'max_results': 25,
        'priority': 3,
        'category': 'corporate'
    },
    'nvidia': {
        'user_id': '61559439',
        'frequency': 'weekly',             # 4회/월 (목요일)
        'max_results': 25,
        'priority': 3,
        'category': 'corporate'
    }
    # 총 호출: 30+8+4+15+8+4+8+8+4+4+4+4+4 = 105회/월 (5회 여유)
}

def should_run_account_today_secondary(username):
    """두 번째 토큰 계정들의 오늘 실행 여부 판단"""
    config = SECONDARY_ACCOUNT_SCHEDULE.get(username)
    if not config:
        return False
    
    frequency = config['frequency']
    today = datetime.now()
    day_of_year = today.timetuple().tm_yday  # 1-365
    day_of_week = today.weekday()  # 0=월요일, 6=일요일
    
    if frequency == 'daily':
        return True
    elif frequency == 'every_2_days':
        # jeffbezos는 홀수일에 실행
        return day_of_year % 2 == 1
    elif frequency == 'every_3_days':
        # 필요시 추가
        return (day_of_year + 1) % 3 == 0
    elif frequency == 'twice_weekly':
        # 계정별로 다른 요일 할당 (Primary 토큰과 겹치지 않게)
        twice_weekly_schedule = {
            'brian_armstrong': [1, 4],    # 화, 금
            'sundarpichai': [0, 3],       # 월, 목 (Primary와 다름)
            'CathieDWood': [2, 5],        # 수, 토
            'mcuban': [1, 4],             # 화, 금
            'CNBC': [0, 3]                # 월, 목
        }
        assigned_days = twice_weekly_schedule.get(username, [0, 3])
        return day_of_week in assigned_days
    elif frequency == 'weekly':
        # 계정별로 다른 요일 할당
        weekly_schedule = {
            'CoinbaseAssets': 6,    # 일요일
            'IBM': 5,               # 토요일  
            'chamath': 6,           # 일요일
            'business': 1,          # 화요일
            'WSJ': 2,               # 수요일
            'Tesla': 4,             # 금요일
            'nvidia': 3             # 목요일
        }
        return day_of_week == weekly_schedule.get(username, 6)
    
    return False

def get_todays_secondary_accounts():
    """오늘 수집할 두 번째 토큰 계정들 반환"""
    todays_accounts = []
    
    for username in SECONDARY_ACCOUNT_SCHEDULE.keys():
        if should_run_account_today_secondary(username):
            todays_accounts.append(username)
    
    # 우선순위 순으로 정렬
    todays_accounts.sort(key=lambda x: SECONDARY_ACCOUNT_SCHEDULE[x]['priority'])
    
    return todays_accounts

def call_x_api_secondary(username, user_id, max_results=50):
    """두 번째 Bearer Token으로 X API 호출"""
    # 두 번째 토큰 사용
    bearer_token = Variable.get('X_API_BEARER_TOKEN_2')
    
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
        "User-Agent": "InvestmentAssistant-Secondary/1.0"
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    return data

def process_tweet_data_secondary(tweet, user_info, source_account, category):
    """트윗 데이터 처리 (카테고리 정보 추가)"""
    
    # 기본 트윗 정보
    processed_data = {
        'tweet_id': tweet['id'],
        'author_id': tweet['author_id'],
        'text': tweet['text'],
        'created_at': tweet['created_at'].replace('Z', '+00:00'),
        'lang': tweet.get('lang', 'en'),
        'source_account': source_account,
        'account_category': category,  # 카테고리 추가
        'collection_source': 'secondary_token',  # 토큰 구분
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

def fetch_secondary_tweets(**context):
    """두 번째 토큰으로 확장 계정들의 트윗 수집"""
    
    # 오늘 수집할 계정들 결정
    todays_accounts = get_todays_secondary_accounts()
    
    if not todays_accounts:
        print("📅 [SECONDARY] 오늘은 수집할 계정이 없습니다")
        context['ti'].xcom_push(key='collected_tweets', value=[])
        return 0
    
    print(f"🎯 [SECONDARY TOKEN] 오늘 수집 대상: {len(todays_accounts)}개 계정")
    
    # 카테고리별 분류
    category_counts = {}
    for account in todays_accounts:
        config = SECONDARY_ACCOUNT_SCHEDULE[account]
        category = config['category']
        if category not in category_counts:
            category_counts[category] = 0
        category_counts[category] += 1
        print(f"   - {account}: {config['frequency']} ({category}, 우선순위 {config['priority']})")
    
    print(f"📊 카테고리별: {dict(category_counts)}")
    
    # 각 계정별 트윗 수집
    all_tweets = []
    total_api_calls = 0
    category_stats = {}
    
    for username in todays_accounts:
        try:
            config = SECONDARY_ACCOUNT_SCHEDULE[username]
            user_id = config['user_id']
            max_results = config['max_results']
            category = config['category']
            
            print(f"\n🔍 [{category.upper()}] {username} 트윗 수집 중 (최대 {max_results}개)...")
            
            # API 호출 (두 번째 토큰 사용)
            api_response = call_x_api_secondary(username, user_id, max_results)
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
                processed_tweet = process_tweet_data_secondary(tweet, user_info, username, category)
                account_tweets.append(processed_tweet)
            
            all_tweets.extend(account_tweets)
            
            # 카테고리별 통계
            if category not in category_stats:
                category_stats[category] = {'tweets': 0, 'accounts': 0, 'api_calls': 0}
            category_stats[category]['tweets'] += len(account_tweets)
            category_stats[category]['accounts'] += 1
            category_stats[category]['api_calls'] += 1
            
            print(f"✅ {username}: {len(account_tweets)}개 트윗 수집")
            
        except Exception as e:
            print(f"❌ {username} 수집 실패: {e}")
            total_api_calls += 1  # 실패해도 API 호출은 차감
            
            # 실패도 통계에 반영
            category = config.get('category', 'unknown')
            if category not in category_stats:
                category_stats[category] = {'tweets': 0, 'accounts': 0, 'api_calls': 0}
            category_stats[category]['api_calls'] += 1
            continue
    
    print(f"\n📊 [SECONDARY TOKEN] 수집 완료:")
    print(f"   📱 총 트윗: {len(all_tweets)}개")
    print(f"   🔑 API 호출: {total_api_calls}회")
    print(f"   📈 카테고리별 결과:")
    for category, stats in category_stats.items():
        print(f"      - {category}: {stats['tweets']}개 트윗 ({stats['accounts']}개 계정, {stats['api_calls']}회 호출)")
    
    # XCom에 결과 저장
    context['ti'].xcom_push(key='collected_tweets', value=all_tweets)
    context['ti'].xcom_push(key='api_calls_made', value=total_api_calls)
    context['ti'].xcom_push(key='category_stats', value=category_stats)
    
    return len(all_tweets)

def store_secondary_tweets_to_db(**context):
    """수집된 확장 계정 트윗을 DB에 저장"""
    
    # XCom에서 수집된 트윗 가져오기
    all_tweets = context['ti'].xcom_pull(key='collected_tweets') or []
    api_calls = context['ti'].xcom_pull(key='api_calls_made') or 0
    category_stats = context['ti'].xcom_pull(key='category_stats') or {}
    
    if not all_tweets:
        print("ℹ️ 저장할 트윗이 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"💾 [SECONDARY] DB 저장 시작: {len(all_tweets)}개 트윗")
    
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
    
    print(f"✅ [SECONDARY] 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 통계 조회
    try:
        # 오늘 수집된 secondary 토큰 트윗
        result = hook.get_first("""
            SELECT COUNT(*) FROM x_posts 
            WHERE collected_at >= NOW() - INTERVAL '1 day'
            AND collection_source = 'secondary_token'
        """)
        secondary_today = result[0] if result else 0
        
        # 전체 트윗 수
        result = hook.get_first("SELECT COUNT(*) FROM x_posts")
        total_all = result[0] if result else 0
        
        print(f"📊 오늘 Secondary 토큰 수집: {secondary_today}개")
        print(f"📊 전체 저장된 트윗: {total_all}개")
        
        # 카테고리별 통계 출력
        print(f"📈 카테고리별 수집 결과:")
        for category, stats in category_stats.items():
            print(f"   - {category}: {stats['tweets']}개 트윗, {stats['api_calls']}회 API 호출")
        
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    print(f"🔥 Secondary 토큰 오늘 API 호출: {api_calls}회")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_x_posts_secondary_k8s',
    default_args=default_args,
    schedule_interval='0 */8 * * *',  # 8시간마다 실행 (Primary와 동일)
    catchup=False,
    description='X API 두 번째 토큰으로 확장 계정 트윗 수집 (암호화폐, 빅테크, 투자기관)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['x_api', 'twitter', 'secondary_token', 'crypto', 'bigtech', 'investment', 'k8s'],
) as dag:
    
    # 테이블 생성 (이미 존재할 수 있으므로 같은 테이블 사용)
    create_table = PostgresOperator(
        task_id='create_x_posts_table_secondary',
        postgres_conn_id='postgres_default',
        sql='create_x_posts.sql',
    )
    
    # 두 번째 토큰으로 확장 계정들의 트윗 수집
    fetch_tweets = PythonOperator(
        task_id='fetch_secondary_tweets',
        python_callable=fetch_secondary_tweets,
    )
    
    # DB 저장
    store_tweets = PythonOperator(
        task_id='store_secondary_tweets_to_db',
        python_callable=store_secondary_tweets_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_tweets >> store_tweets