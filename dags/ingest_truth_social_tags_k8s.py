from datetime import datetime, timedelta, date
import subprocess
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    try:
        cmd = ['truthbrush'] + command_args
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            return result.stdout
        else:
            raise Exception(f"Truthbrush 실행 실패: {result.stderr}")
    except subprocess.TimeoutExpired:
        raise Exception("Truthbrush 명령어 타임아웃")

def classify_tag_market_relevance(tag_name):
    """해시태그의 시장 관련도 점수 계산"""
    market_keywords = {
        # 높은 관련도 (8-10점)
        'economy': 10, 'stock': 10, 'market': 10, 'bitcoin': 10, 'crypto': 10,
        'inflation': 9, 'fed': 9, 'rates': 9, 'dollar': 9, 'trade': 9,
        'jobs': 8, 'gdp': 8, 'nasdaq': 8, 'sp500': 8, 'dow': 8,
        
        # 중간 관련도 (5-7점)
        'energy': 7, 'oil': 7, 'gas': 7, 'gold': 6, 'silver': 6,
        'tech': 6, 'ai': 6, 'tesla': 6, 'apple': 6, 'google': 6,
        'banking': 5, 'finance': 5, 'investment': 5,
        
        # 낮은 관련도 (1-4점)
        'politics': 3, 'election': 3, 'congress': 2, 'senate': 2,
        'trump': 4, 'biden': 2, 'obama': 1
    }
    
    tag_lower = tag_name.lower()
    
    # 직접 매칭
    for keyword, score in market_keywords.items():
        if keyword in tag_lower:
            return score
    
    return 0  # 관련 없음

def classify_tag_category(tag_name):
    """해시태그 카테고리 분류"""
    categories = {
        'economy': ['economy', 'market', 'inflation', 'fed', 'rates', 'gdp', 'jobs'],
        'crypto': ['bitcoin', 'crypto', 'ethereum', 'blockchain', 'defi'],
        'stocks': ['stock', 'nasdaq', 'sp500', 'dow', 'tesla', 'apple', 'google'],
        'energy': ['energy', 'oil', 'gas', 'renewable'],
        'politics': ['trump', 'biden', 'obama', 'congress', 'senate', 'election'],
        'tech': ['ai', 'tech', 'innovation', 'startup']
    }
    
    tag_lower = tag_name.lower()
    
    for category, keywords in categories.items():
        if any(keyword in tag_lower for keyword in keywords):
            return category
    
    return 'general'

def calculate_trend_metrics(history_data):
    """트렌드 메트릭 계산"""
    if not history_data or len(history_data) < 2:
        return 0, 0, 0
    
    # 최근 사용량들
    uses = [int(day.get('uses', 0)) for day in history_data]
    
    # 성장률 계산 (전일 대비)
    if len(uses) >= 2 and uses[1] > 0:
        growth_rate = ((uses[0] - uses[1]) / uses[1]) * 100
    else:
        growth_rate = 0
    
    # 주간 평균
    weekly_average = sum(uses) / len(uses) if uses else 0
    
    # 트렌드 점수 (가중평균 + 성장률)
    if len(uses) >= 3:
        # 최근 3일 가중평균 (최신일 가중치 높음)
        weighted_avg = (uses[0] * 0.5 + uses[1] * 0.3 + uses[2] * 0.2) if len(uses) >= 3 else uses[0]
        trend_score = weighted_avg + (growth_rate * 0.1)
    else:
        trend_score = uses[0] if uses else 0
    
    return round(growth_rate, 2), round(weekly_average, 2), round(trend_score, 2)

def parse_tag_data(raw_tag):
    """해시태그 데이터 파싱"""
    tag_name = raw_tag.get('name', '')
    history = raw_tag.get('history', [])
    
    # 7일간 사용량 추출
    day_uses = [0] * 7
    for i, day_data in enumerate(history[:7]):
        day_uses[i] = int(day_data.get('uses', 0))
    
    # 총 사용량 및 계정 수 (오늘)
    total_uses = day_uses[0] if day_uses else 0
    total_accounts = int(history[0].get('accounts', 0)) if history else 0
    
    # 메트릭 계산
    growth_rate, weekly_average, trend_score = calculate_trend_metrics(history)
    
    # 분류
    market_relevance = classify_tag_market_relevance(tag_name)
    tag_category = classify_tag_category(tag_name)
    
    return {
        'name': tag_name,
        'collected_date': date.today(),
        'url': raw_tag.get('url'),
        'total_uses': total_uses,
        'total_accounts': total_accounts,
        'recent_statuses_count': raw_tag.get('recent_statuses_count', 0),
        'history_data': json.dumps(history),
        'day_0_uses': day_uses[0] if len(day_uses) > 0 else 0,
        'day_1_uses': day_uses[1] if len(day_uses) > 1 else 0,
        'day_2_uses': day_uses[2] if len(day_uses) > 2 else 0,
        'day_3_uses': day_uses[3] if len(day_uses) > 3 else 0,
        'day_4_uses': day_uses[4] if len(day_uses) > 4 else 0,
        'day_5_uses': day_uses[5] if len(day_uses) > 5 else 0,
        'day_6_uses': day_uses[6] if len(day_uses) > 6 else 0,
        'trend_score': trend_score,
        'growth_rate': growth_rate,
        'weekly_average': weekly_average,
        'tag_category': tag_category,
        'market_relevance': market_relevance
    }

def fetch_trending_tags(**context):
    """트렌딩 해시태그 수집"""
    print("🏷️ 트렌딩 해시태그 수집 중...")
    
    try:
        output = run_truthbrush_command(['tags'])
        
        tags = []
        lines = output.strip().split('\n')
        
        # 첫 번째 줄이 JSON 배열인지 확인
        if lines and lines[0].strip().startswith('['):
            try:
                tag_list = json.loads(lines[0])
                
                for tag_data in tag_list:
                    processed_tag = parse_tag_data(tag_data)
                    tags.append(processed_tag)
                    
            except json.JSONDecodeError:
                print("⚠️ 해시태그 데이터 JSON 파싱 실패")
        
        print(f"✅ 트렌딩 해시태그 {len(tags)}개 수집")
        
        # 시장 관련 태그 통계
        market_tags = [tag for tag in tags if tag['market_relevance'] > 0]
        print(f"📊 시장 관련 태그: {len(market_tags)}개")
        
        context['ti'].xcom_push(key='trending_tags', value=tags)
        return len(tags)
        
    except Exception as e:
        print(f"❌ 트렌딩 해시태그 수집 실패: {e}")
        context['ti'].xcom_push(key='trending_tags', value=[])
        return 0

def store_tags_to_db(**context):
    """트렌딩 해시태그를 DB에 저장"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    tags = context['ti'].xcom_pull(key='trending_tags') or []
    success_count = 0
    error_count = 0
    
    for tag in tags:
        try:
            hook.run(UPSERT_TAGS_SQL, parameters=tag)
            success_count += 1
        except Exception as e:
            print(f"❌ 해시태그 저장 실패: {tag.get('name', 'Unknown')} - {e}")
            error_count += 1
    
    print(f"✅ 해시태그 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 시장 관련 태그 통계 조회
    market_query = """
    SELECT COUNT(*), AVG(market_relevance), MAX(total_uses)
    FROM truth_social_tags 
    WHERE collected_date = CURRENT_DATE AND market_relevance > 0
    """
    result = hook.get_first(market_query)
    if result:
        count, avg_relevance, max_uses = result
        print(f"📊 오늘 시장 관련 태그: {count}개, 평균 관련도: {avg_relevance:.1f}, 최대 사용량: {max_uses}")
    
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