from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_market_news_sentiment.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# 15개 추천 쿼리 조합
QUERY_COMBINATIONS = [
    {'type': 'daily_market', 'params': 'sort=LATEST&limit=30'},
    {'type': 'fed_news', 'params': 'topics=economy_monetary&sort=LATEST&limit=20'},
    {'type': 'earnings_season', 'params': 'topics=earnings&sort=LATEST&limit=40'},
    {'type': 'bigtech_megacap', 'params': 'tickers=AAPL,MSFT,NVDA,GOOGL,AMZN,TSLA,META&sort=LATEST&limit=30'},
    {'type': 'bigtech_ai', 'params': 'tickers=NVDA,AMD,INTC,AAPL,MSFT,GOOGL&topics=technology&limit=25'},
    {'type': 'bigtech_cloud', 'params': 'tickers=MSFT,GOOGL,AMZN,CRM,ORCL&topics=technology&limit=20'},
    {'type': 'crypto_coins', 'params': 'tickers=CRYPTO:BTC,CRYPTO:ETH,CRYPTO:SOL&sort=LATEST&limit=20'},
    {'type': 'crypto_stocks', 'params': 'tickers=COIN,MSTR,RIOT,MARA,CLSK&sort=LATEST&limit=25'},
    {'type': 'crypto_surge', 'params': 'tickers=CRYPTO:BTC,CRYPTO:ETH&topics=blockchain,financial_markets&limit=30'},
    {'type': 'crypto_ecosystem', 'params': 'topics=blockchain&sort=LATEST&limit=25'},
    {'type': 'tech_innovation', 'params': 'topics=technology&sort=RELEVANCE&limit=30'},
    {'type': 'ma_opportunities', 'params': 'topics=mergers_and_acquisitions&sort=LATEST&limit=20'},
    {'type': 'financial_policy', 'params': 'tickers=JPM,BAC,WFC,C,GS&topics=economy_monetary&limit=20'},
]

def collect_news_sentiment(**context):
    """
    Market News & Sentiment API에서 데이터 수집
    """
    # API 키 가져오기
    api_key = Variable.get('ALPHA_VANTAGE_NEWS_API_KEY_2')
    if not api_key:
        raise ValueError("🔑 ALPHA_VANTAGE_NEWS_API_KEY_2가 설정되지 않았습니다")
    
    # DB 연결 및 batch_id 생성
    hook = PostgresHook(postgres_conn_id='postgres_default')
    max_batch_result = hook.get_first("SELECT COALESCE(MAX(batch_id), 0) FROM market_news_sentiment")
    current_batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
    
    print(f"🆔 배치 ID: {current_batch_id}")
    
    all_news_data = []
    total_api_calls = 0
    
    for query_combo in QUERY_COMBINATIONS:
        try:
            # API 요청
            url = "https://www.alphavantage.co/query"
            params = f"function=NEWS_SENTIMENT&{query_combo['params']}&apikey={api_key}"
            
            print(f"🚀 API 요청: {query_combo['type']}")
            response = requests.get(f"{url}?{params}", timeout=60)
            response.raise_for_status()
            total_api_calls += 1
            
            # API 응답 검증
            if 'Error Message' in response.text:
                print(f"❌ API 오류 ({query_combo['type']}): {response.text[:100]}")
                continue
                
            if 'Note' in response.text and 'API call frequency' in response.text:
                print(f"⚠️ API 호출 제한 도달: {total_api_calls}번째 호출에서 중단")
                break
            
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패 ({query_combo['type']}): {str(e)}")
                continue
            
            # 뉴스 피드 확인
            if 'feed' not in data:
                print(f"⚠️ 뉴스 피드 없음 ({query_combo['type']})")
                continue
            
            news_count = len(data['feed'])
            print(f"✅ {query_combo['type']}: {news_count}개 뉴스 수집")
            
            # 뉴스 데이터에 쿼리 정보 추가
            for article in data['feed']:
                article['query_type'] = query_combo['type']
                article['query_params'] = query_combo['params']
                article['batch_id'] = current_batch_id
            
            all_news_data.extend(data['feed'])
            
        except Exception as e:
            print(f"❌ 쿼리 실패 ({query_combo['type']}): {str(e)}")
            continue
    
    print(f"🎯 총 {total_api_calls}번 API 호출, {len(all_news_data)}개 뉴스 수집")
    
    # XCom에 저장
    context['ti'].xcom_push(key='news_data', value=all_news_data)
    context['ti'].xcom_push(key='batch_id', value=current_batch_id)
    
    return {
        'batch_id': current_batch_id,
        'total_api_calls': total_api_calls,
        'total_news': len(all_news_data)
    }

def process_and_store_news(**context):
    """
    수집된 뉴스 데이터를 가공하여 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    news_data = context['ti'].xcom_pull(task_ids='collect_news_sentiment', key='news_data')
    batch_id = context['ti'].xcom_pull(task_ids='collect_news_sentiment', key='batch_id')
    
    if not news_data:
        raise ValueError("❌ 이전 태스크에서 뉴스 데이터를 받지 못했습니다")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    
    print(f"🚀 배치 {batch_id} 뉴스 저장 시작: {len(news_data)}개")
    
    for article in news_data:
        try:
            # 필수 필드 검증
            if not article.get('url') or not article.get('title'):
                print(f"⚠️ 필수 필드 누락: {article.get('title', 'Unknown')[:50]}")
                error_count += 1
                continue
            
            # 시간 파싱
            time_published = datetime.strptime(article['time_published'], '%Y%m%dT%H%M%S')
            
            # 작성자 처리 (리스트를 문자열로 변환)
            authors = ', '.join(article.get('authors', [])) if article.get('authors') else None
            
            # 파라미터 준비
            params = {
                'batch_id': batch_id,
                'title': article['title'][:500],  # 길이 제한
                'url': article['url'],
                'time_published': time_published,
                'authors': authors[:200] if authors else None,  # 길이 제한
                'summary': article.get('summary', '')[:1000],  # 길이 제한
                'source': article.get('source', '')[:100],
                'overall_sentiment_score': Decimal(str(article.get('overall_sentiment_score', 0))),
                'overall_sentiment_label': article.get('overall_sentiment_label', 'Neutral'),
                'ticker_sentiment': json.dumps(article.get('ticker_sentiment', [])),
                'topics': json.dumps(article.get('topics', [])),
                'query_type': article['query_type'],
                'query_params': article['query_params']
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=params)
            success_count += 1
            
        except Exception as e:
            print(f"❌ 뉴스 저장 실패: {article.get('title', 'Unknown')[:50]} - {str(e)}")
            error_count += 1
            continue
    
    print(f"🎯 배치 {batch_id} 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    return {
        'batch_id': batch_id,
        'success_count': success_count,
        'error_count': error_count
    }

# DAG 정의
with DAG(
    dag_id='ingest_market_news_sentiment',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Alpha Vantage News & Sentiment API에서 시장 뉴스 및 감성 분석 데이터 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['news', 'sentiment', 'alpha_vantage', 'market', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_market_news_sentiment_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news_sentiment.sql',
    )
    
    # 2. 뉴스 데이터 수집
    collect_news = PythonOperator(
        task_id='collect_news_sentiment',
        python_callable=collect_news_sentiment,
    )
    
    # 3. 데이터 가공 및 저장
    process_news = PythonOperator(
        task_id='process_and_store_news',
        python_callable=process_and_store_news,
    )
    
    # Task 의존성
    create_table >> collect_news >> process_news