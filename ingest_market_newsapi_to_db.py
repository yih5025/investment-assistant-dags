from datetime import datetime, timedelta
import requests
import os
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_market_news.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_market_newsapi_to_db_k8s',
    default_args=default_args,
    schedule_interval='0 4 * * *',  # 매일 새벽 4시
    catchup=False,
    description='Comprehensive news collection via NewsAPI - business, technology, markets',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['market', 'news', 'newsapi', 'comprehensive', 'business', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_market_news_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news.sql',
    )

    def fetch_comprehensive_news(**context):
        """광범위한 비즈니스/시장 뉴스 수집"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('NEWSAPI_API_KEY')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        
        # 시간 범위 설정 (어제 하루)
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()
        from_date = yesterday.isoformat()
        to_date = yesterday.isoformat()
        
        print(f"📅 수집 날짜: {from_date}")
        
        total_articles = 0
        error_count = 0
        
        # ⭐ 전략 1: 카테고리별 헤드라인 수집 (대량 수집)
        categories = ['business', 'technology', 'general', 'politics']
        
        for category in categories:
            try:
                print(f"📰 카테고리 '{category}' 헤드라인 수집 중...")
                
                # NewsAPI Headlines 엔드포인트 (더 많은 결과)
                resp = requests.get(
                    "https://newsapi.org/v2/top-headlines",
                    params={
                        'category': category,
                        'language': 'en',
                        'country': 'us',  # 미국 뉴스
                        'apiKey': api_key,
                        'pageSize': 100,  # 최대 100개
                    },
                    timeout=30
                )
                
                if resp.status_code == 429:
                    print(f"⚠️ Rate Limit 도달")
                    break
                
                resp.raise_for_status()
                data = resp.json()
                articles = data.get("articles", [])
                
                # 어제 날짜 필터링 (헤드라인은 날짜 필터가 없음)
                yesterday_articles = []
                for article in articles:
                    if article.get('publishedAt'):
                        pub_date = datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')).date()
                        if pub_date >= yesterday - timedelta(days=1):  # 어제 또는 오늘
                            yesterday_articles.append(article)
                
                # 저장
                saved_count = 0
                for article in yesterday_articles:
                    try:
                        if not article.get('url'):
                            continue
                        
                        # 중복 체크
                        existing = hook.get_first("""
                            SELECT 1 FROM market_news WHERE url = %s
                        """, parameters=[article['url']])
                        
                        if existing:
                            continue
                        
                        hook.run(UPSERT_SQL, parameters={
                            'source': article["source"]["name"] if article.get("source") else "",
                            'url': article["url"],
                            'author': article.get("author", ""),
                            'title': article.get("title", ""),
                            'description': article.get("description", ""),
                            'content': article.get("content", ""),
                            'published_at': article["publishedAt"],
                        })
                        saved_count += 1
                        
                    except Exception as e:
                        print(f"❌ 기사 저장 실패: {str(e)}")
                        continue
                
                total_articles += saved_count
                print(f"✅ {category}: {saved_count}개 저장 (전체 {len(articles)}개 중)")
                
                # API 호출 간격
                time.sleep(2.0)
                
            except Exception as e:
                print(f"❌ 카테고리 {category} 실패: {str(e)}")
                error_count += 1
                continue
        
        # ⭐ 전략 2: 간단한 키워드로 Everything 검색 (추가 수집)
        simple_keywords = [
            'economy',          # 경제
            'business',         # 비즈니스
            'technology',       # 기술
            'IPO',             # 공개상장
            'inflation',        # 인플레이션
            'tariff',           # 관세
            'trade war',        # 무역 전쟁
            'sanctions',        # 제재
            'war',             # 전쟁
            'politics',         # 정치
            'election',         # 선거
            'government policy', # 정부 정책
            'congress',         # 의회
            'diplomatic',       # 외교
            'nuclear',          # 핵 관련
            'military'          # 군사
        ]
        
        print(f"\n🔍 키워드 검색 시작...")
        
        for i, keyword in enumerate(simple_keywords):
            try:
                print(f"🔍 키워드 '{keyword}' 검색 중... ({i+1}/{len(simple_keywords)})")
                
                resp = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        'q': keyword,
                        'from': from_date,
                        'to': to_date,
                        'sortBy': 'popularity',  # 인기도 순
                        'language': 'en',
                        'apiKey': api_key,
                        'pageSize': 30,  # 키워드당 30개
                    },
                    timeout=30
                )
                
                if resp.status_code == 429:
                    print(f"⚠️ Rate Limit 도달, 키워드 검색 중단")
                    break
                
                resp.raise_for_status()
                data = resp.json()
                articles = data.get("articles", [])
                
                # 저장
                saved_count = 0
                for article in articles:
                    try:
                        if not article.get('url'):
                            continue
                        
                        # 중복 체크
                        existing = hook.get_first("""
                            SELECT 1 FROM market_news WHERE url = %s
                        """, parameters=[article['url']])
                        
                        if existing:
                            continue
                        
                        hook.run(UPSERT_SQL, parameters={
                            'source': article["source"]["name"] if article.get("source") else "",
                            'url': article["url"],
                            'author': article.get("author", ""),
                            'title': article.get("title", ""),
                            'description': article.get("description", ""),
                            'content': article.get("content", ""),
                            'published_at': article["publishedAt"],
                        })
                        saved_count += 1
                        
                    except Exception as e:
                        continue
                
                total_articles += saved_count
                print(f"✅ '{keyword}': {saved_count}개 저장")
                
                # API 호출 간격
                time.sleep(1.5)
                
            except Exception as e:
                print(f"❌ 키워드 '{keyword}' 실패: {str(e)}")
                error_count += 1
                continue
        
        # ⭐ 전략 3: 주요 비즈니스 소스에서 최신 뉴스 (보너스)
        business_sources = [
            'bloomberg',
            'reuters', 
            'cnbc',
            'the-wall-street-journal',
            'business-insider',
            'financial-times'
        ]
        
        print(f"\n📺 주요 소스별 수집...")
        
        for source in business_sources:
            try:
                print(f"📺 {source} 최신 뉴스...")
                
                resp = requests.get(
                    "https://newsapi.org/v2/top-headlines",
                    params={
                        'sources': source,
                        'apiKey': api_key,
                        'pageSize': 20,
                    },
                    timeout=30
                )
                
                if resp.status_code == 429:
                    print(f"⚠️ Rate Limit 도달, 소스별 수집 중단")
                    break
                
                resp.raise_for_status()
                data = resp.json()
                articles = data.get("articles", [])
                
                # 저장 (중복 제거)
                saved_count = 0
                for article in articles:
                    try:
                        if not article.get('url'):
                            continue
                        
                        existing = hook.get_first("""
                            SELECT 1 FROM market_news WHERE url = %s
                        """, parameters=[article['url']])
                        
                        if existing:
                            continue
                        
                        hook.run(UPSERT_SQL, parameters={
                            'source': article["source"]["name"] if article.get("source") else "",
                            'url': article["url"],
                            'author': article.get("author", ""),
                            'title': article.get("title", ""),
                            'description': article.get("description", ""),
                            'content': article.get("content", ""),
                            'published_at': article["publishedAt"],
                        })
                        saved_count += 1
                        
                    except Exception as e:
                        continue
                
                total_articles += saved_count
                print(f"✅ {source}: {saved_count}개 저장")
                
                time.sleep(1.0)
                
            except Exception as e:
                print(f"❌ 소스 {source} 실패: {str(e)}")
                error_count += 1
                continue
        
        # 최종 통계
        result = hook.get_first("SELECT COUNT(*) FROM market_news")
        total_records = result[0] if result else 0
        
        today_added = hook.get_first("""
            SELECT COUNT(*) FROM market_news 
            WHERE fetched_at >= CURRENT_DATE
        """)
        today_count = today_added[0] if today_added else 0
        
        print(f"\n🏁 완료!")
        print(f"✅ 오늘 수집: {total_articles}개")
        print(f"📊 오늘 전체: {today_count}개, 총 레코드: {total_records}개")
        print(f"❌ 에러: {error_count}개")
        
        if total_articles == 0:
            raise AirflowSkipException("뉴스 수집 실패 - Rate Limit 또는 API 문제")
        
        return total_articles

    fetch_upsert = PythonOperator(
        task_id='fetch_comprehensive_news',
        python_callable=fetch_comprehensive_news,
    )

    # Task 의존성
    create_table >> fetch_upsert