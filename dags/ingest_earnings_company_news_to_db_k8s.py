from datetime import datetime, timedelta
import requests
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_company_news.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_company_news_to_db_k8s',
    default_args=default_args,
    schedule_interval='10 * * * *',  # 매시간 10분에 실행
    catchup=False,
    description='Fetch hourly company news from Finnhub API',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['company', 'news', 'finnhub', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_company_news_table',
        postgres_conn_id='postgres_default',
        sql='create_company_news.sql',
    )

    def fetch_and_upsert_news(**context):
        """SP500 기업별 최신 뉴스 수집 및 저장"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        
        # SP500 기업 목록 조회
        rows = hook.get_records("SELECT symbol FROM sp500_top50")
        symbols = [row[0] for row in rows]
        
        print(f"📊 처리할 기업 수: {len(symbols)}개")
        
        success_count = 0
        error_count = 0
        
        # 시간 범위 설정 (최근 1시간)
        from_date = (datetime.today() - timedelta(hours=1)).strftime("%Y-%m-%d")
        to_date = datetime.today().strftime("%Y-%m-%d")
        
        for i, symbol in enumerate(symbols):
            try:
                # Finnhub API 호출
                resp = requests.get(
                    "https://finnhub.io/api/v1/company-news",
                    params={
                        "symbol": symbol,
                        "from": from_date,
                        "to": to_date,
                        "token": api_key,
                    },
                    timeout=30
                )
                resp.raise_for_status()
                
                articles = resp.json() or []
                
                # 각 기사 저장
                for article in articles:
                    try:
                        # 필수 필드 검증
                        if not article.get('url') or not article.get('datetime'):
                            continue
                        
                        # 발행 시간 변환
                        published_at = datetime.fromtimestamp(article['datetime']).isoformat()
                        
                        # 데이터 저장
                        hook.run(UPSERT_SQL, parameters={
                            'symbol': symbol,
                            'source': article.get('source', ''),
                            'url': article['url'],
                            'title': article.get('headline', ''),
                            'description': article.get('summary', ''),
                            'content': article.get('summary', ''),
                            'published_at': published_at,
                        })
                        
                        success_count += 1
                        
                    except Exception as e:
                        print(f"❌ 기사 저장 실패: {symbol} - {str(e)}")
                        error_count += 1
                        continue
                
                # 진행률 표시
                if (i + 1) % 10 == 0:
                    print(f"📊 진행률: {i+1}/{len(symbols)} 기업 처리 완료")
                
            except Exception as e:
                print(f"❌ {symbol} API 호출 실패: {str(e)}")
                error_count += 1
                continue
        
        print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
        
        # 최종 통계
        result = hook.get_first("SELECT COUNT(*) FROM company_news")
        total_records = result[0] if result else 0
        print(f"📊 총 기업 뉴스 레코드 수: {total_records}")
        
        return success_count

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_company_news',
        python_callable=fetch_and_upsert_news,
    )

    # Task 의존성
    create_table >> fetch_upsert