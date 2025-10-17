from datetime import datetime, timedelta
import requests
import os
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 표준 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

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
    schedule_interval='0 3,15 * * *',  # 하루 2번: 새벽 3시, 오후 3시
    catchup=False,
    description='Fetch news for S&P 500 companies (500 companies per run, 2 times daily)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['company', 'news', 'finnhub', 'k8s', 'sp500', 'batch', 'high-frequency'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_company_news_table',
        postgres_conn_id='postgres_default',
        sql='create_company_news.sql',
    )

    def fetch_and_upsert_sp500_news(**context):
        """S&P 500 기업 뉴스 수집 및 저장 (진행형 배치 처리)"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        # 진행상황 추적 테이블 생성
        create_progress_table = """
        CREATE TABLE IF NOT EXISTS company_news_progress (
            id SERIAL PRIMARY KEY,
            collection_name TEXT NOT NULL DEFAULT 'sp500_news',
            current_position INTEGER NOT NULL DEFAULT 0,
            total_symbols INTEGER NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active'
        );
        """
        hook.run(create_progress_table)
        
        # 현재 진행상황 조회
        progress_query = """
        SELECT current_position, total_symbols 
        FROM company_news_progress 
        WHERE collection_name = 'sp500_news' AND status = 'active'
        ORDER BY last_updated DESC 
        LIMIT 1
        """
        
        progress_result = hook.get_first(progress_query)
        
        # S&P 500 기업 심볼 조회 (알파벳 순)
        sp500_query = """
        SELECT symbol, company_name, gics_sector
        FROM sp500_companies 
        WHERE symbol IS NOT NULL
        ORDER BY symbol ASC
        """
        
        all_symbols = hook.get_records(sp500_query)
        
        if not all_symbols:
            print("❌ sp500_companies 테이블에 데이터가 없습니다.")
            return 0
        
        total_count = len(all_symbols)
        
        # 진행상황 초기화 또는 로드
        if not progress_result:
            current_position = 0
            hook.run("""
                INSERT INTO company_news_progress (collection_name, current_position, total_symbols)
                VALUES ('sp500_news', 0, %s)
            """, parameters=[total_count])
        else:
            current_position = progress_result[0]
        
        # 오늘 수집할 배치 크기 (Finnhub Rate Limit: 분당 60회)
        # 2초 딜레이 → 분당 30회 → 500개 전체 처리 가능 (약 16.7분 소요)
        batch_size = 500  # 한 번에 전체 처리
        end_position = min(current_position + batch_size, total_count)
        
        if current_position >= total_count:
            print("✅ 모든 S&P 500 기업 뉴스 수집 완료!")
            hook.run("""
                UPDATE company_news_progress 
                SET status = 'completed', last_updated = CURRENT_TIMESTAMP
                WHERE collection_name = 'sp500_news' AND status = 'active'
            """)
            # 다음 주기를 위해 리셋
            hook.run("""
                UPDATE company_news_progress 
                SET current_position = 0, status = 'active', last_updated = CURRENT_TIMESTAMP
                WHERE collection_name = 'sp500_news'
            """)
            return 0
        
        # 오늘 처리할 기업들
        today_batch = all_symbols[current_position:end_position]
        batch_symbols = [row[0] for row in today_batch]
        
        print(f"🚀 S&P 500 뉴스 수집 시작: {len(batch_symbols)}개 기업 ({current_position}/{total_count})")
        
        success_count = 0
        error_count = 0 
        api_call_count = 0
        successful_symbols = []
        
        # 시간 범위 설정 (최근 7일)
        from_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        to_date = datetime.today().strftime("%Y-%m-%d")
        
        start_time = datetime.now()
        
        for i, row in enumerate(today_batch):
            symbol, company_name, sector = row
            
            try:
                # 2초 딜레이 (Rate Limit 방지: 분당 30회)
                if i > 0:
                    time.sleep(2.0)
                
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
                
                api_call_count += 1
                
                # Rate Limit 체크
                if resp.status_code == 429:
                    print(f"⚠️ Rate Limit: {symbol}")
                    time.sleep(15.0)
                    error_count += 1
                    continue
                
                resp.raise_for_status()
                articles = resp.json() or []
                
                # 각 기사 저장 (중복 체크)
                article_success = 0
                
                for article in articles:
                    try:
                        if not article.get('url') or not article.get('datetime'):
                            continue
                        
                        url = article['url']
                        
                        # 중복 URL 체크 (symbol + url 조합)
                        existing = hook.get_first("""
                            SELECT 1 FROM company_news 
                            WHERE symbol = %s AND url = %s
                        """, parameters=[symbol, url])
                        
                        if existing:
                            continue  # 이미 존재하는 기사는 스킵
                        
                        published_at = datetime.fromtimestamp(article['datetime']).isoformat()
                        
                        hook.run(UPSERT_SQL, parameters={
                            'symbol': symbol,
                            'source': article.get('source', ''),
                            'url': url,
                            'title': article.get('headline', ''),
                            'description': article.get('summary', ''),
                            'content': article.get('summary', ''),
                            'published_at': published_at,
                        })
                        
                        article_success += 1
                        
                    except Exception:
                        continue
                
                if article_success > 0:
                    successful_symbols.append(symbol)
                    success_count += article_success
                
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    print(f"⚠️ Rate Limit: {symbol}")
                    time.sleep(15.0)
                error_count += 1
                
            except requests.exceptions.Timeout:
                time.sleep(5.0)
                error_count += 1
                
            except Exception:
                error_count += 1
                continue
        
        # 진행상황 업데이트 (성공한 만큼만)
        if len(successful_symbols) > 0:
            new_position = current_position + len(successful_symbols)
            hook.run("""
                UPDATE company_news_progress 
                SET current_position = %s, last_updated = CURRENT_TIMESTAMP
                WHERE collection_name = 'sp500_news' AND status = 'active'
            """, parameters=[new_position])
        
        # 최종 통계
        total_elapsed = (datetime.now() - start_time).total_seconds()
        new_position = current_position + len(successful_symbols)
        
        print(f"✅ 완료: {success_count}개 뉴스 ({len(successful_symbols)}개 기업) | "
              f"진행: {new_position}/{total_count} | "
              f"실패: {error_count}개 | "
              f"소요: {total_elapsed/60:.1f}분")
        
        return success_count

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_sp500_news',
        python_callable=fetch_and_upsert_sp500_news,
    )

    # Task 의존성
    create_table >> fetch_upsert