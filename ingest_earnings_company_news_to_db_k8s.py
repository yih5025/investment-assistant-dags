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
    schedule_interval='0 9,15,21 * * *',
    catchup=False,
    description='Fetch news for trending stocks from top_gainers table with Rate Limit protection',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['company', 'news', 'finnhub', 'k8s', 'top-gainers', 'trending', 'rate-limit-safe'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_company_news_table',
        postgres_conn_id='postgres_default',
        sql='create_company_news.sql',
    )

    def fetch_and_upsert_trending_news(**context):
        """최신 트렌딩 종목(50개) 뉴스 수집 및 저장"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        
        # ⭐ 핵심: 최신 batch_id의 50개 종목 모두 조회
        latest_batch_query = """
        SELECT 
            symbol, 
            category, 
            rank_position,
            change_percentage,
            volume,
            price
        FROM top_gainers 
        WHERE batch_id = (
            SELECT MAX(batch_id) FROM top_gainers
        )
        ORDER BY 
            CASE 
                WHEN category = 'top_gainers' THEN 1
                WHEN category = 'most_actively_traded' THEN 2  
                WHEN category = 'top_losers' THEN 3
                ELSE 4
            END,
            rank_position ASC
        """
        
        rows = hook.get_records(latest_batch_query)
        
        if not rows:
            print("❌ top_gainers 테이블에 데이터가 없습니다.")
            return 0
        
        # ⭐ 정확한 50개 확인
        if len(rows) != 50:
            print(f"⚠️ 예상과 다른 데이터 수: {len(rows)}개 (예상: 50개)")
        
        # 배치 정보 상세 출력
        batch_info_query = """
        SELECT 
            batch_id, 
            last_updated, 
            COUNT(*) as total_count,
            COUNT(CASE WHEN category = 'top_gainers' THEN 1 END) as gainers,
            COUNT(CASE WHEN category = 'most_actively_traded' THEN 1 END) as active,
            COUNT(CASE WHEN category = 'top_losers' THEN 1 END) as losers
        FROM top_gainers 
        WHERE batch_id = (SELECT MAX(batch_id) FROM top_gainers)
        GROUP BY batch_id, last_updated
        """
        
        batch_info = hook.get_first(batch_info_query)
        if batch_info:
            batch_id, last_updated, total, gainers, active, losers = batch_info
            print(f"📊 배치 정보: ID={batch_id}, 업데이트={last_updated}")
            print(f"📈 구성: 상승{gainers}개 + 활발{active}개 + 하락{losers}개 = 총{total}개")
            
            # ⭐ 50개 확인
            if total == 50:
                print("✅ 정확히 50개 트렌딩 종목 확인됨")
            else:
                print(f"⚠️ 비정상적인 데이터 수: {total}개")
        
        success_count = 0
        error_count = 0 
        api_call_count = 0
        
        # 시간 범위 설정 (최근 24시간)
        from_date = (datetime.today() - timedelta(hours=24)).strftime("%Y-%m-%d")
        to_date = datetime.today().strftime("%Y-%m-%d")
        
        start_time = datetime.now()
        
        print(f"🚀 50개 트렌딩 종목 뉴스 수집 시작...")
        print(f"⏱️ 예상 소요 시간: {50 * 3 / 60:.1f}분 (3초 딜레이)")
        
        for i, row in enumerate(rows):
            symbol, category, rank_position, change_percentage, volume, price = row
            
            try:
                # ⭐ 50개 모두 3초 딜레이 (Rate Limit 방지)
                if i > 0:
                    print(f"⏳ 3초 대기... ({i+1}/50) - {symbol} ({category})")
                    time.sleep(3.0)
                
                call_start = datetime.now()
                
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
                call_duration = (datetime.now() - call_start).total_seconds()
                
                # Rate Limit 체크
                if resp.status_code == 429:
                    print(f"⚠️ Rate Limit: {symbol} ({category}) - 10초 추가 대기")
                    time.sleep(10.0)
                    error_count += 1
                    continue
                
                resp.raise_for_status()
                articles = resp.json() or []
                
                # ⭐ 트렌딩 종목 상세 정보 로깅
                print(f"📰 {symbol} ({category}, #{rank_position}): {len(articles)}개 기사 "
                      f"[{change_percentage} 변동, ${price}] ({call_duration:.1f}초)")
                
                # 각 기사 저장
                article_success = 0
                for article in articles:
                    try:
                        if not article.get('url') or not article.get('datetime'):
                            continue
                        
                        published_at = datetime.fromtimestamp(article['datetime']).isoformat()
                        
                        # ⭐ 트렌딩 카테고리 메타데이터 포함
                        content_with_meta = f"[{category}_rank_{rank_position}] {article.get('summary', '')}"
                        
                        hook.run(UPSERT_SQL, parameters={
                            'symbol': symbol,
                            'source': article.get('source', ''),
                            'url': article['url'],
                            'title': article.get('headline', ''),
                            'description': article.get('summary', ''),
                            'content': content_with_meta,  # 순위 정보까지 포함
                            'published_at': published_at,
                        })
                        
                        article_success += 1
                        
                    except Exception as e:
                        print(f"❌ 기사 저장 실패: {symbol} - {str(e)}")
                        continue
                
                success_count += article_success
                
                # ⭐ 50개 기준 진행률 표시
                if (i + 1) % 10 == 0:  # 10개마다 표시
                    elapsed = (datetime.now() - start_time).total_seconds()
                    remaining = (50 - (i + 1)) * 3
                    progress_pct = ((i + 1) / 50) * 100
                    print(f"📊 진행률: {i+1}/50 ({progress_pct:.1f}%) "
                          f"- 경과: {elapsed/60:.1f}분, 남은시간: {remaining/60:.1f}분")
                
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    print(f"🚨 HTTP 429: {symbol} ({category}) - 15초 대기")
                    time.sleep(15.0)
                else:
                    print(f"❌ {symbol} ({category}) HTTP 에러: {str(e)}")
                error_count += 1
                
            except requests.exceptions.Timeout:
                print(f"⏱️ {symbol} ({category}) 타임아웃 - 5초 대기 후 계속")
                time.sleep(5.0)
                error_count += 1
                
            except Exception as e:
                print(f"❌ {symbol} ({category}) API 호출 실패: {str(e)}")
                error_count += 1
                continue
        
        # ⭐ 50개 트렌딩 종목 최종 통계
        total_elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n🏁 50개 트렌딩 종목 처리 완료 - 소요시간: {total_elapsed/60:.1f}분")
        print(f"📞 총 API 호출: {api_call_count}회 / 50회")
        
        # 카테고리별 통계
        category_stats = {}
        for row in rows:
            category = row[1]
            category_stats[category] = category_stats.get(category, 0) + 1
        
        print(f"📊 트렌딩 카테고리별 처리:")
        for category, count in category_stats.items():
            print(f"   - {category}: {count}개")
        
        print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
        
        if api_call_count > 0:
            success_rate = (api_call_count - error_count) / api_call_count * 100
            print(f"📈 API 성공률: {success_rate:.1f}%")
        
        # 최종 DB 통계
        result = hook.get_first("SELECT COUNT(*) FROM company_news")
        total_records = result[0] if result else 0
        print(f"📊 총 기업 뉴스 레코드 수: {total_records}")
        
        # ⭐ 오늘 트렌딩 뉴스 상세 통계
        today_trending_query = """
        SELECT 
            COUNT(DISTINCT symbol) as unique_symbols, 
            COUNT(*) as total_articles,
            COUNT(CASE WHEN content LIKE '[top_gainers%' THEN 1 END) as gainer_articles,
            COUNT(CASE WHEN content LIKE '[most_actively_traded%' THEN 1 END) as active_articles,
            COUNT(CASE WHEN content LIKE '[top_losers%' THEN 1 END) as loser_articles
        FROM company_news 
        WHERE fetched_at >= CURRENT_DATE
        AND content LIKE '[%]%'
        """
        
        today_stats = hook.get_first(today_trending_query)
        if today_stats:
            unique_symbols, total_articles, gainer_arts, active_arts, loser_arts = today_stats
            print(f"🔥 오늘 트렌딩 뉴스 요약:")
            print(f"   - 총 {unique_symbols}개 종목, {total_articles}개 기사")
            print(f"   - 상승주 뉴스: {gainer_arts}개")
            print(f"   - 활발주 뉴스: {active_arts}개") 
            print(f"   - 하락주 뉴스: {loser_arts}개")
        
        return success_count

    fetch_upsert = PythonOperator(
        task_id='fetch_and_upsert_trending_news',
        python_callable=fetch_and_upsert_trending_news,
    )

    # Task 의존성
    create_table >> fetch_upsert