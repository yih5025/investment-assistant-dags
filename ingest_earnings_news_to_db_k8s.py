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
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_earnings_news_finnhub.sql"), encoding="utf-8") as f:
    UPSERT_NEWS_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_earnings_news_to_db_k8s',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # 매일 새벽 3시 (company_news 이후)
    catchup=False,
    description='Fetch 14-day earnings news for upcoming reports (dedup optimized)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['earnings', 'news', 'finnhub', '14-day-range', 'dedup', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_earnings_news_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_news_finnhub.sql',
    )

    def fetch_14day_earnings_news(**context):
        """14일 후 실적 기업의 뉴스 수집 (중복 방지)"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        api_key = Variable.get('FINNHUB_API_KEY')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        
        execution_date = context['execution_date'].date()
        
        # ⭐ 핵심: 오늘부터 14일 후까지의 실적 발표 기업들 조회
        start_date = execution_date  # 오늘 (7/31)
        end_date = execution_date + timedelta(days=14)  # 14일 후 (8/14)
        
        print(f"📅 처리 대상 기간: {start_date} ~ {end_date}")
        
        # earnings_calendar에서 해당 기간 실적 발표 기업 조회
        calendar_query = """
        SELECT DISTINCT 
            symbol, 
            report_date,
            company_name
        FROM earnings_calendar
        WHERE report_date BETWEEN %s AND %s
        AND symbol NOT IN (
            -- ⭐ 중복 방지: 이미 뉴스 수집된 기업은 제외
            SELECT DISTINCT symbol 
            FROM earnings_news_finnhub enf
            WHERE enf.report_date BETWEEN %s AND %s
            AND enf.fetched_at >= %s  -- 최근 7일 내 수집된 것만 확인
        )
        ORDER BY report_date ASC, symbol ASC
        """
        
        calendar_entries = hook.get_records(calendar_query, parameters=[
            start_date, end_date,                    # WHERE 절용
            start_date, end_date,                    # NOT IN 절용  
            execution_date - timedelta(days=7)      # 최근 7일
        ])
        
        if not calendar_entries:
            print("📭 처리할 실적 발표 기업이 없습니다.")
            # 데이터 현황 확인
            total_earnings = hook.get_first("""
                SELECT COUNT(*) FROM earnings_calendar 
                WHERE report_date BETWEEN %s AND %s
            """, parameters=[start_date, end_date])
            
            already_processed = hook.get_first("""
                SELECT COUNT(DISTINCT symbol) FROM earnings_news_finnhub 
                WHERE report_date BETWEEN %s AND %s
            """, parameters=[start_date, end_date])
            
            print(f"📊 기간 내 총 실적: {total_earnings[0] if total_earnings else 0}개")
            print(f"📊 이미 처리됨: {already_processed[0] if already_processed else 0}개")
            
            raise AirflowSkipException("처리할 실적 뉴스가 없습니다")
        
        print(f"🎯 {len(calendar_entries)}개 기업의 실적 뉴스 수집 시작")
        
        # 기간별 통계 출력
        date_stats = {}
        for _, report_date, _ in calendar_entries:
            date_str = report_date.isoformat()
            date_stats[date_str] = date_stats.get(date_str, 0) + 1
        
        print(f"📊 일자별 실적 발표 현황:")
        for date_str, count in sorted(date_stats.items())[:5]:  # 상위 5일만 표시
            print(f"   - {date_str}: {count}개")
        if len(date_stats) > 5:
            print(f"   - ... 총 {len(date_stats)}일간")
        
        success_count = 0
        error_count = 0
        api_call_count = 0
        start_time = datetime.now()
        
        for i, (symbol, report_date, company_name) in enumerate(calendar_entries):
            try:
                # ⭐ 핵심: 실적일 기준 14일 전부터 실적일까지 뉴스 수집
                from_date = (report_date - timedelta(days=14)).isoformat()
                to_date = report_date.isoformat()
                
                # ⭐ Rate Limit 안전 처리 (5초 딜레이)
                if i > 0:
                    time.sleep(5.0)  # 로그 없이 조용히 5초 대기
                
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
                    print(f"⚠️ Rate Limit: {symbol} ({report_date}) - 10초 추가 대기")
                    time.sleep(10.0)
                    error_count += 1
                    continue
                
                resp.raise_for_status()
                articles = resp.json() or []
                
                company_display = company_name if company_name else "N/A"
                print(f"📰 {symbol} ({company_display}): {len(articles)}개 기사 "
                      f"[실적일: {report_date}, 범위: {from_date}~{to_date}] ({call_duration:.1f}초)")
                
                # 각 기사 저장
                article_success = 0
                for article in articles:
                    try:
                        if not article.get('url') or not article.get('datetime'):
                            continue
                        
                        # ⭐ 중복 체크 강화 (같은 URL이면 스킵)
                        existing_check = hook.get_first("""
                            SELECT 1 FROM earnings_news_finnhub 
                            WHERE url = %s
                        """, parameters=[article['url']])
                        
                        if existing_check:
                            continue  # 이미 존재하는 뉴스
                        
                        published_at = datetime.fromtimestamp(article['datetime']).isoformat()
                        
                        hook.run(UPSERT_NEWS_SQL, parameters={
                            'symbol': symbol,
                            'report_date': report_date,
                            'category': article.get('category', ''),
                            'article_id': article.get('id'),
                            'headline': article.get('headline', ''),
                            'image': article.get('image', ''),
                            'related': article.get('related', ''),
                            'source': article.get('source', ''),
                            'summary': article.get('summary', ''),
                            'url': article['url'],
                            'published_at': published_at,
                        })
                        
                        article_success += 1
                        
                    except Exception as e:
                        print(f"❌ 기사 저장 실패: {symbol} - {str(e)}")
                        continue
                
                success_count += article_success
                
                # ⭐ 진행률 표시 간소화 (20개마다)
                if (i + 1) % 20 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    remaining = (len(calendar_entries) - (i + 1)) * 5  # 5초 딜레이 반영
                    progress_pct = ((i + 1) / len(calendar_entries)) * 100
                    print(f"📊 진행률: {i+1}/{len(calendar_entries)} ({progress_pct:.0f}%) "
                          f"- 경과: {elapsed/60:.1f}분, 예상잔여: {remaining/60:.0f}분")
                
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    print(f"🚨 Rate Limit: {symbol} - 20초 대기")
                    time.sleep(20.0)
                else:
                    print(f"❌ {symbol} HTTP 에러: {str(e)}")
                error_count += 1
                
            except requests.exceptions.Timeout:
                print(f"⏱️ {symbol} 타임아웃")
                time.sleep(5.0)
                error_count += 1
                
            except Exception as e:
                print(f"❌ {symbol} 실패: {str(e)}")
                error_count += 1
                continue
        
        # ⭐ 최종 통계 (핵심 정보만)
        total_elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n🏁 완료 - 소요시간: {total_elapsed/60:.0f}분")
        print(f"📞 API 호출: {api_call_count}회, 성공률: {((api_call_count-error_count)/api_call_count*100):.0f}%" if api_call_count > 0 else "📞 API 호출: 0회")
        print(f"✅ 뉴스 저장: {success_count}개")
        
        # 간단한 DB 통계
        today_added = hook.get_first("SELECT COUNT(*) FROM earnings_news_finnhub WHERE fetched_at >= CURRENT_DATE")
        total_records = hook.get_first("SELECT COUNT(*) FROM earnings_news_finnhub")
        
        print(f"📊 오늘 추가: {today_added[0] if today_added else 0}개, 전체: {total_records[0] if total_records else 0}개")
        
        return success_count

    fetch_upsert = PythonOperator(
        task_id='fetch_14day_earnings_news',
        python_callable=fetch_14day_earnings_news,
    )

    # Task 의존성
    create_table >> fetch_upsert