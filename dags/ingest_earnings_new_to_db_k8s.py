'''Earnings news ingestion DAG for K3s cluster environment'''
from datetime import datetime, timedelta
import requests
import os
import logging
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# K3s 클러스터 환경에 맞는 경로 설정
BASE_DIR = '/opt/airflow/dags'  # Git-Sync로 마운트된 루트
SQL_DIR = os.path.join(BASE_DIR, 'sql')  # /opt/airflow/dags/sql/

def get_upsert_sql():
    """SQL 파일을 동적으로 읽어오는 함수"""
    sql_file_path = os.path.join(SQL_DIR, "upsert_earnings_news_finnhub.sql")
    try:
        with open(sql_file_path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logging.error(f"SQL 파일을 찾을 수 없습니다: {sql_file_path}")
        # 백업 경로 시도
        backup_path = "/opt/airflow/dags/dags/sql/upsert_earnings_news_finnhub.sql"
        try:
            with open(backup_path, encoding="utf-8") as f:
                logging.info(f"백업 경로에서 SQL 파일 로드: {backup_path}")
                return f.read()
        except FileNotFoundError:
            logging.error(f"백업 경로에서도 SQL 파일을 찾을 수 없습니다: {backup_path}")
            raise

default_args = {
    "owner": "investment_assistant",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 1),
    "retries": 3,  # 클러스터 환경에서는 재시도 횟수 증가
    "retry_delay": timedelta(minutes=2),  # 재시도 간격 증가
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "ingest_earnings_news_to_db",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql', '/opt/airflow/dags/dags/sql'],  # 양쪽 경로 모두 지원
    description='Fetch earnings calendar entries and upsert related news (K3s optimized)',
    tags=['earnings', 'news', 'finnhub', 'k3s'],
) as dag:

    create_news_table = PostgresOperator(
        task_id='create_earnings_news_table',
        postgres_conn_id='postgres_default',
        sql='create_earnings_news_finnhub.sql',
        autocommit=True,  # K3s 환경에서 명시적 커밋
    )

    def fetch_calendar_entries(**ctx):
        """실적 발표 캘린더에서 최근 항목들을 가져오는 함수"""
        try:
            pg = PostgresHook(postgres_conn_id='postgres_default')
            
            # 연결 테스트
            logging.info("PostgreSQL 연결 테스트 중...")
            test_result = pg.get_first("SELECT 1 as test")
            logging.info(f"DB 연결 성공: {test_result}")
            
            # 캘린더 데이터 조회
            query = """
                SELECT symbol, report_date
                FROM public.earnings_calendar
                WHERE report_date BETWEEN 
                    current_date - INTERVAL '3 days' AND 
                    current_date + INTERVAL '7 days'
                ORDER BY report_date
                LIMIT 50;
            """
            
            rows = pg.get_records(query)
            logging.info(f"[fetch_calendar_entries] 조회된 캘린더 항목: {len(rows)}개")
            
            if rows:
                logging.info(f"[fetch_calendar_entries] 샘플 데이터: {rows[:3]}")
            else:
                logging.warning("[fetch_calendar_entries] 캘린더 데이터가 없습니다.")
            
            # XCom에 데이터 저장
            ctx['ti'].xcom_push(key='calendar_entries', value=rows)
            return len(rows)
            
        except Exception as e:
            logging.error(f"[fetch_calendar_entries] 오류 발생: {str(e)}")
            raise

    fetch_calendar = PythonOperator(
        task_id='fetch_calendar_entries',
        python_callable=fetch_calendar_entries,
    )

    def fetch_and_upsert_news(**ctx):
        """캘린더 항목별로 뉴스를 가져와서 DB에 저장하는 함수"""
        try:
            # XCom에서 캘린더 데이터 가져오기
            entries = ctx['ti'].xcom_pull(task_ids='fetch_calendar_entries', key='calendar_entries')
            if not entries:
                raise AirflowSkipException("캘린더 항목이 없어서 작업을 건너뜁니다.")

            # 필요한 리소스 초기화
            pg = PostgresHook(postgres_conn_id='postgres_default')
            
            # API 키 가져오기 (환경변수에서 또는 Variable에서)
            try:
                api_key = Variable.get('FINNHUB_API_KEY')
                logging.info("Finnhub API 키를 Variable에서 가져왔습니다.")
            except:
                api_key = os.environ.get('FINNHUB_API_KEY')
                if not api_key:
                    raise ValueError("FINNHUB_API_KEY를 찾을 수 없습니다. Variable 또는 환경변수를 확인하세요.")
                logging.info("Finnhub API 키를 환경변수에서 가져왔습니다.")

            # SQL 쿼리 동적 로드
            UPSERT_NEWS_SQL = get_upsert_sql()
            logging.info("SQL 쿼리를 성공적으로 로드했습니다.")

            processed = []
            skipped = []
            errors = []

            for i, (symbol, report_date) in enumerate(entries):
                logging.info(f"[{i+1}/{len(entries)}] 처리 시작: {symbol} (보고일: {report_date})")
                
                try:
                    # 날짜 범위 설정 (보고일 기준 14일 전부터)
                    to_date = report_date.isoformat()
                    from_date = (report_date - timedelta(days=14)).isoformat()

                    # Finnhub API 호출
                    params = {
                        "symbol": symbol,
                        "from": from_date,
                        "to": to_date,
                        "token": api_key,
                    }
                    
                    response = requests.get(
                        "https://finnhub.io/api/v1/company-news",
                        params=params,
                        timeout=30  # 타임아웃 설정
                    )
                    response.raise_for_status()

                    articles = response.json() or []
                    logging.info(f"[{symbol}] {len(articles)}개 기사 수집 완료")

                    # 각 기사를 DB에 저장
                    for article in articles:
                        try:
                            published_at = datetime.fromtimestamp(article["datetime"]).isoformat()
                            
                            upsert_params = {
                                "symbol": symbol,
                                "report_date": report_date,
                                "category": article.get("category"),
                                "article_id": article.get("id"),
                                "headline": article.get("headline"),
                                "image": article.get("image"),
                                "related": article.get("related"),
                                "source": article.get("source"),
                                "summary": article.get("summary"),
                                "url": article.get("url"),
                                "published_at": published_at,
                            }
                            
                            # DB에 upsert 실행
                            pg.run(UPSERT_NEWS_SQL, parameters=upsert_params, autocommit=True)
                            
                        except Exception as article_error:
                            logging.error(f"[{symbol}] 기사 저장 실패: {str(article_error)}")
                            continue

                    processed.append(symbol)
                    
                    # API 호출 간격 조절 (Rate Limit 방지)
                    time.sleep(1.2)  # 조금 더 여유있게
                    
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        logging.warning(f"[{symbol}] Rate Limit 도달, 60초 대기 후 스킵")
                        time.sleep(60)
                        skipped.append(symbol)
                    else:
                        logging.error(f"[{symbol}] HTTP 오류: {e}")
                        errors.append(symbol)
                        
                except Exception as e:
                    logging.error(f"[{symbol}] 처리 중 오류: {str(e)}")
                    errors.append(symbol)
                    continue

            # 최종 결과 로깅
            logging.info(f"처리 완료 - 성공: {len(processed)}, 스킵: {len(skipped)}, 실패: {len(errors)}")
            logging.info(f"성공: {processed}")
            if skipped:
                logging.warning(f"스킵: {skipped}")
            if errors:
                logging.error(f"실패: {errors}")

            # 결과를 XCom에 저장
            ctx['ti'].xcom_push(key='processing_summary', value={
                'processed': processed,
                'skipped': skipped,
                'errors': errors,
                'total_entries': len(entries)
            })

            return {
                'processed_count': len(processed),
                'skipped_count': len(skipped),
                'error_count': len(errors)
            }

        except Exception as e:
            logging.error(f"[fetch_and_upsert_news] 전체 작업 실패: {str(e)}")
            raise

    upsert_news = PythonOperator(
        task_id='fetch_and_upsert_news',
        python_callable=fetch_and_upsert_news,
    )

    # Task 의존성 설정
    create_news_table >> fetch_calendar >> upsert_news