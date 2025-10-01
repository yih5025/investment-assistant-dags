# dags/ingest_ipo_calendar_to_db_k8s.py

from datetime import datetime, timedelta
from io import StringIO
import csv
import requests
from decimal import Decimal
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Git-Sync 환경에서 SQL 파일 경로
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# upsert SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_ipo_calendar.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ingest_ipo_calendar_k8s',
    default_args=default_args,
    schedule_interval='@monthly',  # 매월 1일 자동 실행
    catchup=False,
    description='Fetch 3-month IPO calendar from Alpha Vantage and upsert into PostgreSQL',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['ipo', 'calendar', 'alpha_vantage', 'k8s'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_ipo_calendar_table',
        postgres_conn_id='postgres_default',
        sql='create_ipo_calendar.sql',
    )

    def fetch_ipo_calendar(**context):
        """Alpha Vantage API에서 IPO 캘린더 데이터 수집"""
        
        # API 키 가져오기
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY_5')
        
        print(f"🔑 API 키 확인: {api_key[:8]}...")
        print(f"📅 수집 시작: IPO Calendar (3개월)")
        
        # API 요청
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "IPO_CALENDAR",
                "apikey": api_key
            },
            timeout=60
        )
        resp.raise_for_status()
        
        # 응답 검증
        if 'Error Message' in resp.text:
            raise ValueError(f"API 오류: {resp.text}")
        
        if not resp.text or len(resp.text.strip()) == 0:
            raise ValueError("API에서 빈 응답을 받았습니다")
        
        # CSV 데이터 파싱
        rows = list(csv.DictReader(StringIO(resp.text)))
        
        if not rows:
            print("⚠️ IPO 일정이 없습니다 (3개월 내)")
            return 0
        
        print(f"✅ IPO 캘린더 데이터 수집 완료: {len(rows)}개 레코드")
        print(f"📋 샘플 데이터: {rows[0]}")
        
        # 날짜별 통계
        date_counts = {}
        for row in rows:
            ipo_date = row.get('ipoDate', 'Unknown')
            date_counts[ipo_date] = date_counts.get(ipo_date, 0) + 1
        
        print(f"📊 날짜별 IPO 분포 (상위 10개):")
        for date, count in sorted(date_counts.items())[:10]:
            print(f"   {date}: {count}개")
        
        # XCom에 저장
        context['ti'].xcom_push(key='ipo_rows', value=rows)
        return len(rows)

    def upsert_ipo_calendar(**context):
        """데이터를 PostgreSQL에 저장"""
        
        rows = context['ti'].xcom_pull(task_ids='fetch_ipo_calendar', key='ipo_rows')
        
        if not rows:
            print("⚠️ 이전 태스크에서 데이터를 받지 못했습니다")
            return 0
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        success_count = 0
        error_count = 0
        
        print(f"🚀 데이터 저장 시작: {len(rows)}개 레코드")
        
        for i, row in enumerate(rows):
            try:
                # 필수 필드 검증
                if not row.get('symbol') or not row.get('ipoDate'):
                    print(f"⚠️ 필수 필드 누락: {row}")
                    error_count += 1
                    continue
                
                # 날짜 형식 검증 (M/D/YY → YYYY-MM-DD)
                ipo_date_raw = row.get('ipoDate', '').strip()
                if not ipo_date_raw:
                    error_count += 1
                    continue
                
                # 날짜 파싱 (예: "10/1/25" → "2025-10-01")
                try:
                    ipo_date_obj = datetime.strptime(ipo_date_raw, '%m/%d/%y')
                    ipo_date = ipo_date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    # 다른 형식 시도 (YYYY-MM-DD)
                    try:
                        ipo_date_obj = datetime.strptime(ipo_date_raw, '%Y-%m-%d')
                        ipo_date = ipo_date_raw
                    except ValueError:
                        print(f"❌ 날짜 형식 오류: {ipo_date_raw}")
                        error_count += 1
                        continue
                
                # 가격 범위 처리
                price_low = None
                price_high = None
                
                if row.get('priceRangeLow') and str(row.get('priceRangeLow')).strip():
                    try:
                        price_low = Decimal(str(row.get('priceRangeLow')))
                    except:
                        price_low = None
                
                if row.get('priceRangeHigh') and str(row.get('priceRangeHigh')).strip():
                    try:
                        price_high = Decimal(str(row.get('priceRangeHigh')))
                    except:
                        price_high = None
                
                # SQL 실행
                hook.run(UPSERT_SQL, parameters={
                    'symbol': row['symbol'].strip().upper(),
                    'company_name': row.get('name', '').strip() or row['symbol'],
                    'ipo_date': ipo_date,
                    'price_range_low': price_low,
                    'price_range_high': price_high,
                    'currency': row.get('currency', 'USD').strip(),
                    'exchange': row.get('exchange', '').strip() or None
                })
                
                success_count += 1
                
                # 진행률 표시
                if (i + 1) % 10 == 0:
                    print(f"📊 진행률: {i+1}/{len(rows)} ({(i+1)/len(rows)*100:.1f}%)")
                
            except Exception as e:
                print(f"❌ 레코드 저장 실패: {row.get('symbol', 'Unknown')} - {str(e)}")
                error_count += 1
                continue
        
        print(f"\n✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
        
        # 최종 통계
        result = hook.get_first("SELECT COUNT(*) FROM ipo_calendar")
        total_records = result[0] if result else 0
        print(f"📊 총 IPO 캘린더 레코드 수: {total_records}")
        
        # 최근 IPO 일정 확인
        recent_ipos = hook.get_records("""
            SELECT ipo_date, COUNT(*) as count
            FROM ipo_calendar
            WHERE ipo_date >= CURRENT_DATE
            GROUP BY ipo_date
            ORDER BY ipo_date
            LIMIT 10
        """)
        
        if recent_ipos:
            print(f"\n📅 다가오는 IPO 일정:")
            for ipo_date, count in recent_ipos:
                print(f"   {ipo_date}: {count}개")
        
        return success_count

    # Task 정의
    fetch = PythonOperator(
        task_id='fetch_ipo_calendar',
        python_callable=fetch_ipo_calendar,
    )

    upsert = PythonOperator(
        task_id='upsert_ipo_calendar',
        python_callable=upsert_ipo_calendar,
    )

    # Task 의존성
    create_table >> fetch >> upsert