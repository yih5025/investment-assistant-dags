"""
Treasury Yield 데이터 수집 DAG (최적화된 버전)
- Alpha Vantage API에서 핵심 미국 국채 수익률 데이터 수집
- 3개 조합만 수집: monthly 2year, 10year, 30year
- PostgreSQL에 upsert 처리
"""

from datetime import datetime, timedelta
import requests
import os
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# SQL 파일 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_treasury_yield.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# Alpha Vantage 설정 (최적화된 조합)
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
CORE_COMBINATIONS = [
    ('monthly', '2year'),   # 단기 금리 정책
    ('monthly', '10year'),  # 경제 전망 기준점 (가장 중요)
    ('monthly', '30year'),  # 장기 인플레이션 기대
]

def fetch_treasury_yield_data(**context):
    """Alpha Vantage API에서 핵심 Treasury Yield 데이터 수집"""
    
    # API 키 가져오기
    api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY가 설정되지 않았습니다")
    
    all_data = []
    success_count = 0
    error_count = 0
    
    print(f"🚀 Treasury Yield 데이터 수집 시작: {len(CORE_COMBINATIONS)}개 핵심 조합")
    
    # 핵심 조합에 대해서만 데이터 수집
    for interval, maturity in CORE_COMBINATIONS:
        try:
            # API 파라미터 설정
            params = {
                'function': 'TREASURY_YIELD',
                'interval': interval,
                'maturity': maturity,
                'datatype': 'json',
                'apikey': api_key
            }
            
            print(f"📊 요청 중: {interval} {maturity}")
            
            # API 호출
            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            # 응답 검증
            if 'Error Message' in response.text:
                raise ValueError(f"API 오류: {response.text}")
            
            if 'Note:' in response.text and 'API call frequency' in response.text:
                raise ValueError("API 호출 한도 초과")
            
            data = response.json()
            
            # 데이터 구조 확인
            if 'data' not in data:
                print(f"⚠️ 데이터 없음: {interval} {maturity}")
                continue
            
            # 데이터 처리
            time_series = data['data']
            processed_count = 0
            
            for item in time_series:
                try:
                    record = {
                        'date': item['date'],
                        'maturity': maturity,
                        'interval_type': interval,
                        'yield_rate': Decimal(str(item['value'])) if item['value'] else None
                    }
                    all_data.append(record)
                    processed_count += 1
                except (KeyError, ValueError, TypeError) as e:
                    print(f"⚠️ 데이터 파싱 오류: {item} - {str(e)}")
                    continue
            
            success_count += 1
            print(f"✅ 성공: {interval} {maturity} - {processed_count}개 레코드")
            
        except Exception as e:
            error_count += 1
            print(f"❌ 실패: {interval} {maturity} - {str(e)}")
            continue
    
    # 최종 통계
    print(f"🎯 수집 완료: {success_count}개 성공, {error_count}개 실패")
    print(f"📊 총 레코드 수: {len(all_data)}")
    
    if not all_data:
        raise ValueError("수집된 데이터가 없습니다")
    
    # XCom에 데이터 저장
    context['ti'].xcom_push(key='treasury_yield_data', value=all_data)
    return len(all_data)

def upsert_treasury_yield_to_db(**context):
    """Treasury Yield 데이터를 PostgreSQL에 저장"""
    
    # XCom에서 데이터 가져오기
    data = context['ti'].xcom_pull(task_ids='fetch_treasury_yield', key='treasury_yield_data')
    
    if not data:
        raise ValueError("이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    success_count = 0
    error_count = 0
    
    print(f"🚀 데이터 저장 시작: {len(data)}개 레코드")
    
    # 배치 처리 (개별 실패가 전체에 영향 주지 않음)
    for i, item in enumerate(data):
        try:
            # 필수 필드 검증
            if not item.get('date') or not item.get('maturity'):
                error_count += 1
                continue
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=item)
            success_count += 1
            
            # 진행률 표시 (50개마다)
            if (i + 1) % 50 == 0:
                print(f"📊 진행률: {i+1}/{len(data)}")
                
        except Exception as e:
            print(f"❌ 레코드 저장 실패: {item.get('date', 'Unknown')} {item.get('maturity', '')} - {str(e)}")
            error_count += 1
            continue
    
    # 최종 통계
    print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 테이블 총 레코드 수 확인
    result = hook.get_first("SELECT COUNT(*) FROM treasury_yield")
    total_records = result[0] if result else 0
    print(f"📊 총 레코드 수: {total_records}")
    
    # 각 maturity별 최신 데이터 확인
    for interval, maturity in CORE_COMBINATIONS:
        latest = hook.get_first("""
            SELECT date, yield_rate 
            FROM treasury_yield 
            WHERE maturity = %s AND interval_type = %s 
            ORDER BY date DESC 
            LIMIT 1
        """, parameters=[maturity, interval])
        
        if latest:
            print(f"📈 {maturity} 최신: {latest[0]} = {latest[1]}%")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_treasury_yield_to_db',
    default_args=default_args,
    schedule_interval='@monthly',  # 월간 업데이트
    catchup=False,
    description='핵심 미국 국채 수익률 데이터 수집 (2year, 10year, 30year)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['treasury', 'yield', 'economics', 'alphavantage'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_treasury_yield_table',
        postgres_conn_id='postgres_default',
        sql='create_treasury_yield.sql',
    )
    
    # 2. 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_treasury_yield',
        python_callable=fetch_treasury_yield_data,
    )
    
    # 3. 데이터 저장
    upsert_data = PythonOperator(
        task_id='upsert_treasury_yield',
        python_callable=upsert_treasury_yield_to_db,
    )
    
    # Task 의존성
    create_table >> fetch_data >> upsert_data