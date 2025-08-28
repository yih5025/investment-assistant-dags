from datetime import datetime, timedelta
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# SQL 파일 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_inflation.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_inflation_data(**context):
    """인플레이션 데이터 수집"""
    api_key = Variable.get('ALPHA_VANTAGE_API_KEY_1')
    url = 'https://www.alphavantage.co/query'
    
    params = {
        'function': 'INFLATION',
        'datatype': 'json',
        'apikey': api_key
    }
    
    try:
        print("📊 인플레이션 데이터 수집 시작...")
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        # API 오류 확인
        if 'Error Message' in data:
            raise ValueError(f"API 오류: {data['Error Message']}")
            
        if 'Note' in data:
            raise ValueError(f"API 제한: {data['Note']}")
            
        if 'Information' in data:
            raise ValueError(f"API 정보: {data['Information']}")
        
        # 응답 구조 확인
        if 'data' not in data:
            raise ValueError(f"예상하지 못한 응답 구조: {list(data.keys())}")
        
        # 메타데이터 추출
        metadata = {
            'name': data.get('name', 'Inflation, consumer prices for the United States'),
            'interval': data.get('interval', 'annual'),
            'unit': data.get('unit', 'percent')
        }
        
        # 실제 데이터 추출
        time_series = data.get('data', [])
        
        if not time_series:
            raise ValueError("인플레이션 데이터가 없습니다")
        
        # 최근 20년 데이터만 수집
        recent_data = time_series[:20] if len(time_series) > 20 else time_series
        
        print(f"✅ 인플레이션 데이터 수집 완료: {len(recent_data)}개 레코드")
        print(f"📊 메타데이터: {metadata}")
        print(f"📅 데이터 범위: {recent_data[-1]['date']} ~ {recent_data[0]['date']}")
        
        # XCom에 데이터와 메타데이터 저장
        context['ti'].xcom_push(key='inflation_data', value=recent_data)
        context['ti'].xcom_push(key='metadata', value=metadata)
        
        return len(recent_data)
        
    except Exception as e:
        print(f"❌ 인플레이션 데이터 수집 실패: {str(e)}")
        raise

def upsert_inflation_data(**context):
    """인플레이션 데이터 저장"""
    # XCom에서 데이터와 메타데이터 가져오기
    data = context['ti'].xcom_pull(task_ids='fetch_inflation_data', key='inflation_data')
    metadata = context['ti'].xcom_pull(task_ids='fetch_inflation_data', key='metadata')
    
    if not data:
        raise ValueError("이전 태스크에서 인플레이션 데이터를 받지 못했습니다")
    
    if not metadata:
        metadata = {
            'name': 'Inflation, consumer prices for the United States',
            'interval': 'annual', 
            'unit': 'percent'
        }
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    success_count = 0
    error_count = 0
    
    print(f"🚀 인플레이션 데이터 저장 시작: {len(data)}개 레코드")
    print(f"📊 메타데이터: {metadata}")
    
    for item in data:
        try:
            # 필수 필드 검증
            if not item.get('date') or not item.get('value'):
                print(f"⚠️ 필수 필드 누락: {item}")
                error_count += 1
                continue
            
            # value가 "." 또는 빈 값인 경우 처리
            inflation_value = item['value']
            if inflation_value in ['.', '', 'null', None]:
                print(f"⚠️ 유효하지 않은 값: {item['date']} = {inflation_value}")
                inflation_value = None
            else:
                try:
                    inflation_value = float(inflation_value)
                except (ValueError, TypeError):
                    print(f"⚠️ 숫자 변환 실패: {item['date']} = {inflation_value}")
                    inflation_value = None
            
            # 데이터 변환
            processed_item = {
                'date': item['date'],
                'inflation_rate': inflation_value,
                'interval_type': metadata['interval'],
                'unit': metadata['unit'],
                'name': metadata['name']
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=processed_item)
            success_count += 1
                
        except Exception as e:
            print(f"❌ 레코드 저장 실패: {item.get('date', 'Unknown')} - {str(e)}")
            error_count += 1
            continue
    
    # 최종 통계
    print(f"✅ 인플레이션 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 전체 레코드 수 확인
    result = hook.get_first("SELECT COUNT(*) FROM inflation")
    total_records = result[0] if result else 0
    print(f"📊 총 레코드 수: {total_records}")
    
    # 최신 5개 레코드 확인
    latest_records = hook.get_records(
        "SELECT date, inflation_rate FROM inflation ORDER BY date DESC LIMIT 5"
    )
    print(f"📅 최신 5개 레코드:")
    for record in latest_records:
        print(f"   {record[0]}: {record[1]}%")
    
    return success_count

# DAG 정의
with DAG(
    dag_id='ingest_inflation_to_db',
    default_args=default_args,
    schedule_interval='@yearly',  # 연 1회 실행 (연간 데이터)
    catchup=False,
    description='Alpha Vantage INFLATION 데이터 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['economic_indicators', 'alpha_vantage', 'inflation', 'annual'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_inflation_table',
        postgres_conn_id='postgres_default',
        sql='create_inflation.sql',
    )
    
    # 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_inflation_data',
        python_callable=fetch_inflation_data,
    )
    
    # 데이터 저장
    upsert_data = PythonOperator(
        task_id='upsert_inflation_data',
        python_callable=upsert_inflation_data,
    )
    
    # Task 의존성
    create_table >> fetch_data >> upsert_data