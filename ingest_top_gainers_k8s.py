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
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_top_gainers.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def fetch_top_gainers_data(**context):
    """
    Alpha Vantage API에서 Top Gainers/Losers/Most Active 데이터 수집
    """
    # API 키 가져오기
    api_key = Variable.get('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("🔑 ALPHA_VANTAGE_API_KEY가 설정되지 않았습니다")
    
    # API 요청
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'TOP_GAINERS_LOSERS',
        'apikey': api_key
    }
    
    print(f"🚀 API 요청 시작: {url}")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    
    # API 응답 검증
    if 'Error Message' in response.text:
        raise ValueError(f"❌ API 오류: {response.text}")
    
    if 'Note' in response.text and 'API call frequency' in response.text:
        raise ValueError(f"⚠️ API 호출 제한: {response.text}")
    
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ JSON 파싱 실패: {str(e)}")
    
    # 필수 키 확인
    required_keys = ['top_gainers', 'top_losers', 'most_actively_traded', 'last_updated']
    for key in required_keys:
        if key not in data:
            raise ValueError(f"❌ 필수 키 누락: {key}")
    
    # 데이터 개수 확인
    gainers_count = len(data['top_gainers'])
    losers_count = len(data['top_losers'])
    active_count = len(data['most_actively_traded'])
    
    print(f"✅ 데이터 수집 완료:")
    print(f"   📈 Top Gainers: {gainers_count}개")
    print(f"   📉 Top Losers: {losers_count}개") 
    print(f"   🔥 Most Active: {active_count}개")
    print(f"   📅 Last Updated: {data['last_updated']}")
    
    # XCom에 저장
    context['ti'].xcom_push(key='market_data', value=data)
    return {
        'gainers_count': gainers_count,
        'losers_count': losers_count,
        'active_count': active_count,
        'last_updated': data['last_updated']
    }

def process_and_store_data(**context):
    """
    수집된 데이터를 가공하여 PostgreSQL에 저장 (호출 회차별 batch_id 버전)
    - batch_id: 호출 회차 식별자 (1, 2, 3, 4, ...)
    - top_gainers: 20개 전부
    - top_losers: 상위 10개  
    - most_actively_traded: 20개 전부
    """
    # XCom에서 데이터 가져오기
    data = context['ti'].xcom_pull(task_ids='fetch_top_gainers', key='market_data')
    
    if not data:
        raise ValueError("❌ 이전 태스크에서 데이터를 받지 못했습니다")
    
    # DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 🔑 현재 최대 batch_id 조회해서 +1 (호출 회차 증가)
    max_batch_result = hook.get_first("SELECT COALESCE(MAX(batch_id), 0) FROM top_gainers")
    current_batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
    
    # API 응답의 타임스탬프도 파싱 (참고용)
    last_updated_str = data['last_updated']
    api_last_updated = datetime.strptime(
        last_updated_str.split(' US/Eastern')[0], 
        "%Y-%m-%d %H:%M:%S"
    )
    
    print(f"🆔 이번 호출 회차: {current_batch_id}")
    print(f"📡 API 응답 시간: {api_last_updated}")
    print(f"🔄 DAG 실행 시간: {context['execution_date']}")
    
    # 처리할 데이터 정의
    categories = [
        {
            'name': 'top_gainers',
            'data': data['top_gainers'][:20],  # 20개 전부
            'limit': 20
        },
        {
            'name': 'top_losers', 
            'data': data['top_losers'][:10],   # 상위 10개
            'limit': 10
        },
        {
            'name': 'most_actively_traded',
            'data': data['most_actively_traded'][:20],  # 20개 전부
            'limit': 20
        }
    ]
    
    total_success = 0
    total_error = 0
    
    print(f"🚀 배치 {current_batch_id} 데이터 저장 시작")
    
    for category in categories:
        category_name = category['name']
        category_data = category['data']
        
        print(f"📊 처리 중: {category_name} ({len(category_data)}개)")
        
        success_count = 0
        error_count = 0
        
        for rank, item in enumerate(category_data, 1):
            try:
                # 데이터 검증 및 변환
                if not item.get('ticker'):
                    print(f"⚠️ ticker 누락: {item}")
                    error_count += 1
                    continue
                
                # 🔑 파라미터 준비 (호출 회차별 batch_id 포함)
                params = {
                    'batch_id': current_batch_id,  # 🔑 핵심: 호출 회차 번호
                    'last_updated': api_last_updated,  # API 응답 시간 (참고용)
                    'symbol': item['ticker'][:10],  # VARCHAR(10) 제한
                    'category': category_name,
                    'rank_position': rank,
                    'price': Decimal(str(item.get('price', '0'))),
                    'change_amount': Decimal(str(item.get('change_amount', '0'))),
                    'change_percentage': item.get('change_percentage', '0%')[:20],  # VARCHAR(20) 제한
                    'volume': int(item.get('volume', '0'))
                }
                
                # SQL 실행
                hook.run(UPSERT_SQL, parameters=params)
                success_count += 1
                
            except Exception as e:
                print(f"❌ 레코드 저장 실패: {item.get('ticker', 'Unknown')} - {str(e)}")
                error_count += 1
                continue
        
        print(f"✅ {category_name} 완료: {success_count}개 성공, {error_count}개 실패")
        total_success += success_count
        total_error += error_count
    
    # 최종 통계
    print(f"🎯 배치 {current_batch_id} 저장 완료: {total_success}개 성공, {total_error}개 실패")
    
    return {
        'batch_id': current_batch_id,
        'success_count': total_success,
        'error_count': total_error,
        'api_time': api_last_updated.isoformat(),
        'execution_time': context['execution_date'].isoformat()
    }
# DAG 정의
with DAG(
    dag_id='ingest_top_gainers_to_db',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    description='Alpha Vantage API에서 Top Gainers/Losers/Most Active 주식 데이터 수집',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['stocks', 'alpha_vantage', 'top_gainers', 'daily'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_top_gainers_table',
        postgres_conn_id='postgres_default',
        sql='create_top_gainers.sql',  # 실제 파일명에 맞춤
    )
    
    # 2. API 데이터 수집
    fetch_data = PythonOperator(
        task_id='fetch_top_gainers',
        python_callable=fetch_top_gainers_data,
    )
    
    # 3. 데이터 가공 및 저장
    process_data = PythonOperator(
        task_id='process_and_store_data',
        python_callable=process_and_store_data,
    )
    
    # Task 의존성
    create_table >> fetch_data >> process_data