from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_sp500_companies.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,  # Wikipedia 접속 실패 대비
    'retry_delay': timedelta(minutes=5),
}

def scrape_sp500_from_wikipedia(**context):
    """Wikipedia에서 S&P 500 기업 리스트 스크래핑"""
    
    print("🔍 Wikipedia S&P 500 리스트 스크래핑 시작...")
    
    try:
        # User-Agent 설정 (Wikipedia 차단 방지)
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Wikipedia S&P 500 페이지 요청
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        print(f"📡 요청 URL: {url}")
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        print("✅ Wikipedia 페이지 접속 성공")
        
        # Pandas로 테이블 직접 읽기 (가장 간단한 방법)
        tables = pd.read_html(url, header=0)
        
        # 첫 번째 테이블이 S&P 500 구성 요소 테이블
        df = tables[0]
        
        print(f"📊 스크래핑된 기업 수: {len(df)}개")
        print(f"📋 컬럼: {list(df.columns)}")
        
        # 데이터 전처리
        companies_data = []
        
        for index, row in df.iterrows():
            try:
                # Wikipedia 테이블 구조에 맞춰 데이터 추출
                company_data = {
                    'symbol': str(row.get('Symbol', '')).strip(),
                    'company_name': str(row.get('Security', '')).strip(),
                    'gics_sector': str(row.get('GICS Sector', '')).strip(),
                    'gics_sub_industry': str(row.get('GICS Sub-Industry', '')).strip(),
                    'headquarters': str(row.get('Headquarters Location', '')).strip(),
                    'date_added': str(row.get('Date first added', '')).strip() if pd.notna(row.get('Date first added')) else None,
                    'cik': str(row.get('CIK', '')).strip() if pd.notna(row.get('CIK')) else None,
                    'founded': str(row.get('Founded', '')).strip() if pd.notna(row.get('Founded')) else None,
                }
                
                # 빈 값 처리
                for key, value in company_data.items():
                    if value == '' or value == 'nan':
                        company_data[key] = None
                
                # 필수 필드 검증
                if company_data['symbol'] and company_data['company_name']:
                    companies_data.append(company_data)
                else:
                    print(f"⚠️ 필수 필드 누락으로 건너뜀: {row}")
                    
            except Exception as e:
                print(f"❌ 행 처리 실패 (인덱스 {index}): {e}")
                continue
        
        print(f"✅ 전처리 완료: {len(companies_data)}개 기업")
        
        # 샘플 데이터 출력
        if companies_data:
            print("📋 첫 번째 기업 샘플:")
            for key, value in companies_data[0].items():
                print(f"   {key}: {value}")
        
        # XCom에 결과 저장
        context['ti'].xcom_push(key='sp500_companies', value=companies_data)
        
        return len(companies_data)
        
    except Exception as e:
        print(f"❌ Wikipedia 스크래핑 실패: {e}")
        
        # 백업 방법: BeautifulSoup 사용
        print("🔄 백업 방법 시도: BeautifulSoup 스크래핑...")
        return scrape_with_beautifulsoup(**context)

def scrape_with_beautifulsoup(**context):
    """백업 방법: BeautifulSoup을 사용한 스크래핑"""
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # S&P 500 구성 요소 테이블 찾기
        table = soup.find('table', {'class': 'wikitable sortable'})
        
        if not table:
            raise Exception("S&P 500 테이블을 찾을 수 없습니다")
        
        companies_data = []
        rows = table.find_all('tr')[1:]  # 헤더 제외
        
        print(f"📊 테이블에서 {len(rows)}개 행 발견")
        
        for i, row in enumerate(rows):
            try:
                cells = row.find_all('td')
                
                if len(cells) >= 7:  # 최소 필요한 셀 수
                    company_data = {
                        'symbol': cells[0].get_text().strip(),
                        'company_name': cells[1].get_text().strip(),
                        'gics_sector': cells[3].get_text().strip(),
                        'gics_sub_industry': cells[4].get_text().strip(),
                        'headquarters': cells[5].get_text().strip(),
                        'date_added': cells[6].get_text().strip() if len(cells) > 6 else None,
                        'cik': cells[7].get_text().strip() if len(cells) > 7 else None,
                        'founded': cells[8].get_text().strip() if len(cells) > 8 else None,
                    }
                    
                    # 데이터 정리
                    for key, value in company_data.items():
                        if value == '' or value == '—' or value == 'N/A':
                            company_data[key] = None
                    
                    if company_data['symbol'] and company_data['company_name']:
                        companies_data.append(company_data)
                
            except Exception as e:
                print(f"⚠️ 행 {i} 처리 실패: {e}")
                continue
        
        print(f"✅ BeautifulSoup 스크래핑 완료: {len(companies_data)}개 기업")
        
        # XCom에 결과 저장
        context['ti'].xcom_push(key='sp500_companies', value=companies_data)
        
        return len(companies_data)
        
    except Exception as e:
        print(f"❌ BeautifulSoup 스크래핑도 실패: {e}")
        raise

def store_sp500_companies(**context):
    """S&P 500 기업 데이터를 데이터베이스에 저장"""
    
    # XCom에서 스크래핑된 데이터 가져오기
    companies_data = context['ti'].xcom_pull(key='sp500_companies') or []
    
    if not companies_data:
        print("⚠️ 저장할 기업 데이터가 없습니다")
        return 0
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    update_count = 0
    
    print(f"💾 DB 저장 시작: {len(companies_data)}개 기업")
    
    # 기존 데이터와 비교를 위해 현재 DB 상태 확인
    try:
        existing_companies = hook.get_first("SELECT COUNT(*) FROM sp500_companies")
        existing_count = existing_companies[0] if existing_companies else 0
        print(f"📊 기존 DB 기업 수: {existing_count}개")
    except Exception as e:
        print(f"⚠️ 기존 데이터 확인 실패: {e}")
        existing_count = 0
    
    for company_data in companies_data:
        try:
            # UPSERT 실행
            hook.run(UPSERT_SQL, parameters=company_data)
            success_count += 1
            
            # 진행률 표시 (100개마다)
            if success_count % 100 == 0:
                print(f"📊 저장 진행률: {success_count}/{len(companies_data)}")
                
        except Exception as e:
            print(f"❌ 기업 저장 실패: {company_data.get('symbol', 'Unknown')} - {e}")
            error_count += 1
            continue
    
    print(f"✅ 저장 완료: {success_count}개 성공, {error_count}개 실패")
    
    # 최종 통계
    try:
        # 저장 후 총 기업 수
        result = hook.get_first("SELECT COUNT(*) FROM sp500_companies")
        total_companies = result[0] if result else 0
        
        # 새로 추가된 기업 수 계산
        new_companies = total_companies - existing_count
        update_count = success_count - max(0, new_companies)
        
        print(f"\n📈 최종 통계:")
        print(f"   📊 총 S&P 500 기업: {total_companies}개")
        print(f"   🆕 신규 추가: {max(0, new_companies)}개")
        print(f"   🔄 업데이트: {update_count}개")
        print(f"   ❌ 실패: {error_count}개")
        
        # 섹터별 분포 확인
        sector_stats = hook.get_records("""
            SELECT gics_sector, COUNT(*) as count 
            FROM sp500_companies 
            WHERE gics_sector IS NOT NULL
            GROUP BY gics_sector 
            ORDER BY count DESC
        """)
        
        print(f"\n📊 섹터별 분포:")
        for sector, count in sector_stats:
            print(f"   - {sector}: {count}개")
            
    except Exception as e:
        print(f"⚠️ 통계 조회 실패: {e}")
    
    return success_count

def validate_sp500_data(**context):
    """S&P 500 데이터 검증 및 품질 체크"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        print("🔍 데이터 품질 검증 시작...")
        
        # 1. 총 기업 수 확인
        total_count = hook.get_first("SELECT COUNT(*) FROM sp500_companies")[0]
        print(f"📊 총 기업 수: {total_count}개")
        
        # S&P 500은 대략 500개 기업이어야 함 (일부 다중 클래스 주식으로 503개 정도)
        if total_count < 480 or total_count > 520:
            print(f"⚠️ 경고: 기업 수가 예상 범위(480-520)를 벗어남: {total_count}")
        
        # 2. 필수 필드 누락 확인
        missing_symbol = hook.get_first("SELECT COUNT(*) FROM sp500_companies WHERE symbol IS NULL OR symbol = ''")[0]
        missing_name = hook.get_first("SELECT COUNT(*) FROM sp500_companies WHERE company_name IS NULL OR company_name = ''")[0]
        
        print(f"📋 데이터 품질:")
        print(f"   - 심볼 누락: {missing_symbol}개")
        print(f"   - 회사명 누락: {missing_name}개")
        
        if missing_symbol > 0 or missing_name > 0:
            print("⚠️ 경고: 필수 필드에 누락 데이터가 있습니다")
        
        # 3. 중복 심볼 확인
        duplicates = hook.get_records("""
            SELECT symbol, COUNT(*) as count 
            FROM sp500_companies 
            GROUP BY symbol 
            HAVING COUNT(*) > 1
        """)
        
        if duplicates:
            print(f"⚠️ 중복 심볼 발견: {len(duplicates)}개")
            for symbol, count in duplicates:
                print(f"   - {symbol}: {count}번 중복")
        else:
            print("✅ 중복 심볼 없음")
        
        # 4. 최근 업데이트된 기업들 확인
        recent_updates = hook.get_records("""
            SELECT symbol, company_name, updated_at 
            FROM sp500_companies 
            WHERE updated_at >= NOW() - INTERVAL '1 day'
            ORDER BY updated_at DESC
            LIMIT 10
        """)
        
        print(f"\n🔄 최근 업데이트된 기업 (최대 10개):")
        for symbol, name, updated_at in recent_updates:
            print(f"   - {symbol}: {name} ({updated_at})")
        
        # 5. 샘플 데이터 출력
        sample_companies = hook.get_records("""
            SELECT symbol, company_name, gics_sector 
            FROM sp500_companies 
            ORDER BY symbol 
            LIMIT 5
        """)
        
        print(f"\n📋 샘플 데이터:")
        for symbol, name, sector in sample_companies:
            print(f"   - {symbol}: {name} ({sector})")
        
        print("✅ 데이터 검증 완료")
        
        return {
            'total_companies': total_count,
            'missing_data': missing_symbol + missing_name,
            'duplicates': len(duplicates),
            'recent_updates': len(recent_updates)
        }
        
    except Exception as e:
        print(f"❌ 데이터 검증 실패: {e}")
        raise

# DAG 정의
with DAG(
    dag_id='ingest_sp500_companies_wikipedia_k8s',
    default_args=default_args,
    schedule_interval='0 6 * * 1',  # 매주 월요일 오전 6시 실행
    catchup=False,
    description='Wikipedia에서 S&P 500 기업 리스트 수집 및 DB 저장 (완전 무료)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'wikipedia', 'companies', 'scraping', 'free', 'k8s'],
) as dag:
    
    # 테이블 생성
    create_table = PostgresOperator(
        task_id='create_sp500_companies_table',
        postgres_conn_id='postgres_default',
        sql='create_sp500_companies.sql',
    )
    
    # Wikipedia에서 S&P 500 기업 리스트 스크래핑
    scrape_companies = PythonOperator(
        task_id='scrape_sp500_from_wikipedia',
        python_callable=scrape_sp500_from_wikipedia,
    )
    
    # DB에 저장
    store_companies = PythonOperator(
        task_id='store_sp500_companies',
        python_callable=store_sp500_companies,
    )
    
    # 데이터 검증
    validate_data = PythonOperator(
        task_id='validate_sp500_data',
        python_callable=validate_sp500_data,
    )
    
    # Task 의존성
    create_table >> scrape_companies >> store_companies >> validate_data