# dags/create_sp500_earnings_news_calendar.py

import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

# 기본 설정
default_args = {
    'owner': 'investment-assistant',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_sp500_earnings_calendar.sql"), encoding="utf-8") as f:
    UPSERT_CALENDAR_SQL = f.read()

with open(os.path.join(DAGS_SQL_DIR, "upsert_sp500_earnings_news.sql"), encoding="utf-8") as f:
    UPSERT_NEWS_SQL = f.read()

def check_data_sources(**context):
    """기존 데이터 소스 확인 및 통계"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("🔍 SP500 실적 캘린더 데이터 소스 점검...")
    
    # 1. earnings_calendar 테이블 확인
    earnings_count = hook.get_first("SELECT COUNT(*) FROM earnings_calendar")[0]
    print(f"📊 earnings_calendar: {earnings_count:,}개 레코드")
    
    # 2. sp500_companies 테이블 확인
    sp500_count = hook.get_first("SELECT COUNT(*) FROM sp500_companies")[0] 
    print(f"🏢 sp500_companies: {sp500_count:,}개 기업")
    
    # 3. SP500 기업의 실적 일정 확인
    sp500_earnings = hook.get_first("""
        SELECT COUNT(*)
        FROM earnings_calendar ec
        INNER JOIN sp500_companies sp ON ec.symbol = sp.symbol
        WHERE ec.report_date >= CURRENT_DATE - INTERVAL '30 days'
          AND ec.report_date <= CURRENT_DATE + INTERVAL '365 days'
    """)[0]
    print(f"🎯 SP500 기업 실적 일정: {sp500_earnings:,}개")
    
    # 4. 뉴스 테이블별 통계
    news_tables = ['earnings_news_finnhub', 'company_news', 'market_news', 'market_news_sentiment']
    news_stats = {}
    
    for table in news_tables:
        try:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            news_stats[table] = count
            print(f"📰 {table}: {count:,}개 뉴스")
        except Exception as e:
            news_stats[table] = 0
            print(f"❌ {table}: 조회 실패 - {e}")
    
    context['ti'].xcom_push(key='data_stats', value={
        'earnings_count': earnings_count,
        'sp500_count': sp500_count,
        'sp500_earnings': sp500_earnings,
        'news_stats': news_stats
    })
    
    return sp500_earnings

def extract_sp500_earnings_schedule(**context):
    """SP500 기업의 실적 일정 추출"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("📋 SP500 기업 실적 일정 추출 중...")
    
    query = """
        SELECT 
            ec.symbol,
            ec.report_date,
            ec.fiscal_date_ending,
            ec.estimate,
            ec.currency,
            sp.company_name,
            sp.gics_sector,
            sp.gics_sub_industry,
            sp.headquarters
        FROM earnings_calendar ec
        INNER JOIN sp500_companies sp ON ec.symbol = sp.symbol
        WHERE ec.report_date >= CURRENT_DATE - INTERVAL '30 days'
          AND ec.report_date <= CURRENT_DATE + INTERVAL '365 days'
        ORDER BY ec.report_date, ec.symbol
    """
    
    earnings_schedules = hook.get_records(query)
    
    print(f"📊 추출된 SP500 실적 일정: {len(earnings_schedules)}개")
    
    if earnings_schedules:
        print("\n📋 샘플 실적 일정:")
        for i, schedule in enumerate(earnings_schedules[:5]):
            symbol, report_date, _, estimate, _, company_name, sector, _, _ = schedule
            print(f"   {i+1}. {symbol} ({company_name}): {report_date} - {sector}")
    
    earnings_data = [
        {
            'symbol': row[0],
            'report_date': row[1],
            'fiscal_date_ending': row[2],
            'estimate': row[3],
            'currency': row[4],
            'company_name': row[5],
            'gics_sector': row[6],
            'gics_sub_industry': row[7],
            'headquarters': row[8]
        } for row in earnings_schedules
    ]
    
    context['ti'].xcom_push(key='earnings_schedules', value=earnings_data)
    
    return len(earnings_schedules)

def collect_news_from_table(hook: PostgresHook, table_name: str, symbol: str, company_name: str, 
                           start_date: date, end_date: date, news_section: str, report_date: date) -> List[Dict]:
    """특정 테이블에서 뉴스 수집"""
    
    news_list = []
    
    try:
        if table_name == 'earnings_news_finnhub':
            query = """
                SELECT headline, summary, source, datetime, url
                FROM earnings_news_finnhub 
                WHERE symbol = %s 
                  AND datetime BETWEEN %s AND %s
                  AND url IS NOT NULL
                ORDER BY datetime DESC 
                LIMIT 10
            """
            params = (symbol, start_date, end_date)
            
        elif table_name == 'company_news':
            query = """
                SELECT title, description, source, published_at, url
                FROM company_news 
                WHERE symbol = %s 
                  AND published_at BETWEEN %s AND %s
                  AND url IS NOT NULL
                ORDER BY published_at DESC 
                LIMIT 10
            """
            params = (symbol, start_date, end_date)
            
        elif table_name == 'market_news':
            query = """
                SELECT title, description, source, published_at, url
                FROM market_news 
                WHERE (title ILIKE %s OR title ILIKE %s OR content ILIKE %s OR content ILIKE %s)
                  AND published_at BETWEEN %s AND %s
                  AND url IS NOT NULL
                ORDER BY published_at DESC 
                LIMIT 10
            """
            params = (f'%{symbol}%', f'%{company_name}%', f'%{symbol}%', f'%{company_name}%', 
                     start_date, end_date)
            
        elif table_name == 'market_news_sentiment':
            query = """
                SELECT title, summary, source, time_published, url
                FROM market_news_sentiment 
                WHERE ticker_sentiment::text LIKE %s
                  AND time_published BETWEEN %s AND %s
                  AND url IS NOT NULL
                ORDER BY time_published DESC 
                LIMIT 10
            """
            params = (f'%"ticker": "{symbol}"%', start_date, end_date)
            
        else:
            return []
        
        results = hook.get_records(query, params)
        
        for row in results:
            title, content, source, published_at, url = row
            
            if published_at and hasattr(published_at, 'date'):
                days_diff = (published_at.date() - report_date).days
            else:
                days_diff = 0
                
            news_list.append({
                'source_table': table_name,
                'title': title or '',
                'content': content or '',
                'source': source or '',
                'published_at': published_at,
                'url': url,
                'news_section': news_section,
                'days_from_earnings': days_diff
            })
        
    except Exception as e:
        print(f"❌ {table_name} 뉴스 수집 실패 ({symbol}): {e}")
    
    return news_list

def collect_related_news_for_each_schedule(**context):
    """각 실적 일정별로 관련 뉴스 수집"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    earnings_schedules = context['ti'].xcom_pull(key='earnings_schedules', task_ids='extract_sp500_earnings_schedule')
    
    if not earnings_schedules:
        print("❌ 실적 일정 데이터를 찾을 수 없습니다.")
        return 0
    
    print(f"🎯 {len(earnings_schedules)}개 실적 일정에 대한 뉴스 수집 시작...")
    
    news_tables = ['earnings_news_finnhub', 'company_news', 'market_news', 'market_news_sentiment']
    processed_count = 0
    total_news_collected = 0
    
    consolidated_data = []
    
    for i, schedule in enumerate(earnings_schedules):
        symbol = schedule['symbol']
        company_name = schedule['company_name'] or symbol
        report_date = schedule['report_date']
        
        if isinstance(report_date, str):
            report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
        
        try:
            print(f"📊 처리 중 ({i+1}/{len(earnings_schedules)}): {symbol} ({company_name}) - {report_date}")
            
            # 날짜 범위 계산
            forecast_start = report_date - timedelta(days=14)
            forecast_end = report_date - timedelta(days=1)
            reaction_start = report_date + timedelta(days=1)
            reaction_end = report_date + timedelta(days=7)
            
            all_news = []
            
            # 각 뉴스 테이블에서 수집
            for table in news_tables:
                # 예상 구간 뉴스
                forecast_news = collect_news_from_table(
                    hook, table, symbol, company_name, 
                    forecast_start, forecast_end, 'forecast', report_date
                )
                all_news.extend(forecast_news)
                
                # 반응 구간 뉴스
                reaction_news = collect_news_from_table(
                    hook, table, symbol, company_name, 
                    reaction_start, reaction_end, 'reaction', report_date
                )
                all_news.extend(reaction_news)
            
            # URL 기준 중복 제거
            unique_news = {}
            for news in all_news:
                url = news['url']
                if url not in unique_news:
                    unique_news[url] = news
            
            unique_news_list = list(unique_news.values())
            total_count = len(unique_news_list)
            total_news_collected += total_count
            
            # 이벤트 정보 생성
            event_title = f"{symbol} 실적 발표"
            if schedule.get('estimate'):
                event_title += f" (예상 EPS: ${schedule['estimate']})"
                
            event_description = f"{company_name}의 실적 발표 일정입니다."
            if total_count > 0:
                event_description += f" 관련 뉴스 {total_count}개가 수집되었습니다."
            
            
            # 통합 데이터 준비
            consolidated_item = {
                'schedule': schedule,
                'news_list': unique_news_list,
                'event_info': {
                    'event_title': event_title,
                    'event_description': event_description,
                    'total_news_count': total_count,
                    'forecast_news_count': sum(1 for n in unique_news_list if n['news_section'] == 'forecast'),
                    'reaction_news_count': sum(1 for n in unique_news_list if n['news_section'] == 'reaction')
                }
            }
            
            consolidated_data.append(consolidated_item)
            processed_count += 1
            
            forecast_count = sum(1 for n in unique_news_list if n['news_section'] == 'forecast')
            reaction_count = sum(1 for n in unique_news_list if n['news_section'] == 'reaction')
            print(f"✅ {symbol} 완료: 예상 {forecast_count}개, 반응 {reaction_count}개, 총 {total_count}개")
            
            if (i + 1) % 50 == 0:
                print(f"📈 진행률: {i+1}/{len(earnings_schedules)} ({(i+1)/len(earnings_schedules)*100:.1f}%)")
            
        except Exception as e:
            print(f"❌ {symbol} 처리 실패: {e}")
            continue
    
    print(f"\n📊 뉴스 수집 완료:")
    print(f"   ✅ 처리된 실적 일정: {processed_count}/{len(earnings_schedules)}개")
    print(f"   📰 총 수집된 뉴스: {total_news_collected}개")
    if processed_count > 0:
        print(f"   📈 평균 뉴스/일정: {total_news_collected/processed_count:.1f}개")
    
    context['ti'].xcom_push(key='consolidated_data', value=consolidated_data)
    
    return processed_count

def upsert_consolidated_data(**context):
    """통합된 데이터를 데이터베이스에 저장"""
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    consolidated_data = context['ti'].xcom_pull(key='consolidated_data', task_ids='collect_related_news_for_each_schedule')
    
    if not consolidated_data:
        print("❌ 통합 데이터를 찾을 수 없습니다.")
        return 0
    
    print(f"💾 {len(consolidated_data)}개 실적 캘린더 데이터 저장 시작...")
    
    calendar_saved = 0
    news_saved = 0
    
    for item in consolidated_data:
        schedule = item['schedule']
        news_list = item['news_list']
        event_info = item['event_info']
        
        try:
            # 1. 캘린더 데이터 저장
            calendar_params = {
                'symbol': schedule['symbol'],
                'company_name': schedule['company_name'],
                'report_date': schedule['report_date'],
                'fiscal_date_ending': schedule['fiscal_date_ending'],
                'estimate': schedule['estimate'],
                'currency': schedule['currency'],
                'gics_sector': schedule['gics_sector'],
                'gics_sub_industry': schedule['gics_sub_industry'],
                'headquarters': schedule['headquarters'],
                'event_type': 'earnings_report',
                'event_title': event_info['event_title'],
                'event_description': event_info['event_description'],
                'total_news_count': event_info['total_news_count'],
                'forecast_news_count': event_info['forecast_news_count'],
                'reaction_news_count': event_info['reaction_news_count']
            }
            
            hook.run(UPSERT_CALENDAR_SQL, parameters=calendar_params)
            calendar_saved += 1
            
            # 2. calendar_id 조회
            calendar_id = hook.get_first("""
                SELECT id FROM sp500_earnings_calendar 
                WHERE symbol = %s AND report_date = %s
            """, (schedule['symbol'], schedule['report_date']))[0]
            
            # 3. 뉴스 데이터 저장
            for news in news_list:
                news_params = {
                    'calendar_id': calendar_id,
                    'source_table': news['source_table'],
                    'title': news['title'],
                    'url': news['url'],
                    'summary': news['content'],  # content를 summary로 매핑
                    'content': None,  # content 컬럼은 비워둠
                    'source': news['source'],
                    'published_at': news['published_at'],
                    'news_section': news['news_section'],
                    'days_from_earnings': news['days_from_earnings']
                }
                
                try:
                    hook.run(UPSERT_NEWS_SQL, parameters=news_params)
                    news_saved += 1
                except Exception as e:
                    print(f"⚠️ 뉴스 저장 실패 ({schedule['symbol']}, {news['url']}): {e}")
                    continue
            
            print(f"✅ {schedule['symbol']} 저장 완료: 캘린더 1개, 뉴스 {len(news_list)}개")
            
        except Exception as e:
            print(f"❌ {schedule['symbol']} 저장 실패: {e}")
            continue
    
    print(f"\n💾 데이터 저장 완료:")
    print(f"   📅 캘린더: {calendar_saved}개")
    print(f"   📰 뉴스: {news_saved}개")
    
    return calendar_saved

# DAG 정의
with DAG(
    dag_id='create_sp500_earnings_calendar',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='SP500 기업 실적 캘린더 생성 (뉴스 통합)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['sp500', 'earnings', 'calendar', 'news'],
) as dag:
    
    # 테이블 생성
    create_tables = PostgresOperator(
        task_id='create_sp500_earnings_tables',
        postgres_conn_id='postgres_default',
        sql='create_sp500_earnings_calendar.sql',
    )
    
    # 데이터 소스 확인
    check_sources = PythonOperator(
        task_id='check_data_sources',
        python_callable=check_data_sources,
    )
    
    # SP500 실적 일정 추출
    extract_schedules = PythonOperator(
        task_id='extract_sp500_earnings_schedule',
        python_callable=extract_sp500_earnings_schedule,
    )
    
    # 관련 뉴스 수집
    collect_news = PythonOperator(
        task_id='collect_related_news_for_each_schedule',
        python_callable=collect_related_news_for_each_schedule,
    )
    
    # 데이터 저장
    save_data = PythonOperator(
        task_id='upsert_consolidated_data',
        python_callable=upsert_consolidated_data,
    )
    
    # Task 의존성
    create_tables >> check_sources >> extract_schedules >> collect_news >> save_data