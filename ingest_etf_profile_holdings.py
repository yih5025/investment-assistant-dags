# dags/ingest_etf_alphavantage_data_k8s.py
import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# 경로 설정
DAGS_SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_etf_profile_holdings.sql"), encoding="utf-8") as f:
    UPSERT_ALPHAVANTAGE_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

def collect_etf_profile_holdings_data(**context):
    """Alpha Vantage ETF 데이터 수집 (디버깅 정보 포함)"""
    import requests
    import time
    import json
    import traceback
    
    print("🚀 Alpha Vantage ETF 데이터 수집 시작...")
    
    # API 키 확인
    try:
        api_key = Variable.get('ALPHA_VANTAGE_API_KEY_3')
        print(f"🔑 API 키 확인: {api_key[:8]}...{api_key[-4:] if len(api_key) > 12 else '***'}")
    except Exception as e:
        print(f"❌ API 키 로드 실패: {e}")
        raise ValueError("🔑 ALPHA_VANTAGE_API_KEY_3이 설정되지 않았습니다")
    
    # 🔧 수정된 쿼리: 실제 테이블 구조에 맞게 변경
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 기존 수집된 ETF 심볼 확인
    collected_symbols = set()
    try:
        collected_records = hook.get_records("""
            SELECT DISTINCT symbol FROM etf_profile_holdings
        """)
        collected_symbols = {row[0] for row in collected_records}
        print(f"📊 이미 수집된 ETF: {len(collected_symbols)}개")
    except Exception as e:
        print(f"⚠️ 기존 데이터 확인 실패 (테이블이 없을 수 있음): {e}")
    
    # etf_basic_info에서 아직 수집하지 않은 25개 ETF 선정
    priority_etfs = hook.get_records("""
        SELECT symbol FROM etf_basic_info 
        ORDER BY symbol
        LIMIT 52
    """)
    
    # 아직 수집하지 않은 ETF만 필터링
    uncollected_etfs = []
    for (symbol,) in priority_etfs:
        if symbol not in collected_symbols:
            uncollected_etfs.append(symbol)
            if len(uncollected_etfs) >= 25:
                break
    
    print(f"🎯 오늘 수집 대상: {len(uncollected_etfs)}개 ETF")
    for i, symbol in enumerate(uncollected_etfs, 1):
        print(f"   {i:2d}. {symbol}")
    
    if not uncollected_etfs:
        print("✅ 모든 ETF 데이터 수집 완료!")
        return
    
    collected_count = 0
    
    for i, symbol in enumerate(uncollected_etfs, 1):
        try:
            print(f"\n🔍 [{i:2d}/{len(uncollected_etfs)}] {symbol} 수집 중...")
            
            # Alpha Vantage API 호출
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'ETF_PROFILE',
                'symbol': symbol,
                'apikey': api_key
            }
            
            print(f"📤 API 요청: {url}?function=ETF_PROFILE&symbol={symbol}")
            
            response = requests.get(url, params=params, timeout=30)
            print(f"📥 HTTP 상태: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ HTTP 에러: {response.status_code}")
                print(f"❌ 응답 내용: {response.text[:500]}")
                continue
            
            data = response.json()
            
            # 🚨 전체 API 응답 출력 (처음 3개만 상세 출력)
            if i <= 3:
                print("📥 === 전체 API 응답 ===")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                print("📥 === 응답 종료 ===")
            else:
                print(f"📥 응답 키: {list(data.keys()) if isinstance(data, dict) else f'타입: {type(data)}'}")
            
            # API 에러 체크
            if 'Error Message' in data:
                error_msg = data['Error Message']
                print(f"❌ API 에러: {error_msg}")
                if 'Invalid API call' in error_msg or 'invalid API key' in error_msg.lower():
                    # API 키 문제는 즉시 실패 (데이터 저장 불가)
                    raise Exception(f"AlphaVantage API 키 에러: {error_msg}")
                continue
            
            if 'Note' in data:
                print(f"⚠️ API 제한 메시지: {data['Note']}")
                print("⏰ API Rate Limit 도달, 현재까지 수집된 데이터는 저장하고 중단")
                # 수집 중단하지만 Exception은 던지지 않음 (이미 저장된 데이터 보존)
                break
            
            if 'Information' in data:
                info_msg = data['Information']
                print(f"ℹ️ API 정보: {info_msg}")
                if '25 requests per day' in info_msg or 'rate limit' in info_msg.lower():
                    print("⏰ 일일 API 제한 도달, 현재까지 수집된 데이터는 저장하고 중단")
                    # 수집 중단하지만 Exception은 던지지 않음 (이미 저장된 데이터 보존)
                    break
            
            # 🔍 필드별 상세 확인 (디버깅용)
            print(f"🔍 필드별 데이터 상태:")
            key_fields = ['net_assets', 'net_expense_ratio', 'portfolio_turnover', 
                         'dividend_yield', 'inception_date', 'leveraged', 'sectors', 'holdings']
            
            for field in key_fields:
                value = data.get(field)
                value_type = type(value).__name__
                if isinstance(value, list):
                    print(f"   {field}: {value_type}[{len(value)}] = {value[:2] if len(value) > 0 else '[]'}...")
                elif isinstance(value, str) and len(value) > 50:
                    print(f"   {field}: {value_type} = {value[:50]}...")
                else:
                    print(f"   {field}: {value_type} = {value}")
            
            # 데이터가 없어도 계속 진행 (정상적인 상황일 수 있음)
            
            # 📊 API 응답 데이터 구조 확인 및 안전한 변환
            def safe_numeric(value):
                """안전한 숫자 변환"""
                if value in [None, '', 'None', 'null']:
                    return None
                try:
                    # 퍼센트 제거 후 변환
                    if isinstance(value, str) and '%' in value:
                        value = value.replace('%', '')
                    return float(value)
                except (ValueError, TypeError):
                    print(f"⚠️ 숫자 변환 실패: {value} ({type(value)})")
                    return None
            
            def safe_int(value):
                """안전한 정수 변환"""
                if value in [None, '', 'None', 'null']:
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    print(f"⚠️ 정수 변환 실패: {value} ({type(value)})")
                    return None
            
            # API 응답 데이터 그대로 저장
            etf_data = {
                'symbol': symbol,
                'net_assets': safe_int(data.get('net_assets')),
                'net_expense_ratio': safe_numeric(data.get('net_expense_ratio')),
                'portfolio_turnover': safe_numeric(data.get('portfolio_turnover')),
                'dividend_yield': safe_numeric(data.get('dividend_yield')),
                'inception_date': data.get('inception_date') if data.get('inception_date') else None,
                'leveraged': data.get('leveraged'),
                'sectors': json.dumps(data.get('sectors', []), ensure_ascii=False),  # JSON으로 저장
                'holdings': json.dumps(data.get('holdings', []), ensure_ascii=False)  # JSON으로 저장
            }
            
            # 📈 저장될 데이터 미리보기
            print(f"💾 저장될 데이터:")
            for key, value in etf_data.items():
                if isinstance(value, str) and len(value) > 100:
                    print(f"   {key}: {type(value).__name__}[{len(value)}글자] = {value[:100]}...")
                else:
                    print(f"   {key}: {value}")
            
            # 📈 데이터 상세 정보 출력
            holdings_count = len(data.get('holdings', []))
            sectors_count = len(data.get('sectors', []))
            net_assets = etf_data['net_assets']
            
            print(f"📊 ETF 정보 요약:")
            print(f"   💰 순자산: ${net_assets:,}" if net_assets else "   💰 순자산: N/A")
            print(f"   📊 보유종목: {holdings_count}개")
            print(f"   🏭 섹터: {sectors_count}개")
            print(f"   📅 설정일: {data.get('inception_date', 'N/A')}")
            
            # DB에 저장
            try:
                hook.run(UPSERT_ALPHAVANTAGE_SQL, parameters=etf_data)
                collected_count += 1
                print(f"✅ {symbol}: 데이터 저장 완료")
            except Exception as db_error:
                print(f"❌ {symbol}: DB 저장 실패")
                print(f"❌ DB 에러: {db_error}")
                print(f"❌ DB 에러 상세: {traceback.format_exc()}")
                continue
            
            # API 제한 준수 (12초 대기)
            if i < len(uncollected_etfs):
                print(f"⏰ 12초 대기...")
                time.sleep(12)
            
        except requests.exceptions.RequestException as req_error:
            print(f"❌ {symbol} 네트워크 요청 실패: {req_error}")
            print(f"❌ 요청 상세: {traceback.format_exc()}")
            continue
        except json.JSONDecodeError as json_error:
            print(f"❌ {symbol} JSON 파싱 실패: {json_error}")
            print(f"❌ 응답 내용: {response.text[:500] if 'response' in locals() else 'N/A'}")
            continue
        except Exception as e:
            print(f"❌ {symbol} 예상치 못한 오류: {e}")
            print(f"❌ 전체 에러 상세:")
            print(traceback.format_exc())
            continue
    
    
    print(f"\n🎯 수집 완료: {collected_count}/{len(uncollected_etfs)}개 ETF")
    
    # 최종 결과 평가
    if collected_count == 0:
        # 하나도 수집하지 못한 경우만 에러 체크
        print("⚠️ 수집된 ETF가 없습니다.")
        
        # 첫 번째 ETF부터 API 키 에러였다면 실패 처리
        if len(uncollected_etfs) > 0:
            print("🔍 첫 번째 ETF에서 API 키 문제가 있었는지 확인이 필요합니다.")
            print("ℹ️ 로그를 확인하여 'Invalid API call' 또는 'API key' 에러가 있었는지 점검하세요.")
        
        print("ℹ️ 가능한 원인: API 제한 이미 도달, 중복 데이터, ETF 지원 안함")
    
    success_rate = collected_count / len(uncollected_etfs) * 100 if uncollected_etfs else 0
    print(f"📊 성공률: {success_rate:.1f}%")
    
    # API 제한으로 중단된 경우 정보 메시지
    if collected_count < len(uncollected_etfs):
        remaining = len(uncollected_etfs) - collected_count
        print(f"ℹ️ {remaining}개 ETF가 남았습니다 (내일 또는 API 제한 해제 후 수집 예정)")

def validate_profile_holdings_data(**context):
    """Alpha Vantage 데이터 검증"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # 오늘 수집 통계
        today_collected = hook.get_first("""
            SELECT COUNT(*) FROM etf_profile_holdings 
            WHERE DATE(collected_at) = CURRENT_DATE
        """)[0]
        
        # 전체 진행률 (실제 테이블 구조에 맞게 수정)
        total_etfs = hook.get_first("""
            SELECT COUNT(*) FROM etf_basic_info
        """)[0]
        
        completed_etfs = hook.get_first("""
            SELECT COUNT(*) FROM etf_profile_holdings
        """)[0]
        
        print(f"📊 Alpha Vantage 수집 현황:")
        print(f"   오늘 수집: {today_collected}개")
        print(f"   전체 진행률: {completed_etfs}/{total_etfs} ({(completed_etfs/total_etfs*100):.1f}%)")
        print(f"   남은 ETF: {total_etfs - completed_etfs}개")
        
        # 데이터 품질 체크
        if today_collected > 0:
            quality_stats = hook.get_records("""
                SELECT 
                    symbol,
                    CASE 
                        WHEN holdings::text != '[]' AND holdings IS NOT NULL 
                        THEN json_array_length(holdings::json) 
                        ELSE 0 
                    END as holdings_count,
                    CASE 
                        WHEN sectors::text != '[]' AND sectors IS NOT NULL 
                        THEN json_array_length(sectors::json) 
                        ELSE 0 
                    END as sectors_count,
                    net_assets,
                    dividend_yield
                FROM etf_profile_holdings 
                WHERE DATE(collected_at) = CURRENT_DATE
                ORDER BY holdings_count DESC
            """)
            
            print(f"\n📈 오늘 수집된 ETF 상세:")
            for symbol, holdings_count, sectors_count, net_assets, dividend_yield in quality_stats:
                net_assets_str = f"${net_assets:,.0f}" if net_assets else "N/A"
                dividend_str = f"{dividend_yield:.2f}%" if dividend_yield else "N/A"
                print(f"   {symbol}: Holdings {holdings_count}개, Sectors {sectors_count}개, 순자산 {net_assets_str}, 배당률 {dividend_str}")
        
        # 전체 수집 현황 요약
        print(f"\n📋 전체 ETF 수집 현황:")
        print(f"   📊 기본 정보: {total_etfs}개 ETF")
        print(f"   📈 상세 데이터: {completed_etfs}개 ETF ({(completed_etfs/total_etfs*100):.1f}%)")
        
        if completed_etfs >= total_etfs:
            print("🎉 모든 ETF 데이터 수집 완료!")
        else:
            print(f"⏳ 남은 작업: {total_etfs - completed_etfs}개 ETF")
            
    except Exception as e:
        print(f"❌ 데이터 검증 중 오류 발생: {e}")

# DAG 정의
with DAG(
    dag_id='ingest_etf_profile_holdings',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # 매일 오전 8시
    catchup=False,
    description='Alpha Vantage ETF 전체 데이터 수집 (실제 테이블 구조 반영)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['etf', 'alphavantage', 'complete_data', 'json_storage', 'k8s', 'fixed'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_etf_profile_holdings_table',
        postgres_conn_id='postgres_default',
        sql='create_etf_profile_holdings.sql',
    )
    
    # 2. Alpha Vantage 데이터 수집
    collect_data = PythonOperator(
        task_id='collect_profile_holdings_data',
        python_callable=collect_etf_profile_holdings_data,
    )
    
    # 3. 데이터 검증
    validate_data = PythonOperator(
        task_id='validate_profile_holdings_data',
        python_callable=validate_profile_holdings_data,
    )
    
    # Task 의존성
    create_table >> collect_data >> validate_data