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
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# SQL 파일 읽기
with open(os.path.join(DAGS_SQL_DIR, "upsert_market_news_sentiment.sql"), encoding="utf-8") as f:
    UPSERT_SQL = f.read()

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# 요일별 전문화 유지 + 단순한 파라미터 조합
class SimplifiedWeeklySpecializedQueries:
    """
    요일별 전문화 전략 유지하면서 파라미터 조합만 단순화
    """
    
    # ✅ 검증된 에너지 주식들
    ENERGY_TICKERS = ['XOM', 'CVX', 'COP', 'EOG', 'SLB']
    
    # ✅ 검증된 기술 주식들  
    TECH_TICKERS = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'TSLA', 'META']
    
    # ✅ 검증된 금융 주식들
    FINANCE_TICKERS = ['JPM', 'BAC', 'WFC', 'C', 'GS', 'V', 'MA']
    
    # ✅ 검증된 헬스케어 주식들
    HEALTHCARE_TICKERS = ['JNJ', 'PFE', 'UNH', 'MRNA', 'ABBV']
    
    # ✅ 검증된 소비재 주식들
    CONSUMER_TICKERS = ['WMT', 'TGT', 'COST', 'HD', 'LOW']
    
    # ✅ 검증된 암호화폐 관련 주식들
    CRYPTO_TICKERS = ['COIN', 'MSTR', 'RIOT', 'MARA']
    
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.day_of_week = execution_date.weekday()
        self.day_names = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']
    
    def generate_simplified_weekday_queries(self) -> list:
        """요일별 전문화 + 단순한 파라미터 조합"""
        
        if self.day_of_week == 0:  # 월요일
            return self._monday_energy_manufacturing_simple()
        elif self.day_of_week == 1:  # 화요일
            return self._tuesday_technology_ipo_simple()
        elif self.day_of_week == 2:  # 수요일
            return self._wednesday_blockchain_finance_simple()
        elif self.day_of_week == 3:  # 목요일
            return self._thursday_earnings_healthcare_simple()
        elif self.day_of_week == 4:  # 금요일
            return self._friday_retail_mergers_simple()
        elif self.day_of_week == 5:  # 토요일
            return self._saturday_realestate_macro_simple()
        else:  # 일요일
            return self._sunday_markets_policy_simple()
    
    def _monday_energy_manufacturing_simple(self) -> list:
        """월요일: 에너지 & 제조업 (단순화)"""
        return [
            # === 에너지 토픽 단독 (5개) ===
            {'type': 'energy_general', 'params': 'topics=energy_transportation&sort=LATEST'},
            {'type': 'energy_relevant', 'params': 'topics=energy_transportation&sort=RELEVANCE'},
            {'type': 'manufacturing_general', 'params': 'topics=manufacturing&sort=LATEST'},
            {'type': 'manufacturing_relevant', 'params': 'topics=manufacturing&sort=RELEVANCE'},
            {'type': 'energy_manufacturing', 'params': 'topics=energy_transportation'},  # sort 제거
            
            # === 에너지 주식 단독 (10개) ===
            {'type': 'exxon_news', 'params': 'tickers=XOM'},
            {'type': 'chevron_news', 'params': 'tickers=CVX'},
            {'type': 'conocophillips_news', 'params': 'tickers=COP'},
            {'type': 'eog_news', 'params': 'tickers=EOG'},
            {'type': 'slb_news', 'params': 'tickers=SLB'},
            {'type': 'tesla_energy_news', 'params': 'tickers=TSLA'},  # 에너지 관련
            {'type': 'caterpillar_news', 'params': 'tickers=CAT'},    # 제조업
            {'type': 'ge_news', 'params': 'tickers=GE'},             # 제조업
            {'type': 'ford_news', 'params': 'tickers=F'},            # 제조업
            {'type': 'boeing_news', 'params': 'tickers=BA'},         # 제조업
            
            # === 단순 조합 (10개) ===
            {'type': 'exxon_energy', 'params': 'tickers=XOM&topics=energy_transportation'},
            {'type': 'chevron_energy', 'params': 'tickers=CVX&topics=energy_transportation'},
            {'type': 'cop_energy', 'params': 'tickers=COP&topics=energy_transportation'},
            {'type': 'tesla_manufacturing', 'params': 'tickers=TSLA&topics=manufacturing'},
            {'type': 'cat_manufacturing', 'params': 'tickers=CAT&topics=manufacturing'},
            {'type': 'ge_manufacturing', 'params': 'tickers=GE&topics=manufacturing'},
            {'type': 'ford_manufacturing', 'params': 'tickers=F&topics=manufacturing'},
            {'type': 'boeing_manufacturing', 'params': 'tickers=BA&topics=manufacturing'},
            {'type': 'eog_energy', 'params': 'tickers=EOG&topics=energy_transportation'},
            {'type': 'slb_energy', 'params': 'tickers=SLB&topics=energy_transportation'},
        ]
    
    def _tuesday_technology_ipo_simple(self) -> list:
        """화요일: 기술 & IPO (단순화)"""
        return [
            # === 기술 토픽 단독 (3개) ===
            {'type': 'technology_general', 'params': 'topics=technology&sort=LATEST'},
            {'type': 'technology_relevant', 'params': 'topics=technology&sort=RELEVANCE'},
            {'type': 'ipo_general', 'params': 'topics=ipo&sort=LATEST'},
            
            # === 기술 주식 단독 (14개) ===
            {'type': 'apple_news', 'params': 'tickers=AAPL'},
            {'type': 'microsoft_news', 'params': 'tickers=MSFT'},
            {'type': 'nvidia_news', 'params': 'tickers=NVDA'},
            {'type': 'google_news', 'params': 'tickers=GOOGL'},
            {'type': 'amazon_news', 'params': 'tickers=AMZN'},
            {'type': 'tesla_news', 'params': 'tickers=TSLA'},
            {'type': 'meta_news', 'params': 'tickers=META'},
            {'type': 'oracle_news', 'params': 'tickers=ORCL'},
            {'type': 'salesforce_news', 'params': 'tickers=CRM'},
            {'type': 'intel_news', 'params': 'tickers=INTC'},
            {'type': 'amd_news', 'params': 'tickers=AMD'},
            {'type': 'qualcomm_news', 'params': 'tickers=QCOM'},
            {'type': 'adobe_news', 'params': 'tickers=ADBE'},
            {'type': 'ibm_news', 'params': 'tickers=IBM'},
            
            # === 단순 조합 (8개) ===
            {'type': 'apple_tech', 'params': 'tickers=AAPL&topics=technology'},
            {'type': 'microsoft_tech', 'params': 'tickers=MSFT&topics=technology'},
            {'type': 'nvidia_tech', 'params': 'tickers=NVDA&topics=technology'},
            {'type': 'google_tech', 'params': 'tickers=GOOGL&topics=technology'},
            {'type': 'amazon_tech', 'params': 'tickers=AMZN&topics=technology'},
            {'type': 'tesla_tech', 'params': 'tickers=TSLA&topics=technology'},
            {'type': 'meta_tech', 'params': 'tickers=META&topics=technology'},
            {'type': 'coinbase_ipo', 'params': 'tickers=COIN&topics=ipo'},
        ]
    
    def _wednesday_blockchain_finance_simple(self) -> list:
        """수요일: 블록체인 & 금융 (단순화)"""
        return [
            # === 블록체인/금융 토픽 단독 (4개) ===
            {'type': 'blockchain_general', 'params': 'topics=blockchain&sort=LATEST'},
            {'type': 'blockchain_relevant', 'params': 'topics=blockchain&sort=RELEVANCE'},
            {'type': 'finance_general', 'params': 'topics=finance&sort=LATEST'},
            {'type': 'finance_relevant', 'params': 'topics=finance&sort=RELEVANCE'},
            
            # === 금융 주식 단독 (10개) ===
            {'type': 'jpmorgan_news', 'params': 'tickers=JPM'},
            {'type': 'bofa_news', 'params': 'tickers=BAC'},
            {'type': 'wells_fargo_news', 'params': 'tickers=WFC'},
            {'type': 'citigroup_news', 'params': 'tickers=C'},
            {'type': 'goldman_news', 'params': 'tickers=GS'},
            {'type': 'visa_news', 'params': 'tickers=V'},
            {'type': 'mastercard_news', 'params': 'tickers=MA'},
            {'type': 'coinbase_news', 'params': 'tickers=COIN'},
            {'type': 'microstrategy_news', 'params': 'tickers=MSTR'},
            {'type': 'riot_news', 'params': 'tickers=RIOT'},
            
            # === 단순 조합 (11개) ===
            {'type': 'jpmorgan_finance', 'params': 'tickers=JPM&topics=finance'},
            {'type': 'bofa_finance', 'params': 'tickers=BAC&topics=finance'},
            {'type': 'visa_finance', 'params': 'tickers=V&topics=finance'},
            {'type': 'mastercard_finance', 'params': 'tickers=MA&topics=finance'},
            {'type': 'coinbase_blockchain', 'params': 'tickers=COIN&topics=blockchain'},
            {'type': 'microstrategy_blockchain', 'params': 'tickers=MSTR&topics=blockchain'},
            {'type': 'riot_blockchain', 'params': 'tickers=RIOT&topics=blockchain'},
            {'type': 'mara_blockchain', 'params': 'tickers=MARA&topics=blockchain'},
            {'type': 'wells_finance', 'params': 'tickers=WFC&topics=finance'},
            {'type': 'citi_finance', 'params': 'tickers=C&topics=finance'},
            {'type': 'goldman_finance', 'params': 'tickers=GS&topics=finance'},
        ]
    
    def _thursday_earnings_healthcare_simple(self) -> list:
        """목요일: 실적 & 헬스케어 (단순화)"""
        return [
            # === 실적/헬스케어 토픽 단독 (4개) ===
            {'type': 'earnings_general', 'params': 'topics=earnings&sort=LATEST'},
            {'type': 'earnings_relevant', 'params': 'topics=earnings&sort=RELEVANCE'},
            {'type': 'healthcare_general', 'params': 'topics=life_sciences&sort=LATEST'},
            {'type': 'healthcare_relevant', 'params': 'topics=life_sciences&sort=RELEVANCE'},
            
            # === 주요 주식 단독 (12개) ===
            {'type': 'apple_earnings_news', 'params': 'tickers=AAPL'},
            {'type': 'microsoft_earnings_news', 'params': 'tickers=MSFT'},
            {'type': 'nvidia_earnings_news', 'params': 'tickers=NVDA'},
            {'type': 'google_earnings_news', 'params': 'tickers=GOOGL'},
            {'type': 'amazon_earnings_news', 'params': 'tickers=AMZN'},
            {'type': 'tesla_earnings_news', 'params': 'tickers=TSLA'},
            {'type': 'jnj_news', 'params': 'tickers=JNJ'},
            {'type': 'pfizer_news', 'params': 'tickers=PFE'},
            {'type': 'unitedhealth_news', 'params': 'tickers=UNH'},
            {'type': 'moderna_news', 'params': 'tickers=MRNA'},
            {'type': 'abbvie_news', 'params': 'tickers=ABBV'},
            {'type': 'jpmorgan_earnings_news', 'params': 'tickers=JPM'},
            
            # === 단순 조합 (9개) ===
            {'type': 'apple_earnings', 'params': 'tickers=AAPL&topics=earnings'},
            {'type': 'microsoft_earnings', 'params': 'tickers=MSFT&topics=earnings'},
            {'type': 'nvidia_earnings', 'params': 'tickers=NVDA&topics=earnings'},
            {'type': 'google_earnings', 'params': 'tickers=GOOGL&topics=earnings'},
            {'type': 'jnj_healthcare', 'params': 'tickers=JNJ&topics=life_sciences'},
            {'type': 'pfizer_healthcare', 'params': 'tickers=PFE&topics=life_sciences'},
            {'type': 'moderna_healthcare', 'params': 'tickers=MRNA&topics=life_sciences'},
            {'type': 'unitedhealth_healthcare', 'params': 'tickers=UNH&topics=life_sciences'},
            {'type': 'abbvie_healthcare', 'params': 'tickers=ABBV&topics=life_sciences'},
        ]
    
    def _friday_retail_mergers_simple(self) -> list:
        """금요일: 리테일 & M&A (단순화)"""
        return [
            # === 리테일/M&A 토픽 단독 (4개) ===
            {'type': 'retail_general', 'params': 'topics=retail_wholesale&sort=LATEST'},
            {'type': 'retail_relevant', 'params': 'topics=retail_wholesale&sort=RELEVANCE'},
            {'type': 'mergers_general', 'params': 'topics=mergers_and_acquisitions&sort=LATEST'},
            {'type': 'mergers_relevant', 'params': 'topics=mergers_and_acquisitions&sort=RELEVANCE'},
            
            # === 소비재 주식 단독 (12개) ===
            {'type': 'walmart_news', 'params': 'tickers=WMT'},
            {'type': 'target_news', 'params': 'tickers=TGT'},
            {'type': 'costco_news', 'params': 'tickers=COST'},
            {'type': 'homedepot_news', 'params': 'tickers=HD'},
            {'type': 'lowes_news', 'params': 'tickers=LOW'},
            {'type': 'amazon_retail_news', 'params': 'tickers=AMZN'},
            {'type': 'disney_news', 'params': 'tickers=DIS'},
            {'type': 'netflix_news', 'params': 'tickers=NFLX'},
            {'type': 'starbucks_news', 'params': 'tickers=SBUX'},
            {'type': 'nike_news', 'params': 'tickers=NKE'},
            {'type': 'mcdonalds_news', 'params': 'tickers=MCD'},
            {'type': 'cocacola_news', 'params': 'tickers=KO'},
            
            # === 단순 조합 (9개) ===
            {'type': 'walmart_retail', 'params': 'tickers=WMT&topics=retail_wholesale'},
            {'type': 'target_retail', 'params': 'tickers=TGT&topics=retail_wholesale'},
            {'type': 'costco_retail', 'params': 'tickers=COST&topics=retail_wholesale'},
            {'type': 'homedepot_retail', 'params': 'tickers=HD&topics=retail_wholesale'},
            {'type': 'amazon_retail', 'params': 'tickers=AMZN&topics=retail_wholesale'},
            {'type': 'microsoft_mergers', 'params': 'tickers=MSFT&topics=mergers_and_acquisitions'},
            {'type': 'google_mergers', 'params': 'tickers=GOOGL&topics=mergers_and_acquisitions'},
            {'type': 'amazon_mergers', 'params': 'tickers=AMZN&topics=mergers_and_acquisitions'},
            {'type': 'disney_mergers', 'params': 'tickers=DIS&topics=mergers_and_acquisitions'},
        ]
    
    def _saturday_realestate_macro_simple(self) -> list:
        """토요일: 부동산 & 거시경제 (단순화)"""
        return [
            # === 부동산/거시경제 토픽 단독 (6개) ===
            {'type': 'realestate_general', 'params': 'topics=real_estate&sort=LATEST'},
            {'type': 'realestate_relevant', 'params': 'topics=real_estate&sort=RELEVANCE'},
            {'type': 'macro_general', 'params': 'topics=economy_macro&sort=LATEST'},
            {'type': 'macro_relevant', 'params': 'topics=economy_macro&sort=RELEVANCE'},
            {'type': 'manufacturing_macro', 'params': 'topics=manufacturing'},
            {'type': 'energy_macro', 'params': 'topics=energy_transportation'},
            
            # === 부동산/건설 관련 주식 (10개) ===
            {'type': 'homedepot_realestate_news', 'params': 'tickers=HD'},
            {'type': 'lowes_realestate_news', 'params': 'tickers=LOW'},
            {'type': 'caterpillar_construction_news', 'params': 'tickers=CAT'},
            {'type': 'deere_construction_news', 'params': 'tickers=DE'},
            {'type': 'boeing_manufacturing_news', 'params': 'tickers=BA'},
            {'type': 'ge_industrial_news', 'params': 'tickers=GE'},
            {'type': 'ford_auto_news', 'params': 'tickers=F'},
            {'type': 'gm_auto_news', 'params': 'tickers=GM'},
            {'type': 'exxon_macro_news', 'params': 'tickers=XOM'},
            {'type': 'chevron_macro_news', 'params': 'tickers=CVX'},
            
            # === 단순 조합 (9개) ===
            {'type': 'homedepot_realestate', 'params': 'tickers=HD&topics=real_estate'},
            {'type': 'lowes_realestate', 'params': 'tickers=LOW&topics=real_estate'},
            {'type': 'caterpillar_macro', 'params': 'tickers=CAT&topics=economy_macro'},
            {'type': 'boeing_macro', 'params': 'tickers=BA&topics=economy_macro'},
            {'type': 'ford_macro', 'params': 'tickers=F&topics=economy_macro'},
            {'type': 'exxon_realestate', 'params': 'tickers=XOM&topics=real_estate'},
            {'type': 'ge_manufacturing', 'params': 'tickers=GE&topics=manufacturing'},
            {'type': 'deere_manufacturing', 'params': 'tickers=DE&topics=manufacturing'},
            {'type': 'gm_manufacturing', 'params': 'tickers=GM&topics=manufacturing'},
        ]
    
    def _sunday_markets_policy_simple(self) -> list:
        """일요일: 금융시장 & 정책 (단순화) - 기존 실패 조합 완전 교체"""
        return [
            # === 단일 토픽 (8개) - 검증된 것들만 ===
            {'type': 'technology_markets', 'params': 'topics=technology'},
            {'type': 'finance_markets', 'params': 'topics=finance'},
            {'type': 'earnings_markets', 'params': 'topics=earnings'},
            {'type': 'ipo_markets', 'params': 'topics=ipo'},
            {'type': 'blockchain_markets', 'params': 'topics=blockchain'},
            {'type': 'mergers_markets', 'params': 'topics=mergers_and_acquisitions'},
            {'type': 'retail_markets', 'params': 'topics=retail_wholesale'},
            {'type': 'healthcare_markets', 'params': 'topics=life_sciences'},
            
            # === 주요 주식 단독 (10개) ===
            {'type': 'apple_sunday', 'params': 'tickers=AAPL'},
            {'type': 'microsoft_sunday', 'params': 'tickers=MSFT'},
            {'type': 'nvidia_sunday', 'params': 'tickers=NVDA'},
            {'type': 'google_sunday', 'params': 'tickers=GOOGL'},
            {'type': 'amazon_sunday', 'params': 'tickers=AMZN'},
            {'type': 'tesla_sunday', 'params': 'tickers=TSLA'},
            {'type': 'jpmorgan_sunday', 'params': 'tickers=JPM'},
            {'type': 'visa_sunday', 'params': 'tickers=V'},
            {'type': 'coinbase_sunday', 'params': 'tickers=COIN'},
            {'type': 'meta_sunday', 'params': 'tickers=META'},
            
            # === 단순 조합 (7개) ===
            {'type': 'apple_finance', 'params': 'tickers=AAPL&topics=finance'},
            {'type': 'microsoft_finance', 'params': 'tickers=MSFT&topics=finance'},
            {'type': 'jpmorgan_earnings', 'params': 'tickers=JPM&topics=earnings'},
            {'type': 'visa_technology', 'params': 'tickers=V&topics=technology'},
            {'type': 'coinbase_finance', 'params': 'tickers=COIN&topics=finance'},
            {'type': 'nvidia_finance', 'params': 'tickers=NVDA&topics=finance'},
            {'type': 'tesla_finance', 'params': 'tickers=TSLA&topics=finance'},
        ]

def collect_news_sentiment_25(**context):
    """
    25개 제한에 맞춘 요일별 전문화 뉴스 수집
    """
    # API 키 가져오기
    api_key = Variable.get('ALPHA_VANTAGE_NEWS_API_KEY_2')
    if not api_key:
        raise ValueError("🔑 ALPHA_VANTAGE_NEWS_API_KEY_2가 설정되지 않았습니다")
    
    # DB 연결 및 batch_id 생성
    hook = PostgresHook(postgres_conn_id='postgres_default')
    max_batch_result = hook.get_first("SELECT COALESCE(MAX(batch_id), 0) FROM market_news_sentiment")
    current_batch_id = (max_batch_result[0] if max_batch_result else 0) + 1
    
    execution_date = context['execution_date']
    generator = SimplifiedWeeklySpecializedQueries(execution_date)
    day_name = generator.day_names[generator.day_of_week]
    
    print(f"🗓️ 실행 날짜: {execution_date.strftime('%Y-%m-%d')} ({day_name})")
    print(f"🆔 배치 ID: {current_batch_id}")
    print(f"🎯 일일 제한: 25개 호출 (최적화됨)")
    
    # 요일별 25개 전문 쿼리 생성
    query_combinations = generator.generate_simplified_weekday_queries()
    
    print(f"📊 {day_name} 전문 쿼리: {len(query_combinations)}개")
    
    # 쿼리 카테고리별 통계
    if generator.day_of_week == 0:  # 월요일
        print(f"├─ 에너지 전문: 15개")
        print(f"└─ 제조업 전문: 10개")
    elif generator.day_of_week == 1:  # 화요일
        print(f"├─ 기술 전문: 18개")
        print(f"└─ IPO 전문: 7개")
    elif generator.day_of_week == 2:  # 수요일
        print(f"├─ 블록체인 전문: 15개")
        print(f"└─ 금융 전문: 10개")
    elif generator.day_of_week == 3:  # 목요일
        print(f"├─ 실적 전문: 15개")
        print(f"└─ 헬스케어 전문: 10개")
    elif generator.day_of_week == 4:  # 금요일
        print(f"├─ 리테일 전문: 15개")
        print(f"└─ M&A 전문: 10개")
    elif generator.day_of_week == 5:  # 토요일
        print(f"├─ 부동산 전문: 15개")
        print(f"└─ 거시경제 전문: 10개")
    else:  # 일요일
        print(f"├─ 금융시장 전문: 15개")
        print(f"└─ 정책 전문: 10개")
    
    all_news_data = []
    total_api_calls = 0
    successful_queries = 0
    failed_queries = 0
    
    for i, query_combo in enumerate(query_combinations, 1):
        try:
            # API 요청
            url = "https://www.alphavantage.co/query"
            params = f"function=NEWS_SENTIMENT&{query_combo['params']}&apikey={api_key}"
            
            print(f"🚀 [{i:2d}/25] {query_combo['type']}")
            response = requests.get(f"{url}?{params}", timeout=60)
            response.raise_for_status()
            total_api_calls += 1
            
            # API 응답 검증
            if 'Error Message' in response.text:
                print(f"❌ API 오류: {response.text[:100]}")
                failed_queries += 1
                continue
                
            if 'Note' in response.text and 'API call frequency' in response.text:
                print(f"⚠️ API 호출 제한 도달: {total_api_calls}번째 호출에서 중단")
                break
            
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {str(e)}")
                failed_queries += 1
                continue
            
            # 뉴스 피드 확인
            if 'feed' not in data:
                print(f"⚠️ 뉴스 피드 없음")
                failed_queries += 1
                continue
            
            news_count = len(data['feed'])
            print(f"✅ {news_count}개 뉴스 수집")
            successful_queries += 1
            
            # 뉴스 데이터에 쿼리 정보 추가
            for article in data['feed']:
                article['query_type'] = query_combo['type']
                article['query_params'] = query_combo['params']
                article['weekday'] = day_name
                article['weekday_number'] = generator.day_of_week
                article['batch_id'] = current_batch_id
                article['api_version'] = '25_limit_v1'  # 25개 제한 버전 표시
            
            all_news_data.extend(data['feed'])
            
        except Exception as e:
            print(f"❌ 쿼리 실패: {str(e)}")
            failed_queries += 1
            continue
    
    print(f"\n📈 {day_name} 25개 제한 수집 통계:")
    print(f"├─ 총 API 호출: {total_api_calls}회 / 25회")
    print(f"├─ 성공한 쿼리: {successful_queries}개")
    print(f"├─ 실패한 쿼리: {failed_queries}개")
    print(f"├─ 총 뉴스 수집: {len(all_news_data)}개")
    print(f"├─ 호출 효율성: {len(all_news_data)/total_api_calls:.1f}개/호출" if total_api_calls > 0 else "├─ 호출 효율성: N/A")
    print(f"└─ API 제한: 25개 최적화 (50개 → 25개)")
    
    # XCom에 저장
    context['ti'].xcom_push(key='news_data', value=all_news_data)
    context['ti'].xcom_push(key='batch_id', value=current_batch_id)
    
    return {
        'batch_id': current_batch_id,
        'total_api_calls': total_api_calls,
        'successful_queries': successful_queries,
        'failed_queries': failed_queries,
        'total_news': len(all_news_data),
        'weekday': day_name,
        'api_version': '25_limit_v1',
        'execution_date': execution_date.strftime('%Y-%m-%d %A')
    }

def process_and_store_25_limit_news(**context):
    """
    수집된 뉴스 데이터를 PostgreSQL에 저장 (25개 제한 버전)
    """
    # XCom에서 데이터 가져오기
    news_data = context['ti'].xcom_pull(task_ids='collect_news_sentiment_25', key='news_data')
    batch_id = context['ti'].xcom_pull(task_ids='collect_news_sentiment_25', key='batch_id')
    
    if not news_data:
        raise ValueError("❌ 이전 태스크에서 뉴스 데이터를 받지 못했습니다")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    duplicate_count = 0
    
    execution_date = context['execution_date']
    generator = SimplifiedWeeklySpecializedQueries(execution_date)
    day_name = generator.day_names[generator.day_of_week]
    
    print(f"🚀 배치 {batch_id} ({day_name}) 25개 제한 뉴스 저장 시작: {len(news_data)}개")
    
    # 중복 확인을 위한 기존 URL 조회 (최근 7일)
    existing_urls = set()
    try:
        existing_records = hook.get_records(
            "SELECT url FROM market_news_sentiment WHERE batch_id >= %s", 
            parameters=[batch_id - 7]
        )
        existing_urls = {record[0] for record in existing_records}
        print(f"📋 중복 체크: 최근 7일간 {len(existing_urls)}개 URL 확인")
    except Exception as e:
        print(f"⚠️ 중복 체크 실패, 계속 진행: {str(e)}")
    
    for i, article in enumerate(news_data, 1):
        try:
            # 필수 필드 검증
            if not article.get('url') or not article.get('title'):
                print(f"⚠️ [{i:4d}] 필수 필드 누락: {article.get('title', 'Unknown')[:50]}")
                error_count += 1
                continue
            
            # 중복 체크
            if article['url'] in existing_urls:
                duplicate_count += 1
                continue
            
            # 시간 파싱
            time_published = datetime.strptime(article['time_published'], '%Y%m%dT%H%M%S')
            
            # 작성자 처리
            authors = ', '.join(article.get('authors', [])) if article.get('authors') else None
            
            # 파라미터 준비
            params = {
                'batch_id': batch_id,
                'title': article['title'][:500],
                'url': article['url'],
                'time_published': time_published,
                'authors': authors[:200] if authors else None,
                'summary': article.get('summary', '')[:1000],
                'source': article.get('source', '')[:100],
                'overall_sentiment_score': Decimal(str(article.get('overall_sentiment_score', 0))),
                'overall_sentiment_label': article.get('overall_sentiment_label', 'Neutral'),
                'ticker_sentiment': json.dumps(article.get('ticker_sentiment', [])),
                'topics': json.dumps(article.get('topics', [])),
                'query_type': article['query_type'],
                'query_params': article['query_params']
            }
            
            # SQL 실행
            hook.run(UPSERT_SQL, parameters=params)
            success_count += 1
            existing_urls.add(article['url'])
            
            # 진행률 표시
            if i % 100 == 0:
                print(f"📊 진행률: {i}/{len(news_data)} ({i/len(news_data)*100:.1f}%)")
            
        except Exception as e:
            print(f"❌ [{i:4d}] 뉴스 저장 실패: {article.get('title', 'Unknown')[:50]} - {str(e)}")
            error_count += 1
            continue
    
    print(f"\n🎯 배치 {batch_id} ({day_name}) 25개 제한 저장 완료:")
    print(f"├─ 성공 저장: {success_count}개")
    print(f"├─ 중복 제외: {duplicate_count}개")
    print(f"├─ 오류 발생: {error_count}개")
    print(f"├─ 저장 효율: {success_count/(len(news_data))*100:.1f}%")
    print(f"└─ API 버전: 25_limit_v1 (최적화된 호출)")
    
    # 최종 통계
    total_records = hook.get_first("SELECT COUNT(*) FROM market_news_sentiment")[0]
    batch_records = hook.get_first("SELECT COUNT(*) FROM market_news_sentiment WHERE batch_id = %s", parameters=[batch_id])[0]
    
    print(f"📊 데이터베이스 현황:")
    print(f"├─ 이번 배치 ({day_name}): {batch_records}개")
    print(f"└─ 전체 레코드: {total_records}개")
    
    return {
        'batch_id': batch_id,
        'weekday': day_name,
        'success_count': success_count,
        'duplicate_count': duplicate_count,
        'error_count': error_count,
        'batch_records': batch_records,
        'total_records': total_records,
        'api_version': '25_limit_v1'
    }

# DAG 정의
with DAG(
    dag_id='ingest_market_news_sentiment',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Alpha Vantage News & Sentiment API',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['news', 'sentiment', 'alpha_vantage'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_market_news_sentiment_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news_sentiment.sql',
    )
    
    # 2. 25개 제한 요일별 뉴스 수집
    collect_news = PythonOperator(
        task_id='collect_news_sentiment_25',
        python_callable=collect_news_sentiment_25,
    )
    
    # 3. 데이터 가공 및 저장
    process_news = PythonOperator(
        task_id='process_and_store_25_limit_news',
        python_callable=process_and_store_25_limit_news,
    )
    
    # Task 의존성
    create_table >> collect_news >> process_news