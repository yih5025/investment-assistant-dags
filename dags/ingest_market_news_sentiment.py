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

class WeeklySpecialized25QueryGenerator:
    """25개 제한 - 요일별 완전 전문화 쿼리 생성기"""
    
    # ✅ 실제 지원되는 topics만 사용
    VALID_TOPICS = [
        'blockchain', 'earnings', 'ipo', 'mergers_and_acquisitions',
        'financial_markets', 'economy_fiscal', 'economy_monetary', 
        'economy_macro', 'energy_transportation', 'finance',
        'life_sciences', 'manufacturing', 'real_estate', 
        'retail_wholesale', 'technology'
    ]
    
    # ✅ 검증된 ticker들
    STOCK_TICKERS = {
        'megacap': ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'TSLA', 'META'],
        'tech': ['IBM', 'ORCL', 'ADBE', 'CRM', 'INTC', 'AMD', 'QCOM'],
        'finance': ['JPM', 'BAC', 'WFC', 'C', 'GS', 'V', 'MA'],
        'crypto_stocks': ['COIN', 'MSTR', 'RIOT', 'MARA', 'CLSK'],
        'energy': ['XOM', 'CVX', 'COP', 'EOG', 'SLB'],
        'healthcare': ['JNJ', 'PFE', 'UNH', 'MRNA', 'ABBV'],
        'consumer': ['WMT', 'TGT', 'COST', 'HD', 'LOW'],
        'media': ['DIS', 'NFLX', 'CMCSA', 'VZ', 'T']
    }
    
    CRYPTO_TICKERS = ['CRYPTO:BTC', 'CRYPTO:ETH', 'CRYPTO:SOL', 'CRYPTO:ADA']
    FOREX_TICKERS = ['FOREX:USD', 'FOREX:EUR', 'FOREX:JPY', 'FOREX:GBP']
    
    def __init__(self, execution_date: datetime):
        self.execution_date = execution_date
        self.day_of_week = execution_date.weekday()
        self.day_names = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']
    
    def generate_weekday_queries(self) -> list:
        """요일별 25개 전문화 쿼리 생성"""
        
        if self.day_of_week == 0:  # 월요일
            return self._monday_energy_manufacturing()
        elif self.day_of_week == 1:  # 화요일
            return self._tuesday_technology_ipo()
        elif self.day_of_week == 2:  # 수요일
            return self._wednesday_blockchain_finance()
        elif self.day_of_week == 3:  # 목요일
            return self._thursday_earnings_healthcare()
        elif self.day_of_week == 4:  # 금요일
            return self._friday_retail_mergers()
        elif self.day_of_week == 5:  # 토요일
            return self._saturday_realestate_macro()
        else:  # 일요일
            return self._sunday_markets_fiscal()
    
    def _monday_energy_manufacturing(self) -> list:
        """월요일: 에너지 & 제조업 전문 (25개)"""
        return [
            # === 에너지 핵심 (15개) ===
            {'type': 'energy_general', 'params': 'topics=energy_transportation&sort=LATEST'},
            {'type': 'energy_relevant', 'params': 'topics=energy_transportation&sort=RELEVANCE'},
            {'type': 'energy_companies_all', 'params': f'tickers={",".join(self.STOCK_TICKERS["energy"])}&sort=LATEST'},
            {'type': 'energy_exxon', 'params': 'tickers=XOM&sort=LATEST'},
            {'type': 'energy_chevron', 'params': 'tickers=CVX&sort=LATEST'},
            {'type': 'energy_conocophillips', 'params': 'tickers=COP&sort=LATEST'},
            {'type': 'energy_eog', 'params': 'tickers=EOG&sort=LATEST'},
            {'type': 'energy_slb', 'params': 'tickers=SLB&sort=LATEST'},
            {'type': 'tesla_energy', 'params': 'tickers=TSLA&topics=energy_transportation'},
            {'type': 'energy_economy', 'params': 'topics=energy_transportation,economy_macro'},
            {'type': 'energy_fiscal_policy', 'params': 'topics=energy_transportation,economy_fiscal'},
            {'type': 'energy_markets', 'params': 'topics=energy_transportation,financial_markets'},
            {'type': 'energy_manufacturing', 'params': 'topics=energy_transportation,manufacturing'},
            {'type': 'energy_tech_combo', 'params': 'topics=energy_transportation,technology'},
            {'type': 'energy_finance', 'params': 'topics=energy_transportation,finance'},
            
            # === 제조업 전문 (10개) ===
            {'type': 'manufacturing_general', 'params': 'topics=manufacturing&sort=LATEST'},
            {'type': 'manufacturing_relevant', 'params': 'topics=manufacturing&sort=RELEVANCE'},
            {'type': 'manufacturing_caterpillar', 'params': 'tickers=CAT&topics=manufacturing'},
            {'type': 'manufacturing_ge', 'params': 'tickers=GE&topics=manufacturing'},
            {'type': 'manufacturing_deere', 'params': 'tickers=DE&topics=manufacturing'},
            {'type': 'manufacturing_ford', 'params': 'tickers=F&topics=manufacturing'},
            {'type': 'manufacturing_gm', 'params': 'tickers=GM&topics=manufacturing'},
            {'type': 'manufacturing_boeing', 'params': 'tickers=BA&topics=manufacturing'},
            {'type': 'manufacturing_economy', 'params': 'topics=manufacturing,economy_macro'},
            {'type': 'manufacturing_tech', 'params': 'topics=manufacturing,technology'},
        ]
    
    def _tuesday_technology_ipo(self) -> list:
        """화요일: 기술 & IPO 전문 (25개)"""
        return [
            # === 기술 핵심 (18개) ===
            {'type': 'technology_general', 'params': 'topics=technology&sort=LATEST'},
            {'type': 'technology_relevant', 'params': 'topics=technology&sort=RELEVANCE'},
            {'type': 'tech_megacap_all', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"])}&topics=technology'},
            {'type': 'apple_tech', 'params': 'tickers=AAPL&topics=technology'},
            {'type': 'microsoft_tech', 'params': 'tickers=MSFT&topics=technology'},
            {'type': 'nvidia_tech', 'params': 'tickers=NVDA&topics=technology'},
            {'type': 'google_tech', 'params': 'tickers=GOOGL&topics=technology'},
            {'type': 'amazon_tech', 'params': 'tickers=AMZN&topics=technology'},
            {'type': 'tesla_tech', 'params': 'tickers=TSLA&topics=technology'},
            {'type': 'meta_tech', 'params': 'tickers=META&topics=technology'},
            {'type': 'oracle_tech', 'params': 'tickers=ORCL&topics=technology'},
            {'type': 'salesforce_tech', 'params': 'tickers=CRM&topics=technology'},
            {'type': 'intel_tech', 'params': 'tickers=INTC&topics=technology'},
            {'type': 'amd_tech', 'params': 'tickers=AMD&topics=technology'},
            {'type': 'tech_manufacturing', 'params': 'topics=technology,manufacturing'},
            {'type': 'tech_finance', 'params': 'topics=technology,finance'},
            {'type': 'tech_markets', 'params': 'topics=technology,financial_markets'},
            {'type': 'tech_economy', 'params': 'topics=technology,economy_macro'},
            
            # === IPO 전문 (7개) ===
            {'type': 'ipo_general', 'params': 'topics=ipo&sort=LATEST'},
            {'type': 'ipo_relevant', 'params': 'topics=ipo&sort=RELEVANCE'},
            {'type': 'ipo_technology', 'params': 'topics=ipo,technology'},
            {'type': 'ipo_finance', 'params': 'topics=ipo,finance'},
            {'type': 'ipo_markets', 'params': 'topics=ipo,financial_markets'},
            {'type': 'ipo_rblx', 'params': 'tickers=RBLX&topics=ipo'},
            {'type': 'ipo_coin', 'params': 'tickers=COIN&topics=ipo'},
        ]
    
    def _wednesday_blockchain_finance(self) -> list:
        """수요일: 블록체인 & 금융 전문 (25개)"""
        return [
            # === 블록체인 & 암호화폐 (15개) ===
            {'type': 'blockchain_general', 'params': 'topics=blockchain&sort=LATEST'},
            {'type': 'blockchain_relevant', 'params': 'topics=blockchain&sort=RELEVANCE'},
            {'type': 'crypto_bitcoin', 'params': 'tickers=CRYPTO:BTC&sort=LATEST'},
            {'type': 'crypto_ethereum', 'params': 'tickers=CRYPTO:ETH&sort=LATEST'},
            {'type': 'crypto_solana', 'params': 'tickers=CRYPTO:SOL&sort=LATEST'},
            {'type': 'crypto_cardano', 'params': 'tickers=CRYPTO:ADA&sort=LATEST'},
            {'type': 'crypto_all', 'params': f'tickers={",".join(self.CRYPTO_TICKERS)}&sort=LATEST'},
            {'type': 'crypto_coinbase', 'params': 'tickers=COIN&sort=LATEST'},
            {'type': 'crypto_microstrategy', 'params': 'tickers=MSTR&sort=LATEST'},
            {'type': 'crypto_stocks_all', 'params': f'tickers={",".join(self.STOCK_TICKERS["crypto_stocks"])}&sort=LATEST'},
            {'type': 'blockchain_finance', 'params': 'topics=blockchain,finance'},
            {'type': 'blockchain_tech', 'params': 'topics=blockchain,technology'},
            {'type': 'blockchain_markets', 'params': 'topics=blockchain,financial_markets'},
            {'type': 'bitcoin_blockchain', 'params': 'tickers=CRYPTO:BTC&topics=blockchain'},
            {'type': 'ethereum_blockchain', 'params': 'tickers=CRYPTO:ETH&topics=blockchain'},
            
            # === 금융 섹터 (10개) ===
            {'type': 'finance_general', 'params': 'topics=finance&sort=LATEST'},
            {'type': 'finance_relevant', 'params': 'topics=finance&sort=RELEVANCE'},
            {'type': 'finance_banks_all', 'params': f'tickers={",".join(self.STOCK_TICKERS["finance"][:5])}&sort=LATEST'},
            {'type': 'jpmorgan_finance', 'params': 'tickers=JPM&sort=LATEST'},
            {'type': 'bofa_finance', 'params': 'tickers=BAC&sort=LATEST'},
            {'type': 'visa_mastercard', 'params': 'tickers=V,MA&sort=LATEST'},
            {'type': 'finance_tech', 'params': 'topics=finance,technology'},
            {'type': 'finance_markets', 'params': 'topics=finance,financial_markets'},
            {'type': 'finance_economy', 'params': 'topics=finance,economy_macro'},
            {'type': 'finance_monetary', 'params': 'topics=finance,economy_monetary'},
        ]
    
    def _thursday_earnings_healthcare(self) -> list:
        """목요일: 실적 & 헬스케어 전문 (25개)"""
        return [
            # === 실적 시즌 (15개) ===
            {'type': 'earnings_general', 'params': 'topics=earnings&sort=LATEST'},
            {'type': 'earnings_relevant', 'params': 'topics=earnings&sort=RELEVANCE'},
            {'type': 'earnings_megacap', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"])}&topics=earnings'},
            {'type': 'apple_earnings', 'params': 'tickers=AAPL&topics=earnings'},
            {'type': 'microsoft_earnings', 'params': 'tickers=MSFT&topics=earnings'},
            {'type': 'nvidia_earnings', 'params': 'tickers=NVDA&topics=earnings'},
            {'type': 'google_earnings', 'params': 'tickers=GOOGL&topics=earnings'},
            {'type': 'amazon_earnings', 'params': 'tickers=AMZN&topics=earnings'},
            {'type': 'tesla_earnings', 'params': 'tickers=TSLA&topics=earnings'},
            {'type': 'meta_earnings', 'params': 'tickers=META&topics=earnings'},
            {'type': 'jpmorgan_earnings', 'params': 'tickers=JPM&topics=earnings'},
            {'type': 'earnings_finance', 'params': 'topics=earnings,finance'},
            {'type': 'earnings_tech', 'params': 'topics=earnings,technology'},
            {'type': 'earnings_markets', 'params': 'topics=earnings,financial_markets'},
            {'type': 'earnings_manufacturing', 'params': 'topics=earnings,manufacturing'},
            
            # === 헬스케어 & 생명과학 (10개) ===
            {'type': 'healthcare_general', 'params': 'topics=life_sciences&sort=LATEST'},
            {'type': 'healthcare_relevant', 'params': 'topics=life_sciences&sort=RELEVANCE'},
            {'type': 'healthcare_companies', 'params': f'tickers={",".join(self.STOCK_TICKERS["healthcare"])}&sort=LATEST'},
            {'type': 'jnj_healthcare', 'params': 'tickers=JNJ&sort=LATEST'},
            {'type': 'pfizer_healthcare', 'params': 'tickers=PFE&sort=LATEST'},
            {'type': 'unitedhealth_healthcare', 'params': 'tickers=UNH&sort=LATEST'},
            {'type': 'moderna_healthcare', 'params': 'tickers=MRNA&sort=LATEST'},
            {'type': 'healthcare_earnings', 'params': 'topics=life_sciences,earnings'},
            {'type': 'healthcare_tech', 'params': 'topics=life_sciences,technology'},
            {'type': 'healthcare_finance', 'params': 'topics=life_sciences,finance'},
        ]
    
    def _friday_retail_mergers(self) -> list:
        """금요일: 리테일 & 인수합병 전문 (25개)"""
        return [
            # === 리테일 & 소비재 (15개) ===
            {'type': 'retail_general', 'params': 'topics=retail_wholesale&sort=LATEST'},
            {'type': 'retail_relevant', 'params': 'topics=retail_wholesale&sort=RELEVANCE'},
            {'type': 'consumer_companies', 'params': f'tickers={",".join(self.STOCK_TICKERS["consumer"])}&sort=LATEST'},
            {'type': 'walmart_retail', 'params': 'tickers=WMT&sort=LATEST'},
            {'type': 'target_retail', 'params': 'tickers=TGT&sort=LATEST'},
            {'type': 'costco_retail', 'params': 'tickers=COST&sort=LATEST'},
            {'type': 'homedepot_retail', 'params': 'tickers=HD&sort=LATEST'},
            {'type': 'lowes_retail', 'params': 'tickers=LOW&sort=LATEST'},
            {'type': 'amazon_retail', 'params': 'tickers=AMZN&topics=retail_wholesale'},
            {'type': 'retail_tech', 'params': 'topics=retail_wholesale,technology'},
            {'type': 'retail_finance', 'params': 'topics=retail_wholesale,finance'},
            {'type': 'retail_earnings', 'params': 'topics=retail_wholesale,earnings'},
            {'type': 'retail_markets', 'params': 'topics=retail_wholesale,financial_markets'},
            {'type': 'retail_economy', 'params': 'topics=retail_wholesale,economy_macro'},
            {'type': 'retail_manufacturing', 'params': 'topics=retail_wholesale,manufacturing'},
            
            # === 인수합병 & M&A (10개) ===
            {'type': 'mergers_general', 'params': 'topics=mergers_and_acquisitions&sort=LATEST'},
            {'type': 'mergers_relevant', 'params': 'topics=mergers_and_acquisitions&sort=RELEVANCE'},
            {'type': 'mergers_tech', 'params': 'topics=mergers_and_acquisitions,technology'},
            {'type': 'mergers_finance', 'params': 'topics=mergers_and_acquisitions,finance'},
            {'type': 'mergers_healthcare', 'params': 'topics=mergers_and_acquisitions,life_sciences'},
            {'type': 'microsoft_mergers', 'params': 'tickers=MSFT&topics=mergers_and_acquisitions'},
            {'type': 'google_mergers', 'params': 'tickers=GOOGL&topics=mergers_and_acquisitions'},
            {'type': 'amazon_mergers', 'params': 'tickers=AMZN&topics=mergers_and_acquisitions'},
            {'type': 'mergers_markets', 'params': 'topics=mergers_and_acquisitions,financial_markets'},
            {'type': 'mergers_retail', 'params': 'topics=mergers_and_acquisitions,retail_wholesale'},
        ]
    
    def _saturday_realestate_macro(self) -> list:
        """토요일: 부동산 & 거시경제 전문 (25개)"""
        return [
            # === 부동산 섹터 (15개) ===
            {'type': 'realestate_general', 'params': 'topics=real_estate&sort=LATEST'},
            {'type': 'realestate_relevant', 'params': 'topics=real_estate&sort=RELEVANCE'},
            {'type': 'realestate_finance', 'params': 'topics=real_estate,finance'},
            {'type': 'realestate_economy', 'params': 'topics=real_estate,economy_macro'},
            {'type': 'realestate_monetary', 'params': 'topics=real_estate,economy_monetary'},
            {'type': 'realestate_fiscal', 'params': 'topics=real_estate,economy_fiscal'},
            {'type': 'realestate_manufacturing', 'params': 'topics=real_estate,manufacturing'},
            {'type': 'realestate_tech', 'params': 'topics=real_estate,technology'},
            {'type': 'realestate_retail', 'params': 'topics=real_estate,retail_wholesale'},
            {'type': 'realestate_markets', 'params': 'topics=real_estate,financial_markets'},
            {'type': 'realestate_earnings', 'params': 'topics=real_estate,earnings'},
            {'type': 'home_builders', 'params': 'tickers=HD,LOW&topics=real_estate'},
            {'type': 'realestate_energy', 'params': 'topics=real_estate,energy_transportation'},
            {'type': 'realestate_ipo', 'params': 'topics=real_estate,ipo'},
            {'type': 'realestate_mergers', 'params': 'topics=real_estate,mergers_and_acquisitions'},
            
            # === 거시경제 지표 (10개) ===
            {'type': 'macro_general', 'params': 'topics=economy_macro&sort=LATEST'},
            {'type': 'macro_relevant', 'params': 'topics=economy_macro&sort=RELEVANCE'},
            {'type': 'macro_finance', 'params': 'topics=economy_macro,finance'},
            {'type': 'macro_markets', 'params': 'topics=economy_macro,financial_markets'},
            {'type': 'macro_monetary', 'params': 'topics=economy_macro,economy_monetary'},
            {'type': 'macro_fiscal', 'params': 'topics=economy_macro,economy_fiscal'},
            {'type': 'macro_manufacturing', 'params': 'topics=economy_macro,manufacturing'},
            {'type': 'macro_energy', 'params': 'topics=economy_macro,energy_transportation'},
            {'type': 'forex_macro', 'params': f'tickers={",".join(self.FOREX_TICKERS)}&topics=economy_macro'},
            {'type': 'usd_macro', 'params': 'tickers=FOREX:USD&topics=economy_macro'},
        ]
    
    def _sunday_markets_fiscal(self) -> list:
        """일요일: 금융시장 & 재정정책 전문 (25개)"""
        return [
            # === 금융시장 전반 (15개) ===
            {'type': 'markets_general', 'params': 'topics=financial_markets&sort=LATEST'},
            {'type': 'markets_relevant', 'params': 'topics=financial_markets&sort=RELEVANCE'},
            {'type': 'markets_tech', 'params': 'topics=financial_markets,technology'},
            {'type': 'markets_finance', 'params': 'topics=financial_markets,finance'},
            {'type': 'markets_earnings', 'params': 'topics=financial_markets,earnings'},
            {'type': 'markets_macro', 'params': 'topics=financial_markets,economy_macro'},
            {'type': 'markets_monetary', 'params': 'topics=financial_markets,economy_monetary'},
            {'type': 'markets_energy', 'params': 'topics=financial_markets,energy_transportation'},
            {'type': 'markets_healthcare', 'params': 'topics=financial_markets,life_sciences'},
            {'type': 'markets_retail', 'params': 'topics=financial_markets,retail_wholesale'},
            {'type': 'markets_realestate', 'params': 'topics=financial_markets,real_estate'},
            {'type': 'markets_manufacturing', 'params': 'topics=financial_markets,manufacturing'},
            {'type': 'markets_blockchain', 'params': 'topics=financial_markets,blockchain'},
            {'type': 'markets_ipo', 'params': 'topics=financial_markets,ipo'},
            {'type': 'markets_mergers', 'params': 'topics=financial_markets,mergers_and_acquisitions'},
            
            # === 통화정책 & 재정정책 (10개) ===
            {'type': 'monetary_general', 'params': 'topics=economy_monetary&sort=LATEST'},
            {'type': 'monetary_relevant', 'params': 'topics=economy_monetary&sort=RELEVANCE'},
            {'type': 'fiscal_general', 'params': 'topics=economy_fiscal&sort=LATEST'},
            {'type': 'fiscal_relevant', 'params': 'topics=economy_fiscal&sort=RELEVANCE'},
            {'type': 'monetary_finance', 'params': 'topics=economy_monetary,finance'},
            {'type': 'monetary_markets', 'params': 'topics=economy_monetary,financial_markets'},
            {'type': 'fiscal_finance', 'params': 'topics=economy_fiscal,finance'},
            {'type': 'fiscal_markets', 'params': 'topics=economy_fiscal,financial_markets'},
            {'type': 'monetary_fiscal', 'params': 'topics=economy_monetary,economy_fiscal'},
            {'type': 'usd_policy', 'params': 'tickers=FOREX:USD&topics=economy_monetary'},
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
    generator = WeeklySpecialized25QueryGenerator(execution_date)
    day_name = generator.day_names[generator.day_of_week]
    
    print(f"🗓️ 실행 날짜: {execution_date.strftime('%Y-%m-%d')} ({day_name})")
    print(f"🆔 배치 ID: {current_batch_id}")
    print(f"🎯 일일 제한: 25개 호출 (최적화됨)")
    
    # 요일별 25개 전문 쿼리 생성
    query_combinations = generator.generate_weekday_queries()
    
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
    generator = WeeklySpecialized25QueryGenerator(execution_date)
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