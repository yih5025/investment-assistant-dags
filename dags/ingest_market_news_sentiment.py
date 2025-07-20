from datetime import datetime, timedelta
import os
import requests
from decimal import Decimal
import json
import random

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

class CorrectWeeklyQueryGenerator:
    """올바른 API 파라미터를 사용한 요일별 쿼리 생성기"""
    
    # ✅ 실제 지원되는 topics만 사용
    VALID_TOPICS = [
        'blockchain', 'earnings', 'ipo', 'mergers_and_acquisitions',
        'financial_markets', 'economy_fiscal', 'economy_monetary', 
        'economy_macro', 'energy_transportation', 'finance',
        'life_sciences', 'manufacturing', 'real_estate', 
        'retail_wholesale', 'technology'
    ]
    
    # ✅ 실제 존재하는 ticker들만 사용
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
    
    # ✅ 지원되는 암호화폐 및 포렉스
    CRYPTO_TICKERS = ['CRYPTO:BTC', 'CRYPTO:ETH', 'CRYPTO:SOL', 'CRYPTO:ADA']
    FOREX_TICKERS = ['FOREX:USD', 'FOREX:EUR', 'FOREX:JPY', 'FOREX:GBP']
    
    def __init__(self, execution_date: datetime):
        self.execution_date = execution_date
        self.day_of_week = execution_date.weekday()
        self.day_names = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']
    
    def get_time_from_hours(self, hours: int) -> str:
        """시간 전 날짜를 YYYYMMDDTHHMM 형식으로 반환"""
        target_time = self.execution_date - timedelta(hours=hours)
        return target_time.strftime('%Y%m%dT%H%M')
    
    def generate_weekday_queries(self) -> list:
        """요일별 올바른 API 파라미터로 50개 쿼리 생성"""
        
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
        """월요일: 에너지 & 제조업 중심 (50개)"""
        queries = []
        
        # === 에너지 섹터 집중 (20개) ===
        # 1. 에너지 토픽 단독 쿼리들
        queries.extend([
            {'type': 'energy_general', 'params': 'topics=energy_transportation&sort=LATEST&limit=50'},
            {'type': 'energy_relevant', 'params': 'topics=energy_transportation&sort=RELEVANCE&limit=40'},
            {'type': 'energy_recent_24h', 'params': f'topics=energy_transportation&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'energy_recent_48h', 'params': f'topics=energy_transportation&time_from={self.get_time_from_hours(48)}&limit=30'},
        ])
        
        # 2. 에너지 기업들
        energy_companies = self.STOCK_TICKERS['energy']
        queries.extend([
            {'type': 'energy_companies_all', 'params': f'tickers={",".join(energy_companies)}&sort=LATEST&limit=35'},
            {'type': 'energy_companies_xom', 'params': 'tickers=XOM&sort=LATEST&limit=25'},
            {'type': 'energy_companies_cvx', 'params': 'tickers=CVX&sort=LATEST&limit=25'},
            {'type': 'energy_companies_cop', 'params': 'tickers=COP&sort=LATEST&limit=20'},
            {'type': 'energy_companies_eog', 'params': 'tickers=EOG&sort=LATEST&limit=20'},
        ])
        
        # 3. 에너지 + 다른 토픽 조합
        queries.extend([
            {'type': 'energy_economy', 'params': 'topics=energy_transportation,economy_macro&limit=25'},
            {'type': 'energy_fiscal', 'params': 'topics=energy_transportation,economy_fiscal&limit=20'},
            {'type': 'energy_markets', 'params': 'topics=energy_transportation,financial_markets&limit=20'},
            {'type': 'energy_manufacturing', 'params': 'topics=energy_transportation,manufacturing&limit=15'},
        ])
        
        # 4. 테슬라 에너지 관련
        queries.extend([
            {'type': 'tesla_energy', 'params': 'tickers=TSLA&topics=energy_transportation&limit=25'},
            {'type': 'tesla_general', 'params': 'tickers=TSLA&sort=LATEST&limit=20'},
        ])
        
        # 5. 시간 범위별 에너지
        queries.extend([
            {'type': 'energy_72h', 'params': f'topics=energy_transportation&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'energy_week', 'params': f'topics=energy_transportation&time_from={self.get_time_from_hours(168)}&limit=15'},
        ])
        
        # === 제조업 집중 (15개) ===
        queries.extend([
            {'type': 'manufacturing_general', 'params': 'topics=manufacturing&sort=LATEST&limit=40'},
            {'type': 'manufacturing_relevant', 'params': 'topics=manufacturing&sort=RELEVANCE&limit=35'},
            {'type': 'manufacturing_recent', 'params': f'topics=manufacturing&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'manufacturing_economy', 'params': 'topics=manufacturing,economy_macro&limit=25'},
            {'type': 'manufacturing_fiscal', 'params': 'topics=manufacturing,economy_fiscal&limit=20'},
            {'type': 'manufacturing_tech', 'params': 'topics=manufacturing,technology&limit=20'},
            {'type': 'manufacturing_energy', 'params': 'topics=manufacturing,energy_transportation&limit=15'},
            {'type': 'manufacturing_48h', 'params': f'topics=manufacturing&time_from={self.get_time_from_hours(48)}&limit=15'},
            {'type': 'manufacturing_72h', 'params': f'topics=manufacturing&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'cat_manufacturing', 'params': 'tickers=CAT&topics=manufacturing&limit=20'},
            {'type': 'ge_manufacturing', 'params': 'tickers=GE&topics=manufacturing&limit=15'},
            {'type': 'de_manufacturing', 'params': 'tickers=DE&topics=manufacturing&limit=15'},
            {'type': 'f_manufacturing', 'params': 'tickers=F&topics=manufacturing&limit=15'},
            {'type': 'gm_manufacturing', 'params': 'tickers=GM&topics=manufacturing&limit=15'},
            {'type': 'ba_manufacturing', 'params': 'tickers=BA&topics=manufacturing&limit=10'},
        ])
        
        # === 글로벌 경제 (15개) ===
        queries.extend([
            {'type': 'macro_economy', 'params': 'topics=economy_macro&sort=LATEST&limit=35'},
            {'type': 'macro_relevant', 'params': 'topics=economy_macro&sort=RELEVANCE&limit=30'},
            {'type': 'macro_recent', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(24)}&limit=25'},
            {'type': 'macro_manufacturing', 'params': 'topics=economy_macro,manufacturing&limit=20'},
            {'type': 'macro_energy', 'params': 'topics=economy_macro,energy_transportation&limit=20'},
            {'type': 'macro_markets', 'params': 'topics=economy_macro,financial_markets&limit=15'},
            {'type': 'forex_usd', 'params': 'tickers=FOREX:USD&sort=LATEST&limit=20'},
            {'type': 'forex_eur', 'params': 'tickers=FOREX:EUR&sort=LATEST&limit=15'},
            {'type': 'forex_jpy', 'params': 'tickers=FOREX:JPY&sort=LATEST&limit=15'},
            {'type': 'forex_all', 'params': f'tickers={",".join(self.FOREX_TICKERS[:3])}&limit=15'},
            {'type': 'china_stocks', 'params': 'tickers=BABA,NIO&sort=LATEST&limit=15'},
            {'type': 'global_mixed_1', 'params': 'topics=economy_macro,finance&limit=10'},
            {'type': 'global_mixed_2', 'params': 'topics=economy_macro,technology&limit=10'},
            {'type': 'global_mixed_3', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(72)}&limit=10'},
            {'type': 'global_mixed_4', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(168)}&limit=10'},
        ])
        
        return queries
    
    def _tuesday_technology_ipo(self) -> list:
        """화요일: 기술 & IPO 중심 (50개)"""
        queries = []
        
        # === 기술 섹터 집중 (25개) ===
        queries.extend([
            {'type': 'technology_general', 'params': 'topics=technology&sort=LATEST&limit=50'},
            {'type': 'technology_relevant', 'params': 'topics=technology&sort=RELEVANCE&limit=40'},
            {'type': 'technology_recent_24h', 'params': f'topics=technology&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'technology_recent_48h', 'params': f'topics=technology&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'tech_companies_mega', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"])}&topics=technology&limit=35'},
            {'type': 'tech_companies_others', 'params': f'tickers={",".join(self.STOCK_TICKERS["tech"])}&topics=technology&limit=30'},
            {'type': 'apple_tech', 'params': 'tickers=AAPL&topics=technology&limit=25'},
            {'type': 'microsoft_tech', 'params': 'tickers=MSFT&topics=technology&limit=25'},
            {'type': 'nvidia_tech', 'params': 'tickers=NVDA&topics=technology&limit=25'},
            {'type': 'google_tech', 'params': 'tickers=GOOGL&topics=technology&limit=25'},
            {'type': 'meta_tech', 'params': 'tickers=META&topics=technology&limit=20'},
            {'type': 'amazon_tech', 'params': 'tickers=AMZN&topics=technology&limit=20'},
            {'type': 'oracle_tech', 'params': 'tickers=ORCL&topics=technology&limit=20'},
            {'type': 'salesforce_tech', 'params': 'tickers=CRM&topics=technology&limit=20'},
            {'type': 'intel_tech', 'params': 'tickers=INTC&topics=technology&limit=15'},
            {'type': 'amd_tech', 'params': 'tickers=AMD&topics=technology&limit=15'},
            {'type': 'qualcomm_tech', 'params': 'tickers=QCOM&topics=technology&limit=15'},
            {'type': 'tech_manufacturing', 'params': 'topics=technology,manufacturing&limit=15'},
            {'type': 'tech_finance', 'params': 'topics=technology,finance&limit=15'},
            {'type': 'tech_markets', 'params': 'topics=technology,financial_markets&limit=15'},
            {'type': 'tech_72h', 'params': f'topics=technology&time_from={self.get_time_from_hours(72)}&limit=10'},
            {'type': 'tech_week', 'params': f'topics=technology&time_from={self.get_time_from_hours(168)}&limit=10'},
            {'type': 'tech_mix_1', 'params': f'tickers=IBM,ADBE&topics=technology&limit=10'},
            {'type': 'tech_mix_2', 'params': f'tickers=AAPL,MSFT,GOOGL&sort=LATEST&limit=10'},
            {'type': 'tech_mix_3', 'params': f'topics=technology&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
        ])
        
        # === IPO 및 기업공개 (15개) ===
        queries.extend([
            {'type': 'ipo_general', 'params': 'topics=ipo&sort=LATEST&limit=40'},
            {'type': 'ipo_relevant', 'params': 'topics=ipo&sort=RELEVANCE&limit=35'},
            {'type': 'ipo_recent_24h', 'params': f'topics=ipo&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'ipo_recent_48h', 'params': f'topics=ipo&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'ipo_technology', 'params': 'topics=ipo,technology&limit=25'},
            {'type': 'ipo_finance', 'params': 'topics=ipo,finance&limit=20'},
            {'type': 'ipo_markets', 'params': 'topics=ipo,financial_markets&limit=20'},
            {'type': 'ipo_manufacturing', 'params': 'topics=ipo,manufacturing&limit=15'},
            {'type': 'ipo_healthcare', 'params': 'topics=ipo,life_sciences&limit=15'},
            {'type': 'ipo_retail', 'params': 'topics=ipo,retail_wholesale&limit=15'},
            {'type': 'ipo_72h', 'params': f'topics=ipo&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'ipo_week', 'params': f'topics=ipo&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'rblx_ipo', 'params': 'tickers=RBLX&topics=ipo&limit=10'},
            {'type': 'hood_ipo', 'params': 'tickers=HOOD&topics=ipo&limit=10'},
            {'type': 'coin_ipo', 'params': 'tickers=COIN&topics=ipo&limit=10'},
        ])
        
        # === 혼합 쿼리 (10개) ===
        queries.extend([
            {'type': 'mixed_tech_ipo_1', 'params': 'topics=technology,ipo,financial_markets&limit=20'},
            {'type': 'mixed_tech_ipo_2', 'params': f'topics=technology,ipo&time_from={self.get_time_from_hours(36)}&limit=15'},
            {'type': 'mixed_megacap', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"][:4])}&sort=LATEST&limit=15'},
            {'type': 'mixed_tech_companies', 'params': f'tickers={",".join(self.STOCK_TICKERS["tech"][:4])}&sort=LATEST&limit=15'},
            {'type': 'mixed_growth_stocks', 'params': 'tickers=TSLA,NVDA,AMD,CRM&sort=LATEST&limit=12'},
            {'type': 'mixed_cloud_stocks', 'params': 'tickers=MSFT,GOOGL,AMZN,ORCL&sort=LATEST&limit=12'},
            {'type': 'mixed_recent_12h', 'params': f'topics=technology&time_from={self.get_time_from_hours(12)}&sort=LATEST&limit=10'},
            {'type': 'mixed_recent_6h', 'params': f'topics=ipo&time_from={self.get_time_from_hours(6)}&sort=LATEST&limit=10'},
            {'type': 'mixed_relevance_tech', 'params': 'topics=technology&sort=RELEVANCE&limit=8'},
            {'type': 'mixed_relevance_ipo', 'params': 'topics=ipo&sort=RELEVANCE&limit=8'},
        ])
        
        return queries
    
    def _wednesday_blockchain_finance(self) -> list:
        """수요일: 블록체인 & 금융 중심 (50개)"""
        queries = []
        
        # === 블록체인 및 암호화폐 (25개) ===
        queries.extend([
            {'type': 'blockchain_general', 'params': 'topics=blockchain&sort=LATEST&limit=50'},
            {'type': 'blockchain_relevant', 'params': 'topics=blockchain&sort=RELEVANCE&limit=40'},
            {'type': 'blockchain_recent_24h', 'params': f'topics=blockchain&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'blockchain_recent_48h', 'params': f'topics=blockchain&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'crypto_bitcoin', 'params': 'tickers=CRYPTO:BTC&sort=LATEST&limit=40'},
            {'type': 'crypto_ethereum', 'params': 'tickers=CRYPTO:ETH&sort=LATEST&limit=35'},
            {'type': 'crypto_solana', 'params': 'tickers=CRYPTO:SOL&sort=LATEST&limit=30'},
            {'type': 'crypto_cardano', 'params': 'tickers=CRYPTO:ADA&sort=LATEST&limit=25'},
            {'type': 'crypto_all', 'params': f'tickers={",".join(self.CRYPTO_TICKERS)}&sort=LATEST&limit=30'},
            {'type': 'crypto_stocks_coin', 'params': 'tickers=COIN&sort=LATEST&limit=30'},
            {'type': 'crypto_stocks_mstr', 'params': 'tickers=MSTR&sort=LATEST&limit=25'},
            {'type': 'crypto_stocks_riot', 'params': 'tickers=RIOT&sort=LATEST&limit=20'},
            {'type': 'crypto_stocks_mara', 'params': 'tickers=MARA&sort=LATEST&limit=20'},
            {'type': 'crypto_stocks_all', 'params': f'tickers={",".join(self.STOCK_TICKERS["crypto_stocks"])}&sort=LATEST&limit=25'},
            {'type': 'blockchain_finance', 'params': 'topics=blockchain,finance&limit=25'},
            {'type': 'blockchain_tech', 'params': 'topics=blockchain,technology&limit=20'},
            {'type': 'blockchain_markets', 'params': 'topics=blockchain,financial_markets&limit=20'},
            {'type': 'bitcoin_blockchain', 'params': 'tickers=CRYPTO:BTC&topics=blockchain&limit=20'},
            {'type': 'ethereum_blockchain', 'params': 'tickers=CRYPTO:ETH&topics=blockchain&limit=20'},
            {'type': 'coin_blockchain', 'params': 'tickers=COIN&topics=blockchain&limit=15'},
            {'type': 'blockchain_72h', 'params': f'topics=blockchain&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'blockchain_week', 'params': f'topics=blockchain&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'crypto_mix_1', 'params': 'tickers=CRYPTO:BTC,CRYPTO:ETH&sort=RELEVANCE&limit=15'},
            {'type': 'crypto_mix_2', 'params': f'tickers=CRYPTO:SOL,CRYPTO:ADA&time_from={self.get_time_from_hours(24)}&limit=10'},
            {'type': 'crypto_mix_3', 'params': f'topics=blockchain&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
        ])
        
        # === 금융 섹터 (25개) ===
        finance_companies = self.STOCK_TICKERS['finance']
        queries.extend([
            {'type': 'finance_general', 'params': 'topics=finance&sort=LATEST&limit=40'},
            {'type': 'finance_relevant', 'params': 'topics=finance&sort=RELEVANCE&limit=35'},
            {'type': 'finance_recent_24h', 'params': f'topics=finance&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'finance_recent_48h', 'params': f'topics=finance&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'finance_companies_all', 'params': f'tickers={",".join(finance_companies)}&sort=LATEST&limit=30'},
            {'type': 'jpmorgan_finance', 'params': 'tickers=JPM&sort=LATEST&limit=25'},
            {'type': 'bofa_finance', 'params': 'tickers=BAC&sort=LATEST&limit=25'},
            {'type': 'wells_fargo_finance', 'params': 'tickers=WFC&sort=LATEST&limit=20'},
            {'type': 'citi_finance', 'params': 'tickers=C&sort=LATEST&limit=20'},
            {'type': 'goldman_finance', 'params': 'tickers=GS&sort=LATEST&limit=20'},
            {'type': 'visa_finance', 'params': 'tickers=V&sort=LATEST&limit=20'},
            {'type': 'mastercard_finance', 'params': 'tickers=MA&sort=LATEST&limit=20'},
            {'type': 'finance_blockchain', 'params': 'topics=finance,blockchain&limit=20'},
            {'type': 'finance_tech', 'params': 'topics=finance,technology&limit=15'},
            {'type': 'finance_markets', 'params': 'topics=finance,financial_markets&limit=15'},
            {'type': 'finance_economy', 'params': 'topics=finance,economy_macro&limit=15'},
            {'type': 'finance_monetary', 'params': 'topics=finance,economy_monetary&limit=15'},
            {'type': 'finance_72h', 'params': f'topics=finance&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'finance_week', 'params': f'topics=finance&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'banks_mix_1', 'params': 'tickers=JPM,BAC,WFC&topics=finance&limit=15'},
            {'type': 'banks_mix_2', 'params': 'tickers=C,GS&topics=finance&limit=10'},
            {'type': 'payments_mix', 'params': 'tickers=V,MA,SQ,PYPL&sort=LATEST&limit=15'},
            {'type': 'fintech_mix', 'params': 'tickers=SQ,PYPL,HOOD&topics=finance&limit=10'},
            {'type': 'finance_mix_recent', 'params': f'topics=finance&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'finance_mix_relevant', 'params': 'topics=finance&sort=RELEVANCE&limit=8'},
        ])
        
        return queries
    
    def _thursday_earnings_healthcare(self) -> list:
        """목요일: 실적 & 헬스케어 중심 (50개)"""
        queries = []
        
        # === 실적 시즌 (25개) ===
        queries.extend([
            {'type': 'earnings_general', 'params': 'topics=earnings&sort=LATEST&limit=50'},
            {'type': 'earnings_relevant', 'params': 'topics=earnings&sort=RELEVANCE&limit=40'},
            {'type': 'earnings_recent_24h', 'params': f'topics=earnings&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'earnings_recent_48h', 'params': f'topics=earnings&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'earnings_megacap', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"])}&topics=earnings&limit=30'},
            {'type': 'earnings_tech', 'params': f'tickers={",".join(self.STOCK_TICKERS["tech"])}&topics=earnings&limit=25'},
            {'type': 'earnings_finance', 'params': f'tickers={",".join(self.STOCK_TICKERS["finance"])}&topics=earnings&limit=25'},
            {'type': 'apple_earnings', 'params': 'tickers=AAPL&topics=earnings&limit=25'},
            {'type': 'microsoft_earnings', 'params': 'tickers=MSFT&topics=earnings&limit=25'},
            {'type': 'nvidia_earnings', 'params': 'tickers=NVDA&topics=earnings&limit=20'},
            {'type': 'google_earnings', 'params': 'tickers=GOOGL&topics=earnings&limit=20'},
            {'type': 'amazon_earnings', 'params': 'tickers=AMZN&topics=earnings&limit=20'},
            {'type': 'tesla_earnings', 'params': 'tickers=TSLA&topics=earnings&limit=20'},
            {'type': 'meta_earnings', 'params': 'tickers=META&topics=earnings&limit=20'},
            {'type': 'jpmorgan_earnings', 'params': 'tickers=JPM&topics=earnings&limit=15'},
            {'type': 'earnings_finance_combo', 'params': 'topics=earnings,finance&limit=20'},
            {'type': 'earnings_tech_combo', 'params': 'topics=earnings,technology&limit=15'},
            {'type': 'earnings_markets', 'params': 'topics=earnings,financial_markets&limit=15'},
            {'type': 'earnings_72h', 'params': f'topics=earnings&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'earnings_week', 'params': f'topics=earnings&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'earnings_mix_1', 'params': 'tickers=AAPL,MSFT,GOOGL&sort=LATEST&limit=15'},
            {'type': 'earnings_mix_2', 'params': 'tickers=AMZN,TSLA,META&sort=LATEST&limit=15'},
            {'type': 'earnings_mix_3', 'params': f'topics=earnings&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'earnings_mix_4', 'params': f'topics=earnings&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'earnings_mix_5', 'params': 'topics=earnings&sort=RELEVANCE&limit=8'},
        ])
        
        # === 헬스케어 & 생명과학 (25개) ===
        healthcare_companies = self.STOCK_TICKERS['healthcare']
        queries.extend([
            {'type': 'healthcare_general', 'params': 'topics=life_sciences&sort=LATEST&limit=40'},
            {'type': 'healthcare_relevant', 'params': 'topics=life_sciences&sort=RELEVANCE&limit=35'},
            {'type': 'healthcare_recent_24h', 'params': f'topics=life_sciences&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'healthcare_recent_48h', 'params': f'topics=life_sciences&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'healthcare_companies', 'params': f'tickers={",".join(healthcare_companies)}&sort=LATEST&limit=30'},
            {'type': 'jnj_healthcare', 'params': 'tickers=JNJ&sort=LATEST&limit=25'},
            {'type': 'pfizer_healthcare', 'params': 'tickers=PFE&sort=LATEST&limit=25'},
            {'type': 'unitedhealth_healthcare', 'params': 'tickers=UNH&sort=LATEST&limit=20'},
            {'type': 'moderna_healthcare', 'params': 'tickers=MRNA&sort=LATEST&limit=20'},
            {'type': 'abbvie_healthcare', 'params': 'tickers=ABBV&sort=LATEST&limit=20'},
            {'type': 'healthcare_earnings', 'params': 'topics=life_sciences,earnings&limit=20'},
            {'type': 'healthcare_tech', 'params': 'topics=life_sciences,technology&limit=15'},
            {'type': 'healthcare_finance', 'params': 'topics=life_sciences,finance&limit=15'},
            {'type': 'healthcare_markets', 'params': 'topics=life_sciences,financial_markets&limit=15'},
            {'type': 'healthcare_manufacturing', 'params': 'topics=life_sciences,manufacturing&limit=10'},
            {'type': 'jnj_earnings', 'params': 'tickers=JNJ&topics=earnings&limit=15'},
            {'type': 'pfizer_earnings', 'params': 'tickers=PFE&topics=earnings&limit=15'},
            {'type': 'healthcare_72h', 'params': f'topics=life_sciences&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'healthcare_week', 'params': f'topics=life_sciences&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'pharma_mix_1', 'params': 'tickers=JNJ,PFE&topics=life_sciences&limit=15'},
            {'type': 'pharma_mix_2', 'params': 'tickers=UNH,MRNA&topics=life_sciences&limit=10'},
            {'type': 'biotech_mix', 'params': 'tickers=MRNA,ABBV&sort=LATEST&limit=10'},
            {'type': 'healthcare_mix_recent', 'params': f'topics=life_sciences&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'healthcare_mix_relevant', 'params': 'topics=life_sciences&sort=RELEVANCE&limit=8'},
            {'type': 'healthcare_mix_combo', 'params': f'tickers=JNJ,PFE,UNH&time_from={self.get_time_from_hours(24)}&limit=8'},
        ])
        
        return queries
    
    def _friday_retail_mergers(self) -> list:
        """금요일: 리테일 & 인수합병 중심 (50개)"""
        queries = []
        
        # === 소매업 & 소비재 (25개) ===
        consumer_companies = self.STOCK_TICKERS['consumer']
        queries.extend([
            {'type': 'retail_general', 'params': 'topics=retail_wholesale&sort=LATEST&limit=50'},
            {'type': 'retail_relevant', 'params': 'topics=retail_wholesale&sort=RELEVANCE&limit=40'},
            {'type': 'retail_recent_24h', 'params': f'topics=retail_wholesale&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'retail_recent_48h', 'params': f'topics=retail_wholesale&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'consumer_companies', 'params': f'tickers={",".join(consumer_companies)}&sort=LATEST&limit=30'},
            {'type': 'walmart_retail', 'params': 'tickers=WMT&sort=LATEST&limit=25'},
            {'type': 'target_retail', 'params': 'tickers=TGT&sort=LATEST&limit=25'},
            {'type': 'costco_retail', 'params': 'tickers=COST&sort=LATEST&limit=20'},
            {'type': 'homedepot_retail', 'params': 'tickers=HD&sort=LATEST&limit=20'},
            {'type': 'lowes_retail', 'params': 'tickers=LOW&sort=LATEST&limit=20'},
            {'type': 'amazon_retail', 'params': 'tickers=AMZN&topics=retail_wholesale&limit=25'},
            {'type': 'retail_tech', 'params': 'topics=retail_wholesale,technology&limit=20'},
            {'type': 'retail_finance', 'params': 'topics=retail_wholesale,finance&limit=15'},
            {'type': 'retail_earnings', 'params': 'topics=retail_wholesale,earnings&limit=15'},
            {'type': 'retail_markets', 'params': 'topics=retail_wholesale,financial_markets&limit=15'},
            {'type': 'retail_economy', 'params': 'topics=retail_wholesale,economy_macro&limit=10'},
            {'type': 'retail_72h', 'params': f'topics=retail_wholesale&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'retail_week', 'params': f'topics=retail_wholesale&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'retail_mix_1', 'params': 'tickers=WMT,TGT&topics=retail_wholesale&limit=15'},
            {'type': 'retail_mix_2', 'params': 'tickers=COST,HD&topics=retail_wholesale&limit=15'},
            {'type': 'retail_mix_3', 'params': f'topics=retail_wholesale&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'ecommerce_mix', 'params': 'tickers=AMZN,SHOP&topics=retail_wholesale&limit=10'},
            {'type': 'consumer_mix', 'params': f'tickers=WMT,TGT,COST&time_from={self.get_time_from_hours(24)}&limit=10'},
            {'type': 'retail_mix_recent', 'params': f'topics=retail_wholesale&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=8'},
            {'type': 'retail_mix_relevant', 'params': 'topics=retail_wholesale&sort=RELEVANCE&limit=8'},
        ])
        
        # === 인수합병 & M&A (25개) ===
        queries.extend([
            {'type': 'mergers_general', 'params': 'topics=mergers_and_acquisitions&sort=LATEST&limit=40'},
            {'type': 'mergers_relevant', 'params': 'topics=mergers_and_acquisitions&sort=RELEVANCE&limit=35'},
            {'type': 'mergers_recent_24h', 'params': f'topics=mergers_and_acquisitions&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'mergers_recent_48h', 'params': f'topics=mergers_and_acquisitions&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'mergers_tech', 'params': 'topics=mergers_and_acquisitions,technology&limit=25'},
            {'type': 'mergers_finance', 'params': 'topics=mergers_and_acquisitions,finance&limit=20'},
            {'type': 'mergers_healthcare', 'params': 'topics=mergers_and_acquisitions,life_sciences&limit=20'},
            {'type': 'mergers_retail', 'params': 'topics=mergers_and_acquisitions,retail_wholesale&limit=15'},
            {'type': 'mergers_energy', 'params': 'topics=mergers_and_acquisitions,energy_transportation&limit=15'},
            {'type': 'mergers_manufacturing', 'params': 'topics=mergers_and_acquisitions,manufacturing&limit=15'},
            {'type': 'mergers_markets', 'params': 'topics=mergers_and_acquisitions,financial_markets&limit=15'},
            {'type': 'microsoft_mergers', 'params': 'tickers=MSFT&topics=mergers_and_acquisitions&limit=20'},
            {'type': 'google_mergers', 'params': 'tickers=GOOGL&topics=mergers_and_acquisitions&limit=15'},
            {'type': 'amazon_mergers', 'params': 'tickers=AMZN&topics=mergers_and_acquisitions&limit=15'},
            {'type': 'apple_mergers', 'params': 'tickers=AAPL&topics=mergers_and_acquisitions&limit=15'},
            {'type': 'meta_mergers', 'params': 'tickers=META&topics=mergers_and_acquisitions&limit=10'},
            {'type': 'megacap_mergers', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"][:4])}&topics=mergers_and_acquisitions&limit=15'},
            {'type': 'mergers_72h', 'params': f'topics=mergers_and_acquisitions&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'mergers_week', 'params': f'topics=mergers_and_acquisitions&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'mergers_mix_1', 'params': f'topics=mergers_and_acquisitions&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'mergers_mix_2', 'params': f'topics=mergers_and_acquisitions&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'mergers_mix_3', 'params': 'topics=mergers_and_acquisitions&sort=RELEVANCE&limit=8'},
            {'type': 'ma_banks', 'params': 'tickers=JPM,BAC&topics=mergers_and_acquisitions&limit=8'},
            {'type': 'ma_pharma', 'params': 'tickers=JNJ,PFE&topics=mergers_and_acquisitions&limit=8'},
            {'type': 'ma_recent_relevant', 'params': f'topics=mergers_and_acquisitions&sort=RELEVANCE&time_from={self.get_time_from_hours(6)}&limit=5'},
        ])
        
        return queries
    
    def _saturday_realestate_macro(self) -> list:
        """토요일: 부동산 & 거시경제 중심 (50개)"""
        queries = []
        
        # === 부동산 섹터 (25개) ===
        queries.extend([
            {'type': 'realestate_general', 'params': 'topics=real_estate&sort=LATEST&limit=50'},
            {'type': 'realestate_relevant', 'params': 'topics=real_estate&sort=RELEVANCE&limit=40'},
            {'type': 'realestate_recent_24h', 'params': f'topics=real_estate&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'realestate_recent_48h', 'params': f'topics=real_estate&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'realestate_finance', 'params': 'topics=real_estate,finance&limit=25'},
            {'type': 'realestate_economy', 'params': 'topics=real_estate,economy_macro&limit=20'},
            {'type': 'realestate_monetary', 'params': 'topics=real_estate,economy_monetary&limit=20'},
            {'type': 'realestate_fiscal', 'params': 'topics=real_estate,economy_fiscal&limit=15'},
            {'type': 'realestate_manufacturing', 'params': 'topics=real_estate,manufacturing&limit=15'},
            {'type': 'realestate_tech', 'params': 'topics=real_estate,technology&limit=15'},
            {'type': 'realestate_retail', 'params': 'topics=real_estate,retail_wholesale&limit=15'},
            {'type': 'realestate_markets', 'params': 'topics=real_estate,financial_markets&limit=15'},
            {'type': 'realestate_earnings', 'params': 'topics=real_estate,earnings&limit=10'},
            {'type': 'realestate_72h', 'params': f'topics=real_estate&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'realestate_week', 'params': f'topics=real_estate&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'realestate_mix_1', 'params': f'topics=real_estate&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'realestate_mix_2', 'params': f'topics=real_estate&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'realestate_mix_3', 'params': 'topics=real_estate&sort=RELEVANCE&limit=8'},
            {'type': 'home_builders', 'params': 'tickers=HD,LOW&topics=real_estate&limit=10'},
            {'type': 'construction_mix', 'params': 'topics=real_estate,manufacturing&sort=LATEST&limit=8'},
            {'type': 'mortgage_rates', 'params': 'topics=real_estate,economy_monetary&sort=LATEST&limit=8'},
            {'type': 'housing_market', 'params': f'topics=real_estate,economy_macro&time_from={self.get_time_from_hours(24)}&limit=8'},
            {'type': 'commercial_real_estate', 'params': 'topics=real_estate,finance&sort=RELEVANCE&limit=8'},
            {'type': 'reit_market', 'params': 'topics=real_estate,financial_markets&sort=LATEST&limit=8'},
            {'type': 'property_tech', 'params': 'topics=real_estate,technology&sort=RELEVANCE&limit=5'},
        ])
        
        # === 거시경제 지표 (25개) ===
        queries.extend([
            {'type': 'macro_general', 'params': 'topics=economy_macro&sort=LATEST&limit=40'},
            {'type': 'macro_relevant', 'params': 'topics=economy_macro&sort=RELEVANCE&limit=35'},
            {'type': 'macro_recent_24h', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'macro_recent_48h', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'macro_finance', 'params': 'topics=economy_macro,finance&limit=25'},
            {'type': 'macro_markets', 'params': 'topics=economy_macro,financial_markets&limit=20'},
            {'type': 'macro_monetary', 'params': 'topics=economy_macro,economy_monetary&limit=20'},
            {'type': 'macro_fiscal', 'params': 'topics=economy_macro,economy_fiscal&limit=20'},
            {'type': 'macro_manufacturing', 'params': 'topics=economy_macro,manufacturing&limit=15'},
            {'type': 'macro_energy', 'params': 'topics=economy_macro,energy_transportation&limit=15'},
            {'type': 'macro_retail', 'params': 'topics=economy_macro,retail_wholesale&limit=15'},
            {'type': 'macro_tech', 'params': 'topics=economy_macro,technology&limit=15'},
            {'type': 'macro_realestate', 'params': 'topics=economy_macro,real_estate&limit=15'},
            {'type': 'macro_72h', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'macro_week', 'params': f'topics=economy_macro&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'forex_macro', 'params': f'tickers={",".join(self.FOREX_TICKERS)}&topics=economy_macro&limit=15'},
            {'type': 'usd_macro', 'params': 'tickers=FOREX:USD&topics=economy_macro&limit=15'},
            {'type': 'eur_macro', 'params': 'tickers=FOREX:EUR&topics=economy_macro&limit=10'},
            {'type': 'global_economy', 'params': f'topics=economy_macro&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'economic_indicators', 'params': f'topics=economy_macro&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=10'},
            {'type': 'inflation_trends', 'params': 'topics=economy_macro,economy_monetary&sort=LATEST&limit=10'},
            {'type': 'gdp_growth', 'params': f'topics=economy_macro&sort=RELEVANCE&time_from={self.get_time_from_hours(48)}&limit=8'},
            {'type': 'employment_data', 'params': 'topics=economy_macro&sort=RELEVANCE&limit=8'},
            {'type': 'consumer_confidence', 'params': 'topics=economy_macro,retail_wholesale&sort=LATEST&limit=8'},
            {'type': 'business_sentiment', 'params': 'topics=economy_macro,manufacturing&sort=RELEVANCE&limit=5'},
        ])
        
        return queries
    
    def _sunday_markets_fiscal(self) -> list:
        """일요일: 금융시장 & 재정정책 중심 (50개)"""
        queries = []
        
        # === 금융시장 전반 (25개) ===
        queries.extend([
            {'type': 'markets_general', 'params': 'topics=financial_markets&sort=LATEST&limit=50'},
            {'type': 'markets_relevant', 'params': 'topics=financial_markets&sort=RELEVANCE&limit=40'},
            {'type': 'markets_recent_24h', 'params': f'topics=financial_markets&time_from={self.get_time_from_hours(24)}&limit=35'},
            {'type': 'markets_recent_48h', 'params': f'topics=financial_markets&time_from={self.get_time_from_hours(48)}&limit=30'},
            {'type': 'markets_tech', 'params': 'topics=financial_markets,technology&limit=25'},
            {'type': 'markets_finance', 'params': 'topics=financial_markets,finance&limit=25'},
            {'type': 'markets_earnings', 'params': 'topics=financial_markets,earnings&limit=20'},
            {'type': 'markets_macro', 'params': 'topics=financial_markets,economy_macro&limit=20'},
            {'type': 'markets_monetary', 'params': 'topics=financial_markets,economy_monetary&limit=20'},
            {'type': 'markets_energy', 'params': 'topics=financial_markets,energy_transportation&limit=15'},
            {'type': 'markets_healthcare', 'params': 'topics=financial_markets,life_sciences&limit=15'},
            {'type': 'markets_retail', 'params': 'topics=financial_markets,retail_wholesale&limit=15'},
            {'type': 'markets_realestate', 'params': 'topics=financial_markets,real_estate&limit=15'},
            {'type': 'markets_manufacturing', 'params': 'topics=financial_markets,manufacturing&limit=15'},
            {'type': 'markets_blockchain', 'params': 'topics=financial_markets,blockchain&limit=15'},
            {'type': 'markets_ipo', 'params': 'topics=financial_markets,ipo&limit=15'},
            {'type': 'markets_mergers', 'params': 'topics=financial_markets,mergers_and_acquisitions&limit=15'},
            {'type': 'markets_72h', 'params': f'topics=financial_markets&time_from={self.get_time_from_hours(72)}&limit=15'},
            {'type': 'markets_week', 'params': f'topics=financial_markets&time_from={self.get_time_from_hours(168)}&limit=15'},
            {'type': 'sp500_markets', 'params': f'tickers={",".join(self.STOCK_TICKERS["megacap"][:4])}&topics=financial_markets&limit=15'},
            {'type': 'dow_components', 'params': f'tickers={",".join(self.STOCK_TICKERS["finance"][:3])}&topics=financial_markets&limit=10'},
            {'type': 'nasdaq_tech', 'params': f'tickers={",".join(self.STOCK_TICKERS["tech"][:3])}&topics=financial_markets&limit=10'},
            {'type': 'markets_mix_1', 'params': f'topics=financial_markets&sort=RELEVANCE&time_from={self.get_time_from_hours(36)}&limit=10'},
            {'type': 'markets_mix_2', 'params': f'topics=financial_markets&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=8'},
            {'type': 'markets_mix_3', 'params': 'topics=financial_markets&sort=RELEVANCE&limit=8'},
        ])
        
        # === 통화정책 & 재정정책 (25개) ===
        queries.extend([
            {'type': 'monetary_general', 'params': 'topics=economy_monetary&sort=LATEST&limit=40'},
            {'type': 'monetary_relevant', 'params': 'topics=economy_monetary&sort=RELEVANCE&limit=35'},
            {'type': 'monetary_recent_24h', 'params': f'topics=economy_monetary&time_from={self.get_time_from_hours(24)}&limit=30'},
            {'type': 'monetary_recent_48h', 'params': f'topics=economy_monetary&time_from={self.get_time_from_hours(48)}&limit=25'},
            {'type': 'fiscal_general', 'params': 'topics=economy_fiscal&sort=LATEST&limit=30'},
            {'type': 'fiscal_relevant', 'params': 'topics=economy_fiscal&sort=RELEVANCE&limit=25'},
            {'type': 'fiscal_recent_24h', 'params': f'topics=economy_fiscal&time_from={self.get_time_from_hours(24)}&limit=20'},
            {'type': 'fiscal_recent_48h', 'params': f'topics=economy_fiscal&time_from={self.get_time_from_hours(48)}&limit=20'},
            {'type': 'monetary_finance', 'params': 'topics=economy_monetary,finance&limit=20'},
            {'type': 'monetary_markets', 'params': 'topics=economy_monetary,financial_markets&limit=20'},
            {'type': 'fiscal_finance', 'params': 'topics=economy_fiscal,finance&limit=15'},
            {'type': 'fiscal_markets', 'params': 'topics=economy_fiscal,financial_markets&limit=15'},
            {'type': 'monetary_fiscal', 'params': 'topics=economy_monetary,economy_fiscal&limit=15'},
            {'type': 'fed_policy', 'params': 'topics=economy_monetary&sort=RELEVANCE&limit=20'},
            {'type': 'interest_rates', 'params': f'topics=economy_monetary&sort=LATEST&time_from={self.get_time_from_hours(24)}&limit=15'},
            {'type': 'inflation_policy', 'params': 'topics=economy_monetary,economy_macro&limit=15'},
            {'type': 'government_spending', 'params': 'topics=economy_fiscal&sort=LATEST&limit=15'},
            {'type': 'tax_policy', 'params': 'topics=economy_fiscal&sort=RELEVANCE&limit=15'},
            {'type': 'monetary_72h', 'params': f'topics=economy_monetary&time_from={self.get_time_from_hours(72)}&limit=10'},
            {'type': 'fiscal_72h', 'params': f'topics=economy_fiscal&time_from={self.get_time_from_hours(72)}&limit=10'},
            {'type': 'central_banks', 'params': f'tickers={",".join(self.FOREX_TICKERS[:3])}&topics=economy_monetary&limit=10'},
            {'type': 'usd_policy', 'params': 'tickers=FOREX:USD&topics=economy_monetary&limit=10'},
            {'type': 'policy_mix_1', 'params': f'topics=economy_monetary&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=8'},
            {'type': 'policy_mix_2', 'params': f'topics=economy_fiscal&sort=RELEVANCE&time_from={self.get_time_from_hours(12)}&limit=8'},
            {'type': 'policy_mix_3', 'params': 'topics=economy_monetary,economy_fiscal&sort=RELEVANCE&limit=5'},
        ])
        
        return queries

def collect_news_sentiment_corrected(**context):
    """
    올바른 API 파라미터로 요일별 뉴스 수집
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
    generator = CorrectWeeklyQueryGenerator(execution_date)
    day_name = generator.day_names[generator.day_of_week]
    
    print(f"🗓️ 실행 날짜: {execution_date.strftime('%Y-%m-%d')} ({day_name})")
    print(f"🆔 배치 ID: {current_batch_id}")
    print(f"✅ 올바른 API 파라미터 사용: {len(generator.VALID_TOPICS)}개 토픽")
    
    # 요일별 올바른 쿼리 생성
    query_combinations = generator.generate_weekday_queries()
    
    print(f"📊 {day_name} 올바른 쿼리: {len(query_combinations)}개")
    
    # 쿼리 카테고리별 통계
    if generator.day_of_week == 0:  # 월요일
        print(f"├─ 에너지 섹터: 20개 (energy_transportation 토픽)")
        print(f"├─ 제조업: 15개 (manufacturing 토픽)")
        print(f"└─ 거시경제: 15개 (economy_macro 토픽)")
    elif generator.day_of_week == 1:  # 화요일
        print(f"├─ 기술 섹터: 25개 (technology 토픽)")
        print(f"├─ IPO: 15개 (ipo 토픽)")
        print(f"└─ 혼합 쿼리: 10개")
    elif generator.day_of_week == 2:  # 수요일
        print(f"├─ 블록체인: 25개 (blockchain 토픽)")
        print(f"└─ 금융: 25개 (finance 토픽)")
    elif generator.day_of_week == 3:  # 목요일
        print(f"├─ 실적: 25개 (earnings 토픽)")
        print(f"└─ 헬스케어: 25개 (life_sciences 토픽)")
    elif generator.day_of_week == 4:  # 금요일
        print(f"├─ 리테일: 25개 (retail_wholesale 토픽)")
        print(f"└─ M&A: 25개 (mergers_and_acquisitions 토픽)")
    elif generator.day_of_week == 5:  # 토요일
        print(f"├─ 부동산: 25개 (real_estate 토픽)")
        print(f"└─ 거시경제: 25개 (economy_macro 토픽)")
    else:  # 일요일
        print(f"├─ 금융시장: 25개 (financial_markets 토픽)")
        print(f"└─ 정책: 25개 (economy_monetary, economy_fiscal 토픽)")
    
    all_news_data = []
    total_api_calls = 0
    successful_queries = 0
    failed_queries = 0
    
    for i, query_combo in enumerate(query_combinations, 1):
        try:
            # API 요청
            url = "https://www.alphavantage.co/query"
            params = f"function=NEWS_SENTIMENT&{query_combo['params']}&apikey={api_key}"
            
            print(f"🚀 [{i:2d}/50] {query_combo['type']}")
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
                article['api_version'] = 'corrected_v1'  # 수정된 버전 표시
            
            all_news_data.extend(data['feed'])
            
        except Exception as e:
            print(f"❌ 쿼리 실패: {str(e)}")
            failed_queries += 1
            continue
    
    print(f"\n📈 {day_name} 올바른 파라미터 수집 통계:")
    print(f"├─ 총 API 호출: {total_api_calls}회 / 50회")
    print(f"├─ 성공한 쿼리: {successful_queries}개")
    print(f"├─ 실패한 쿼리: {failed_queries}개")
    print(f"├─ 총 뉴스 수집: {len(all_news_data)}개")
    print(f"├─ 호출 효율성: {len(all_news_data)/total_api_calls:.1f}개/호출" if total_api_calls > 0 else "├─ 호출 효율성: N/A")
    print(f"└─ API 파라미터: ✅ 검증된 topics & tickers 사용")
    
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
        'api_version': 'corrected_v1',
        'execution_date': execution_date.strftime('%Y-%m-%d %A')
    }

def process_and_store_corrected_news(**context):
    """
    수집된 뉴스 데이터를 PostgreSQL에 저장
    """
    # XCom에서 데이터 가져오기
    news_data = context['ti'].xcom_pull(task_ids='collect_news_sentiment_corrected', key='news_data')
    batch_id = context['ti'].xcom_pull(task_ids='collect_news_sentiment_corrected', key='batch_id')
    
    if not news_data:
        raise ValueError("❌ 이전 태스크에서 뉴스 데이터를 받지 못했습니다")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    success_count = 0
    error_count = 0
    duplicate_count = 0
    
    execution_date = context['execution_date']
    generator = CorrectWeeklyQueryGenerator(execution_date)
    day_name = generator.day_names[generator.day_of_week]
    
    print(f"🚀 배치 {batch_id} ({day_name}) 올바른 파라미터 뉴스 저장 시작: {len(news_data)}개")
    
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
    
    print(f"\n🎯 배치 {batch_id} ({day_name}) 올바른 파라미터 저장 완료:")
    print(f"├─ 성공 저장: {success_count}개")
    print(f"├─ 중복 제외: {duplicate_count}개")
    print(f"├─ 오류 발생: {error_count}개")
    print(f"├─ 저장 효율: {success_count/(len(news_data))*100:.1f}%")
    print(f"└─ API 버전: corrected_v1 (검증된 파라미터)")
    
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
        'api_version': 'corrected_v1'
    }

# DAG 정의
with DAG(
    dag_id='ingest_market_news_sentiment_corrected_weekday',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Alpha Vantage News & Sentiment API - 올바른 파라미터로 요일별 전문화 (50개 쿼리)',
    template_searchpath=[INITDB_SQL_DIR],
    tags=['news', 'sentiment', 'alpha_vantage', 'corrected_parameters', 'weekday_specialized'],
) as dag:
    
    # 1. 테이블 생성
    create_table = PostgresOperator(
        task_id='create_market_news_sentiment_table',
        postgres_conn_id='postgres_default',
        sql='create_market_news_sentiment.sql',
    )
    
    # 2. 올바른 파라미터로 요일별 뉴스 수집
    collect_news = PythonOperator(
        task_id='collect_news_sentiment_corrected',
        python_callable=collect_news_sentiment_corrected,
    )
    
    # 3. 데이터 가공 및 저장
    process_news = PythonOperator(
        task_id='process_and_store_corrected_news',
        python_callable=process_and_store_corrected_news,
    )
    
    # Task 의존성
    create_table >> collect_news >> process_news