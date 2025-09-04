"""
빗썸-CoinGecko 매칭 테이블 생성 및 관리 DAG
- 빗썸 414개 코인을 CoinGecko ID와 1:1 매칭
- 스마트 매칭 알고리즘 (시가총액 + 이름 유사도)
- 수동 예외 처리 및 검증 시스템
"""

from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ========================================================================================
# 설정 및 상수
# ========================================================================================

# SQL 파일 경로
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "initdb")

# 수동 매칭이 필요한 특수 케이스들 (경험적으로 확인된 매칭)
MANUAL_MAPPINGS = {
    # 메이저 코인들 (정확한 매칭 필요)
    'BTC': 'bitcoin',
    'ETH': 'ethereum', 
    'BNB': 'binancecoin',
    'SOL': 'solana',
    'ADA': 'cardano',
    'AVAX': 'avalanche-2',
    'DOT': 'polkadot',
    'MATIC': 'polygon-ecosystem-token',  # Polygon (MATIC)
    'ATOM': 'cosmos',
    'LINK': 'chainlink',
    'UNI': 'uniswap',
    'LTC': 'litecoin',
    'BCH': 'bitcoin-cash',
    'XRP': 'ripple',
    
    # 문제 케이스들 (이름이 다른 경우)
    'KNC': 'kyber-network-crystal',  # Kyber Network vs Kyber Network Crystal
    'CRO': 'crypto-com-chain',       # Crypto.com Coin
    'FTM': 'fantom',                 # Fantom
    'SAND': 'the-sandbox',           # The Sandbox
    'MANA': 'decentraland',          # Decentraland
    'AXS': 'axie-infinity',          # Axie Infinity
    'CHZ': 'chiliz',                 # Chiliz
    'ENJ': 'enjincoin',              # Enjin Coin
    'BAT': 'basic-attention-token',  # Basic Attention Token
    'ZIL': 'zilliqa',                # Zilliqa
    'ICX': 'icon',                   # ICON
    'QTUM': 'qtum',                  # Qtum
    'OMG': 'omg',                    # OMG Network
    'ZRX': '0x',                     # 0x Protocol
    'REP': 'augur',                  # Augur
    'GNT': 'golem',                  # Golem (GNT → GLM 변경됨)
    'STORJ': 'storj',                # Storj
    'SNT': 'status',                 # Status Network Token
    'POWR': 'powerledger',           # Power Ledger
    'REN': 'republic-protocol',      # Ren Protocol
    'LRC': 'loopring',               # Loopring
    'COMP': 'compound-governance-token',  # Compound
    'YFI': 'yearn-finance',          # Yearn Finance
    'SUSHI': 'sushi',                # SushiSwap
    'AAVE': 'aave',                  # Aave
    'MKR': 'maker',                  # MakerDAO
    'SNX': 'havven',                 # Synthetix Network Token
    'CRV': 'curve-dao-token',        # Curve DAO Token
    '1INCH': '1inch',                # 1inch Network
    
    # 스테이블 코인들
    'USDT': 'tether',                # Tether USD
    'USDC': 'usd-coin',              # USD Coin
    'BUSD': 'binance-usd',           # Binance USD
    'DAI': 'dai',                    # Dai Stablecoin
    'TUSD': 'true-usd',              # TrueUSD
    'PAX': 'paxos-standard',         # Paxos Standard
    'GUSD': 'gemini-dollar',         # Gemini Dollar
    
    # 한국 관련 코인들
    'KLAY': 'klaytn',                # Klaytn
    'ICX': 'icon',                   # ICON
    'JST': 'just',                   # JUST
    'TRX': 'tronix',                 # TRON
    'BTT': 'bittorrent-2',           # BitTorrent (New)
    'WIN': 'winklink',               # WINkLink
    'SUN': 'sun-token',              # SUN
}

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ========================================================================================
# DAG 정의
# ========================================================================================

with DAG(
    dag_id='create_bithumb_coingecko_mapping_k8s',
    default_args=default_args,
    description='빗썸-CoinGecko ID 매칭 테이블 생성 및 관리',
    schedule_interval='@once',  # 한 번만 실행 (수동 트리거)
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR], 
    tags=['bithumb', 'coingecko', 'mapping', 'setup'],
) as dag:

    def calculate_name_similarity(name1: str, name2: str) -> float:
        """이름 유사도 계산"""
        if not name1 or not name2:
            return 0.0
        
        # 대소문자 무시하고 정리
        clean_name1 = name1.lower().strip()
        clean_name2 = name2.lower().strip()
        
        # 정확히 일치
        if clean_name1 == clean_name2:
            return 1.0
        
        # 부분 문자열 매칭
        if clean_name1 in clean_name2 or clean_name2 in clean_name1:
            return 0.8
        
        # 단어 기반 매칭
        words1 = clean_name1.split()
        words2 = clean_name2.split()
        
        if words1 and words2:
            # 첫 번째 단어 비교
            if words1[0] == words2[0]:
                return 0.7
            
            # 공통 단어 개수
            common_words = set(words1) & set(words2)
            if common_words:
                return 0.6
        
        # 순서 매칭 (difflib)
        return SequenceMatcher(None, clean_name1, clean_name2).ratio()

    def select_best_coingecko_match(bithumb_coin: Dict, candidates: List[Dict]) -> Tuple[Optional[Dict], str, float]:
        """최적의 CoinGecko 매칭 선택"""
        
        if not candidates:
            return None, "NO_CANDIDATES", 0.0
        
        symbol = bithumb_coin['symbol']
        bithumb_english_name = bithumb_coin['english_name']
        
        # 1. 수동 매핑 최우선
        if symbol in MANUAL_MAPPINGS:
            target_id = MANUAL_MAPPINGS[symbol]
            for candidate in candidates:
                if candidate['coingecko_id'] == target_id:
                    return candidate, "MANUAL_MAPPING", 100.0
        
        # 2. 자동 매칭: 시가총액 순위 + 이름 유사도 조합
        best_candidate = None
        best_score = 0.0
        best_method = "AUTO_MATCHING"
        
        for candidate in candidates:
            score = 0.0
            
            # 시가총액 순위 점수 (1위=50점, 2위=49점, ..., 100위=1점, 101위 이상=0점)
            rank = candidate.get('market_cap_rank')
            if rank and rank <= 100:
                rank_score = max(0, 51 - rank)
            else:
                rank_score = 0
            
            # 이름 유사도 점수 (0~50점)
            name_similarity = calculate_name_similarity(
                bithumb_english_name, 
                candidate['name']
            )
            name_score = name_similarity * 50
            
            # 종합 점수 (순위 50% + 이름 50%)
            total_score = rank_score + name_score
            
            if total_score > best_score:
                best_score = total_score
                best_candidate = candidate
                
                # 매칭 방법 세분화
                if name_similarity >= 0.9:
                    best_method = "EXACT_NAME_MATCH"
                elif rank and rank <= 10:
                    best_method = "TOP_10_RANK"
                elif name_similarity >= 0.7:
                    best_method = "HIGH_NAME_SIMILARITY"
                else:
                    best_method = "RANK_PRIORITY"
        
        return best_candidate, best_method, best_score

    def create_bithumb_coingecko_mapping(**context):
        """빗썸-CoinGecko 매칭 프로세스 실행"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logging.info("🔄 빗썸-CoinGecko ID 매칭 프로세스 시작")
        
        # 1. 빗썸 코인 리스트 조회
        bithumb_query = """
        SELECT 
            market_code,
            REPLACE(market_code, 'KRW-', '') as symbol,
            korean_name,
            english_name,
            market_warning
        FROM market_code_bithumb 
        WHERE market_code LIKE 'KRW-%'
        ORDER BY market_code
        """
        
        bithumb_records = hook.get_records(bithumb_query)
        bithumb_coins = []
        
        for record in bithumb_records:
            bithumb_coins.append({
                'market_code': record[0],
                'symbol': record[1], 
                'korean_name': record[2],
                'english_name': record[3],
                'market_warning': record[4]
            })
        
        logging.info(f"📊 빗썸 코인 수: {len(bithumb_coins)}개")
        
        # 2. 매칭 결과 저장
        matched_count = 0
        failed_matches = []
        
        for bithumb_coin in bithumb_coins:
            symbol = bithumb_coin['symbol']
            market_code = bithumb_coin['market_code']
            
            # CoinGecko 후보들 조회
            coingecko_query = """
            SELECT 
                coingecko_id,
                symbol,
                name,
                market_cap_rank,
                market_cap_usd,
                current_price_usd
            FROM coingecko_id_mapping 
            WHERE UPPER(symbol) = UPPER(%s)
            ORDER BY 
                CASE 
                    WHEN market_cap_rank IS NULL THEN 999999 
                    ELSE market_cap_rank 
                END ASC,
                market_cap_usd DESC NULLS LAST
            """
            
            candidates = hook.get_records(coingecko_query, parameters=(symbol,))
            
            if not candidates:
                failed_matches.append({
                    'market_code': market_code,
                    'symbol': symbol,
                    'reason': 'NO_COINGECKO_CANDIDATES'
                })
                logging.warning(f"❌ CoinGecko 후보 없음: {symbol}")
                continue
            
            # 후보들을 딕셔너리로 변환
            candidate_dicts = []
            for candidate in candidates:
                candidate_dicts.append({
                    'coingecko_id': candidate[0],
                    'symbol': candidate[1],
                    'name': candidate[2],
                    'market_cap_rank': candidate[3],
                    'market_cap_usd': candidate[4],
                    'current_price_usd': candidate[5]
                })
            
            # 최적 매칭 선택
            best_match, match_method, match_score = select_best_coingecko_match(
                bithumb_coin, candidate_dicts
            )
            
            if best_match:
                # 매칭 결과 삽입
                insert_query = """
                INSERT INTO bithumb_coingecko_mapping (
                    market_code, symbol, bithumb_korean_name, bithumb_english_name,
                    coingecko_id, coingecko_name, market_cap_rank,
                    market_cap_usd, current_price_usd, 
                    match_method, match_score, is_verified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # 수동 매핑은 자동으로 검증됨으로 표시
                is_verified = (match_method == "MANUAL_MAPPING")
                
                hook.run(insert_query, parameters=(
                    market_code,
                    symbol,
                    bithumb_coin['korean_name'],
                    bithumb_coin['english_name'],
                    best_match['coingecko_id'],
                    best_match['name'],
                    best_match.get('market_cap_rank'),
                    best_match.get('market_cap_usd'),
                    best_match.get('current_price_usd'),
                    match_method,
                    round(match_score, 2),
                    is_verified
                ))
                
                matched_count += 1
                logging.info(f"✅ 매칭 성공 ({matched_count:3d}): {symbol:8} → {best_match['coingecko_id']:25} "
                           f"({match_method}, Score: {match_score:.1f})")
            else:
                failed_matches.append({
                    'market_code': market_code,
                    'symbol': symbol,
                    'reason': 'NO_SUITABLE_MATCH'
                })
                logging.warning(f"❌ 매칭 실패: {symbol}")
        
        # 3. 결과 요약
        total_coins = len(bithumb_coins)
        success_rate = (matched_count / total_coins * 100) if total_coins > 0 else 0
        
        logging.info("=" * 80)
        logging.info("📊 빗썸-CoinGecko 매칭 완료 결과")
        logging.info("-" * 80)
        logging.info(f"✅ 매칭 성공:   {matched_count:4d}개")
        logging.info(f"❌ 매칭 실패:   {len(failed_matches):4d}개")
        logging.info(f"📈 성공률:      {success_rate:5.1f}%")
        logging.info(f"🎯 목표 달성:   {matched_count}/414개")
        
        # 매칭 방법별 통계
        method_stats_query = """
        SELECT match_method, COUNT(*) as count
        FROM bithumb_coingecko_mapping
        GROUP BY match_method
        ORDER BY count DESC
        """
        method_stats = hook.get_records(method_stats_query)
        
        if method_stats:
            logging.info(f"📋 매칭 방법별 통계:")
            for method, count in method_stats:
                logging.info(f"    {method:20}: {count:3d}개")
        
        # 실패 목록 출력
        if failed_matches:
            logging.info(f"❌ 매칭 실패 목록 ({len(failed_matches)}개):")
            for failed in failed_matches[:10]:  # 처음 10개만 출력
                logging.info(f"    {failed['market_code']:12} ({failed['symbol']:8}): {failed['reason']}")
            if len(failed_matches) > 10:
                logging.info(f"    ... 외 {len(failed_matches)-10}개 더")
        
        logging.info("=" * 80)
        
        return {
            'matched_count': matched_count,
            'failed_count': len(failed_matches),
            'success_rate': success_rate,
            'failed_matches': failed_matches
        }

    def verify_mapping_quality(**context):
        """매칭 품질 검증"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 1. 전체 매칭 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_mappings,
            COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_count,
            COUNT(CASE WHEN match_method = 'MANUAL_MAPPING' THEN 1 END) as manual_count,
            COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 100 THEN 1 END) as top100_count,
            AVG(match_score) as avg_score
        FROM bithumb_coingecko_mapping
        """
        
        stats = hook.get_first(stats_query)
        
        if stats:
            logging.info("=" * 80)
            logging.info("📊 매칭 품질 검증 결과")
            logging.info("-" * 80)
            logging.info(f"📈 전체 매칭 수:     {stats[0]:4d}개")
            logging.info(f"✅ 검증 완료:       {stats[1]:4d}개 ({stats[1]/stats[0]*100:.1f}%)")
            logging.info(f"🔧 수동 매칭:       {stats[2]:4d}개 ({stats[2]/stats[0]*100:.1f}%)")
            logging.info(f"🏆 Top 100 코인:    {stats[3]:4d}개 ({stats[3]/stats[0]*100:.1f}%)")
            logging.info(f"📊 평균 점수:       {stats[4]:.1f}점")
        
        # 2. 낮은 점수 매칭 확인 (수동 검토 필요)
        low_score_query = """
        SELECT market_code, symbol, coingecko_id, coingecko_name, 
               match_method, match_score
        FROM bithumb_coingecko_mapping
        WHERE match_score < 60 AND is_verified = false
        ORDER BY match_score ASC
        LIMIT 10
        """
        
        low_scores = hook.get_records(low_score_query)
        
        if low_scores:
            logging.info(f"⚠️  낮은 점수 매칭 (수동 검토 권장):")
            for record in low_scores:
                logging.info(f"    {record[0]:12} ({record[1]:6}) → {record[2]:25} "
                           f"({record[4]}, {record[5]:.1f}점)")
        
        # 3. 중복 CoinGecko ID 확인
        duplicate_query = """
        SELECT coingecko_id, COUNT(*) as count, 
               STRING_AGG(market_code || '(' || symbol || ')', ', ') as markets
        FROM bithumb_coingecko_mapping
        GROUP BY coingecko_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        """
        
        duplicates = hook.get_records(duplicate_query)
        
        if duplicates:
            logging.info(f"⚠️  중복 CoinGecko ID ({len(duplicates)}개):")
            for record in duplicates:
                logging.info(f"    {record[0]:25}: {record[1]}개 - {record[2]}")
        else:
            logging.info(f"✅ 중복 CoinGecko ID 없음 (완벽한 1:1 매칭)")
        
        logging.info("=" * 80)

    # ====================================================================================
    # Task 정의
    # ====================================================================================

    # Task 1: 매칭 테이블 생성
    create_mapping_table = PostgresOperator(
        task_id='create_bithumb_coingecko_mapping_table',
        postgres_conn_id='postgres_default',
        sql='create_bithumb_coingecko_mapping.sql',
    )

    # Task 2: 매칭 프로세스 실행
    run_matching = PythonOperator(
        task_id='create_bithumb_coingecko_mapping',
        python_callable=create_bithumb_coingecko_mapping,
    )

    # Task 3: 매칭 품질 검증
    verify_quality = PythonOperator(
        task_id='verify_mapping_quality',
        python_callable=verify_mapping_quality,
    )

    # ====================================================================================
    # Task 의존성 설정
    # ====================================================================================

    create_mapping_table >> run_matching >> verify_quality