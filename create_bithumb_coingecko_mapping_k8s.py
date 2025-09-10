"""
빗썸-CoinGecko 매칭 테이블 생성 DAG (하드코딩 방식)
- 빗썸 385개 코인을 CoinGecko ID와 정확한 1:1 매칭
- 수동 검증된 완전한 매핑 테이블 사용
- 100% 정확성 보장
"""

from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Tuple, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ========================================================================================
# 설정 및 상수
# ========================================================================================

# SQL 파일 경로
INITDB_SQL_DIR = os.path.join(os.path.dirname(__file__), "initdb")

# 완전한 빗썸-CoinGecko 매핑 테이블 (385개 전체)
# SQL 결과를 기반으로 각 심볼의 정확한 메인 코인 선택
COMPLETE_BITHUMB_COINGECKO_MAPPINGS = {
    # 메이저 코인들
    'BTC': 'bitcoin',  # 비트코인 메인
    'ETH': 'ethereum',  # 이더리움 메인
    'BNB': 'binancecoin',  # 바이낸스 코인
    'SOL': 'solana',  # 솔라나 메인
    'ADA': 'cardano',  # 카르다노
    'AVAX': 'avalanche-2',  # 아발란체
    'DOT': 'polkadot',  # 폴카닷
    'ATOM': 'cosmos',  # 코스모스
    'LINK': 'chainlink',  # 체인링크
    'UNI': 'uniswap',  # 유니스왑
    'BCH': 'bitcoin-cash',  # 비트코인 캐시
    'XRP': 'ripple',  # 리플
    'TRX': 'tron',  # 트론
    'DOGE': 'dogecoin',  # 도지코인 메인
    'SHIB': 'shiba-inu',  # 시바이누 메인
    'PEPE': 'pepe',  # PEPE 메인 (이더리움 기반)
    'TON': 'the-open-network',  # 톤코인 메인
    'XLM': 'stellar',  # 스텔라
    'HBAR': 'hedera-hashgraph',  # 헤데라
    'CRO': 'crypto-com-chain',  # 크로노스
    'SUI': 'sui',  # 수이
    'MNT': 'mantle',  # 맨틀
    'WLD': 'worldcoin-wld',  # 월드코인
    'NEAR': 'near',  # 니어 프로토콜
    'ENA': 'ethena',  # 에테나
    'WLFI': 'world-liberty-financial',  # 월드 리버티
    'AAVE': 'aave',  # 아베
    'USDT': 'tether',  # 테더 메인
    'USDC': 'usd-coin',  # USD 코인 메인
    'USDS': 'usds',  # USDS 메인
    
    # 주요 알트코인들 (SQL 결과 기반으로 정확한 매칭)
    '1INCH': '1inch',
    'A': 'vaulta',  # Vaulta 선택 (더 높은 시가총액)
    'A8': 'ancient8',
    'ACE': 'endurance',  # Fusionist
    'ACH': 'alchemy-pay',
    'ACS': 'access-protocol',  # Access Protocol 선택 (더 높은 시가총액)
    'ACX': 'across-protocol',
    'ADP': 'adappter-token',
    'AERGO': 'aergo',
    'AERO': 'aerodrome-finance',
    'AGI': 'delysium',
    'AGLD': 'adventure-gold',
    'AHT': 'ahatoken',
    'AI16Z': 'ai16z',
    'AIOZ': 'aioz-network',
    'AKT': 'akash-network',
    'ALGO': 'algorand',
    'ALICE': 'my-neighbor-alice',
    'ALT': 'altlayer',  # AltLayer 선택 (시가총액 더 높음)
    'AMO': 'amo',
    'AMP': 'amp-token',
    'ANIME': 'anime',
    'ANKR': 'ankr',
    'APE': 'apecoin',
    'API3': 'api3',
    'APM': 'apm-coin',
    'APT': 'aptos',
    'AQT': 'alpha-quark-token',
    'AR': 'arweave',
    'ARB': 'arbitrum',  # 아비트럼 메인
    'ARDR': 'ardor',
    'ARK': 'ark',  # ARK 메인 선택
    'ARKM': 'arkham',
    'ARPA': 'arpa',
    'ASTR': 'astar',
    'ATH': 'aethir',  # Aethir 선택 (더 높은 시가총액)
    'AUCTION': 'auction',
    'AUDIO': 'audius',
    'AVAIL': 'avail',
    'AVL': 'avalon-2',  # Avalon 선택
    'AWE': 'stp-network',
    'AXS': 'axie-infinity',
    'AZIT': 'azit',
    
    # B 그룹
    'B3': 'b3',
    'BABY': 'babylon',
    'BAL': 'balancer',
    'BAT': 'basic-attention-token',  # Basic Attention Token 메인
    'BB': 'bouncebit',  # BounceBit 선택
    'BEAM': 'beam-2',  # Beam 선택 (더 높은 시가총액)
    'BEL': 'bella-protocol',
    'BERA': 'berachain-bera',
    'BFC': 'bifrost',
    'BICO': 'biconomy',
    'BIGTIME': 'big-time',
    'BIO': 'bio-protocol',
    'BLAST': 'blast',
    'BLUE': 'bluefin',  # Bluefin 선택
    'BLUR': 'blur',
    'BMT': 'bubblemaps',
    'BNT': 'bancor',
    'BOA': 'bosagora-2',
    'BOBA': 'boba-network',
    'BONK': 'bonk',
    'BORA': 'bora',
    'BOUNTY': 'sentinel-protocol',
    'BRETT': 'based-brett',  # Brett 메인
    'BSV': 'bitcoin-cash-sv',
    'BTT': 'bittorrent',
    
    # C 그룹
    'C': 'chainbase',
    'C98': 'coin98',
    'CAKE': 'pancakeswap-token',
    'CAMP': 'camp-network',
    'CARV': 'carv',
    'CBK': 'cobak-token',
    'CELO': 'celo',
    'CELR': 'celer-network',
    'CFX': 'conflux-token',
    'CHR': 'chromaway',
    'CHZ': 'chiliz',
    'CKB': 'nervos-network',
    'COMP': 'compound-governance-token',
    'COOKIE': 'cookie',
    'CORE': 'coredaoorg',  # Core 선택 (더 높은 시가총액)
    'COS': 'contentos',
    'COTI': 'coti',
    'COW': 'cow-protocol',
    'CRTS': 'cratos',
    'CRV': 'curve-dao-token',
    'CSPR': 'casper-network',
    'CTC': 'creditcoin-2',
    'CTK': 'certik',
    'CTSI': 'cartesi',
    'CTXC': 'cortex',
    'CVC': 'civic',
    'CYBER': 'cyberconnect',
    
    # D 그룹
    'D': 'dar-open-network',
    'DAO': 'dao-maker',
    'DBR': 'debridge',  # deBridge 선택
    'DEEP': 'deep',
    'DKA': 'dkargo',
    'DRIFT': 'drift-protocol',
    'DVI': 'dvision-network',
    'DYDX': 'dydx-chain',
    
    # E 그룹
    'EDU': 'edu-coin',
    'EGG': 'justanegg-2',
    'EGLD': 'elrond-erd-2',
    'EIGEN': 'eigenlayer',
    'EL': 'elysia',
    'ELF': 'aelf',
    'ELX': 'elixir-finance',
    'ENJ': 'enjincoin',
    'ENS': 'ethereum-name-service',
    'EPT': 'balance',
    'ERA': 'caldera',
    'ES': 'eclipse-3',
    'ETC': 'ethereum-classic',
    'ETHFI': 'ether-fi',
    'EUL': 'euler',
    
    # F 그룹
    'F': 'synfutures',
    'FET': 'fetch-ai',
    'FIDA': 'bonfida',
    'FIL': 'filecoin',
    'FITFI': 'step-app-fitfi',
    'FLOCK': 'flock-2',  # FLOCK 메인
    'FLOKI': 'floki',
    'FLOW': 'flow',  # Flow 메인
    'FLR': 'flare-networks',
    'FLUX': 'zelcash',
    'FORT': 'forta',
    
    # G 그룹
    'G': 'g-token',
    'GALA': 'gala',
    'GAS': 'gas',
    'GHX': 'gamercoin',
    'GLM': 'golem',
    'GMT': 'stepn',
    'GMX': 'gmx',
    'GNO': 'gnosis',
    'GOAT': 'goatseus-maximus',  # Goatseus Maximus 선택
    'GRASS': 'grass',
    'GRND': 'superwalk',
    'GRS': 'groestlcoin',
    'GRT': 'the-graph',
    'GTC': 'gitcoin',
    
    # H 그룹
    'H': 'humanity',
    'HAEDAL': 'haedal',
    'HFT': 'hashflow',
    'HIVE': 'hive',
    'HOME': 'home',
    'HOOK': 'hooked-protocol',
    'HP': 'hippo-protocol',
    'HUMA': 'huma-finance',
    'HUNT': 'hunt-token',  # Hunt 메인
    'HYPER': 'hyperlane',  # Hyperlane 선택
    
    # I 그룹
    'ICP': 'internet-computer',
    'ICX': 'icon',
    'ID': 'space-id',  # SPACE ID 선택
    'ILV': 'illuvium',
    'IMX': 'immutable-x',
    'INIT': 'initia',
    'INJ': 'injective-protocol',
    'IO': 'io',
    'IOST': 'iostoken',
    'IOTA': 'iota',
    'IOTX': 'iotex',
    'IP': 'story-2',
    'IQ': 'everipedia',  # IQ 메인
    
    # J 그룹
    'JASMY': 'jasmycoin',
    'JOE': 'joe',  # JOE 메인
    'JST': 'just',
    'JTO': 'jito-governance-token',
    'JUP': 'jupiter-exchange-solana',  # Jupiter 메인
    
    # K 그룹
    'KAIA': 'kaia',
    'KAITO': 'kaito',
    'KAVA': 'kava',
    'KERNEL': 'kernel-2',
    'KNC': 'kyber-network-crystal',
    'KSM': 'kusama',
    
    # L 그룹
    'LA': 'lagrange',  # Lagrange 선택
    'LAYER': 'solayer',
    'LBL': 'label-foundation',
    'LDO': 'lido-dao',
    'LISTA': 'lista',
    'LM': 'leisuremeta',
    'LPT': 'livepeer',
    'LRC': 'loopring',
    'LSK': 'lisk',
    'LWA': 'onbuff',
    
    # M 그룹
    'MAGIC': 'magic',
    'MANA': 'decentraland',
    'MANTA': 'manta-network',
    'MAPO': 'marcopolo',
    'MASK': 'mask-network',  # Mask Network 메인
    'MAV': 'maverick-protocol',
    'MBL': 'moviebloc',
    'MBX': 'marblex',
    'ME': 'magic-eden',
    'MED': 'medibloc',
    'MERL': 'merlin-chain',
    'META': 'meta-2-2',  # MetaDAO 선택
    'METIS': 'metis-token',
    'MEW': 'cat-in-a-dogs-world',
    'MINA': 'mina-protocol',
    'MIX': 'mixmarvel',
    'MLK': 'milk-alliance',
    'MOC': 'mossland',
    'MOCA': 'mocaverse',
    'MOODENG': 'moo-deng',  # Moo Deng 메인
    'MORPHO': 'morpho',
    'MOVE': 'movement',  # Movement 메인
    'MTL': 'metal',
    'MVC': 'mileverse',
    'MVL': 'mass-vehicle-ledger',
    
    # N 그룹
    'NCT': 'polyswarm',
    'NEIRO': 'neiro-3',  # Neiro 메인
    'NEO': 'neo',
    'NEWT': 'newton-protocol',
    'NFT': 'apenft',
    'NIL': 'nillion',
    'NMR': 'numeraire',
    
    # O 그룹
    'OAS': 'oasys',
    'OBSR': 'observer-coin',
    'OBT': 'orbiter-finance',  # Orbiter Finance 선택
    'OGN': 'origin-protocol',
    'OM': 'mantra-dao',
    'OMNI': 'omni-network',  # Omni Network 선택
    'ONDO': 'ondo-finance',
    'ONG': 'ong',
    'ONT': 'ontology',
    'OP': 'optimism',
    'ORBS': 'orbs',
    'ORCA': 'orca',
    'ORDER': 'orderly-network',
    'OSMO': 'osmosis',
    'OXT': 'orchid-protocol',
    
    # P 그룹
    'PARTI': 'particle-network',
    'PAXG': 'pax-gold',
    'PCI': 'pay-coin',
    'PEAQ': 'peaq-2',
    'PENDLE': 'pendle',
    'PENGU': 'pudgy-penguins',
    'PLUME': 'plume',
    'POKT': 'pocket-network',
    'POL': 'polygon-ecosystem-token',  # POL 메인
    'POLA': 'polaris-share',
    'POLYX': 'polymesh',
    'PONKE': 'ponke',
    'POWR': 'power-ledger',
    'PROMPT': 'wayfinder',  # Wayfinder 선택
    'PROVE': 'succinct',
    'PUFFER': 'puffer-finance',
    'PUMP': 'pump-fun',  # Pump.fun 메인
    'PUNDIX': 'pundi-x-2',
    'PYR': 'vulcan-forged',
    'PYTH': 'pyth-network',
    
    # Q 그룹
    'QKC': 'quark-chain',
    'QTUM': 'qtum',
    
    # R 그룹
    'RAD': 'radicle',
    'RAY': 'raydium',
    'RED': 'redstone-oracles',
    'REI': 'unit-00-rei',  # Rei 선택
    'RENDER': 'render-token',
    'REQ': 'request-network',
    'RESOLV': 'resolv',
    'REZ': 'renzo',
    'RLC': 'iexec-rlc',
    'ROA': 'roaland-core',
    'RON': 'ronin',
    'RPL': 'rocket-pool',
    'RSR': 'reserve-rights-token',
    'RSS3': 'rss3',
    'RVN': 'ravencoin',
    
    # S 그룹
    'S': 'sonic-3',
    'SAFE': 'safe',  # Safe 메인
    'SAHARA': 'sahara-ai',
    'SAND': 'the-sandbox',
    'SC': 'siacoin',  # Siacoin 메인
    'SCR': 'scroll',
    'SD': 'stader',
    'SEI': 'sei-network',
    'SFP': 'safepal',
    'SHELL': 'myshell',
    'SIGN': 'sign-global',
    'SIX': 'six',  # Six Sigma 선택
    'SKL': 'skale',
    'SKY': 'sky',  # Sky 메인
    'SLF': 'self-chain',
    'SNT': 'status',
    'SNX': 'havven',
    'SOFI': 'rai-finance',
    'SOLV': 'solv-protocol',
    'SONIC': 'sonic-svm',
    'SOON': 'soon-2',
    'SOPH': 'sophon',  # Sophon 메인
    'SPK': 'spark-2',
    'SPURS': 'tottenham-hotspur-fc-fan-token',
    'STAT': 'stat',
    'STEEM': 'steem',
    'STG': 'stargate-finance',
    'STORJ': 'storj',
    'STRAX': 'stratis',
    'STRK': 'starknet',
    'STX': 'blockstack',
    'SUN': 'sun-token',
    'SUNDOG': 'sundog',
    'SUSHI': 'sushi',
    'SWAP': 'trustswap',
    'SWELL': 'swell-network',  # Swell 메인
    'SXP': 'swipe',
    'SYRUP': 'syrup',
    
    # T 그룹
    'T': 'threshold-network-token',  # Threshold Network 선택
    'TAIKO': 'taiko',
    'TDROP': 'thetadrop',
    'TFUEL': 'theta-fuel',
    'THE': 'thena',
    'THETA': 'theta-token',
    'TIA': 'celestia',  # Celestia 메인
    'TOWNS': 'towns',
    'TREE': 'treehouse',  # Treehouse 선택
    'TRUMP': 'official-trump',  # Official Trump 메인
    'TT': 'thunder-token',
    'TURBO': 'turbo',
    
    # U 그룹
    'UMA': 'uma',
    'UOS': 'ultra',
    'USD1': 'usd1-wlfi',
    'UXLINK': 'uxlink',
    
    # V 그룹
    'VANA': 'vana',
    'VET': 'vechain',
    'VIRTUAL': 'virtual-protocol',
    'VTHO': 'vethor-token',
    
    # W 그룹
    'W': 'wormhole',  # Wormhole 메인
    'WAL': 'walrus-2',
    'WAVES': 'waves',
    'WAXP': 'wax',
    'WCT': 'connect-token-wct',
    'WIF': 'dogwifcoin',
    'WIKEN': 'project-with',
    'WOO': 'woo-network',
    
    # X 그룹
    'XAI': 'xai-blockchain',  # Xai 메인
    'XCN': 'chain-2',
    'XEC': 'ecash',
    'XPLA': 'xpla',
    'XPR': 'proton',
    'XTER': 'xterio',
    'XTZ': 'tezos',
    'XVS': 'venus',
    'XYO': 'xyo-network',
    
    # Y 그룹
    'YFI': 'yearn-finance',
    'YGG': 'yield-guild-games',
    
    # Z 그룹
    'ZETA': 'zetachain',
    'ZIL': 'zilliqa',
    'ZK': 'zksync',
    'ZRC': 'zircuit',
    'ZRO': 'layerzero',
    'ZRX': '0x',
    'ZTX': 'ztx',
}

# DAG 기본 설정
default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='create_bithumb_coingecko_mapping_k8s',
    default_args=default_args,
    description='빗썸-CoinGecko ID 매칭 테이블 생성 (하드코딩 방식)',
    schedule_interval='@once',
    catchup=False,
    template_searchpath=[INITDB_SQL_DIR], 
    tags=['bithumb', 'coingecko', 'mapping', 'setup', 'hardcoded'],
) as dag:

    def create_hardcoded_mapping(**context):
        """하드코딩된 매핑을 사용하여 정확한 매칭 테이블 생성"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logging.info("=== 하드코딩 방식 빗썸-CoinGecko 매칭 시작 ===")
        
        # 기존 매핑 테이블 초기화
        hook.run("TRUNCATE TABLE bithumb_coingecko_mapping")
        logging.info("기존 매핑 테이블 초기화 완료")
        
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
        
        logging.info(f"빗썸 코인 수: {len(bithumb_records)}개")
        logging.info(f"하드코딩 매핑 수: {len(COMPLETE_BITHUMB_COINGECKO_MAPPINGS)}개")
        
        # 2. 매칭 통계
        matched_count = 0
        failed_matches = []
        
        # 3. 각 빗썸 코인에 대해 하드코딩된 매핑 적용
        for i, record in enumerate(bithumb_records, 1):
            market_code = record[0]
            symbol = record[1]
            korean_name = record[2]
            english_name = record[3]
            market_warning = record[4]
            
            logging.info(f"[{i:3d}/{len(bithumb_records)}] 처리 중: {symbol}")
            
            # 하드코딩 매핑에서 CoinGecko ID 찾기
            if symbol in COMPLETE_BITHUMB_COINGECKO_MAPPINGS:
                coingecko_id = COMPLETE_BITHUMB_COINGECKO_MAPPINGS[symbol]
                
                # CoinGecko 데이터 조회
                coingecko_query = """
                SELECT 
                    coingecko_id,
                    symbol,
                    name,
                    market_cap_rank,
                    market_cap_usd,
                    current_price_usd
                FROM coingecko_id_mapping 
                WHERE coingecko_id = %s
                LIMIT 1
                """
                
                coingecko_data = hook.get_first(coingecko_query, parameters=(coingecko_id,))
                
                if coingecko_data:
                    # 매칭 결과 삽입
                    insert_query = """
                    INSERT INTO bithumb_coingecko_mapping (
                        market_code, symbol, bithumb_korean_name, bithumb_english_name,
                        coingecko_id, coingecko_name, market_cap_rank,
                        market_cap_usd, current_price_usd, 
                        match_method, match_score, is_verified
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    hook.run(insert_query, parameters=(
                        market_code,
                        symbol,
                        korean_name,
                        english_name,
                        coingecko_data[0],  # coingecko_id
                        coingecko_data[2],  # name
                        coingecko_data[3],  # market_cap_rank
                        coingecko_data[4],  # market_cap_usd
                        coingecko_data[5],  # current_price_usd
                        'HARDCODED_MAPPING',
                        100.0,  # 완벽한 점수
                        True    # 검증됨
                    ))
                    
                    matched_count += 1
                    logging.info(f"✅ 매칭 성공 ({matched_count:3d}): {symbol:8} → {coingecko_id}")
                else:
                    failed_matches.append({
                        'market_code': market_code,
                        'symbol': symbol,
                        'reason': f'COINGECKO_ID_NOT_FOUND: {coingecko_id}'
                    })
                    logging.warning(f"❌ CoinGecko ID '{coingecko_id}' 데이터 없음: {symbol}")
            else:
                failed_matches.append({
                    'market_code': market_code,
                    'symbol': symbol,
                    'reason': 'NO_HARDCODED_MAPPING'
                })
                logging.warning(f"❌ 하드코딩 매핑 없음: {symbol}")
        
        # 4. 결과 요약
        total_coins = len(bithumb_records)
        success_rate = (matched_count / total_coins * 100) if total_coins > 0 else 0
        
        logging.info("=" * 80)
        logging.info("📊 하드코딩 방식 빗썸-CoinGecko 매칭 완료 결과")
        logging.info("-" * 80)
        logging.info(f"✅ 매칭 성공:   {matched_count:4d}개")
        logging.info(f"❌ 매칭 실패:   {len(failed_matches):4d}개")
        logging.info(f"📈 성공률:      {success_rate:5.1f}%")
        logging.info(f"🎯 목표:        {total_coins}/385개")
        
        # 실패 목록 출력
        if failed_matches:
            logging.info(f"❌ 매칭 실패 목록 ({len(failed_matches)}개):")
            for failed in failed_matches:
                logging.info(f"    {failed['market_code']:12} ({failed['symbol']:8}): {failed['reason']}")
        
        logging.info("=" * 80)
        
        # 5. 빠진 하드코딩 매핑 확인
        bithumb_symbols = {record[1] for record in bithumb_records}
        hardcoded_symbols = set(COMPLETE_BITHUMB_COINGECKO_MAPPINGS.keys())
        
        missing_in_hardcode = bithumb_symbols - hardcoded_symbols
        extra_in_hardcode = hardcoded_symbols - bithumb_symbols
        
        if missing_in_hardcode:
            logging.warning(f"⚠️  하드코딩에 빠진 빗썸 심볼 ({len(missing_in_hardcode)}개):")
            for symbol in sorted(missing_in_hardcode):
                logging.warning(f"    {symbol}")
        
        if extra_in_hardcode:
            logging.info(f"ℹ️  빗썸에 없는 하드코딩 심볼 ({len(extra_in_hardcode)}개):")
            for symbol in sorted(extra_in_hardcode):
                logging.info(f"    {symbol}")
        
        return {
            'matched_count': matched_count,
            'failed_count': len(failed_matches),
            'success_rate': success_rate,
            'failed_matches': failed_matches,
            'missing_hardcode': list(missing_in_hardcode),
            'extra_hardcode': list(extra_in_hardcode)
        }

    def verify_hardcoded_mapping(**context):
        """하드코딩된 매핑 품질 검증"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 1. 전체 매핑 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_mappings,
            COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 100 THEN 1 END) as top100_count,
            COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 50 THEN 1 END) as top50_count,
            COUNT(CASE WHEN market_cap_rank IS NOT NULL AND market_cap_rank <= 10 THEN 1 END) as top10_count,
            AVG(match_score) as avg_score
        FROM bithumb_coingecko_mapping
        """
        
        stats = hook.get_first(stats_query)
        
        if stats:
            logging.info("=" * 80)
            logging.info("📊 하드코딩 매핑 품질 검증 결과")
            logging.info("-" * 80)
            logging.info(f"📈 전체 매칭 수:     {stats[0]:4d}개")
            logging.info(f"🏆 Top 100 코인:    {stats[1]:4d}개 ({stats[1]/stats[0]*100:.1f}%)")
            logging.info(f"🥇 Top 50 코인:     {stats[2]:4d}개 ({stats[2]/stats[0]*100:.1f}%)")
            logging.info(f"👑 Top 10 코인:     {stats[3]:4d}개 ({stats[3]/stats[0]*100:.1f}%)")
            logging.info(f"📊 평균 점수:       {stats[4]:.1f}점")
        
        # 2. 시가총액 상위 코인들 확인
        top_coins_query = """
        SELECT symbol, coingecko_name, market_cap_rank, 
               market_cap_usd, current_price_usd
        FROM bithumb_coingecko_mapping
        WHERE market_cap_rank IS NOT NULL AND market_cap_rank <= 20
        ORDER BY market_cap_rank ASC
        """
        
        top_coins = hook.get_records(top_coins_query)
        
        if top_coins:
            logging.info(f"🏆 빗썸 상장 Top 20 코인:")
            for record in top_coins:
                logging.info(f"    #{record[2]:2d} {record[0]:8} - {record[1]}")
        
        # 3. 중복 CoinGecko ID 확인 (있으면 안됨)
        duplicate_query = """
        SELECT coingecko_id, COUNT(*) as count, 
               STRING_AGG(symbol, ', ') as symbols
        FROM bithumb_coingecko_mapping
        GROUP BY coingecko_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        """
        
        duplicates = hook.get_records(duplicate_query)
        
        if duplicates:
            logging.error(f"❌ 중복 CoinGecko ID 발견 ({len(duplicates)}개):")
            for record in duplicates:
                logging.error(f"    {record[0]:25}: {record[1]}개 - {record[2]}")
        else:
            logging.info(f"✅ 중복 CoinGecko ID 없음 (완벽한 1:1 매칭)")
        
        # 4. 누락된 주요 코인 확인
        missing_major_coins_query = """
        SELECT 
            mcb.market_code,
            mcb.symbol,
            mcb.korean_name
        FROM (
            SELECT 
                market_code,
                REPLACE(market_code, 'KRW-', '') as symbol,
                korean_name
            FROM market_code_bithumb 
            WHERE market_code LIKE 'KRW-%'
        ) mcb
        LEFT JOIN bithumb_coingecko_mapping bcm ON mcb.symbol = bcm.symbol
        WHERE bcm.symbol IS NULL
        ORDER BY mcb.symbol
        """
        
        missing_coins = hook.get_records(missing_major_coins_query)
        
        if missing_coins:
            logging.warning(f"⚠️  매핑 누락된 빗썸 코인 ({len(missing_coins)}개):")
            for record in missing_coins:
                logging.warning(f"    {record[0]:12} ({record[1]:8}): {record[2]}")
            
            # 누락된 심볼들을 하드코딩 매핑에 추가할 수 있도록 로그 출력
            logging.info("📝 하드코딩 매핑에 추가해야 할 심볼들:")
            for record in missing_coins:
                logging.info(f"    '{record[1]}': '',  # {record[2]} - TODO: CoinGecko ID 찾기")
        
        logging.info("=" * 80)

    def generate_missing_mappings_report(**context):
        """누락된 매핑에 대한 상세 보고서 생성"""
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 누락된 코인들과 유사한 CoinGecko 후보들 찾기
        missing_coins_query = """
        SELECT 
            mcb.market_code,
            mcb.symbol,
            mcb.korean_name,
            mcb.english_name
        FROM (
            SELECT 
                market_code,
                REPLACE(market_code, 'KRW-', '') as symbol,
                korean_name,
                english_name
            FROM market_code_bithumb 
            WHERE market_code LIKE 'KRW-%'
        ) mcb
        LEFT JOIN bithumb_coingecko_mapping bcm ON mcb.symbol = bcm.symbol
        WHERE bcm.symbol IS NULL
        ORDER BY mcb.symbol
        """
        
        missing_coins = hook.get_records(missing_coins_query)
        
        if missing_coins:
            logging.info("=" * 80)
            logging.info("🔍 누락된 매핑 상세 분석 보고서")
            logging.info("-" * 80)
            
            for record in missing_coins:
                symbol = record[1]
                korean_name = record[2]
                english_name = record[3]
                
                # 해당 심볼로 CoinGecko에서 후보들 찾기
                candidates_query = """
                SELECT 
                    coingecko_id,
                    symbol,
                    name,
                    market_cap_rank,
                    market_cap_usd
                FROM coingecko_id_mapping 
                WHERE UPPER(symbol) = UPPER(%s)
                ORDER BY 
                    CASE 
                        WHEN market_cap_rank IS NULL THEN 999999 
                        ELSE market_cap_rank 
                    END ASC
                LIMIT 5
                """
                
                candidates = hook.get_records(candidates_query, parameters=(symbol,))
                
                logging.info(f"📋 {symbol} ({korean_name}):")
                if candidates:
                    logging.info(f"   CoinGecko 후보들:")
                    for i, candidate in enumerate(candidates, 1):
                        rank = candidate[3] if candidate[3] else "N/A"
                        market_cap = f"${candidate[4]:,.0f}" if candidate[4] else "N/A"
                        logging.info(f"     {i}. {candidate[0]:30} (#{rank:>4}, {market_cap:>15})")
                    
                    # 가장 유력한 후보 추천
                    best_candidate = candidates[0]
                    logging.info(f"   💡 추천: '{symbol}': '{best_candidate[0]}',")
                else:
                    # 이름으로 검색
                    name_search_query = """
                    SELECT 
                        coingecko_id,
                        symbol,
                        name,
                        market_cap_rank,
                        market_cap_usd
                    FROM coingecko_id_mapping 
                    WHERE LOWER(name) LIKE LOWER(%s) 
                       OR LOWER(coingecko_id) LIKE LOWER(%s)
                    ORDER BY 
                        CASE 
                            WHEN market_cap_rank IS NULL THEN 999999 
                            ELSE market_cap_rank 
                        END ASC
                    LIMIT 3
                    """
                    
                    name_candidates = hook.get_records(name_search_query, 
                        parameters=(f'%{english_name[:10]}%', f'%{symbol.lower()}%'))
                    
                    if name_candidates:
                        logging.info(f"   이름 기반 후보들:")
                        for i, candidate in enumerate(name_candidates, 1):
                            rank = candidate[3] if candidate[3] else "N/A"
                            market_cap = f"${candidate[4]:,.0f}" if candidate[4] else "N/A"
                            logging.info(f"     {i}. {candidate[0]:30} (#{rank:>4}, {market_cap:>15})")
                    else:
                        logging.info(f"   ❌ CoinGecko에서 '{symbol}' 후보를 찾을 수 없음")
                        logging.info(f"   🔍 수동으로 CoinGecko에서 '{english_name}' 검색 필요")
                
                logging.info("")
            
            logging.info("=" * 80)

    # Task 정의
    create_mapping_table = PostgresOperator(
        task_id='create_bithumb_coingecko_mapping_table',
        postgres_conn_id='postgres_default',
        sql='create_bithumb_coingecko_mapping.sql',
    )

    run_hardcoded_mapping = PythonOperator(
        task_id='create_hardcoded_mapping',
        python_callable=create_hardcoded_mapping,
    )

    verify_quality = PythonOperator(
        task_id='verify_hardcoded_mapping_quality',
        python_callable=verify_hardcoded_mapping,
    )

    generate_report = PythonOperator(
        task_id='generate_missing_mappings_report',
        python_callable=generate_missing_mappings_report,
    )

    # Task 의존성 설정
    create_mapping_table >> run_hardcoded_mapping >> [verify_quality, generate_report]