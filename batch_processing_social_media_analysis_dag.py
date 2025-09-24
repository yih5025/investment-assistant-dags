# social_media_analysis_dag.py - 100개 제한 버전
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import json
import logging

try:
    from utils.asset_matcher import SocialMediaAnalyzer
    from utils.market_data_collector import MarketDataCollector
    from utils.market_data_collector import MarketAnalyzer
except ImportError:
    from dags.utils.asset_matcher import SocialMediaAnalyzer
    from dags.utils.market_data_collector import MarketDataCollector
    from dags.utils.market_data_collector import MarketAnalyzer

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 21),
    'retries': None,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='batch_processing_social_media_market_analysis',
    default_args=default_args,
    description='소셜미디어 게시글 시장 영향 분석 - 50개씩 제한 처리',
    schedule_interval='0 */2 * * *',  # 2시간마다 실행
    catchup=False,
    max_active_runs=1,
    tags=['social_media', 'market_analysis', 'batch_limited']
)
def social_media_analysis_dag():
    
    @task
    def get_unanalyzed_posts_limited():
        """분석되지 않은 게시글 50개씩만 조회"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 각 소스별로 최대 50개씩만 (총 150개 제한)
        LIMIT_PER_SOURCE = 50
        
        # X 게시글 - 50개만
        x_query = """
        SELECT tweet_id as post_id, 'x' as source, username,
                text as content, created_at as post_timestamp
        FROM x_posts
        WHERE tweet_id NOT IN (
            SELECT post_id FROM post_analysis_cache WHERE post_source = 'x'
        )
        AND text NOT LIKE '@%%'
        AND text IS NOT NULL
        ORDER BY created_at DESC
        LIMIT %s
        """
        
        # Truth Social 게시글 - 50개만
        truth_posts_query = """
        SELECT id as post_id, 'truth_social_posts' as source, username,
               clean_content as content, created_at as post_timestamp
        FROM truth_social_posts
        WHERE id NOT IN (
            SELECT post_id FROM post_analysis_cache
            WHERE (post_source = 'truth_social_posts' OR post_source = 'truth_social_trends')
        )
        ORDER BY created_at DESC
        LIMIT %s
        """
        
        # Truth Social 트렌드 - 50개만
        truth_trends_query = """
        SELECT id as post_id, 'truth_social_trends' as source, username,
               clean_content as content, created_at as post_timestamp
        FROM truth_social_trends
        WHERE id NOT IN (
            SELECT post_id FROM post_analysis_cache
            WHERE (post_source = 'truth_social_posts' OR post_source = 'truth_social_trends')
        )
        ORDER BY created_at DESC
        LIMIT %s
        """
        
        x_posts = pg_hook.get_records(x_query, parameters=[LIMIT_PER_SOURCE])
        truth_posts = pg_hook.get_records(truth_posts_query, parameters=[LIMIT_PER_SOURCE])
        truth_trends = pg_hook.get_records(truth_trends_query, parameters=[LIMIT_PER_SOURCE])
        
        # 딕셔너리 형태로 변환
        all_posts = []
        
        for row in x_posts:
            all_posts.append({
                'post_id': row[0],
                'source': row[1], 
                'username': row[2],
                'content': row[3],
                'post_timestamp': row[4]
            })
        
        for row in truth_posts:
            all_posts.append({
                'post_id': row[0],
                'source': row[1],
                'username': row[2], 
                'content': row[3],
                'post_timestamp': row[4]
            })
            
        for row in truth_trends:
            all_posts.append({
                'post_id': row[0],
                'source': row[1],
                'username': row[2], 
                'content': row[3],
                'post_timestamp': row[4]
            })
        
        total_posts = len(all_posts)
        logger.info(f"이번 배치에서 처리할 게시글: {total_posts}개 (X: {len(x_posts)}, Truth Posts: {len(truth_posts)}, Truth Trends: {len(truth_trends)})")
        
        if total_posts == 0:
            logger.info("🎉 모든 게시글 분석 완료!")
        
        return all_posts
    
    @task
    def analyze_posts_batch(posts):
        """게시글 배치 분석 + 시장 분석 추가"""
        if not posts:
            logger.info("처리할 게시글이 없습니다.")
            return []
        
        analyzer = SocialMediaAnalyzer()
        collector = MarketDataCollector()
        market_analyzer = MarketAnalyzer(collector.pg_hook)  # 새로 추가된 분석기
        results = []

        logger.info(f"🚀 {len(posts)}개 게시글 분석 시작 (시장 분석 포함)")
        logger.info("📊 MarketAnalyzer 초기화 완료")
        
        for i, post in enumerate(posts):
            try:
                logger.info(f"게시글 분석 중 {i+1}/{len(posts)}: {post['post_id']}")
                
                # 1. 기존 자산 매칭
                affected_assets = analyzer.determine_affected_assets(
                    username=post['username'],
                    content=post['content'],
                    timestamp=post['post_timestamp'],
                    post_id=post['post_id'],
                    post_source=post['source']
                )
                
                # 2. 기존 시장 데이터 수집
                market_data = collector.collect_market_data(
                    affected_assets, post['post_timestamp']
                )
                
                # 3. 새로운 시장 분석 추가
                price_analysis = {}
                volume_analysis = {}

                logger.info(f"📈 {len(affected_assets)}개 자산에 대한 시장 분석 시작")

                for asset in affected_assets:
                    symbol = asset['symbol']
                    if symbol in market_data:
                        # 가격 변화 분석
                        price_changes = market_analyzer.calculate_price_changes(
                            symbol, post['post_timestamp'], market_data[symbol]
                        )
                        if price_changes:
                            price_analysis[symbol] = price_changes

                        # 거래량 변화 분석
                        volume_changes = market_analyzer.calculate_volume_changes(
                            symbol, post['post_timestamp'], market_data[symbol]
                        )
                        if volume_changes:
                            volume_analysis[symbol] = volume_changes
                
                # 분석 상태 결정
                analysis_status = 'complete' if affected_assets else 'partial'
                
                result = {
                    'post_id': post['post_id'],
                    'post_source': post['source'],
                    'post_timestamp': post['post_timestamp'],
                    'author_username': post['username'],
                    'affected_assets': affected_assets,
                    'market_data': market_data,
                    'price_analysis': price_analysis,  # 새로 추가
                    'volume_analysis': volume_analysis,  # 새로 추가
                    'analysis_status': analysis_status
                }
                
                results.append(result)
                logger.info(f"✅ 분석 완료 {post['post_id']} - {len(affected_assets)}개 자산, {len(price_analysis)}개 가격분석, {len(volume_analysis)}개 거래량분석")
                
            except Exception as e:
                logger.error(f"❌ 분석 실패 {post['post_id']}: {e}")
                results.append({
                    'post_id': post['post_id'],
                    'post_source': post['source'],
                    'post_timestamp': post['post_timestamp'],
                    'author_username': post['username'],
                    'affected_assets': [],
                    'market_data': {},
                    'price_analysis': {},
                    'volume_analysis': {},
                    'analysis_status': 'failed',
                    'error': str(e)
                })
        
        logger.info(f"✅ 배치 분석 완료: {len(results)}개 게시글 처리됨")
        logger.info(f"📊 전체 통계 - 가격분석: {sum(len(r.get('price_analysis', {})) for r in results)}개, 거래량분석: {sum(len(r.get('volume_analysis', {})) for r in results)}개")
        return results
    
    @task
    def save_analysis_results(analysis_results):
        """강화된 분석 결과 저장"""
        if not analysis_results:
            logger.info("저장할 결과가 없습니다.")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logger.info(f"💾 {len(analysis_results)}개 분석 결과 저장 중")
        
        for result in analysis_results:
            try:
                upsert_query = """
                INSERT INTO post_analysis_cache 
                (post_id, post_source, post_timestamp, author_username, 
                affected_assets, market_data, price_analysis, volume_analysis, 
                analysis_status, error_message)
                VALUES (%(post_id)s, %(post_source)s, %(post_timestamp)s, 
                        %(author_username)s, %(affected_assets)s, %(market_data)s, 
                        %(price_analysis)s, %(volume_analysis)s,
                        %(analysis_status)s, %(error_message)s)
                ON CONFLICT (post_id, post_source) 
                DO UPDATE SET 
                    affected_assets = EXCLUDED.affected_assets,
                    market_data = EXCLUDED.market_data,
                    price_analysis = EXCLUDED.price_analysis,
                    volume_analysis = EXCLUDED.volume_analysis,
                    analysis_status = EXCLUDED.analysis_status,
                    error_message = EXCLUDED.error_message,
                    updated_at = NOW()
                """
                
                params = {
                    'post_id': result['post_id'],
                    'post_source': result['post_source'],
                    'post_timestamp': result['post_timestamp'],
                    'author_username': result['author_username'],
                    'affected_assets': json.dumps(result['affected_assets']),
                    'market_data': json.dumps(result['market_data']),
                    'price_analysis': json.dumps(result['price_analysis']),
                    'volume_analysis': json.dumps(result['volume_analysis']),
                    'analysis_status': result['analysis_status'],
                    'error_message': result.get('error', None)
                }
                
                pg_hook.run(upsert_query, parameters=params)
                
            except Exception as e:
                logger.error(f"저장 실패 {result['post_id']}: {e}")
                continue
        
        logger.info("✅ 분석 결과 저장 완료")
    
    @task
    def finalize_keywords():
        """키워드 정리 작업"""
        analyzer = SocialMediaAnalyzer()
        analyzer.finalize_keywords()
        logger.info("키워드 정리 완료")

    # DAG 태스크 실행 흐름 (기존과 동일)
    posts = get_unanalyzed_posts_limited()
    analysis_results = analyze_posts_batch(posts)
    save_analysis_results(analysis_results)
    finalize_keywords()

# DAG 인스턴스 생성
dag_instance = social_media_analysis_dag()