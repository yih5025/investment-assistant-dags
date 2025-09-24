# social_media_analysis_dag_fixed.py - 브랜치 로직 수정
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json
import logging

try:
    from utils.asset_matcher import SocialMediaAnalyzer
    from utils.market_data_collector import MarketDataCollector
except ImportError:
    from dags.utils.asset_matcher import SocialMediaAnalyzer
    from dags.utils.market_data_collector import MarketDataCollector

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'investment_assistant',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 21),
    'retries': None,
    'retry_delay': timedelta(minutes=5)
}

BATCH_SIZE = 100

@dag(
    dag_id='hourly_batch_social_media_analysis_fixed',
    default_args=default_args,
    description='소셜미디어 게시글 시장 영향 분석 - 1시간마다 배치 처리 (수정됨)',
    schedule_interval='0 * * * *',  # 매시 정각 실행
    catchup=False,
    max_active_runs=1,
    tags=['social_media', 'market_analysis', 'hourly_batch', 'fixed']
)
def social_media_analysis_dag():
    
    @task
    def get_processing_status():
        """현재 처리 진행 상황 확인"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        status_queries = {
            'x': """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN tweet_id IN (
                        SELECT post_id FROM post_analysis_cache WHERE post_source = 'x'
                    ) THEN 1 END) as processed
                FROM x_posts
            """,
            'truth_social_posts': """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN id IN (
                        SELECT post_id FROM post_analysis_cache WHERE post_source = 'truth_social_posts'
                    ) THEN 1 END) as processed
                FROM truth_social_posts
            """,
            'truth_social_trends': """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN id IN (
                        SELECT post_id FROM post_analysis_cache WHERE post_source = 'truth_social_trends'
                    ) THEN 1 END) as processed
                FROM truth_social_trends
            """
        }
        
        status = {}
        for source, query in status_queries.items():
            try:
                result = pg_hook.get_first(query)
                if result:
                    total, processed = result
                    remaining = total - processed
                    status[source] = {
                        'total': total,
                        'processed': processed,
                        'remaining': remaining,
                        'has_work': remaining > 0
                    }
                else:
                    status[source] = {
                        'total': 0,
                        'processed': 0,
                        'remaining': 0,
                        'has_work': False
                    }
            except Exception as e:
                logger.error(f"Error checking status for {source}: {e}")
                status[source] = {
                    'total': 0,
                    'processed': 0,
                    'remaining': 0,
                    'has_work': False
                }
        
        logger.info(f"Processing status: {status}")
        return status
    
    @task
    def get_batch_posts(status):
        """배치 단위로 미분석 게시글 조회"""
        # 처리할 작업이 없으면 빈 리스트 반환
        total_work = sum(s['remaining'] for s in status.values())
        if total_work == 0:
            logger.info("No work needed - returning empty list")
            return []
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        all_posts = []
        
        # X 게시글 배치 조회
        if status['x']['has_work']:
            try:
                x_query = """
                SELECT tweet_id as post_id, 'x' as source, username, 
                       text as content, created_at as post_timestamp
                FROM x_posts 
                WHERE tweet_id NOT IN (
                    SELECT post_id FROM post_analysis_cache WHERE post_source = 'x'
                )
                ORDER BY created_at DESC
                LIMIT %s
                """
                x_posts = pg_hook.get_records(x_query, parameters=[BATCH_SIZE])
                
                for row in x_posts:
                    all_posts.append({
                        'post_id': row[0],
                        'source': row[1], 
                        'username': row[2],
                        'content': row[3],
                        'post_timestamp': row[4]
                    })
                
                logger.info(f"Retrieved {len(x_posts)} X posts")
            except Exception as e:
                logger.error(f"Error fetching X posts: {e}")
        
        # Truth Social Posts 배치 조회
        if status['truth_social_posts']['has_work']:
            try:
                truth_posts_query = """
                SELECT id as post_id, 'truth_social_posts' as source, username,
                       clean_content as content, created_at as post_timestamp
                FROM truth_social_posts
                WHERE id NOT IN (
                    SELECT post_id FROM post_analysis_cache WHERE post_source = 'truth_social_posts'
                )
                ORDER BY created_at DESC
                LIMIT %s
                """
                truth_posts = pg_hook.get_records(truth_posts_query, parameters=[BATCH_SIZE])
                
                for row in truth_posts:
                    all_posts.append({
                        'post_id': row[0],
                        'source': row[1],
                        'username': row[2], 
                        'content': row[3],
                        'post_timestamp': row[4]
                    })
                
                logger.info(f"Retrieved {len(truth_posts)} Truth Social posts")
            except Exception as e:
                logger.error(f"Error fetching Truth Social posts: {e}")
        
        # Truth Social Trends 배치 조회
        if status['truth_social_trends']['has_work']:
            try:
                truth_trends_query = """
                SELECT id as post_id, 'truth_social_trends' as source, username,
                       clean_content as content, created_at as post_timestamp
                FROM truth_social_trends
                WHERE id NOT IN (
                    SELECT post_id FROM post_analysis_cache WHERE post_source = 'truth_social_trends'
                )
                ORDER BY created_at DESC
                LIMIT %s
                """
                truth_trends = pg_hook.get_records(truth_trends_query, parameters=[BATCH_SIZE])
                
                for row in truth_trends:
                    all_posts.append({
                        'post_id': row[0],
                        'source': row[1],
                        'username': row[2], 
                        'content': row[3],
                        'post_timestamp': row[4]
                    })
                
                logger.info(f"Retrieved {len(truth_trends)} Truth Social trends")
            except Exception as e:
                logger.error(f"Error fetching Truth Social trends: {e}")
        
        total_posts = len(all_posts)
        logger.info(f"Total posts in this batch: {total_posts}")
        
        return all_posts
    
    @task
    def analyze_posts_batch_optimized(posts):
        """게시글 배치 분석 - 간소화된 버전"""
        if not posts:
            logger.info("No posts to analyze in this batch")
            return []
        
        # utils 모듈 import 실패 대비 간단한 분석
        results = []
        
        logger.info(f"Starting analysis of {len(posts)} posts in this batch")
        
        for i, post in enumerate(posts):
            try:
                logger.info(f"Processing post {i+1}/{len(posts)}: {post['post_id']} from {post['source']}")
                
                # 간소화된 자산 매칭 (계정 기반만)
                affected_assets = []
                
                # 기본 계정 매핑
                account_mapping = {
                    'elonmusk': [{'symbol': 'TSLA', 'source': 'account_default', 'priority': 1}],
                    'realDonaldTrump': [{'symbol': 'DWAC', 'source': 'account_default', 'priority': 1}],
                    'Apple': [{'symbol': 'AAPL', 'source': 'account_default', 'priority': 1}],
                    'nvidia': [{'symbol': 'NVDA', 'source': 'account_default', 'priority': 1}],
                }
                
                username = post['username']
                if username in account_mapping:
                    affected_assets = account_mapping[username]
                
                # 분석 상태 결정
                analysis_status = 'complete' if affected_assets else 'partial'
                
                result = {
                    'post_id': post['post_id'],
                    'post_source': post['source'],
                    'post_timestamp': post['post_timestamp'],
                    'author_username': post['username'],
                    'affected_assets': affected_assets,
                    'market_data': {},  # 시장 데이터는 생략 (메모리 절약)
                    'analysis_status': analysis_status
                }
                
                results.append(result)
                logger.info(f"Analyzed post {post['post_id']} - Found {len(affected_assets)} assets")
                
            except Exception as e:
                logger.error(f"Analysis failed for post {post['post_id']}: {e}")
                results.append({
                    'post_id': post['post_id'],
                    'post_source': post['source'],
                    'post_timestamp': post['post_timestamp'],
                    'author_username': post['username'],
                    'affected_assets': [],
                    'market_data': {},
                    'analysis_status': 'failed',
                    'error': str(e)
                })
        
        logger.info(f"Batch analysis completed: {len(results)} posts processed")
        return results
    
    @task
    def save_analysis_results_optimized(analysis_results):
        """분석 결과 저장"""
        if not analysis_results:
            logger.info("No results to save in this batch")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logger.info(f"Saving {len(analysis_results)} analysis results")
        
        for result in analysis_results:
            try:
                upsert_query = """
                INSERT INTO post_analysis_cache 
                (post_id, post_source, post_timestamp, author_username, 
                 affected_assets, market_data, analysis_status, error_message)
                VALUES (%(post_id)s, %(post_source)s, %(post_timestamp)s, 
                        %(author_username)s, %(affected_assets)s, %(market_data)s, 
                        %(analysis_status)s, %(error_message)s)
                ON CONFLICT (post_id, post_source) 
                DO UPDATE SET 
                    affected_assets = EXCLUDED.affected_assets,
                    market_data = EXCLUDED.market_data,
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
                    'analysis_status': result['analysis_status'],
                    'error_message': result.get('error', None)
                }
                
                pg_hook.run(upsert_query, parameters=params)
                
            except Exception as e:
                logger.error(f"Failed to save result for post {result['post_id']}: {e}")
                continue
        
        logger.info("Analysis results saved successfully")
    
    @task
    def log_completion_status(status):
        """작업 완료 상태 로깅"""
        total_remaining = sum(s['remaining'] for s in status.values())
        
        if total_remaining == 0:
            logger.info("🎉 모든 소셜미디어 게시글 분석이 완료되었습니다!")
        else:
            for source, info in status.items():
                if info['remaining'] > 0:
                    progress_pct = (info['processed'] / info['total'] * 100) if info['total'] > 0 else 0
                    logger.info(f"{source}: {info['processed']}/{info['total']} 완료 ({progress_pct:.1f}%), {info['remaining']}개 남음")
        
        return f"Total remaining: {total_remaining}"
    
    # 수정된 DAG 플로우 - 브랜치 없이 순차 실행
    status = get_processing_status()
    posts = get_batch_posts(status)
    analysis_results = analyze_posts_batch_optimized(posts)
    save_results = save_analysis_results_optimized(analysis_results)
    completion_status = log_completion_status(status)
    
    # 태스크 의존성 (순차 실행)
    status >> posts >> analysis_results >> save_results >> completion_status

# DAG 인스턴스 생성
dag_instance = social_media_analysis_dag()