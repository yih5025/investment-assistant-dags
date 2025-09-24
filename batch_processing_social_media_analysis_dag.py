# social_media_analysis_dag_optimized.py
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook
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
    'retry_delay': timedelta(minutes=2)
}

# 배치 처리 설정
BATCH_SIZE = 100  # 각 소스별 100개씩

@dag(
    dag_id='batch_processing_social_media_market_analysis',
    default_args=default_args,
    description='소셜미디어 게시글 시장 영향 분석 - 배치 처리',
    schedule_interval='0 * * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['social_media', 'market_analysis', 'batch_processing']
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
            result = pg_hook.get_first(query)
            total, processed = result
            remaining = total - processed
            status[source] = {
                'total': total,
                'processed': processed,
                'remaining': remaining,
                'has_work': remaining > 0
            }
        
        logger.info(f"Processing status: {status}")
        return status
    
    @task
    def get_batch_posts(status):
        """배치 단위로 미분석 게시글 조회 (각 소스별 100개씩)"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        all_posts = []
        
        # X 게시글 배치 조회
        if status['x']['has_work']:
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
        
        # Truth Social Posts 배치 조회
        if status['truth_social_posts']['has_work']:
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
        
        # Truth Social Trends 배치 조회
        if status['truth_social_trends']['has_work']:
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
        
        total_posts = len(all_posts)
        logger.info(f"Total posts in this batch: {total_posts}")
        
        return all_posts
    
    @task
    def check_if_work_needed(status):
        """처리할 데이터가 있는지 확인"""
        has_work = any(s['has_work'] for s in status.values())
        
        if not has_work:
            logger.info("🎉 모든 게시글 분석 완료! DAG 실행을 건너뜁니다.")
            return False
        
        # 진행 상황 로그
        for source, info in status.items():
            if info['remaining'] > 0:
                progress_pct = (info['processed'] / info['total'] * 100) if info['total'] > 0 else 0
                logger.info(f"{source}: {info['processed']}/{info['total']} 완료 ({progress_pct:.1f}%), {info['remaining']}개 남음")
        
        return True
    
    @task
    def analyze_posts_batch_optimized(posts):
        """게시글 배치 분석 - 메모리 최적화"""
        if not posts:
            logger.info("No posts to analyze in this batch")
            return []
        
        analyzer = SocialMediaAnalyzer()
        collector = MarketDataCollector()
        results = []
        
        logger.info(f"🚀 Starting analysis of {len(posts)} posts in this batch")
        
        for i, post in enumerate(posts):
            try:
                logger.info(f"Analyzing post {i+1}/{len(posts)}: {post['post_id']} from {post['source']}")
                
                # 1단계: 자산 매칭 (통계적 분석 간소화)
                affected_assets = analyzer.determine_affected_assets_optimized(
                    username=post['username'],
                    content=post['content'],
                    timestamp=post['post_timestamp'],
                    post_id=post['post_id'],
                    post_source=post['source']
                )
                
                # 2단계: 시장 데이터 수집 (제한적)
                market_data = collector.collect_market_data_optimized(
                    affected_assets, post['post_timestamp']
                )
                
                # 분석 상태 결정
                analysis_status = 'complete' if affected_assets else 'partial'
                
                result = {
                    'post_id': post['post_id'],
                    'post_source': post['source'],
                    'post_timestamp': post['post_timestamp'],
                    'author_username': post['username'],
                    'affected_assets': affected_assets,
                    'market_data': market_data,
                    'analysis_status': analysis_status
                }
                
                results.append(result)
                logger.info(f"✅ Analyzed post {post['post_id']} - Found {len(affected_assets)} assets")
                
            except Exception as e:
                logger.error(f"❌ Analysis failed for post {post['post_id']}: {e}")
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
        
        logger.info(f"✅ Batch analysis completed: {len(results)} posts processed")
        return results
    
    @task
    def save_analysis_results_optimized(analysis_results):
        """분석 결과 저장 - 배치 최적화"""
        if not analysis_results:
            logger.info("No results to save in this batch")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        logger.info(f"💾 Saving {len(analysis_results)} analysis results")
        
        # 배치 인서트를 위한 데이터 준비
        insert_data = []
        for result in analysis_results:
            insert_data.append({
                'post_id': result['post_id'],
                'post_source': result['post_source'],
                'post_timestamp': result['post_timestamp'],
                'author_username': result['author_username'],
                'affected_assets': json.dumps(result['affected_assets']),
                'market_data': json.dumps(result['market_data']),
                'analysis_status': result['analysis_status'],
                'error_message': result.get('error', None)
            })
        
        # 배치 UPSERT 실행
        try:
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
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
            
            # executemany를 사용한 배치 처리
            cursor.executemany(upsert_query, insert_data)
            conn.commit()
            cursor.close()
            
            logger.info(f"✅ Successfully saved {len(analysis_results)} results")
            
        except Exception as e:
            logger.error(f"❌ Failed to save batch results: {e}")
            raise
    
    @task.branch
    def decide_execution_branch(work_needed):
        """처리할 데이터가 있을 때만 분석 실행"""
        if work_needed:
            return "analyze_posts_batch_optimized"
        else:
            return "log_completion"
    
    @task
    def log_completion():
        """전체 처리 완료 로그"""
        logger.info("🎉 모든 소셜미디어 게시글 분석이 완료되었습니다!")
        return "All processing completed"
    
    # DAG 태스크 플로우
    status = get_processing_status()
    work_needed = check_if_work_needed(status)
    
    # 브랜치: 작업이 필요한 경우에만 실행
    branch_decision = decide_execution_branch(work_needed)
    
    # 분석 브랜치
    posts = get_batch_posts(status)
    analysis_results = analyze_posts_batch_optimized(posts)
    save_results = save_analysis_results_optimized(analysis_results)
    
    # 완료 브랜치  
    completion_log = log_completion()
    
    # 태스크 의존성 설정
    status >> work_needed >> branch_decision
    branch_decision >> posts >> analysis_results >> save_results
    branch_decision >> completion_log

# DAG 인스턴스 생성
dag_instance = social_media_analysis_dag()