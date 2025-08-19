# monitoring_utils.py - DAG 폴더에 추가할 파일
import time
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import contextmanager

class DataCollectionMonitor:
    """데이터 수집 모니터링 클래스"""
    
    def __init__(self, dag_id, task_id, run_id, execution_date):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 메트릭 데이터 초기화
        self.metrics = {
            'api_calls_attempted': 0,
            'api_calls_successful': 0,
            'api_calls_failed': 0,
            'api_rate_limited': 0,
            'records_fetched': 0,
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'duplicate_records': 0,
            'invalid_records': 0,
            'error_message': None,
            'warning_message': None
        }
        
        self.start_time = time.time()
        print(f"📊 모니터링 시작: {dag_id}.{task_id}")
    
    def log_api_call(self, success=True, rate_limited=False):
        """API 호출 결과 기록"""
        self.metrics['api_calls_attempted'] += 1
        
        if success:
            self.metrics['api_calls_successful'] += 1
            print(f"✅ API 호출 성공 (총 {self.metrics['api_calls_successful']}/{self.metrics['api_calls_attempted']})")
        else:
            self.metrics['api_calls_failed'] += 1
            print(f"❌ API 호출 실패 (총 {self.metrics['api_calls_failed']}/{self.metrics['api_calls_attempted']})")
            
        if rate_limited:
            self.metrics['api_rate_limited'] += 1
            print(f"⏱️ Rate Limit 발생 (총 {self.metrics['api_rate_limited']}회)")
    
    def log_data_processing(self, fetched=0, processed=0, inserted=0, updated=0, skipped=0, duplicates=0, invalid=0):
        """데이터 처리 결과 기록"""
        self.metrics['records_fetched'] += fetched
        self.metrics['records_processed'] += processed
        self.metrics['records_inserted'] += inserted
        self.metrics['records_updated'] += updated
        self.metrics['records_skipped'] += skipped
        self.metrics['duplicate_records'] += duplicates
        self.metrics['invalid_records'] += invalid
        
        print(f"📦 데이터 처리: 가져옴({fetched}), 처리({processed}), 삽입({inserted}), 스킵({skipped})")
    
    def set_error(self, error_message):
        """에러 메시지 설정"""
        self.metrics['error_message'] = str(error_message)[:1000]
        print(f"🚨 에러 기록: {str(error_message)[:100]}...")
    
    def set_warning(self, warning_message):
        """경고 메시지 설정"""
        self.metrics['warning_message'] = str(warning_message)[:1000]
        print(f"⚠️ 경고 기록: {str(warning_message)[:100]}...")
    
    def determine_task_status(self):
        """태스크 상태 자동 결정"""
        if self.metrics['error_message']:
            return 'failed'
        elif self.metrics['records_inserted'] == 0 and self.metrics['records_updated'] == 0:
            if self.metrics['api_calls_attempted'] > 0:
                return 'no_data_collected'
            else:
                return 'no_api_calls'
        elif self.metrics['api_calls_failed'] > 0 or self.metrics['api_rate_limited'] > 0:
            return 'partial_success'
        else:
            return 'success'
    
    def finalize_and_save(self, latest_data_timestamp=None):
        """메트릭 최종 저장"""
        execution_duration = int(time.time() - self.start_time)
        task_status = self.determine_task_status()
        
        # 데이터 신선도 계산
        data_freshness = None
        if latest_data_timestamp:
            try:
                if isinstance(latest_data_timestamp, str):
                    latest_data_timestamp = datetime.fromisoformat(latest_data_timestamp.replace('Z', '+00:00'))
                now = datetime.now()
                freshness = (now - latest_data_timestamp.replace(tzinfo=None)).total_seconds() / 60
                data_freshness = int(freshness)
            except:
                pass
        
        # DB에 저장
        insert_sql = """
        INSERT INTO data_collection_metrics (
            dag_id, task_id, run_id, execution_date,
            api_calls_attempted, api_calls_successful, api_calls_failed, api_rate_limited,
            records_fetched, records_processed, records_inserted, records_updated, records_skipped,
            task_status, execution_duration_seconds, error_message, warning_message,
            data_freshness_minutes, duplicate_records, invalid_records
        ) VALUES (
            %(dag_id)s, %(task_id)s, %(run_id)s, %(execution_date)s,
            %(api_calls_attempted)s, %(api_calls_successful)s, %(api_calls_failed)s, %(api_rate_limited)s,
            %(records_fetched)s, %(records_processed)s, %(records_inserted)s, %(records_updated)s, %(records_skipped)s,
            %(task_status)s, %(execution_duration_seconds)s, %(error_message)s, %(warning_message)s,
            %(data_freshness_minutes)s, %(duplicate_records)s, %(invalid_records)s
        )
        ON CONFLICT (dag_id, task_id, run_id, execution_date) 
        DO UPDATE SET
            api_calls_attempted = EXCLUDED.api_calls_attempted,
            api_calls_successful = EXCLUDED.api_calls_successful,
            api_calls_failed = EXCLUDED.api_calls_failed,
            api_rate_limited = EXCLUDED.api_rate_limited,
            records_fetched = EXCLUDED.records_fetched,
            records_processed = EXCLUDED.records_processed,
            records_inserted = EXCLUDED.records_inserted,
            records_updated = EXCLUDED.records_updated,
            records_skipped = EXCLUDED.records_skipped,
            task_status = EXCLUDED.task_status,
            execution_duration_seconds = EXCLUDED.execution_duration_seconds,
            error_message = EXCLUDED.error_message,
            warning_message = EXCLUDED.warning_message,
            data_freshness_minutes = EXCLUDED.data_freshness_minutes,
            duplicate_records = EXCLUDED.duplicate_records,
            invalid_records = EXCLUDED.invalid_records,
            updated_at = NOW()
        """
        
        params = {
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'run_id': self.run_id,
            'execution_date': self.execution_date,
            'task_status': task_status,
            'execution_duration_seconds': execution_duration,
            'data_freshness_minutes': data_freshness,
            **self.metrics
        }
        
        try:
            self.hook.run(insert_sql, parameters=params)
            print(f"📊 모니터링 완료 저장: {task_status}")
            print(f"   📈 실행시간: {execution_duration}초")
            print(f"   🔢 API: {self.metrics['api_calls_successful']}/{self.metrics['api_calls_attempted']}")
            print(f"   💾 데이터: {self.metrics['records_inserted']}개 삽입")
        except Exception as e:
            print(f"⚠️ 모니터링 메트릭 저장 실패: {e}")
    
    @contextmanager
    def monitor_api_call(self):
        """API 호출 자동 모니터링"""
        try:
            yield self
            self.log_api_call(success=True)
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "rate limit" in error_str or "too many requests" in error_str:
                self.log_api_call(success=False, rate_limited=True)
            else:
                self.log_api_call(success=False)
            raise

def create_monitor(context):
    """Context에서 모니터 생성 (DAG에서 사용)"""
    return DataCollectionMonitor(
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        run_id=context['dag_run'].run_id,
        execution_date=context['execution_date']
    )