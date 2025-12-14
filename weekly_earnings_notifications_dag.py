import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

# 로거 설정
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def send_email_via_smtp(to_email: str, subject: str, html_content: str) -> bool:
    """
    smtp_default connection을 사용하여 직접 이메일 발송
    """
    try:
        # Airflow connection에서 SMTP 설정 가져오기
        conn = BaseHook.get_connection('smtp_default')
        
        smtp_host = conn.host
        smtp_port = conn.port or 587
        smtp_user = conn.login
        smtp_password = conn.password
        
        logger.info(f"📬 SMTP Config: {smtp_host}:{smtp_port}, user: {smtp_user}")
        
        # 이메일 메시지 생성
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = to_email
        
        # HTML 본문 추가
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        # SMTP 서버 연결 및 발송
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()  # TLS 시작
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, [to_email], msg.as_string())
        
        return True
        
    except Exception as e:
        logger.error(f"SMTP Error: {e}")
        raise e

with DAG(
    'weekly_earnings_notifications_dag',
    default_args=default_args,
    description='Send weekly earnings forecast emails',
    schedule_interval='0 0 * * 0', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['notification', 'sp500', 'earnings']
) as dag:

    def create_and_send_email(**context):
        # [로그] 태스크 시작
        logger.info("🚀 Starting weekly earnings notification task.")

        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # 1. 날짜 계산
            today = datetime.now().date()
            next_monday = today + timedelta(days=(7 - today.weekday()))
            next_sunday = next_monday + timedelta(days=6)
            
            # [로그] 날짜 확인
            logger.info(f"📅 Calculated Date Range: {next_monday} ~ {next_sunday}")

            # 2. S&P 500 실적 발표 데이터 조회
            earnings_sql = f"""
                SELECT 
                    ec.report_date,
                    ec.symbol,
                    sp.company_name,
                    ec.estimate,
                    sp.gics_sector
                FROM earnings_calendar ec
                JOIN sp500_companies sp ON ec.symbol = sp.symbol
                WHERE ec.report_date BETWEEN '{next_monday}' AND '{next_sunday}'
                ORDER BY ec.report_date ASC, sp.market_cap DESC;
            """
            
            # [로그] 쿼리 실행 직전
            logger.info("🔍 Executing SQL query to fetch earnings data...")
            
            earnings_data = pg_hook.get_records(earnings_sql)
            
            row_count = len(earnings_data) if earnings_data else 0
            # [로그] 데이터 건수 확인
            logger.info(f"📊 Query Result: Found {row_count} earnings events.")

            if not earnings_data:
                logger.warning("⚠️ No earnings scheduled for next week. Skipping email sending.")
                return "No Data"

            # 3. 이메일 본문 생성 함수
            def generate_email_body(token):
                # 실제 서버 주소로 변경 필요
                unsubscribe_link = f"https://api.investment-assistant.site/api/unsubscribe?token={token}"
                
                html = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; color: #333; }}
                        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
                        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                        th {{ background-color: #f4f4f4; }}
                        .footer {{ margin-top: 20px; font-size: 12px; color: #888; }}
                        a {{ color: #007bff; text-decoration: none; }}
                    </style>
                </head>
                <body>
                    <h2>📅 다음 주 S&P 500 실적 발표 일정</h2>
                    <p>안녕하세요! <b>{next_monday}</b>부터 <b>{next_sunday}</b>까지 예정된 주요 기업의 실적 발표 일정입니다.</p>
                    <table>
                        <thead>
                            <tr>
                                <th>날짜</th>
                                <th>티커</th>
                                <th>기업명</th>
                                <th>섹터</th>
                                <th>예상 EPS</th>
                            </tr>
                        </thead>
                        <tbody>
                """
                
                for row in earnings_data:
                    r_date = row[0]
                    symbol = row[1]
                    name = row[2]
                    est = row[3] if row[3] is not None else '-'
                    sector = row[4] if row[4] else '-'
                    
                    html += f"""
                            <tr>
                                <td>{r_date}</td>
                                <td><b>{symbol}</b></td>
                                <td>{name}</td>
                                <td>{sector}</td>
                                <td>{est}</td>
                            </tr>
                    """
                    
                html += f"""
                        </tbody>
                    </table>
                    <div class="footer">
                        <hr>
                        <p>본 메일은 투자 정보 제공을 위해 발송되었습니다.<br>
                        더 이상 알림을 원치 않으시면 <a href="{unsubscribe_link}">여기</a>를 클릭하여 구독을 취소하세요.</p>
                    </div>
                </body>
                </html>
                """
                return html

            # 4. 구독자 조회 (인증 완료된 구독자만)
            subs_sql = """
                SELECT email, unsubscribe_token 
                FROM email_subscriptions 
                WHERE is_active = TRUE 
                  AND is_verified = TRUE 
                  AND scope = 'SP500'
            """
            subscribers = pg_hook.get_records(subs_sql)
            
            # [로그] 구독자 수 확인
            logger.info(f"👥 Found {len(subscribers)} active & verified subscribers.")

            # 5. 이메일 발송 (직접 SMTP 사용)
            sent_count = 0
            error_count = 0

            for email, token in subscribers:
                try:
                    # [로그] 발송 시도
                    logger.info(f"📧 Sending email to: {email}")
                    
                    email_content = generate_email_body(token)
                    subject = f"[WE INVESTING] 다음 주 S&P 500 실적 발표 ({next_monday} 주간)"
                    
                    # 직접 SMTP로 이메일 발송
                    send_email_via_smtp(email, subject, email_content)
                    sent_count += 1
                    logger.info(f"✅ Successfully sent email to: {email}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to send email to {email}: {e}")
                    error_count += 1
            
            # [로그] 최종 완료
            logger.info(f"✅ Task Finished. Sent: {sent_count}, Errors: {error_count}")

        except Exception as e:
            # [로그] 치명적 에러 발생 시
            logger.error(f"🔥 Critical Error occurred: {e}")
            raise e

    task_send_email = PythonOperator(
        task_id='send_weekly_earnings_email',
        python_callable=create_and_send_email,
        provide_context=True
    )
