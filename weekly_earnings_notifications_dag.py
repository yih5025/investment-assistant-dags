from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

# 기본 설정: 매주 일요일 오전 9시 (KST 기준, UTC 00:00)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 1. 날짜 계산 (다음주 월요일 ~ 일요일)
        today = datetime.now().date()
        next_monday = today + timedelta(days=(7 - today.weekday())) # 돌아오는 월요일
        next_sunday = next_monday + timedelta(days=6)               # 그 주 일요일
        
        print(f"Fetching earnings for: {next_monday} ~ {next_sunday}")

        # 2. S&P 500 실적 발표 데이터 조회 (JOIN 활용)
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
        
        earnings_data = pg_hook.get_records(earnings_sql)
        
        if not earnings_data:
            print("No earnings scheduled for next week. Skipping email.")
            return "No Data"

        # 3. 이메일 본문(HTML) 생성 함수
        def generate_email_body(token):
            # 구독 취소 링크 (프론트엔드/백엔드 주소에 맞게 수정 필요)
            unsubscribe_link = f"http://YOUR_SERVER_IP:8000/api/unsubscribe?token={token}"
            
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

        # 4. 구독자 조회 (SP500 스코프 & 활성 사용자)
        subs_sql = """
            SELECT email, unsubscribe_token 
            FROM email_subscriptions 
            WHERE is_active = TRUE AND scope = 'SP500'
        """
        subscribers = pg_hook.get_records(subs_sql)
        
        print(f"Found {len(subscribers)} active subscribers.")

        # 5. 개별 이메일 발송 (구독 취소 토큰 때문에 개별 발송 필요)
        for email, token in subscribers:
            try:
                email_content = generate_email_body(token)
                send_email(
                    to=[email],
                    subject=f"[Investment Assistant] 다음 주 S&P 500 실적 발표 ({next_monday} 주간)",
                    html_content=email_content
                )
                print(f"Sent email to {email}")
            except Exception as e:
                print(f"Failed to send email to {email}: {e}")

    task_send_email = PythonOperator(
        task_id='send_weekly_earnings_email',
        python_callable=create_and_send_email,
        provide_context=True
    )