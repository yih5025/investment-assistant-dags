<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Investment Assistant 프로젝트 아키텍처</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72, #2a5298);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1800px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 40px;
            color: white;
        }
        
        .header h1 {
            font-size: 3em;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }
        
        .header .subtitle {
            font-size: 1.3em;
            opacity: 0.9;
        }
        
        .architecture-canvas {
            background: white;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.2);
            position: relative;
            overflow: hidden;
        }
        
        .layer {
            position: relative;
            margin: 30px 0;
            padding: 25px;
            border-radius: 15px;
            border: 2px solid;
            background: rgba(255,255,255,0.9);
        }
        
        .layer-title {
            position: absolute;
            top: -15px;
            left: 30px;
            background: white;
            padding: 8px 20px;
            border-radius: 25px;
            font-weight: bold;
            font-size: 16px;
            border: inherit;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        /* 계층별 색상 */
        .external-layer {
            border-color: #ff6b6b;
            background: linear-gradient(145deg, #ffe0e0, #fff5f5);
        }
        
        .infrastructure-layer {
            border-color: #4ecdc4;
            background: linear-gradient(145deg, #e0f8f7, #f0fffe);
        }
        
        .data-layer {
            border-color: #45b7d1;
            background: linear-gradient(145deg, #e0f4fd, #f0f9ff);
        }
        
        .application-layer {
            border-color: #f9ca24;
            background: linear-gradient(145deg, #fff8e0, #fffef0);
        }
        
        .monitoring-layer {
            border-color: #6c5ce7;
            background: linear-gradient(145deg, #f0eeff, #f8f7ff);
        }
        
        .component-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        
        .component {
            background: rgba(255,255,255,0.95);
            border: 1px solid rgba(0,0,0,0.1);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            transition: all 0.3s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            position: relative;
            overflow: hidden;
        }
        
        .component:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 16px rgba(0,0,0,0.2);
        }
        
        .component-icon {
            font-size: 3em;
            margin-bottom: 15px;
            display: block;
        }
        
        .component-name {
            font-size: 1.2em;
            font-weight: bold;
            margin-bottom: 8px;
            color: #2c3e50;
        }
        
        .component-desc {
            font-size: 0.9em;
            color: #7f8c8d;
            line-height: 1.4;
        }
        
        .component-status {
            position: absolute;
            top: 10px;
            right: 10px;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #27ae60;
            box-shadow: 0 0 8px rgba(39, 174, 96, 0.5);
        }
        
        .data-flow {
            margin: 40px 0;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 20px;
        }
        
        .flow-node {
            background: linear-gradient(145deg, #ffffff, #f8f9fa);
            border: 2px solid #3498db;
            border-radius: 15px;
            padding: 20px;
            text-align: center;
            min-width: 150px;
            position: relative;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        
        .flow-arrow {
            font-size: 2em;
            color: #3498db;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0% { opacity: 0.6; }
            50% { opacity: 1; }
            100% { opacity: 0.6; }
        }
        
        .stats-section {
            background: linear-gradient(145deg, #2c3e50, #34495e);
            color: white;
            border-radius: 15px;
            padding: 30px;
            margin: 30px 0;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        
        .stat-card {
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            backdrop-filter: blur(10px);
        }
        
        .stat-number {
            font-size: 2.5em;
            font-weight: bold;
            color: #3498db;
            display: block;
        }
        
        .stat-label {
            opacity: 0.8;
            margin-top: 5px;
        }
        
        .tech-stack {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        
        .tech-item {
            background: linear-gradient(145deg, #f8f9fa, #e9ecef);
            border-left: 4px solid #3498db;
            padding: 15px;
            border-radius: 8px;
            transition: all 0.3s ease;
        }
        
        .tech-item:hover {
            transform: translateX(5px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        
        .network-diagram {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 30px;
            margin: 30px 0;
            position: relative;
        }
        
        .server-rack {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            margin: 20px 0;
        }
        
        .server {
            background: linear-gradient(145deg, #ffffff, #f0f0f0);
            border: 2px solid #bdc3c7;
            border-radius: 10px;
            padding: 15px;
            text-align: center;
            transition: all 0.3s ease;
            position: relative;
        }
        
        .server:hover {
            transform: scale(1.05);
            border-color: #3498db;
        }
        
        .server.master {
            border-color: #e74c3c;
            background: linear-gradient(145deg, #fff5f5, #ffe6e6);
        }
        
        .server.worker {
            border-color: #3498db;
            background: linear-gradient(145deg, #f0f8ff, #e6f3ff);
        }
        
        .connection-line {
            position: absolute;
            height: 2px;
            background: #3498db;
            z-index: 1;
        }
        
        @media (max-width: 768px) {
            .component-grid {
                grid-template-columns: 1fr;
            }
            
            .server-rack {
                grid-template-columns: repeat(2, 1fr);
            }
            
            .data-flow {
                flex-direction: column;
            }
            
            .flow-arrow {
                transform: rotate(90deg);
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏗️ Investment Assistant</h1>
            <div class="subtitle">실시간 금융 데이터 수집 및 분석 플랫폼 아키텍처</div>
        </div>
        
        <div class="architecture-canvas">
            <!-- 외부 데이터 소스 계층 -->
            <div class="layer external-layer">
                <div class="layer-title">🌐 External Data Sources</div>
                <div class="component-grid">
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🪙</span>
                        <div class="component-name">빗썸 API</div>
                        <div class="component-desc">실시간 암호화폐 가격 데이터<br>160만+ 메시지/일 수집</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📈</span>
                        <div class="component-name">Finnhub WebSocket</div>
                        <div class="component-desc">S&P 500 실시간 주식 데이터<br>50개 심볼 동적 구독</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🐦</span>
                        <div class="component-name">X (Twitter) API</div>
                        <div class="component-desc">금융 인플루언서 포스트<br>8개 계정 모니터링</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📊</span>
                        <div class="component-name">Alpha Vantage</div>
                        <div class="component-desc">시장 뉴스 및 감성 분석<br>50개 API 호출/일</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🏛️</span>
                        <div class="component-name">Truth Social</div>
                        <div class="component-desc">정치/금융 관련 포스트<br>Stanford Truthbrush 활용</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📰</span>
                        <div class="component-name">FRED API</div>
                        <div class="component-desc">경제 지표 데이터<br>배치 수집</div>
                    </div>
                </div>
            </div>

            <!-- 인프라 계층 -->
            <div class="layer infrastructure-layer">
                <div class="layer-title">🖥️ Physical Infrastructure</div>
                <div class="network-diagram">
                    <h3 style="text-align: center; margin-bottom: 20px;">5대 서버 K3s 클러스터</h3>
                    <div class="server-rack">
                        <div class="server master">
                            <div style="font-size: 2em;">🎯</div>
                            <div style="font-weight: bold;">Master</div>
                            <div style="font-size: 0.9em;">Control Plane</div>
                        </div>
                        <div class="server worker">
                            <div style="font-size: 2em;">⚡</div>
                            <div style="font-weight: bold;">Worker 1</div>
                            <div style="font-size: 0.9em;">Data Processing</div>
                        </div>
                        <div class="server worker">
                            <div style="font-size: 2em;">💾</div>
                            <div style="font-weight: bold;">Worker 2</div>
                            <div style="font-size: 0.9em;">Storage</div>
                        </div>
                        <div class="server worker">
                            <div style="font-size: 2em;">📊</div>
                            <div style="font-weight: bold;">Worker 3</div>
                            <div style="font-size: 0.9em;">Monitoring</div>
                        </div>
                        <div class="server worker">
                            <div style="font-size: 2em;">🔄</div>
                            <div style="font-weight: bold;">Worker 4</div>
                            <div style="font-size: 0.9em;">Application</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 데이터 플랫폼 계층 -->
            <div class="layer data-layer">
                <div class="layer-title">🗄️ Data Platform</div>
                <div class="component-grid">
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🐘</span>
                        <div class="component-name">PostgreSQL</div>
                        <div class="component-desc">투자 데이터 저장소<br>10개 테이블, 20Gi 스토리지</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🔴</span>
                        <div class="component-name">Redis</div>
                        <div class="component-desc">Airflow 백엔드 캐시<br>5Gi 메모리 스토리지</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📡</span>
                        <div class="component-name">Kafka Cluster</div>
                        <div class="component-desc">실시간 메시지 큐<br>2 Brokers + 1 Controller</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📁</span>
                        <div class="component-name">Git-Sync</div>
                        <div class="component-desc">DAG 자동 동기화<br>60초 간격 업데이트</div>
                    </div>
                </div>
            </div>

            <!-- 애플리케이션 계층 -->
            <div class="layer application-layer">
                <div class="layer-title">🚀 Application Services</div>
                <div class="component-grid">
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🌪️</span>
                        <div class="component-name">Airflow</div>
                        <div class="component-desc">워크플로우 오케스트레이션<br>15개 DAG 파이프라인</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📤</span>
                        <div class="component-name">Kafka Producers</div>
                        <div class="component-desc">실시간 데이터 수집<br>5개 Producer 서비스</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📥</span>
                        <div class="component-name">Kafka Consumers</div>
                        <div class="component-desc">데이터 처리 및 저장<br>127.3 msg/sec 처리율</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🔄</span>
                        <div class="component-name">Batch Processing</div>
                        <div class="component-desc">배치 데이터 수집<br>API Rate Limit 준수</div>
                    </div>
                </div>
            </div>

            <!-- 모니터링 계층 -->
            <div class="layer monitoring-layer">
                <div class="layer-title">👁️ Monitoring & Observability</div>
                <div class="component-grid">
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📊</span>
                        <div class="component-name">Prometheus</div>
                        <div class="component-desc">메트릭 수집 및 저장<br>클러스터 전체 모니터링</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📈</span>
                        <div class="component-name">Grafana</div>
                        <div class="component-desc">시각화 대시보드<br>실시간 성능 모니터링</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">🚨</span>
                        <div class="component-name">AlertManager</div>
                        <div class="component-desc">경고 및 알림 관리<br>장애 상황 즉시 대응</div>
                    </div>
                    <div class="component">
                        <div class="component-status"></div>
                        <span class="component-icon">📋</span>
                        <div class="component-name">로그 시스템</div>
                        <div class="component-desc">중앙화된 로그 관리<br>디버깅 및 추적</div>
                    </div>
                </div>
            </div>

            <!-- 데이터 플로우 -->
            <div class="layer">
                <div class="layer-title">🔄 Real-time Data Flow</div>
                <div class="data-flow">
                    <div class="flow-node">
                        <div style="font-size: 2em;">🌐</div>
                        <strong>External APIs</strong>
                        <div style="font-size: 0.9em;">7개 데이터 소스</div>
                    </div>
                    <div class="flow-arrow">→</div>
                    <div class="flow-node">
                        <div style="font-size: 2em;">🚀</div>
                        <strong>Producers</strong>
                        <div style="font-size: 0.9em;">실시간 수집</div>
                    </div>
                    <div class="flow-arrow">→</div>
                    <div class="flow-node">
                        <div style="font-size: 2em;">📡</div>
                        <strong>Kafka</strong>
                        <div style="font-size: 0.9em;">메시지 큐잉</div>
                    </div>
                    <div class="flow-arrow">→</div>
                    <div class="flow-node">
                        <div style="font-size: 2em;">⚡</div>
                        <strong>Consumers</strong>
                        <div style="font-size: 0.9em;">데이터 처리</div>
                    </div>
                    <div class="flow-arrow">→</div>
                    <div class="flow-node">
                        <div style="font-size: 2em;">🗄️</div>
                        <strong>PostgreSQL</strong>
                        <div style="font-size: 0.9em;">영구 저장</div>
                    </div>
                </div>
            </div>

            <!-- 성능 통계 -->
            <div class="stats-section">
                <h2 style="text-align: center; margin-bottom: 20px;">📊 실시간 성능 지표</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <span class="stat-number">28+</span>
                        <div class="stat-label">Running Pods</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">160만+</span>
                        <div class="stat-label">일일 메시지 처리</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">127.3</span>
                        <div class="stat-label">초당 메시지 처리</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">99.9%</span>
                        <div class="stat-label">시스템 가용성</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">503</span>
                        <div class="stat-label">S&P 500 기업</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">15</span>
                        <div class="stat-label">Airflow DAGs</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">10</span>
                        <div class="stat-label">데이터베이스 테이블</div>
                    </div>
                    <div class="stat-card">
                        <span class="stat-number">5</span>
                        <div class="stat-label">물리 서버</div>
                    </div>
                </div>
            </div>

            <!-- 기술 스택 -->
            <div class="layer">
                <div class="layer-title">🛠️ Technology Stack</div>
                <div class="tech-stack">
                    <div class="tech-item">
                        <strong>🎯 오케스트레이션</strong><br>
                        K3s (Kubernetes), Helm Charts
                    </div>
                    <div class="tech-item">
                        <strong>🗄️ 데이터 저장</strong><br>
                        PostgreSQL, Redis, Local-Path Storage
                    </div>
                    <div class="tech-item">
                        <strong>📡 실시간 스트리밍</strong><br>
                        Apache Kafka, Strimzi Operator
                    </div>
                    <div class="tech-item">
                        <strong>🌪️ 워크플로우</strong><br>
                        Apache Airflow, Git-Sync
                    </div>
                    <div class="tech-item">
                        <strong>📊 모니터링</strong><br>
                        Prometheus, Grafana, AlertManager
                    </div>
                    <div class="tech-item">
                        <strong>🔄 데이터 처리</strong><br>
                        Python, Kafka Producers/Consumers
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // 실시간 상태 업데이트 시뮬레이션
        function updateStats() {
            const statNumbers = document.querySelectorAll('.stat-number');
            statNumbers.forEach(stat => {
                if (stat.textContent.includes('127.3')) {
                    const currentValue = parseFloat(stat.textContent);
                    const variation = (Math.random() - 0.5) * 10;
                    const newValue = Math.max(100, currentValue + variation);
                    stat.textContent = newValue.toFixed(1);
                }
            });
        }

        // 컴포넌트 상태 표시기 애니메이션
        function animateStatusIndicators() {
            const statusIndicators = document.querySelectorAll('.component-status');
            statusIndicators.forEach((indicator, index) => {
                setTimeout(() => {
                    indicator.style.animation = 'pulse 2s infinite';
                }, index * 200);
            });
        }

        // 툴팁 기능
        document.querySelectorAll('.component').forEach(component => {
            component.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-8px) scale(1.02)';
                this.style.zIndex = '10';
            });
            
            component.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0) scale(1)';
                this.style.zIndex = '1';
            });
        });

        // 초기화
        document.addEventListener('DOMContentLoaded', function() {
            animateStatusIndicators();
            // setInterval(updateStats, 5000); // 5초마다 통계 업데이트
        });
    </script>
</body>
</html>