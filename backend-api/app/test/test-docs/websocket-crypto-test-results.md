# WebSocket Crypto 테스트 결과

## 테스트 일시
2025-01-04 01:15:00 (최종 수정)

## 문제 발생 및 해결

### 1. 초기 문제: HTTP 404 에러
- **문제**: WebSocket 엔드포인트가 API 라우터에 등록되지 않음
- **에러**: `server rejected WebSocket connection: HTTP 404`
- **해결**: `app/api/api_v1.py`에 WebSocket 엔드포인트 추가

```python
# 추가된 라우터 설정
{
    "router": websocket_endpoint.router,
    "prefix": "",  # WebSocket은 이미 "/ws" prefix를 가지고 있음
    "tags": ["websocket"],
    "description": "실시간 WebSocket 데이터 API"
}
```

### 2. FastAPI Pod CrashLoopBackOff
- **문제**: SQLAlchemy 테이블 중복 정의 에러
- **에러**: `Table 'top_gainers' is already defined for this MetaData instance`
- **원인**: 
  - `top_gainers_model.py`와 `company_news_model.py`에서 같은 테이블명 중복 정의
  - SQLAlchemy MetaData 인스턴스에서 중복 등록 발생

- **해결책**:
  1. `top_gainers_model.py`에 `__table_args__ = {'extend_existing': True}` 추가
  2. `company_news_model.py`에서 중복된 `TopGainers` 클래스 제거

### 3. Pydantic v2 호환성
- **문제**: `json()` 메서드 deprecated 경고
- **해결**: `model_dump_json()` 메서드로 변경

## WebSocket 엔드포인트 구조

### 사용 가능한 엔드포인트
1. **전체 암호화폐**: `ws://localhost:30888/ws/crypto`
2. **특정 암호화폐**: `ws://localhost:30888/ws/crypto/{symbol}` (예: KRW-BTC)
3. **전체 Top Gainers**: `ws://localhost:30888/ws/stocks/topgainers`
4. **특정 주식**: `ws://localhost:30888/ws/stocks/topgainers/{symbol}`
5. **전체 SP500**: `ws://localhost:30888/ws/stocks/sp500`
6. **특정 SP500**: `ws://localhost:30888/ws/stocks/sp500/{symbol}`
7. **대시보드**: `ws://localhost:30888/ws/dashboard`

### HTTP 상태 엔드포인트
- **상태 조회**: `GET /ws/status`
- **통계 조회**: `GET /ws/stats`

## 다음 단계
1. Pod 재시작 대기
2. WebSocket 연결 재테스트
3. 실시간 데이터 스트리밍 확인

## 테스트 클라이언트 코드
테스트 클라이언트는 `websocket-crypto-test.py`에 저장됨
- asyncio + websockets 라이브러리 사용
- 연결 상태 확인, 실시간 데이터 수신, 하트비트 처리 포함