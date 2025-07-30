backend-api/
├── app/
│   ├── __init__.py
│   ├── main.py                 # 🚀 FastAPI 앱 진입점
│   ├── config.py               # ⚙️ 설정 관리 (DB, Redis 연결 등)
│   ├── database.py             # 🗄️ 데이터베이스 연결 설정
│   ├── dependencies.py         # 🔗 의존성 주입 (DB 세션 등)
│   │
│   ├── models/                 # 📊 데이터베이스 모델 (SQLAlchemy ORM)
│   │   ├── __init__.py
│   │   ├── base.py             # 기본 모델 클래스
│   │   ├── crypto.py           # 암호화폐 모델
│   │   ├── stocks.py           # 주식 모델
│   │   ├── news.py             # 뉴스 모델
│   │   └── market.py           # 시장 데이터 모델
│   │
│   ├── schemas/                # 📝 Pydantic 스키마 (API 입출력 검증)
│   │   ├── __init__.py
│   │   ├── crypto.py           # 암호화폐 API 스키마
│   │   ├── stocks.py           # 주식 API 스키마
│   │   ├── news.py             # 뉴스 API 스키마
│   │   └── common.py           # 공통 스키마 (응답 포맷 등)
│   │
│   ├── routers/                # 🛣️ API 라우터 (엔드포인트 그룹화)
│   │   ├── __init__.py
│   │   ├── crypto.py           # /api/v1/crypto/* 엔드포인트
│   │   ├── stocks.py           # /api/v1/stocks/* 엔드포인트
│   │   ├── news.py             # /api/v1/news/* 엔드포인트
│   │   ├── market.py           # /api/v1/market/* 엔드포인트
│   │   └── health.py           # /health, /status 등 시스템 엔드포인트
│   │
│   ├── services/               # 🧠 비즈니스 로직 (데이터 처리, 계산)
│   │   ├── __init__.py
│   │   ├── crypto_service.py   # 암호화폐 데이터 처리 로직
│   │   ├── stocks_service.py   # 주식 데이터 분석 로직
│   │   ├── news_service.py     # 뉴스 감성 분석, 필터링
│   │   └── analytics_service.py # 투자 분석, 지표 계산
│   │
│   ├── utils/                  # 🔧 유틸리티 함수
│   │   ├── __init__.py
│   │   ├── cache.py            # Redis 캐싱 유틸
│   │   ├── datetime_helper.py  # 시간 처리 유틸
│   │   ├── validators.py       # 데이터 검증 함수
│   │   └── formatters.py       # 데이터 포맷 변환
│   │
│   └── core/                   # 🔐 핵심 설정 (보안, 미들웨어)
│       ├── __init__.py
│       ├── security.py         # JWT 인증, API 키 관리
│       ├── middleware.py       # CORS, 로깅 미들웨어
│       └── exceptions.py       # 커스텀 예외 처리
│
├── tests/                      # 🧪 테스트 코드
│   ├── __init__.py
│   ├── test_crypto.py
│   ├── test_stocks.py
│   └── conftest.py             # 테스트 설정
│
├── requirements.txt            # 📦 Python 패키지 의존성
├── Dockerfile                  # 🐳 Docker 이미지 빌드
├── .env.example               # 🔐 환경변수 예시
└── README.md                  # 📖 API 문서