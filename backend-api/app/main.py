# 주석 추가

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import logging.config
import time
import json

from .config import settings, get_log_config
from .database import test_db_connection
from .dependencies import verify_db_connection

# 로깅 설정
logging.config.dictConfig(get_log_config())
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"{settings.app_name} v{settings.app_version} 시작 중...")
    if test_db_connection():
        logger.info("✅ 데이터베이스 연결 성공")
    else:
        logger.error("❌ 데이터베이스 연결 실패")
    logger.info(f"🚀 서버가 http://{settings.host}:{settings.port} 에서 실행 중")
    logger.info(f"📚 API 문서: http://{settings.host}:{settings.port}/docs")
    
    yield
    
    # Shutdown
    logger.info("🛑 애플리케이션 종료 중...")
    logger.info("✅ 정리 작업 완료")

# FastAPI 애플리케이션 생성
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="투자 도우미 서비스의 데이터 API",
    docs_url="/docs",          # API 문서 경로: http://localhost:8888/docs
    redoc_url="/redoc",        # ReDoc 문서 경로: http://localhost:8888/redoc
    openapi_url="/openapi.json",  # OpenAPI 스키마 경로
    lifespan=lifespan
)

# 상세 API 로깅 미들웨어
@app.middleware("http")
async def detailed_logging_middleware(request: Request, call_next):
    """
    상세한 API 요청/응답 로깅 미들웨어
    """
    start_time = time.time()
    
    # 요청 정보 수집
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    method = request.method
    url = str(request.url)
    query_params = dict(request.query_params)
    
    # 요청 로깅
    logger.info(f"📥 {method} {url} - IP: {client_ip}")
    if query_params:
        logger.info(f"   Query params: {json.dumps(query_params, ensure_ascii=False)}")
    if user_agent != "unknown":
        logger.info(f"   User-Agent: {user_agent}")
    
    # 요청 처리
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # 응답 로깅
        if response.status_code >= 400:
            logger.warning(f"❌ {method} {url} - {response.status_code} ({process_time:.3f}s)")
            if response.status_code == 404:
                logger.warning(f"   🔍 404 상세: 경로 '{request.url.path}'를 찾을 수 없음")
                logger.warning(f"   💡 사용 가능한 경로 확인: {url.split('?')[0].replace(request.url.path, '')}/docs")
        else:
            logger.info(f"✅ {method} {url} - {response.status_code} ({process_time:.3f}s)")
            
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"💥 {method} {url} - ERROR ({process_time:.3f}s): {str(e)}")
        raise

# CORS 미들웨어 설정
# 프론트엔드(React, Django)에서 API 호출할 수 있도록 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,  # config.py에서 설정한 허용 오리진
    allow_credentials=True,                  # 쿠키, 인증 헤더 허용
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # 허용할 HTTP 메서드
    allow_headers=["*"],                     # 모든 헤더 허용
)



# 루트 엔드포인트
@app.get("/", tags=["Root"])
async def root():
    """
    루트 경로 - API 기본 정보 제공
    
    브라우저에서 http://localhost:8888/ 접속 시 보여지는 페이지
    """
    return {
        "message": f"Welcome to {settings.app_name}!",
        "version": settings.app_version,
        "docs": "/docs",
        "redoc": "/redoc",
        "status": "running"
    }

# 헬스체크 엔드포인트
@app.get("/health", tags=["Health"])
async def health_check():
    """
    헬스체크 - 서비스 상태 확인
    
    로드밸런서나 모니터링 시스템에서 서비스 상태를 확인할 때 사용
    데이터베이스 연결 상태도 함께 체크
    """
    db_status = "connected" if test_db_connection() else "disconnected"
    
    return {
        "status": "healthy" if db_status == "connected" else "unhealthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": db_status,
        "debug_mode": settings.debug
    }

# 상세 헬스체크 엔드포인트 (DB 연결 필수)
@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check(_: None = Depends(verify_db_connection)):
    """
    상세 헬스체크 - 모든 의존성 서비스 상태 확인
    
    데이터베이스 연결이 실패하면 503 에러를 반환합니다.
    중요한 서비스에서 사용하기 전에 호출하여 상태를 확인할 수 있습니다.
    """
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": "connected",
        "services": {
            "postgresql": "connected",
            "redis": "not_implemented",  # 나중에 Redis 상태 체크 추가
        },
        "debug_mode": settings.debug
    }

# API 라우터 등록
from .api.api_v1 import api_router
app.include_router(api_router, prefix=settings.api_v1_prefix)

# 전역 예외 처리기
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """
    전역 예외 처리기
    
    예상하지 못한 에러가 발생했을 때 일관된 에러 응답을 제공합니다.
    운영 환경에서는 에러 상세 정보를 숨기고, 개발 환경에서는 표시합니다.
    """
    logger.error(f"예상하지 못한 에러 발생: {str(exc)}", exc_info=True)
    
    if settings.debug:
        # 개발 환경: 상세 에러 정보 제공
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": str(exc),
                "type": type(exc).__name__
            }
        )
    else:
        # 운영 환경: 간단한 에러 메시지만 제공
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": "서버에 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
            }
        )

# 개발 환경에서만 사용할 디버그 정보
if settings.debug:
    @app.get("/debug/info", tags=["Debug"])
    async def debug_info():
        """
        디버그 정보 - 개발 환경에서만 사용
        
        현재 설정값들을 확인할 수 있습니다.
        운영 환경에서는 보안상 비활성화됩니다.
        """
        return {
            "settings": {
                "db_host": settings.db_host,
                "db_port": settings.db_port,
                "db_name": settings.db_name,
                "redis_host": settings.redis_host,
                "redis_port": settings.redis_port,
                "debug": settings.debug,
                "log_level": settings.log_level
            },
            "database_url": settings.database_url.replace(settings.db_password, "***"),  # 비밀번호 숨김
            "allowed_origins": settings.allowed_origins
        }