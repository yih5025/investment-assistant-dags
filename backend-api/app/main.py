# main.py 수정된 CORS 설정

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
    
    # WebSocket 서비스들 초기화
    try:
        from .api.endpoints.websocket_endpoint import initialize_websocket_services
        await initialize_websocket_services()
        logger.info("✅ WebSocket 서비스 초기화 완료")
    except Exception as e:
        logger.error(f"❌ WebSocket 서비스 초기화 실패: {e}")
    
    logger.info(f"🚀 서버가 http://{settings.host}:{settings.port} 에서 실행 중 입니다....")
    logger.info(f"📚 API 문서: http://{settings.host}:{settings.port}/docs")
    
    yield
    
    # Shutdown
    logger.info("🛑 애플리케이션 종료 중...")
    try:
        from .api.endpoints.websocket_endpoint import shutdown_websocket_services
        await shutdown_websocket_services()
        logger.info("✅ WebSocket 서비스 종료 완료")
    except Exception as e:
        logger.error(f"❌ WebSocket 서비스 종료 실패: {e}")
    logger.info("✅ 정리 작업 완료")

# FastAPI 애플리케이션 생성
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="투자 도우미 서비스의 데이터 API",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# CORS 디버깅을 위한 미들웨어 (CORS 설정 전에 배치)
@app.middleware("http")
async def cors_debug_middleware(request: Request, call_next):
    """CORS 요청 디버깅을 위한 미들웨어"""
    
    # 요청 정보 로깅
    origin = request.headers.get("origin")
    method = request.method
    
    if origin:
        logger.info(f"🌍 CORS 요청 감지: {method} {request.url} from {origin}")
        logger.info(f"📋 요청 헤더: {dict(request.headers)}")
    
    # OPTIONS 요청 특별 처리 (Preflight)
    if method == "OPTIONS":
        logger.info("✈️ Preflight 요청 처리 중...")
        
        # 수동으로 CORS 헤더 설정
        response = JSONResponse(
            content={"message": "OK"},
            status_code=200
        )
        
        # 모든 Vercel 도메인 허용
        if origin and ".vercel.app" in origin:
            response.headers["Access-Control-Allow-Origin"] = origin
        elif origin and origin in [
            "https://investment-assistant.site",
            "http://localhost:30333",
            "http://127.0.0.1:30333"
        ]:
            response.headers["Access-Control-Allow-Origin"] = origin
        else:
            response.headers["Access-Control-Allow-Origin"] = "*"
            
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Allow-Credentials"] = "false"
        response.headers["Access-Control-Max-Age"] = "86400"
        
        logger.info(f"✅ Preflight 응답: {dict(response.headers)}")
        return response
    
    # 일반 요청 처리
    response = await call_next(request)
    
    # 응답에 CORS 헤더 추가
    if origin:
        if ".vercel.app" in origin:
            response.headers["Access-Control-Allow-Origin"] = origin
        elif origin in [
            "https://investment-assistant.site",
            "http://localhost:30333", 
            "http://127.0.0.1:30333"
        ]:
            response.headers["Access-Control-Allow-Origin"] = origin
        else:
            response.headers["Access-Control-Allow-Origin"] = "*"
            
        response.headers["Access-Control-Allow-Credentials"] = "false"
        logger.info(f"✅ CORS 응답 헤더 추가됨: Origin={response.headers.get('Access-Control-Allow-Origin')}")
    
    return response

# 상세 API 로깅 미들웨어 (기존 코드와 동일)
@app.middleware("http")
async def detailed_logging_middleware(request: Request, call_next):
    """상세한 API 요청/응답 로깅 미들웨어"""
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

# 기존 CORS 미들웨어 제거하고 새로운 설정 적용
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://wei-service.vercel.app",
        "https://wei-service-git-main-ilhan-yus-projects.vercel.app",
        "https://wei-service-hxigyrhwl-ilhan-yus-projects.vercel.app",
        "https://investment-assistant.site",
        "http://localhost:30333",
        "http://127.0.0.1:30333",
        "http://localhost:3000",
        "http://localhost:5173"
    ],
    allow_origin_regex=r"^https://.*\.vercel\.app$",  # 모든 Vercel 도메인 허용
    allow_credentials=False,  # credentials 비활성화로 CORS 단순화
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# 루트 엔드포인트 (기존과 동일)
@app.get("/", tags=["Root"])
async def root():
    """루트 경로 - API 기본 정보 제공"""
    return {
        "message": f"Welcome to {settings.app_name}!",
        "version": settings.app_version,
        "docs": "/docs",
        "redoc": "/redoc",
        "status": "running",
        "cors_enabled": True,  # CORS 활성화 표시
        "allowed_origins": [
            "https://wei-service.vercel.app",
            "https://wei-service-git-main-ilhan-yus-projects.vercel.app",
            "https://wei-service-hxigyrhwl-ilhan-yus-projects.vercel.app",
            "https://investment-assistant.site"
        ],
        "uvicorn_reload_test": "SUCCESS 3",
        "timestamp": "2025-08-14 15:07:34"
    }

# 헬스체크 엔드포인트 (기존과 동일)
@app.get("/health", tags=["Health"])
async def health_check():
    """헬스체크 - 서비스 상태 확인"""
    db_status = "connected" if test_db_connection() else "disconnected"
    
    return {
        "status": "healthy" if db_status == "connected" else "unhealthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": db_status,
        "debug_mode": settings.debug,
        "cors_status": "enabled"
    }

# CORS 테스트 전용 엔드포인트 추가
@app.get("/cors-test", tags=["Debug"])
async def cors_test(request: Request):
    """CORS 테스트 전용 엔드포인트"""
    origin = request.headers.get("origin", "No Origin")
    user_agent = request.headers.get("user-agent", "No User-Agent")
    
    return {
        "message": "CORS 테스트 성공!",
        "origin": origin,
        "user_agent": user_agent,
        "headers": dict(request.headers),
        "timestamp": time.time()
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