from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

# FastAPI 앱 인스턴스 생성
app = FastAPI(
    title="Investment Assistant API",
    description="실시간 투자 데이터 수집 및 분석 API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 개발용: 모든 오리진 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 환경변수 확인
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "Investment Assistant API",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """헬스체크 엔드포인트"""
    return {
        "status": "healthy",
        "database_url": DATABASE_URL[:20] + "..." if DATABASE_URL else "Not configured",
        "redis_url": REDIS_URL[:20] + "..." if REDIS_URL else "Not configured"
    }

@app.get("/test")
async def test_endpoint():
    """테스트 엔드포인트"""
    return {
        "message": "FastAPI server is working!",
        "git_sync": "v4 successful",
        "path": "/app/backend-api/backend-api"
    }

# 추가 라우터들은 여기에 포함
# from .routers import crypto, stocks, news
# app.include_router(crypto.router, prefix="/api/v1/crypto", tags=["crypto"])
# app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["stocks"])
# app.include_router(news.router, prefix="/api/v1/news", tags=["news"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)