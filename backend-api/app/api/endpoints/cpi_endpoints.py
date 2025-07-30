from fastapi import APIRouter

router = APIRouter()

@router.get("/cpi")
async def get_cpi():
    return {"message": "Hello, World!"}