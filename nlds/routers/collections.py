from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get():
    return {}

@router.put("/")
async def put():
    return {}

@router.post("/")
async def post():
    return {}

@router.delete("/")
async def delete():
    return {}
