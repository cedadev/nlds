from fastapi import APIRouter
from .routing_methods import rabbit_publish_response

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
