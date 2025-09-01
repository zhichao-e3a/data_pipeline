from api.v1.endpoints import jobs, ws
from fastapi import APIRouter

api_router = APIRouter(
    prefix="/v1",
    tags=["v1"]
)

api_router.include_router(jobs.router)

api_router.include_router(ws.router)
