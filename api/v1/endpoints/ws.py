from services.notifier import ws_status
from fastapi import APIRouter, WebSocket

router = APIRouter(prefix="/ws", tags=["ws"])

@router.websocket("/status/{job_id}")
async def ws_status_route(websocket: WebSocket, job_id: str):
    await ws_status(websocket, job_id)
