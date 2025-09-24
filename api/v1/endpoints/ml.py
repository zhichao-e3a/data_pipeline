from services.run_streaming import run_streaming

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/ml", tags=["jobs"])

@router.get(
    path="/stream_data",
    status_code=200
)
async def stream_data(
        coll: str = Query(alias="coll")
):

    return StreamingResponse(
        run_streaming(coll),
        media_type="application/vnd.apache.arrow.stream"
    )