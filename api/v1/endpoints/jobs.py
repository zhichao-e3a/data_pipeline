from core import states
from schemas.pipeline import ResponseModel
from services.run_pipeline import run_pipeline
from services.run_consolidate import run_consolidate

import uuid
from fastapi import APIRouter, BackgroundTasks

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.post(
    path="/pipeline_job/{data_origin}",
    response_model=ResponseModel,
    status_code=202
)
async def pipeline_job(background_tasks: BackgroundTasks, data_origin: str):

    job_id = uuid.uuid4().hex

    states.PROGRESS[job_id] = {
        "progress": 0,
        "state": "running",
        "message": "Starting…"
    }

    background_tasks.add_task(
        run_pipeline,
        job_id,
        data_origin
    )

    return {"job_id": job_id}

@router.post(
    path="/consolidate_job",
    response_model=ResponseModel,
    status_code=202
)
async def consolidate_job(background_tasks: BackgroundTasks):

    job_id = uuid.uuid4().hex

    states.PROGRESS[job_id] = {
        "progress": 0,
        "state": "running",
        "message": "Starting…"
    }

    background_tasks.add_task(
        run_consolidate,
        job_id
    )

    return {"job_id": job_id}

@router.post("/cancel_job/{job_id}")
async def cancel_job(job_id: str):

    states.CANCELLED.add(job_id)

    return {"job_id": job_id, "status": "cancelling"}
