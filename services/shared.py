from core import states

import asyncio
from datetime import datetime

def check_cancel(
        job_id: str
) -> None:

    if job_id in states.CANCELLED:
        raise asyncio.CancelledError(f"Pipeline {job_id} was cancelled")

def log_watermark(
        pipeline_name   : str,
        utime           : str,
        job_id          : str,
) -> dict[str, str]:

    log = {

        "pipeline_name" : pipeline_name,
        "last_utime"    : utime,
        "last_job_id"   : job_id,
        "time"          : datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
    }

    return log