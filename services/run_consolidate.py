from __future__ import annotations

from core import states

from database.MongoDBConnector import MongoDBConnector

from pipeline.model_raw import model_raw

from services.notifier import set_progress
from services.shared import check_cancel

import os
import time
import logging
import asyncio
import traceback
from typing import Callable

mongo   = MongoDBConnector(mode=os.getenv("MODE"))
logger  = logging.getLogger(__name__)

class Ctx(logging.LoggerAdapter):

    def process(self, msg, kwargs):

        extra = kwargs.get("extra", {})

        for k, v in self.extra.items():
            extra.setdefault(k, v)

        kwargs["extra"] = extra

        return msg, kwargs

class time_block:

    def __init__(
        self,
        emit: Callable[[float], None]
    ):
        self.emit = emit

    # Context management (Enter)
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    # Context management (Exit)
    def __exit__(self, exc_type, exc, tb):
        elapsed = time.perf_counter()-self.t0
        self.emit(round(elapsed, 2))
        return False

async def run_consolidate(
        job_id: str
) -> None:

    curr        = 0
    total_time  = 0

    plog = Ctx(
        logger  = logger,
        extra   = {
            "job_id"        : job_id,
            "data_origin"   : "all"
        }
    )
    plog.info(
        msg = "consolidate_start"
    )

    with time_block(
            lambda ms: plog.info(
                msg     = "consolidate_end",
                extra   = {
                    "duration"   : ms
                }
            )
    ):
        try:
            ######################################## QUERY ########################################
            tlog = Ctx(
                logger  = logger.getChild("query"),
                extra   = {
                    "job_id"        : job_id,
                    "data_origin"   : "all",
                    "task"          : "query",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {
                "n_onset"   : None,
                "n_add"     : None
            }
            with time_block(
                lambda ms: tlog.info(
                    msg     = "task_end",
                    extra   = {
                        "duration"  : ms,
                        "n_onset"   : placeholder["n_onset"],
                        "n_add"     : placeholder["n_add"]
                    }
                )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = ":material/database: QUERYING MONGODB"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                metadata_1 = await model_raw(
                    mongo = mongo
                )

                n_onset = metadata_1["n_onset"]
                n_add   = metadata_1["n_add"]

                placeholder["n_onset"]  = n_onset
                placeholder["n_add"]    = n_add

                curr += 1
                end = time.perf_counter()
                total_time += end-start

                message = f":material/groups: {n_onset} MEASUREMENTS WITH ONSET"
                set_progress(
                    job_id,
                    progress=None,
                    message=message,
                    state=None
                )

                message = f":material/groups: {n_add} MEASUREMENTS WITH ADD"
                set_progress(
                    job_id,
                    progress=None,
                    message=message,
                    state=None
                )

            message = f":material/done_outline: PIPELINE FINISHED IN {total_time:.2f} s"
            set_progress(
                job_id,
                progress=None,
                message=message,
                state="completed"
            )

        except asyncio.CancelledError:

            plog.exception(
                msg = "pipeline_cancelled",
                extra = {
                    "cancelled" : True
                }
            )

            states.CANCELLED.discard(job_id)

        except Exception as e:

            plog.exception(
                msg="pipeline_failed",
                extra={
                    "error": ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                }
            )

            message = f":material/error: Pipeline encountered an error\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}"
            set_progress(
                job_id,
                message = message,
                state = "failed"
            )
