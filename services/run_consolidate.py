from __future__ import annotations

from core import states

from database.MongoDBConnector import MongoDBConnector

from pipeline.model_raw import model_raw
from pipeline.model_raw_excl import model_raw_excl

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

    steps       = 2
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
                "rec_patients"          : None,
                "hist_patients"         : None,
                "valid_patients"        : None,
                "valid_measurements"    : None,
                "no_onset"              : None,
                "c_sections"            : None
            }
            with time_block(
                lambda ms: tlog.info(
                    msg     = "task_end",
                    extra   = {
                        "duration"            : ms,
                        "rec_patients"        : placeholder["rec_patients"],
                        "hist_patients"       : placeholder["hist_patients"],
                        "valid_patients"      : placeholder["valid_patients"],
                        "valid_measurements"  : placeholder["valid_measurements"],
                        "no_onset"            : placeholder["no_onset"],
                        "c_sections"          : placeholder["c_sections"]
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

                rec_patients        = metadata_1["rec_patients"]
                hist_patients       = metadata_1["hist_patients"]
                valid_patients      = metadata_1["valid_patients"]
                valid_measurements  = metadata_1["valid_measurements"]
                no_onset            = metadata_1["no_onset"]
                c_sections          = metadata_1["c_sections"]

                placeholder["rec_patients"]         = rec_patients
                placeholder["hist_patients"]        = hist_patients
                placeholder["valid_patients"]       = valid_patients
                placeholder["valid_measurements"]   = valid_measurements
                placeholder["no_onset"]             = no_onset
                placeholder["c_sections"]           = c_sections

                curr += 1
                end = time.perf_counter()
                total_time += end-start

                message = f":material/groups: {rec_patients} RECRUITED PATIENTS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/groups: {hist_patients} HISTORICAL PATIENTS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/sentiment_satisfied: {valid_patients} VALID PATIENTS (ONSET, NON C-SECTIONS)"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/person_alert: SKIPPED {no_onset} DUE TO NO ONSET DATE"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/person_alert: SKIPPED {c_sections} DUE TO C-SECTION"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )
            ######################################## FILTER ########################################
            tlog = Ctx(
                logger = logger.getChild("filter"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : "all",
                    "task"          : "filter",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {
                "rows_remaining": None,
                "rows_removed": None,
            }
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"          : ms,
                            "rows_remaining"    : placeholder["rows_remaining"],
                            "rows_removed"      : placeholder["rows_removed"],
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = ":material/conveyor_belt: FILTERING MEASUREMENTS"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                metadata_2 = await model_raw_excl(
                    mongo = mongo
                )

                rows_remaining  = metadata_2["n_rows"]
                rows_removed    = metadata_2["rows_skipped"]

                placeholder["rows_remaining"]   = rows_remaining
                placeholder["rows_removed"]     = rows_removed

                end = time.perf_counter()
                curr += 1
                total_time += end-start

                message = f":material/done_outline: [{end-start:.2f} s] {rows_remaining} ROWS (FILTERED {rows_removed} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
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

            # watermark_log = log_watermark(
            #     pipeline_name=data_origin,
            #     utime=latest_utime,
            #     job_id=job_id,
            # )
            #
            # await mongo.upsert_records_hashed([watermark_log], "watermarks")
