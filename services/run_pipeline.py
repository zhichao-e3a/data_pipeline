from __future__ import annotations

from core import states

from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector

from services.notifier import set_progress
from services.shared import check_cancel

from pipeline.query import query
from pipeline.filter import filter
from pipeline.process import process

import os
import time
import logging
import asyncio
import traceback
from typing import Callable

sql     = SQLDBConnector()
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

async def run_pipeline(

        job_id: str,
        data_origin: str

) -> None:

    steps       = 3
    curr        = 0
    total_time  = 0

    plog = Ctx(
        logger  = logger,
        extra   = {
            "job_id"        : job_id,
            "data_origin"   : data_origin
        }
    )
    plog.info(
        msg     = "pipeline_start",
    )

    with time_block(
            lambda ms: plog.info(
                msg     = "pipeline_end",
                extra   = {
                    "duration"   : ms
                }
            )
    ):
        try:
            ######################################## QUERY ########################################
            # Create logger for QUERY task
            tlog = Ctx(
                logger  = logger.getChild("query"),
                extra   = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "query",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"rows_queried" : None, "cols_queried" : None}
            with time_block(
                lambda ms: tlog.info(
                    msg     = "task_end",
                    extra   = {
                        "duration"      : ms,
                        "rows_queried"  : placeholder["rows_queried"],
                        "cols_queried"  : placeholder["cols_queried"]
                    }
                )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = ":material/database: QUERYING MYSQL DATABASE"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                metadata_1 = await query(
                    job_id  = job_id,
                    sql     = sql,
                    mongo   = mongo,
                    origin  = data_origin
                )

                rows_queried                = metadata_1['n_rows']
                cols_queried                = metadata_1['n_cols']
                placeholder["rows_queried"] = rows_queried
                placeholder["cols_queried"] = cols_queried

                if rows_queried == 0:

                    end = time.perf_counter()

                    message = f":material/done_outline: NO NEW ROWS"
                    set_progress(
                        job_id,
                        progress=None,
                        message=message,
                        state="None"
                    )

                    message = f":material/person_celebrate: PIPELINE FINISHED IN {end - start:.2f} s"
                    set_progress(
                        job_id,
                        progress=None,
                        message=message,
                        state="completed"
                    )

                    return

                end = time.perf_counter()
                curr += 1
                total_time += end-start

                message = f":material/done_outline: [{end-start:.2f} s] QUERIED {rows_queried} ROWS {cols_queried} COLS"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## FILTER ########################################
            # Create logger for FILTER task
            tlog = Ctx(
                logger = logger.getChild("filter"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "filter_rows",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {
                "rows_remaining"    : None,
                "cols_remaining"    : None,
                "rows_removed"      : None,
                "no_gest_age"       : None,
                "bad_measurements"  : None
            }
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"          : ms,
                            "rows_remaining"    : placeholder["rows_remaining"],
                            "cols_remaining"    : placeholder["cols_remaining"],
                            "rows_removed"      : placeholder["rows_removed"],
                            "no_gest_age"       : placeholder["no_gest_age"],
                            "bad_measurements"  : placeholder["bad_measurements"]
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

                metadata_2 = await filter(
                    job_id  = job_id,
                    mongo   = mongo,
                    origin  = data_origin,
                )

                rows_remaining = metadata_2["n_rows"]
                cols_remaining = metadata_2["n_cols"]
                bad_uc_fhr     = metadata_2["bad_uc_fhr"]
                no_gest_age    = metadata_2["no_gest_age"]
                rows_removed   = bad_uc_fhr + no_gest_age

                placeholder["rows_remaining"]   = rows_remaining
                placeholder["cols_remaining"]   = cols_remaining
                placeholder["bad_measurements"] = bad_uc_fhr
                placeholder["no_gest_age"]      = no_gest_age
                placeholder["rows_removed"]     = rows_removed

                message = f":material/priority_high: {bad_uc_fhr} ROWS FILTERED DUE TO BAD UC, FHR MEASUREMENTS"
                set_progress(
                    job_id,
                    progress=None,
                    message=message,
                    state="None"
                )

                message = f":material/priority_high: {no_gest_age} ROWS FILTERED DUE TO NO GESTATIONAL AGE"
                set_progress(
                    job_id,
                    progress=None,
                    message=message,
                    state="None"
                )

                if rows_remaining == 0:

                    end = time.perf_counter()

                    message = f":material/done_outline: NO ROWS AFTER FILTERING"
                    set_progress(
                        job_id,
                        progress = None,
                        message = message,
                        state = "None"
                    )

                    message = f":material/person_celebrate: PIPELINE FINISHED IN {end - start:.2f} s"
                    set_progress(
                        job_id,
                        progress = None,
                        message = message,
                        state = "completed"
                    )

                    return

                end = time.perf_counter()
                curr += 1
                total_time += end-start

                message = f":material/done_outline: [{end-start:.2f} s] {rows_remaining} ROWS {cols_remaining} COLS (FILTERED {rows_removed} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## PROCESS ########################################
            # Create logger for PROCESS task
            tlog = Ctx(
                logger = logger.getChild("processing"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "processing",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {
                "rows_remaining"    : None,
                "cols_remaining"    : None,
                "rows_removed"      : None
            }
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"          : ms,
                            "rows_remaining"    : placeholder["rows_remaining"],
                            "cols_remaining"    : placeholder["cols_remaining"],
                            "rows_removed"      : placeholder["rows_removed"]
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = ":material/conveyor_belt: PROCESSING UC, FHR SIGNALS"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                metadata_3 = await process(
                    job_id  = job_id,
                    mongo   = mongo,
                    origin  = data_origin
                )

                rows_remaining  = metadata_3["n_rows"]
                cols_remaining  = metadata_3["n_cols"]
                rows_removed    = metadata_3["rows_skipped"]

                placeholder["rows_remaining"]   = rows_remaining
                placeholder["cols_remaining"]   = cols_remaining
                placeholder["rows_removed"]     = rows_removed

                if rows_remaining == 0:
                    end = time.perf_counter()

                    message = f":material/done_outline: NO ROWS AFTER PROCESSING"
                    set_progress(
                        job_id,
                        progress = None,
                        message = message,
                        state = "None"
                    )

                    message = f":material/person_celebrate: PIPELINE FINISHED IN {end - start:.2f} s"
                    set_progress(
                        job_id,
                        progress = None,
                        message = message,
                        state = "completed"
                    )

                    return

                end = time.perf_counter()
                curr += 1
                total_time += end - start

                message = f":material/done_outline: [{end-start:.2f} s] {rows_remaining} ROWS {cols_remaining} COLS (FILTERED {rows_removed} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )

            message = f":material/done_outline: PIPELINE FINISHED IN {total_time:.2f} s"
            set_progress(
                job_id,
                progress = None,
                message = message,
                state   = "completed"
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
                state="failed"
            )
