from __future__ import annotations

from libcst.testing.utils import data_provider

from core import states

from database.queries import HISTORICAL, RECRUITED
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector

from utils.data_download import async_process_urls
from utils.data_process import process_data
from utils.signal_processing import process_signals

from services.notifier import set_progress
from services.shared import check_cancel, log_watermark

import time
import logging
import anyio
import asyncio
import traceback
import pandas as pd
from datetime import datetime
from typing import Callable

db      = SQLDBConnector()
mongo   = MongoDBConnector(remote=True)
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

    steps       = 6
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
        extra   = {
            "status" : "start"
        }
    )

    with time_block(
            lambda ms: plog.info(
                msg     = "pipeline_end",
                extra   = {
                    "status"     : "end",
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
                    "data_origin"   : data_origin,
                    "task"          : "query",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"rows" : None, "cols" : None}
            with time_block(
                lambda ms: tlog.info(
                    msg     = "task_end",
                    extra   = {
                        "duration"  : ms,
                        "rows"      : placeholder["rows"],
                        "cols"      : placeholder["cols"]
                    }
                )
            ):
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    progress = None,
                    message = ":material/database: QUERYING MYSQL DATABASE",
                    state = None
                )

                curr_watermark = await mongo.get_all_documents(
                    coll_name = "watermarks",
                    query = {
                        "_id" : {
                            "$eq" : data_origin
                        },
                    }
                )

                last_utime = curr_watermark[0]["last_utime"]

                # Extract Recruited data from MySQL + MongoDB
                if data_origin == "rec":

                    consolidated_patients = await mongo.get_all_documents("patients_unified")
                    recruited_patients = [
                        i for i in consolidated_patients if i["recruitment_type"] == "recruited"
                    ]

                    # Get string of numbers for Recruited patients
                    query_string = ",".join(
                        [
                            f"'{i["patient_id"]}'" for i in recruited_patients
                        ]
                    )

                    df = await anyio.to_thread.run_sync(
                        lambda: db.query_to_dataframe(
                            sql=RECRUITED.format(
                                start="'2025-03-01 00:00:00'",
                                end=f"'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'",
                                numbers=query_string,
                                last_utime=last_utime
                            )
                        )
                    )

                    placeholder["rows"] = len(df)
                    placeholder["cols"] = len(df.columns)

                    # If no rows queried
                    if len(df) == 0:
                        end = time.perf_counter()
                        set_progress(
                            job_id,
                            progress = None,
                            message =f":material/done_outline: NO NEW ROWS",
                            state = "None"
                        )
                        set_progress(
                            job_id,
                            progress = None,
                            message = f":material/person_celebrate: PIPELINE FINISHED IN {end-start:.2f} s",
                            state = "completed"
                        )
                        return

                    last_menstrual      = {
                        i["patient_id"]: i["last_menstrual_period"] for i in recruited_patients
                    }

                    expected_delivery   = {
                        i["patient_id"]: i["estimated_delivery_date"] for i in recruited_patients
                    }

                    actual_delivery     = {
                        i["patient_id"]: i["delivery_datetime"] for i in recruited_patients
                    }

                else:
                    df = await anyio.to_thread.run_sync(
                        lambda: db.query_to_dataframe(
                            sql=HISTORICAL.format(
                                last_utime=last_utime
                            )
                        )
                    )

                    placeholder["rows"] = len(df)
                    placeholder["cols"] = len(df.columns)

                    if len(df) == 0:
                        end = time.perf_counter()
                        set_progress(
                            job_id,
                            progress = None,
                            message = f":material/done_outline: NO NEW ROWS",
                            state = None
                        )
                        set_progress(
                            job_id,
                            progress = None,
                            message = f":material/done_outline: PIPELINE FINISHED IN {end-start:.2f} s",
                            state = "completed"
                        )
                        return

                latest_utime = pd.to_datetime(df["utime"])\
                    .max().strftime("%Y-%m-%d %H:%M:%S")

                watermark_log = log_watermark(
                    pipeline_name=data_origin,
                    utime=latest_utime,
                    job_id=job_id,
                )

                await mongo.upsert_records_hashed([watermark_log], "watermarks")

                end = time.perf_counter()
                curr += 1
                total_time += end-start
                message = f":material/done_outline: [{end-start:.2f} s] QUERIED {len(df)} ROWS {len(df.columns)} COLS"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## DOWNLOAD ########################################
            tlog = Ctx(
                logger = logger.getChild("download"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "download",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"n_links" : None}
            with time_block(
                    lambda ms: tlog.info(
                        msg     = "task_end",
                        extra   = {
                            "duration"  : ms,
                            "n_links"   : placeholder["n_links"]
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    progress = None,
                    message = ":material/cloud_download: DOWNLOADING UC, FHR LINKS",
                    state = None
                )

                uc_results, fhr_results = await async_process_urls(df)
                placeholder["n_links"] = len(uc_results)*2

                end = time.perf_counter()
                curr += 1
                total_time += end-start
                message = f":material/done_outline: [{end-start:.2f} s] DOWNLOADED {len(uc_results)*2} LINKS"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## FILTER ########################################
            tlog = Ctx(
                logger = logger.getChild("filter"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "filter",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"rows" : None, "cols" : None}
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"  : ms,
                            "rows"      : placeholder["rows"],
                            "cols"      : placeholder["cols"]
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    progress = None,
                    message = ":material/conveyor_belt: FILTERING MEASUREMENTS",
                    state = None
                )

                async with states.PROCESS_SEM:
                    if data_origin == "rec":
                        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(
                            lambda: process_data(
                                df,
                                uc_results,
                                fhr_results,
                                data_origin,
                                last_menstrual=last_menstrual,
                                expected_delivery=expected_delivery,
                                actual_delivery=actual_delivery
                            )
                        )
                    else:
                        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(
                            lambda: process_data(
                                df,
                                uc_results,
                                fhr_results,
                                data_origin
                            )
                        )

                    placeholder["rows"] = len(processed_list_1)
                    placeholder["cols"] = len(processed_list_1[0])

                if len(processed_list_1) == 0:
                    end = time.perf_counter()
                    set_progress(
                        job_id,
                        progress = None,
                        message = f":material/done_outline: NO ROWS AFTER FILTERING",
                        state = "None"
                    )
                    set_progress(
                        job_id,
                        progress = None,
                        message = f":material/person_celebrate: PIPELINE FINISHED IN {end - start:.2f} s",
                        state = "completed"
                    )
                    return

                end = time.perf_counter()
                curr += 1
                total_time += end-start
                message = f":material/done_outline: [{end-start:.2f} s] {len(processed_list_1)} ROWS {len(processed_list_1[0])} COLS (FILTERED {skipped_1} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## UPLOAD RAW ########################################
            tlog = Ctx(
                logger = logger.getChild("upload_raw"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "upload_raw",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"      : ms,
                            "rows_added"    : len(rows_added_raw) if rows_added_raw in locals() else None,
                        }
                    )
            ):
                coll_raw = "rec_raw_data" if data_origin == "rec" else "hist_raw_data"
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    message=f":material/database_upload: UPLOADING TO MONGODB ({coll_raw})"
                )

                initial_rows = await mongo.count_documents(coll_raw)
                rows_added_raw = len(processed_list_1) - initial_rows
                await mongo.upsert_records_hashed(processed_list_1, coll_raw)

                end = time.perf_counter()
                curr += 1
                total_time += end - start
                message = f":material/done_outline: [{end-start:.2f} s] UPLOADED {rows_added_raw} ROWS ({coll_raw})"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## PROCESSING ########################################
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
            placeholder = {"rows" : None, "cols" : None}
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration" : ms,
                            "rows" : placeholder["rows"],
                            "cols" : placeholder["cols"]
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    progress = None,
                    message = ":material/conveyor_belt: PROCESSING UC, FHR SIGNALS",
                    state = None
                )

                processed_list_2, skipped_2 = await anyio.to_thread.run_sync(
                    lambda: process_signals(processed_list_1)
                )

                placeholder["rows"] = len(processed_list_1)
                placeholder["cols"] = len(processed_list_1[0])

                if len(processed_list_2) == 0:
                    end = time.perf_counter()
                    set_progress(
                        job_id,
                        progress = None,
                        message = f":material/done_outline: NO ROWS AFTER PROCESSING",
                        state = "None"
                    )
                    set_progress(
                        job_id,
                        progress = None,
                        message = f":material/person_celebrate: PIPELINE FINISHED IN {end - start:.2f} s",
                        state = "completed"
                    )
                    return

                end = time.perf_counter()
                curr += 1
                total_time += end - start
                message = f":material/done_outline: [{end-start:.2f} s] {len(processed_list_2)} ROWS {len(processed_list_2[0])} COLS (FILTERED {skipped_2} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
            ######################################## UPLOAD PROCESSED ########################################
            tlog = Ctx(
                logger = logger.getChild("upload_processed"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : data_origin,
                    "task"          : "upload_processed",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra={
                            "duration"      : ms,
                            "rows_added"    : len(rows_added_processed) if rows_added_processed in locals() else None
                        }
                    )
            ):
                coll_proc = "rec_processed_data" if data_origin == "rec" else "hist_processed_data"
                start = time.perf_counter()
                check_cancel(job_id)
                set_progress(
                    job_id,
                    progress=None,
                    message=f":material/database_upload: UPLOADING TO MONGODB ({coll_proc})",
                    state=None
                )

                initial_rows = await mongo.count_documents(coll_proc)
                rows_added_processed = len(processed_list_2) - initial_rows
                await mongo.upsert_records_hashed(processed_list_2, coll_proc)

                end = time.perf_counter()
                curr += 1
                total_time += end - start
                message = f":material/done_outline: [{end-start:.2f} s] UPLOADED {rows_added_processed} ROWS ({coll_proc})"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message
                )

            set_progress(
                job_id,
                progress = None,
                message = f":material/done_outline: PIPELINE FINISHED IN {total_time:.2f} s",
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

            set_progress(
                job_id,
                message=f":material/error: Pipeline encountered an error\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}",
                state="failed"
            )
