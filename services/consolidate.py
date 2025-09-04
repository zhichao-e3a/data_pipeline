from __future__ import annotations

import anyio.to_thread

from core import states

from database.MongoDBConnector import MongoDBConnector

from utils.data_consolidate import consolidate, remove_poor
from utils.feature_extraction import get_extracted_features

from services.notifier import set_progress
from services.shared import check_cancel

import time
import logging
import asyncio
import traceback
from typing import Callable

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

async def run_consolidate(
        job_id: str
) -> None:

    steps       = 5
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
        msg     = "consolidate_start",
        extra   = {
            "status" : "start"
        }
    )

    with time_block(
            lambda ms: plog.info(
                msg     = "consolidate_end",
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
                    "data_origin"   : "all",
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

                message = ":material/database: QUERYING MONGODB"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                all_patients = await mongo.get_all_documents(coll_name="patients_unified")

                # SYNCHRONOUS
                rec_patients, hist_patients = [], []
                valid_patients = 0
                for patient in all_patients:

                    data_origin = patient["recruitment_type"]
                    onset = patient["onset_datetime"]
                    delivery = patient["delivery_type"]

                    if data_origin == "recruited":
                        rec_patients.append(patient)

                    elif data_origin == "historical":
                        hist_patients.append(patient)

                    if onset and delivery != "c_section":
                        valid_patients += 1

                rec_measurements = await mongo.get_all_documents(
                    coll_name="rec_processed_data",
                    query={
                        "mobile": {
                            "$in": [i["patient_id"] for i in rec_patients]
                        }
                    }
                )

                hist_measurements = await mongo.get_all_documents(
                    coll_name="hist_processed_data",
                    query={
                        "mobile": {
                            "$in": [i["patient_id"] for i in hist_patients]
                        }
                    }
                )

                rec_data, rec_skipped   = await anyio.to_thread.run_sync(
                    lambda : consolidate(rec_measurements, rec_patients)
                )

                hist_data, hist_skipped = await anyio.to_thread.run_sync(
                    lambda : consolidate(hist_measurements, hist_patients)
                )

                curr += 1
                end = time.perf_counter()
                total_time += end-start

                message = f":material/groups: {len(rec_patients)} RECRUITED PATIENTS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/groups: {len(hist_patients)} HISTORICAL PATIENTS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/sentiment_satisfied: {valid_patients} VALID PATIENTS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/done_outline: [{end - start:.2f} s] QUERIED {len(rec_measurements)+len(hist_measurements)} ROWS"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/person_alert: SKIPPED {rec_skipped["no_onset"]+hist_skipped["no_onset"]} DUE TO NO ONSET DATE"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )

                message = f":material/person_alert: SKIPPED {rec_skipped["c_section"]+hist_skipped["c_section"]} DUE TO C-SECTION"
                set_progress(
                    job_id,
                    progress=round((curr / steps) * 100),
                    message=message,
                    state=None
                )
            ######################################## UPLOAD ALL ########################################
            tlog = Ctx(
                logger = logger.getChild("upload_all"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : "all",
                    "task"          : "upload_all",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"rows" : None}
            with time_block(
                    lambda ms: tlog.info(
                        msg     = "task_end",
                        extra   = {
                            "duration"  : ms,
                            "rows"   : placeholder["rows"]
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = ":material/database_upload: UPLOADING TO MONGODB (consolidated_data)"
                set_progress(
                    job_id,
                    progress = None,
                    message = message,
                    state = None
                )

                await mongo.upsert_records_hashed(
                    rec_data + hist_data,
                    coll_name = "consolidated_data"
                )

                end = time.perf_counter()
                curr += 1
                total_time += end-start

                message = f":material/done_outline: [{end-start:.2f} s] UPLOADED {len(rec_data) + len(hist_data)} ROWS (consolidated_data)"
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
                    "data_origin"   : "all",
                    "task"          : "filter",
                    "task_n"        : curr
                }
            )
            tlog.info(
                msg = "task_start"
            )
            placeholder = {"rows" : None}
            with time_block(
                    lambda ms: tlog.info(
                        msg = "task_end",
                        extra = {
                            "duration"  : ms,
                            "rows"      : placeholder["rows"]
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

                good_measurements, skipped = remove_poor(rec_data + hist_data)

                end = time.perf_counter()
                curr += 1
                total_time += end-start

                message = f":material/done_outline: [{end-start:.2f} s] {len(good_measurements)} ROWS (FILTERED {skipped} ROWS)"
                set_progress(
                    job_id,
                    progress = round((curr/steps)*100),
                    message = message,
                    state = None
                )
                ######################################## EXTRACT FEATURES ########################################
                tlog = Ctx(
                    logger=logger.getChild("extract_features"),
                    extra={
                        "job_id" : job_id,
                        "data_origin" : "all",
                        "task" : "extract_features",
                        "task_n" : curr
                    }
                )
                tlog.info(
                    msg="task_start"
                )
                with time_block(
                        lambda ms: tlog.info(
                            msg="task_end",
                            extra={
                                "duration": ms,
                                "rows_added": None,
                            }
                        )
                ):
                    start = time.perf_counter()
                    check_cancel(job_id)

                    message = f":material/database_upload: EXTRACTING FEATURES"
                    set_progress(
                        job_id,
                        message = message
                    )

                    extracted_measurements = await anyio.to_thread.run_sync(
                        lambda : get_extracted_features(good_measurements)
                    )

                    end = time.perf_counter()
                    curr += 1
                    total_time += end - start

                    message = f":material/done_outline: [{end - start:.2f} s] FEATURE EXTRACTION DONE"
                    set_progress(
                        job_id,
                        progress=round((curr / steps) * 100),
                        message=message,
                        state=None
                    )
            ######################################## UPLOAD GOOD ########################################
            tlog = Ctx(
                logger = logger.getChild("upload_good"),
                extra = {
                    "job_id"        : job_id,
                    "data_origin"   : "all",
                    "task"          : "upload_good",
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
                            "rows_added"    : None,
                        }
                    )
            ):
                start = time.perf_counter()
                check_cancel(job_id)

                message = f":material/database_upload: UPLOADING TO MONGODB (consolidated_data_good)"
                set_progress(
                    job_id,
                    message = message
                )

                await mongo.upsert_records_hashed(
                    extracted_measurements,
                    coll_name = "consolidated_data_good"
                )

                end = time.perf_counter()
                curr += 1
                total_time += end - start

                message = f":material/done_outline: [{end-start:.2f} s] UPLOADED {len(extracted_measurements)} ROWS (consolidated_data_good)"
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
                state = "failed"
            )

            # watermark_log = log_watermark(
            #     pipeline_name=data_origin,
            #     utime=latest_utime,
            #     job_id=job_id,
            # )
            #
            # await mongo.upsert_records_hashed([watermark_log], "watermarks")
