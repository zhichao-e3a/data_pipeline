from __future__ import annotations

import pandas as pd

from core import states
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector
from database.queries import HISTORICAL, RECRUITED
from utils.data_download import async_process_urls
from utils.data_process import process_data
from utils.signal_processing import process_signals
from services.notifier import set_progress
from services.shared import _check_cancel, log_watermark

import time, anyio, asyncio, traceback
from datetime import datetime

db      = SQLDBConnector()
mongo   = MongoDBConnector()

async def run_pipeline(
        job_id: str,
        data_origin: str
) -> None:

    steps       = 6
    curr        = 0
    total_time  = 0.0

    try:
        watermarks = await mongo.get_all_documents("watermarks")
        curr_watermark = [i for i in watermarks if i["_id"] == data_origin]
        last_utime = curr_watermark[0]["last_utime"]

        _check_cancel(job_id)
        set_progress(
            job_id,
            progress=0,
            message=":material/database: Querying MySQL database"
        )

        start = time.perf_counter()

        if data_origin == "rec":

            consolidated = await mongo.get_all_documents("consolidated_patients")
            recruited_patients = [i for i in consolidated if i["data_origin"] == "recruited"]

            query_string = ",".join(
                [
                    f"'{i["_id"]}'" for i in recruited_patients
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

            if len(df) == 0:
                end = time.perf_counter()
                set_progress(
                    job_id,
                    message=f":material/done_outline: [{end-start:.2f} s] No new rows",
                    state="completed"
                )
                return

            last_menstrual      = {
                i["_id"]: i["last_menstrual"] for i in recruited_patients
            }

            expected_delivery   = {
                i["_id"]: i["expected_delivery"] for i in recruited_patients
            }

            actual_delivery     = {
                i["_id"]: i["actual_delivery"] for i in recruited_patients
            }

        else:
            df = await anyio.to_thread.run_sync(
                lambda: db.query_to_dataframe(
                    sql=HISTORICAL.format(
                        last_utime=last_utime
                    )
                )
            )

            if len(df) == 0:
                end = time.perf_counter()
                set_progress(
                    job_id,
                    message=f":material/done_outline: [{end-start:.2f} s] No new rows",
                    state="completed"
                )
                return

        # Log watermark for SQL
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
        total_time += end - start
        message = f":material/done_outline: [{end - start:.2f} s] Queried {len(df)} rows {len(df.columns)} cols"
        set_progress(
            job_id,
            progress=round((curr/steps)*100),
            message=message
        )

        _check_cancel(job_id)

        set_progress(
            job_id,
            message=":material/cloud_download: Downloading .gz links"
        )

        start = time.perf_counter()

        uc_results, fhr_results = await async_process_urls(df)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} s] Downloaded {len(uc_results)*2} .gz links"
        set_progress(
            job_id,
            progress=round((curr/steps)*100),
            message=message
        )

        _check_cancel(job_id)

        set_progress(
            job_id,
            message=":material/conveyor_belt: Processing data"
        )

        start = time.perf_counter()

        # Use Semaphore to guard heavy step
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

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} s] {len(processed_list_1)} rows {len(processed_list_1[0])} cols ({skipped_1} filtered)"
        set_progress(job_id,
                     progress=round((curr/steps)*100),
                     message=message
        )

        _check_cancel(job_id)

        coll_raw = "rec_raw_data" if data_origin == "rec" else "hist_raw_data"
        set_progress(
            job_id,
            message=f":material/database_upload: Uploading to MongoDB ({coll_raw})"
        )

        start = time.perf_counter()

        initial_rows = await mongo.count_documents(coll_raw)
        rows_added_raw = len(processed_list_1) - initial_rows
        await mongo.upsert_records_hashed(processed_list_1, coll_raw)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} s] Uploaded {rows_added_raw} rows ({coll_raw})"
        set_progress(
            job_id,
            progress=round((curr/steps)*100),
            message=message
        )

        _check_cancel(job_id)

        set_progress(
            job_id,
            message=":material/conveyor_belt: Cleaning UC, FHR signals"
        )

        start = time.perf_counter()

        processed_list_2, skipped_2 = await anyio.to_thread.run_sync(
            lambda: process_signals(processed_list_1)
        )

        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} s] {len(processed_list_2)} rows {len(processed_list_2[0])} cols ({skipped_2} filtered)"
        set_progress(
            job_id,
            progress=round((curr/steps)*100),
            message=message
        )

        _check_cancel(job_id)

        coll_proc = "rec_processed_data" if data_origin == "rec" else "hist_processed_data"
        set_progress(
            job_id,
            message=f":material/database_upload: Uploading to MongoDB ({coll_proc})"
        )

        start = time.perf_counter()

        initial_rows = await mongo.count_documents(coll_proc)
        rows_added_processed = len(processed_list_2) - initial_rows
        await mongo.upsert_records_hashed(processed_list_2, coll_proc)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} s] Uploaded {rows_added_processed} rows ({coll_proc})"
        set_progress(
            job_id,
            progress=round((curr/steps)*100),
            message=message
        )

        set_progress(
            job_id,
            message=f":material/done_outline: Logging completed, pipeline finished in {total_time:.2f} s",
            state="completed"
        )

    except asyncio.CancelledError:
        set_progress(
            job_id,
            message=f":material/cancel: Pipeline {job_id} cancelled by user",
            state="cancelled"
        )

        states.CANCELLED.discard(job_id)

    except Exception as e:
        set_progress(
            job_id,
            message=f":material/error: Pipeline encountered an error\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}",
            state="failed"
        )
