from database.queries import HISTORICAL, RECRUITED
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector
from utils.data_download import *
from utils.data_process import *
from utils.signal_processing import *

import time
import uuid
import anyio
import asyncio
from pydantic import BaseModel
from collections import defaultdict
import traceback

from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

app     = FastAPI()
db      = SQLDBConnector()
mongo   = MongoDBConnector()

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r".*",
    allow_origins=[
        "http://localhost:8501",
        "http://127.0.0.1:8501",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=[
        "localhost",
        "127.0.0.1",
        "[::1]",
        "*.local"
    ]
)

PROGRESS            = {}
CANCELLED           = set()
subscribers         = defaultdict(set)

class ResponseModel(BaseModel):
    job_id: str

@app.websocket("/ws/status/{job_id}")
async def ws_status(websocket : WebSocket, job_id):

    # Completes WS handsake (WS protocol)
    await websocket.accept()

    # Stores socket in a per-job subscriber set
    subscribers[job_id].add(websocket)

    # Grab current status from in memory
    snapshot = PROGRESS.get(
        job_id,
        {"progress": 0, "state": "unknown", "message": "No such job"}
    )

    # Send to client
    await websocket.send_json({"type": "status", "job_id": job_id, **snapshot})

    try:
        while True:
            # Keep connection alive by pinging every 25 seconds
            # Some proxies idle-timeout quiet WS connections
            await asyncio.sleep(25)
            await websocket.send_json({"type": "ping"})

    except WebSocketDisconnect:
        pass

    finally:
        subscribers[job_id].discard(websocket)

async def _publish(job_id, payload):

    dead = []

    # Iterate through current sockets and send update to them
    for ws in list(subscribers.get(job_id, [])):

        try:
            await ws.send_json(payload)

        except Exception as e:
            print(e)
            dead.append(ws)

    # Discard dead sockets
    for ws in dead:
        subscribers[job_id].discard(ws)

def _emit(job_id, payload):

    # Schedule the async '_publish' without blocking caller
    try:
        asyncio.get_running_loop().create_task(_publish(job_id, payload))

    except RuntimeError:
        pass

def _set_progress(job_id, progress=None, state=None, message=None):

    st = PROGRESS.setdefault(
        job_id,
        {"progress": 0, "state": "running", "message": ""}
    )

    if progress is not None: st["progress"] = int(progress)
    if message  is not None: st["message"]  = str(message)
    if state    is not None: st["state"]    = state

    # Push update to client
    _emit(job_id, {"type": "status", "job_id": job_id, **st})

@app.post("/cancel/{job_id}")
async def cancel_pipeline(job_id: str):

    CANCELLED.add(job_id)
    _set_progress(job_id, state="cancelled", message=":material/cancel: Cancellation requested")

    return {"job_id": job_id, "status": "cancelling"}

def _check_cancel(job_id):

    if job_id in CANCELLED:

        raise asyncio.CancelledError(f"Pipeline {job_id} was cancelled")

@app.post("/run_pipeline/{data_origin}", response_model=ResponseModel, status_code=202)
async def run_pipeline(background_tasks: BackgroundTasks, data_origin: str):

    job_id = uuid.uuid4().hex
    PROGRESS[job_id] = {"progress": 0, "state": "running", "message": "Starting…"}
    background_tasks.add_task(pipeline, job_id, data_origin)

    return {"job_id": job_id}

async def pipeline(job_id, data_origin):

    steps       = 6
    curr        = 0
    total_time  = 0

    try:
        logging = {
            "job_id"    : job_id,
            "type"      : data_origin,
            "date"      : datetime.now().isoformat(),
            "logs"      : {}
        }

        _check_cancel(job_id)

        _set_progress(job_id, progress=0, message=":material/database: Querying MySQL database")

        start = time.perf_counter()

        if data_origin == "rec":

            consolidated_patients   = await mongo.get_all_documents("consolidated_patients")
            recruited_patients      = [i for i in consolidated_patients if i["data_origin"] == "recruited"]

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
                        numbers=query_string
                    )
                )
            )

            last_menstrual_dates = {
                i["_id"]: i["last_menstrual"] for i in recruited_patients
            }

            expected_delivery = {
                i["_id"]: i["expected_delivery"] for i in recruited_patients
            }

            actual_delivery = {
                i["_id"]: i["actual_delivery"] for i in recruited_patients
            }

        else:
            df = await anyio.to_thread.run_sync(lambda: db.query_to_dataframe(sql=HISTORICAL))

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end - start:.2f} seconds] Queried {len(df)} rows {len(df.columns)} columns from MySQL database"
        log = {
            "Duration"          : round(end-start,2),
            "Rows queried"      : len(df),
            "Columns queried"   : len(df.columns),
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"]["Query MySQL"]  = log
        logging["rows_queried"]         = len(df)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _check_cancel(job_id)

        _set_progress(job_id, message=":material/cloud_download: Downloading .gz links")

        start = time.perf_counter()

        uc_results, fhr_results = await async_process_urls(df)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} seconds] Downloaded {len(uc_results)*2} .gz links from Amazon Cloud"
        log = {
            "Duration"          : round(end-start,2),
            "Links downloaded"  : len(uc_results)*2,
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"]["Download gz links"] = log
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        _check_cancel(job_id)

        _set_progress(job_id, message=":material/conveyor_belt: Processing data")

        start = time.perf_counter()

        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(
            lambda: process_data(
                df,
                uc_results,
                fhr_results,
                data_origin,
                last_menstrual=last_menstrual_dates,
                expected_delivery=expected_delivery,
                actual_delivery=actual_delivery
            )
        )

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} seconds] {len(processed_list_1)} rows {len(processed_list_1[0])} columns remaining ({skipped_1} rows filtered)"
        log = {
            "Duration"          : round(end-start,2),
            "Initial rows"      : len(df),
            "Initial columns"   : len(df.columns),
            "Remaining rows"    : len(processed_list_1),
            "Remaining columns" : len(processed_list_1[0]),
            "Rows skipped"      : skipped_1,
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"]["Process Data"] = log
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _check_cancel(job_id)

        coll_name = "rec_raw_data" if data_origin == "rec" else "hist_raw_data"
        _set_progress(job_id, message=f":material/database_upload: Uploading to MongoDB ({coll_name})")

        start = time.perf_counter()

        initial_rows = await mongo.count_documents(coll_name)
        rows_added_raw = len(processed_list_1) - initial_rows
        await mongo.upsert_records(processed_list_1, coll_name)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} seconds] Uploaded {rows_added_raw} rows to MongoDB ({coll_name})"
        log = {
            "Duration"          : round(end-start,2),
            "Collection name"   : coll_name,
            "Initial rows"      : initial_rows,
            "Rows added"        : rows_added_raw,
            "New rows"          : len(processed_list_1),
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"][f"Upload to MongoDB ({coll_name})"] = log
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _check_cancel(job_id)

        _set_progress(job_id, message=":material/conveyor_belt: Cleaning UC, FHR signals")

        start = time.perf_counter()

        processed_list_2, skipped_2 = await anyio.to_thread.run_sync(
            lambda: process_signals(processed_list_1)
        )

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} seconds] {len(processed_list_2)} rows {len(processed_list_2[0])} columns remaining ({skipped_2} rows filtered)"
        log = {
            "Duration"          : round(end - start, 2),
            "Initial rows"      : len(processed_list_1),
            "Initial columns"   : len(processed_list_1[0]),
            "Remaining rows"    : len(processed_list_2),
            "Remaining columns" : len(processed_list_2[0]),
            "Rows skipped"      : skipped_2,
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"]["Process Signals"] = log
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _check_cancel(job_id)

        coll_name = "rec_processed_data" if data_origin == "rec" else "hist_processed_data"
        _set_progress(job_id, message=f":material/database_upload: Uploading to MongoDB ({coll_name})")

        start = time.perf_counter()

        initial_rows = await mongo.count_documents(coll_name)
        rows_added_processed = len(processed_list_2) - initial_rows
        await mongo.upsert_records(processed_list_2, coll_name)

        end = time.perf_counter()

        curr += 1
        total_time += end - start
        message = f":material/done_outline: [{end-start:.2f} seconds] Uploaded {rows_added_processed} rows to MongoDB ({coll_name})"
        log = {
            "Duration"          : round(end - start, 2),
            "Collection name"   : coll_name,
            "Initial rows"      : initial_rows,
            "Rows added"        : rows_added_processed,
            "New rows"          : len(processed_list_2),
            "Status"            : "Completed",
            "Message"           : message
        }
        logging["logs"][f"Upload to MongoDB ({coll_name})"] = log
        logging["rows_added"] = rows_added_processed
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        logging["status"] = "completed"
        logging["total_time"] = total_time

        await mongo.upsert_records(logging, coll_name="logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time:.2f} seconds"
        _set_progress(job_id, message=message, state="completed")

    except asyncio.CancelledError:

        message = f":material/cancel: Pipeline {job_id} cancelled by user"
        _set_progress(job_id, message=message, state="cancelled")

        logging["status"]       = "Cancelled"
        logging["total_time"]   = total_time

        await mongo.upsert_records(logging, coll_name="logs")
        CANCELLED.discard(job_id)

    except Exception as e:

        message = f":material/error: Pipeline encountered an error when running\n{"".join(traceback.format_exception(type(e), e, e.__traceback__))}"
        _set_progress(job_id, message=message, state="failed")

        logging["logs"]["Error"]    = message
        logging["status"]           = "Failed"
        logging["total_time"]       = total_time

        await mongo.upsert_records(logging, coll_name="logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time} seconds"
        _set_progress(job_id, message=message)

        print(message)