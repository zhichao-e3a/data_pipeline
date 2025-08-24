from database.queries import HISTORICAL
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector
from utils.data_download import *
from utils.data_process import *

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

@app.post("/run_rec_pipeline", response_model=ResponseModel, status_code=202)
async def run_rec_pipeline(background_tasks: BackgroundTasks):

    job_id = uuid.uuid4().hex
    PROGRESS[job_id] = {"progress": 0, "state": "running", "message": "Starting…"}
    background_tasks.add_task(rec_pipeline, job_id)

    return {"job_id": job_id}

async def rec_pipeline(job_id):

    steps       = 7
    curr        = 1
    total_time  = 0
    logs        = []

    try:
        # (2) Query merged data from MongoDB
        _set_progress(job_id, progress=0, message=":material/database: Querying MongoDB")
        start = time.perf_counter()
        merged_df = pd.DataFrame(await mongo.get_all_documents("rec_merged_data"))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Queried {len(merged_df)} rows, {len(merged_df.columns)} columns in {end - start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        # (3) Download gz links
        _set_progress(job_id, message=":material/cloud_download: Downloading .gz links")
        start = time.perf_counter()
        uc_results, fhr_results = await async_process_urls(merged_df)
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Downloaded {len(uc_results)} .gz links in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        # (4) Process data 1
        _set_progress(job_id, message=":material/conveyor_belt: Processing data (1)")
        start = time.perf_counter()
        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(lambda: process_data_1(merged_df, uc_results, fhr_results, data_type="rec"))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: {skipped_1} rows filtered in {end-start:.2f} seconds ({len(processed_list_1)} rows, {len(processed_list_1[0])} columns remaining)"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        # (5) Upload to MongoDB (rec_raw_data)
        rows_added_raw = len(processed_list_1) - await mongo.count_documents("rec_raw_data")
        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (rec_raw_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_1, "rec_raw_data")
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Uploaded {rows_added_raw} rows to MongoDB (rec_raw_data) in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        # (6) Process data 2
        _set_progress(job_id, message=":material/conveyor_belt: Processing data (2)")
        start = time.perf_counter()
        processed_list_2, skipped_2 = await anyio.to_thread.run_sync(lambda: process_data_2(processed_list_1))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: {skipped_2} rows filtered in {end-start:.2f} seconds ({len(processed_list_2)} rows, {len(processed_list_2[0])} columns remaining)"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        # (7) Upload to MongoDB (rec_processed_data)
        rows_added_processed = len(processed_list_2) - await mongo.count_documents("rec_processed_data")
        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (rec_processed_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_2, "rec_processed_data")
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Uploaded {rows_added_processed} rows to MongoDB (rec_processed_data) in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        logging = {
            "job_id"                : job_id,
            "type"                  : "recruited",
            "date"                  : datetime.now().isoformat(),
            "status"                : "completed",
            "rows_retrieved"        : len(merged_df),
            "rows_added_raw"        : rows_added_raw,
            "rows_added_processed"  : rows_added_processed,
            "total_time"            : total_time,
            "logs"                  : logs
        }

        await mongo.upsert_records(logging, "logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time:.2f} seconds"
        _set_progress(job_id, message=message, state="completed")

    except Exception as e:

        message = f":material/error: Pipeline encountered an error when running\n{"".join(traceback.format_exception(type(e), e, e.__traceback__))}"
        _set_progress(job_id, message=message, state="failed")

        logging = {
            "job_id"        : job_id,
            "type"          : "recruited",
            "date"          : datetime.now().isoformat(),
            "status"        : "failed",
            "error"         : message,
            "total_time"    : total_time,
            "logs"          : logs
        }

        await mongo.upsert_records(logging, "logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time} seconds"
        _set_progress(job_id, message=message)

@app.post("/run_hist_pipeline", response_model=ResponseModel, status_code=202)
async def run_hist_pipeline(background_tasks: BackgroundTasks):

    job_id = uuid.uuid4().hex
    PROGRESS[job_id] = {"progress": 0, "state": "running", "message": "Starting…"}
    background_tasks.add_task(hist_pipeline, job_id)

    return {"job_id": job_id}

async def hist_pipeline(job_id):

    steps       = 6
    curr        = 0
    total_time  = 0
    logs        = []

    try:
        _set_progress(job_id, progress=0, message=":material/database: Querying MySQL")
        start = time.perf_counter()
        df = await anyio.to_thread.run_sync(lambda: db.query_to_dataframe(sql=HISTORICAL))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Queried {len(df)} rows, {len(df.columns)} columns in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        _set_progress(job_id, message=":material/cloud_download: Downloading .gz links")
        start = time.perf_counter()
        uc_results, fhr_results = await async_process_urls(df)
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Downloaded {len(uc_results)} .gz links in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        _set_progress(job_id, message=":material/conveyor_belt: Processing data (1)")
        start = time.perf_counter()
        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(lambda: process_data_1(df, uc_results, fhr_results, data_type="hist"))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: {skipped_1} rows filtered in {end-start:.2f} seconds ({len(processed_list_1)} rows, {len(processed_list_1[0])} columns remaining)"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        rows_added_raw = len(processed_list_1) - await mongo.count_documents("hist_raw_data")
        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (hist_raw_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_1, "hist_raw_data")
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Uploaded {rows_added_raw} rows to MongoDB (hist_raw_data) in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _set_progress(job_id, message=":material/conveyor_belt: Processing data (2)")
        start = time.perf_counter()
        processed_list_2, skipped_2 = await anyio.to_thread.run_sync(lambda: process_data_2(processed_list_1))
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: {skipped_2} rows filtered in {end-start:.2f} seconds ({len(processed_list_2)} rows, {len(processed_list_2[0])} columns remaining)"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        rows_added_processed = len(processed_list_2) - await mongo.count_documents("hist_processed_data")
        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (hist_processed_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_2, "hist_processed_data")
        end = time.perf_counter()
        curr += 1
        total_time += end - start
        message = f":material/done_outline: Uploaded {rows_added_processed} rows to MongoDB (hist_processed_data) in {end-start:.2f} seconds"
        logs.append(message)
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        logging = {
            "job_id"                : job_id,
            "type"                  : "historical",
            "date"                  : datetime.now().isoformat(),
            "status"                : "completed",
            "rows_retrieved"        : len(df),
            "rows_added_raw"        : rows_added_raw,
            "rows_added_processed"  : rows_added_processed,
            "total_time"            : total_time,
            "logs"                  : logs
        }

        await mongo.upsert_records(logging, "logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time:.2f} seconds"
        _set_progress(job_id, message=message, state="completed")

    except Exception as e:

        message = f":material/error: Pipeline encountered an error when running\n{"".join(traceback.format_exception(type(e), e, e.__traceback__))}"
        _set_progress(job_id, message=message, state="failed")

        logging = {
            "job_id"        : job_id,
            "type"          : "historical",
            "date"          : datetime.now().isoformat(),
            "status"        : "failed",
            "error"         : message,
            "total_time"    : total_time,
            "logs"          : logs
        }

        await mongo.upsert_records(logging, "logs")
        message = f":material/done_outline: Logging completed, pipeline finished in a total of {total_time} seconds"
        _set_progress(job_id, message=message)

        print(message)