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

PROGRESS    = {}
subscribers = defaultdict(set)

class ResponseModel(BaseModel):
    job_id: str

@app.post("/run_pipeline", response_model=ResponseModel, status_code=202)
async def run_pipeline(background_tasks: BackgroundTasks):

    job_id = uuid.uuid4().hex
    PROGRESS[job_id] = {"progress": 0, "state": "running", "message": "Starting…"}
    background_tasks.add_task(hist_pipeline, job_id)

    return {"job_id": job_id}

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

async def hist_pipeline(job_id):

    steps   = 6
    curr    = 0

    try:
        _set_progress(job_id, progress=0, message=":material/database: Querying MySQL")
        start = time.perf_counter()
        df      = await anyio.to_thread.run_sync(lambda: db.query_to_dataframe(sql=HISTORICAL))
        curr    += 1
        end = time.perf_counter()
        message = f":material/done_outline: Queried {len(df)} rows, {len(df.columns)} columns in {end-start} seconds"
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        _set_progress(job_id, message=":material/cloud_download: Downloading .gz links")
        start = time.perf_counter()
        uc_results, fhr_results = await async_process_urls(df)
        curr += 1
        end = time.perf_counter()
        message = f":material/done_outline: Downloaded {len(uc_results)} .gz links in {end - start} seconds"
        _set_progress(job_id, progress=round((curr/steps)*100), message=message)

        _set_progress(job_id, message=":material/conveyor_belt: Processing data (1)")
        start = time.perf_counter()
        processed_list_1, skipped_1 = await anyio.to_thread.run_sync(lambda: process_data_1(df, uc_results, fhr_results))
        curr += 1
        end = time.perf_counter()
        message = f":material/done_outline: {skipped_1} rows filtered in {end-start} seconds\n{len(processed_list_1)} rows, {len(processed_list_1[0])} columns remaining"
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (raw_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_1, "raw_data")
        curr += 1
        end = time.perf_counter()
        message = f":material/done_outline: Uploaded to MongoDB (raw_data) in {end - start} seconds"
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _set_progress(job_id, message=":material/conveyor_belt: Processing data (2)")
        start = time.perf_counter()
        processed_list_2, skipped_2 = await anyio.to_thread.run_sync(lambda: process_data_2(processed_list_1))
        curr += 1
        end = time.perf_counter()
        message = f":material/done_outline: {skipped_2} rows filtered in {end-start} seconds\n{len(processed_list_2)} rows, {len(processed_list_2[0])} columns remaining"
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message)

        _set_progress(job_id, message=":material/database_upload: Uploading to MongoDB (processed_data)")
        start = time.perf_counter()
        await mongo.upsert_records(processed_list_2, "processed_data")
        curr += 1
        end = time.perf_counter()
        message = f":material/done_outline: Uploaded to MongoDB (processed_data) in {end - start} seconds"
        _set_progress(job_id, progress=round((curr / steps) * 100), message=message, state="completed")

    except Exception as e:
        _set_progress(job_id, message=e, state="failed")
        print(e)
        import traceback
        traceback.print_exc()