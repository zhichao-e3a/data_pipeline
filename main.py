from api.v1.router import api_router
from core import states
from core.middleware import install_middleware
from core.logging_config import setup_logging

import os, multiprocessing as mp, concurrent.futures

from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):

    ctx = mp.get_context("spawn")

    states.LOOP = __import__("asyncio").get_running_loop()

    states.LOG_LISTEN = setup_logging(
        mode = os.getenv("MODE")
    )

    states.PROC_POOL = concurrent.futures.ProcessPoolExecutor(
        max_workers = os.cpu_count(),
        mp_context = ctx
    )

    try:
        yield
    finally:

        listener = getattr(states, "log_listener", None)

        if listener:
            listener.stop()

            pool = getattr(states, "PROC_POOL", None)
            if pool:
                pool.shutdown(wait=False, cancel_futures=True)

app = FastAPI(
    lifespan=lifespan,
    title="E3A Data Pipeline API"
)

install_middleware(app)
app.include_router(api_router)

# uvicorn main:app --host 127.0.0.1 --port 8502
