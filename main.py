from api.v1.router import api_router
from core import states
from core.middleware import install_middleware

from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):

    states.LOOP = __import__("asyncio").get_event_loop()
    yield

app = FastAPI(
    lifespan=lifespan,
    title="E3A Data Pipeline API"
)

install_middleware(app)

app.include_router(
    api_router
)

# uvicorn main:app --host 127.0.0.1 --port 8502