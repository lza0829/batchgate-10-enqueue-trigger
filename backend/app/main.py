from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import APP_NAME
from app.routers import api
from app.seed import init_db
from app.worker import worker


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # init_db also requeues any request left 'running' by a crashed process.
    init_db()
    worker.start()
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(title=APP_NAME, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api)
