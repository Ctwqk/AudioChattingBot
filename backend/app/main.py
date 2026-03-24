import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings

logger = logging.getLogger(__name__)


async def _recover_stale_jobs():
    """On startup, find PENDING/RUNNING jobs and restart them."""
    from sqlalchemy import select
    from app.db import async_session
    from app.models.job import Job, JobStatus
    from app.orchestrator.engine import engine

    async with async_session() as db:
        stmt = select(Job).where(Job.status.in_([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PLANNING]))
        result = await db.execute(stmt)
        stale_jobs = list(result.scalars().all())

    for job in stale_jobs:
        logger.info(f"Recovering stale job {job.id} (status={job.status.value})")
        asyncio.create_task(engine.start_job(job.id))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: launch the orchestrator event listener
    from app.orchestrator.event_listener import event_listener
    task = asyncio.create_task(event_listener())
    logger.info("Orchestrator event listener background task started")

    # Recover jobs that were in-flight when the server last shut down
    await _recover_stale_jobs()

    yield
    # Shutdown: cancel the event listener
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("Orchestrator event listener stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="VideoProcess API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.api.node_types import router as node_types_router
    app.include_router(node_types_router)

    from app.api.pipelines import router as pipelines_router
    app.include_router(pipelines_router)

    from app.api.assets import router as assets_router
    from app.api.artifacts import router as artifacts_router
    app.include_router(assets_router)
    app.include_router(artifacts_router)

    from app.api.jobs import router as jobs_router
    app.include_router(jobs_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
