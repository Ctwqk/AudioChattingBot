import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings

logger = logging.getLogger(__name__)
STALE_NODE_RECOVERY_THRESHOLD = timedelta(minutes=10)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def _prepare_job_for_recovery(db, job) -> bool:
    """Reset clearly abandoned QUEUED/RUNNING nodes so startup recovery can redispatch them."""
    from app.models.job import JobStatus, NodeStatus

    now = datetime.now(timezone.utc)
    changed = False

    for node in job.node_executions:
        if node.status not in (NodeStatus.QUEUED, NodeStatus.RUNNING):
            continue

        reference_time = _ensure_utc(node.started_at or node.queued_at or job.started_at or job.submitted_at)
        if not reference_time or (now - reference_time) < STALE_NODE_RECOVERY_THRESHOLD:
            continue

        logger.warning(
            "Resetting stale node %s for job %s from %s to PENDING during startup recovery",
            node.node_id, job.id, node.status.value,
        )
        node.status = NodeStatus.PENDING
        node.worker_id = None
        node.queued_at = None
        node.started_at = None
        node.completed_at = None
        node.progress = 0
        node.error_message = None
        node.input_artifact_ids = []
        changed = True

    if changed and job.status in (JobStatus.RUNNING, JobStatus.PLANNING):
        job.status = JobStatus.PENDING
        job.error_message = None
        job.completed_at = None

    return changed


async def _recover_stale_jobs():
    """On startup, find PENDING/RUNNING jobs and restart them."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.db import async_session
    from app.models.job import Job, JobStatus
    from app.orchestrator.engine import engine

    async with async_session() as db:
        stmt = (
            select(Job)
            .where(Job.status.in_([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PLANNING]))
            .options(selectinload(Job.node_executions))
        )
        result = await db.execute(stmt)
        stale_jobs = list(result.scalars().all())

        for job in stale_jobs:
            changed = await _prepare_job_for_recovery(db, job)
            if not changed:
                await engine._maybe_finalize_job(db, job)
        await db.commit()

    for job in stale_jobs:
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PLANNING):
            continue
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

    from app.api.llm import router as llm_router
    app.include_router(llm_router)

    from app.api.materials import router as materials_router
    app.include_router(materials_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
