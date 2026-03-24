from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import get_db
from app.models.artifact import Artifact
from app.schemas.job import (
    JobCreate, BatchJobCreate, JobResponse, JobDetailResponse, JobListResponse,
    NodeExecutionResponse,
)
from app.services.job_service import (
    create_job, create_job_from_snapshot, get_job, list_jobs, cancel_job, delete_job,
)

router = APIRouter(prefix="/api/v1", tags=["jobs"])


def _to_response(job) -> JobResponse:
    return JobResponse(
        id=str(job.id),
        pipeline_id=str(job.pipeline_id),
        status=job.status.value,
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error_message=job.error_message,
        submitted_by=job.submitted_by,
        retry_count=job.retry_count,
    )


async def _load_output_artifacts(db: AsyncSession, job) -> dict[uuid.UUID, Artifact]:
    artifact_ids = [ne.output_artifact_id for ne in job.node_executions if ne.output_artifact_id]
    if not artifact_ids:
        return {}

    result = await db.execute(select(Artifact).where(Artifact.id.in_(artifact_ids)))
    artifacts = result.scalars().all()
    return {artifact.id: artifact for artifact in artifacts}


async def _to_detail(db: AsyncSession, job) -> JobDetailResponse:
    artifacts_by_id = await _load_output_artifacts(db, job)
    return JobDetailResponse(
        id=str(job.id),
        pipeline_id=str(job.pipeline_id),
        status=job.status.value,
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error_message=job.error_message,
        submitted_by=job.submitted_by,
        retry_count=job.retry_count,
        pipeline_snapshot=job.pipeline_snapshot,
        execution_plan=job.execution_plan,
        node_executions=[
            NodeExecutionResponse(
                id=str(ne.id),
                node_id=ne.node_id,
                node_type=ne.node_type,
                node_label=ne.node_label,
                status=ne.status.value,
                progress=ne.progress,
                worker_id=ne.worker_id,
                queued_at=ne.queued_at,
                started_at=ne.started_at,
                completed_at=ne.completed_at,
                error_message=ne.error_message,
                input_artifact_ids=[str(a) for a in (ne.input_artifact_ids or [])],
                output_artifact_id=str(ne.output_artifact_id) if ne.output_artifact_id else None,
                output_artifact_filename=(
                    artifacts_by_id[ne.output_artifact_id].filename
                    if ne.output_artifact_id and ne.output_artifact_id in artifacts_by_id
                    else None
                ),
                output_artifact_media_info=(
                    artifacts_by_id[ne.output_artifact_id].media_info
                    if ne.output_artifact_id and ne.output_artifact_id in artifacts_by_id
                    else None
                ),
            )
            for ne in job.node_executions
        ],
    )


@router.post("/jobs", response_model=JobDetailResponse, status_code=201)
async def submit_job(data: JobCreate, db: AsyncSession = Depends(get_db)):
    try:
        job = await create_job(db, uuid.UUID(data.pipeline_id))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Start execution asynchronously (don't block the response)
    import asyncio
    from app.orchestrator.engine import engine
    asyncio.create_task(engine.start_job(job.id))

    return await _to_detail(db, job)


@router.get("/jobs", response_model=JobListResponse)
async def list_all(
    skip: int = 0,
    limit: int = Query(default=50, le=100),
    pipeline_id: str | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    pid = uuid.UUID(pipeline_id) if pipeline_id else None
    items, total = await list_jobs(db, skip, limit, pid, status)
    return JobListResponse(
        items=[_to_response(j) for j in items],
        total=total,
    )


@router.get("/jobs/{job_id}", response_model=JobDetailResponse)
async def get_one(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    job = await get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return await _to_detail(db, job)


@router.post("/jobs/{job_id}/cancel", response_model=JobDetailResponse)
async def cancel(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    job = await cancel_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return await _to_detail(db, job)


@router.post("/jobs/batch", response_model=list[JobDetailResponse], status_code=201)
async def submit_batch(data: BatchJobCreate, db: AsyncSession = Depends(get_db)):
    """Submit multiple jobs for the same pipeline with different inputs."""
    import asyncio
    from app.orchestrator.engine import engine

    jobs = []
    for input_overrides in data.inputs:
        try:
            job = await create_job(db, uuid.UUID(data.pipeline_id), input_overrides=input_overrides)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        jobs.append(job)

    # Start all jobs
    for job in jobs:
        asyncio.create_task(engine.start_job(job.id))

    return [await _to_detail(db, j) for j in jobs]


@router.post("/jobs/{job_id}/rerun", response_model=JobDetailResponse, status_code=201)
async def rerun(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Re-run a job by creating a new job from the same pipeline."""
    old_job = await get_job(db, job_id)
    if not old_job:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        new_job = await create_job_from_snapshot(db, old_job.pipeline_id, old_job.pipeline_snapshot)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    import asyncio
    from app.orchestrator.engine import engine
    asyncio.create_task(engine.start_job(new_job.id))

    return await _to_detail(db, new_job)


@router.delete("/jobs/{job_id}", status_code=200)
async def delete_one(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        deleted = await delete_job(db, job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"status": "deleted"}
