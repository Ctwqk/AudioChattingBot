from __future__ import annotations
import uuid
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline
from app.schemas.pipeline import PipelineDefinition
from app.orchestrator.dag import validate_pipeline


def _apply_input_overrides(
    definition: PipelineDefinition,
    input_overrides: dict | None = None,
) -> PipelineDefinition:
    if not input_overrides:
        return definition

    data = definition.model_dump()
    top_level_asset_applied = False

    for node in data["nodes"]:
        if node["type"] != "source":
            continue

        config = dict(node["data"].get("config") or {})
        if node["id"] in input_overrides:
            override_value = input_overrides[node["id"]]
            if isinstance(override_value, dict):
                config.update(override_value)
            else:
                config["asset_id"] = override_value
        elif "asset_id" in input_overrides and not top_level_asset_applied:
            config["asset_id"] = input_overrides["asset_id"]
            top_level_asset_applied = True

        node["data"]["config"] = config

    return PipelineDefinition.model_validate(data)


async def _create_job_from_definition(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    definition: PipelineDefinition,
) -> Job:
    validation = validate_pipeline(definition)
    if not validation.valid:
        error_msgs = "; ".join(e.message for e in validation.errors)
        raise ValueError(f"Pipeline validation failed: {error_msgs}")

    job = Job(
        pipeline_id=pipeline_id,
        pipeline_snapshot=definition.model_dump(),
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()

    for node in definition.nodes:
        config = dict(node.data.config or {})
        node_exec = NodeExecution(
            job_id=job.id,
            node_id=node.id,
            node_type=node.type,
            node_label=node.data.label or node.type,
            node_config=config,
            status=NodeStatus.PENDING,
        )
        if node.type == "source":
            asset_id = config.get("asset_id") or node.data.asset_id
            if asset_id:
                node_exec.node_config = {**node_exec.node_config, "asset_id": asset_id}
        db.add(node_exec)

    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def create_job(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    input_overrides: dict | None = None,
) -> Job:
    """Create a new job from a pipeline. Does NOT start execution - that's the orchestrator's job.

    input_overrides: optional dict to override source node configs for batch jobs.
        Keys can be node_id or "asset_id" (applied to the first source node).
    """
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise ValueError(f"Pipeline {pipeline_id} not found")

    definition = PipelineDefinition.model_validate(pipeline.definition)
    effective_definition = _apply_input_overrides(definition, input_overrides)
    return await _create_job_from_definition(db, pipeline_id, effective_definition)


async def create_job_from_snapshot(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    pipeline_snapshot: dict,
) -> Job:
    definition = PipelineDefinition.model_validate(pipeline_snapshot)
    return await _create_job_from_definition(db, pipeline_id, definition)


async def get_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    """Get a job with all node executions eagerly loaded."""
    stmt = (
        select(Job)
        .where(Job.id == job_id)
        .options(selectinload(Job.node_executions))
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def list_jobs(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 50,
    pipeline_id: uuid.UUID | None = None,
    status: str | None = None,
) -> tuple[list[Job], int]:
    base = select(Job)
    count_q = select(func.count()).select_from(Job)

    if pipeline_id:
        base = base.where(Job.pipeline_id == pipeline_id)
        count_q = count_q.where(Job.pipeline_id == pipeline_id)
    if status:
        base = base.where(Job.status == status)
        count_q = count_q.where(Job.status == status)

    total = (await db.execute(count_q)).scalar() or 0
    stmt = base.order_by(Job.submitted_at.desc()).offset(skip).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all()), total


async def cancel_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    """Cancel a job and all its pending/queued/running node executions."""
    job = await get_job(db, job_id)
    if not job:
        return None
    if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED):
        return job  # already terminal

    job.status = JobStatus.CANCELLED
    for ne in job.node_executions:
        if ne.status in (NodeStatus.PENDING, NodeStatus.QUEUED, NodeStatus.RUNNING):
            ne.status = NodeStatus.CANCELLED

    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def delete_job(db: AsyncSession, job_id: uuid.UUID) -> bool:
    job = await db.get(Job, job_id)
    if not job:
        return False

    if job.status in (JobStatus.PENDING, JobStatus.PLANNING, JobStatus.RUNNING):
        raise ValueError("Only terminal jobs can be deleted")

    await db.delete(job)
    await db.commit()
    return True
