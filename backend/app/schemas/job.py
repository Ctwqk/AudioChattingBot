from __future__ import annotations
import uuid
from datetime import datetime
from pydantic import BaseModel


class JobCreate(BaseModel):
    pipeline_id: str


class BatchJobCreate(BaseModel):
    pipeline_id: str
    inputs: list[dict]  # list of per-job input overrides, e.g. [{"asset_id": "..."}, ...]


class NodeExecutionResponse(BaseModel):
    id: str
    node_id: str
    node_type: str
    node_label: str
    status: str
    progress: int
    worker_id: str | None
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    input_artifact_ids: list[str]
    output_artifact_id: str | None


class JobResponse(BaseModel):
    id: str
    pipeline_id: str
    status: str
    submitted_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    submitted_by: str
    retry_count: int


class JobDetailResponse(JobResponse):
    pipeline_snapshot: dict
    execution_plan: dict | None
    node_executions: list[NodeExecutionResponse]


class JobListResponse(BaseModel):
    items: list[JobResponse]
    total: int
