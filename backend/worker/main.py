from __future__ import annotations
import asyncio
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.config import settings
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.storage.manager import get_storage
from worker.handlers import HANDLER_MAP
from worker.handlers.base import CancelledError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

TASK_STREAM = "vp:tasks:ffmpeg"
EVENT_STREAM = "vp:events"
CONSUMER_GROUP = "ffmpeg-workers"
WORKER_ID = f"ffmpeg-worker-{os.getpid()}"

PEL_RECLAIM_INTERVAL = 60  # seconds between periodic PEL reclaims
PEL_MIN_IDLE = 30000       # ms a message must be idle before reclaim

# DB session for worker
engine_db = create_async_engine(settings.database_url, echo=False)
worker_session = async_sessionmaker(engine_db, expire_on_commit=False)


async def _is_cancelled(node_execution_id: str) -> bool:
    """Check DB to see if this node execution has been cancelled."""
    async with worker_session() as db:
        ne = await db.get(NodeExecution, uuid.UUID(node_execution_id))
        if ne and ne.status == NodeStatus.CANCELLED:
            return True
        # Also check job-level cancellation
        if ne:
            job = await db.get(Job, ne.job_id)
            if job and job.status == JobStatus.CANCELLED:
                return True
    return False


async def process_task(data: dict) -> None:
    """Process a single node execution task."""
    job_id = data["job_id"]
    node_execution_id = data["node_execution_id"]
    node_type = data["node_type"]
    config = json.loads(data.get("config", "{}"))
    input_artifacts_map = json.loads(data.get("input_artifacts", "{}"))

    logger.info(f"Processing node {data['node_id']} (type={node_type}) for job {job_id}")

    # Get handler
    handler_cls = HANDLER_MAP.get(node_type)
    if not handler_cls:
        await _report_failure(job_id, node_execution_id, f"No handler for node type: {node_type}")
        return

    storage = get_storage()

    # Check if cancelled before starting, then update status to RUNNING
    async with worker_session() as db:
        ne = await db.get(NodeExecution, uuid.UUID(node_execution_id))
        if ne:
            if ne.status == NodeStatus.CANCELLED:
                logger.info(f"Node {data['node_id']} already cancelled, skipping")
                return
            # Also check job-level cancel
            job = await db.get(Job, ne.job_id)
            if job and job.status == JobStatus.CANCELLED:
                logger.info(f"Job {job_id} cancelled, skipping node {data['node_id']}")
                return
            ne.status = NodeStatus.RUNNING
            ne.started_at = datetime.utcnow()
            ne.worker_id = WORKER_ID
            await db.commit()

    handler = handler_cls()

    # Background task: periodically check cancel status and kill handler if needed
    cancel_check_task = None

    async def _cancel_watcher():
        while True:
            await asyncio.sleep(2)
            if await _is_cancelled(node_execution_id):
                logger.info(f"Cancel detected for node {data['node_id']}, killing handler")
                handler.cancel()
                return

    temp_files: list[str] = []  # track temp files for cleanup (for MinIO)
    try:
        # Resolve input artifact paths to local file paths
        input_paths: dict[str, str] = {}
        async with worker_session() as db:
            for port_name, artifact_id_str in input_artifacts_map.items():
                artifact = await db.get(Artifact, uuid.UUID(artifact_id_str))
                if not artifact:
                    raise FileNotFoundError(f"Input artifact {artifact_id_str} not found")
                local_path = storage.get_local_path(artifact.storage_path)
                if not local_path:
                    # MinIO or remote storage: download to temp file
                    content = await storage.read(artifact.storage_path)
                    ext = Path(artifact.filename).suffix or ".mp4"
                    fd, tmp_path = tempfile.mkstemp(suffix=ext, prefix="vp_input_")
                    os.close(fd)
                    with open(tmp_path, "wb") as f:
                        f.write(content)
                    local_path = tmp_path
                    temp_files.append(tmp_path)
                input_paths[port_name] = local_path

        # Prepare output path
        output_ext = _get_output_extension(node_type, config)
        output_filename = f"{node_execution_id}{output_ext}"
        output_storage_path = f"artifacts/{job_id}/{output_filename}"
        output_local_dir = Path(settings.storage_local_root) / "artifacts" / job_id
        output_local_dir.mkdir(parents=True, exist_ok=True)
        output_local_path = str(output_local_dir / output_filename)

        # Start cancel watcher
        cancel_check_task = asyncio.create_task(_cancel_watcher())

        # Execute handler
        await handler.execute(config, input_paths, output_local_path)

        # Verify output exists
        if not os.path.exists(output_local_path):
            raise RuntimeError(f"Handler did not produce output file: {output_local_path}")

        file_size = os.path.getsize(output_local_path)

        # If using remote storage (MinIO), upload the output file
        if settings.storage_backend != "local":
            with open(output_local_path, "rb") as f:
                await storage.save(output_storage_path, f)

        # Create artifact record
        async with worker_session() as db:
            artifact = Artifact(
                job_id=uuid.UUID(job_id),
                node_execution_id=uuid.UUID(node_execution_id),
                kind=ArtifactKind.INTERMEDIATE,
                filename=output_filename,
                mime_type=_guess_mime(output_ext),
                file_size=file_size,
                storage_backend=settings.storage_backend,
                storage_path=output_storage_path if settings.storage_backend != "local" else str(output_local_path),
            )
            db.add(artifact)
            await db.flush()
            artifact_id = str(artifact.id)
            await db.commit()

        # Report success
        await _report_success(job_id, node_execution_id, artifact_id)
        logger.info(f"Node {data['node_id']} completed successfully")

    except CancelledError:
        logger.info(f"Node {data['node_id']} cancelled, cleaning up")
        # Don't report failure — orchestrator already knows about the cancel
    except Exception as e:
        logger.exception(f"Node {data['node_id']} failed")
        await _report_failure(job_id, node_execution_id, str(e))
    finally:
        if cancel_check_task and not cancel_check_task.done():
            cancel_check_task.cancel()
            try:
                await cancel_check_task
            except asyncio.CancelledError:
                pass
        # Clean up any temp files downloaded from remote storage
        for tmp in temp_files:
            try:
                os.unlink(tmp)
            except OSError:
                pass


async def _report_success(job_id: str, node_execution_id: str, artifact_id: str) -> None:
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await r.xadd(EVENT_STREAM, {
            "event": "node_completed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "output_artifact_id": artifact_id,
        })
    finally:
        await r.aclose()


async def _report_failure(job_id: str, node_execution_id: str, error: str) -> None:
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await r.xadd(EVENT_STREAM, {
            "event": "node_failed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "error": error[:2000],
        })
    finally:
        await r.aclose()


def _get_output_extension(node_type: str, config: dict) -> str:
    """Determine output file extension based on node type and config."""
    if node_type == "transcode":
        fmt = config.get("format", "mp4")
        return f".{fmt}"
    fmt = config.get("output_format", "mp4")
    if fmt:
        return f".{fmt}"
    return ".mp4"


def _guess_mime(ext: str) -> str:
    return {
        ".mp4": "video/mp4",
        ".mkv": "video/x-matroska",
        ".webm": "video/webm",
        ".avi": "video/x-msvideo",
        ".mov": "video/quicktime",
    }.get(ext, "video/mp4")


async def _reclaim_pending(r: aioredis.Redis) -> None:
    """Reclaim stale pending messages from any consumer in the group."""
    try:
        claimed = await r.xautoclaim(
            TASK_STREAM, CONSUMER_GROUP, WORKER_ID,
            min_idle_time=PEL_MIN_IDLE,
            start_id="0-0",
            count=50,
        )
        if claimed and len(claimed) > 1 and claimed[1]:
            for msg_id, data in claimed[1]:
                if data:
                    logger.info(f"Reclaimed pending task {msg_id}")
                    await process_task(data)
                    await r.xack(TASK_STREAM, CONSUMER_GROUP, msg_id)
    except Exception:
        logger.exception("PEL reclaim failed")


async def main() -> None:
    """Main worker loop: consume tasks from Redis Stream."""
    r = aioredis.from_url(settings.redis_url, decode_responses=True)

    # Create consumer group
    try:
        await r.xgroup_create(TASK_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    semaphore = asyncio.Semaphore(concurrency)

    logger.info(f"Worker {WORKER_ID} started (concurrency={concurrency})")

    # Initial PEL recovery on startup
    await _reclaim_pending(r)

    last_reclaim = asyncio.get_event_loop().time()

    try:
        while True:
            try:
                # Periodic PEL reclaim
                now = asyncio.get_event_loop().time()
                if now - last_reclaim > PEL_RECLAIM_INTERVAL:
                    await _reclaim_pending(r)
                    last_reclaim = now

                messages = await r.xreadgroup(
                    CONSUMER_GROUP,
                    WORKER_ID,
                    {TASK_STREAM: ">"},
                    count=1,
                    block=5000,
                )

                if not messages:
                    continue

                for stream_name, entries in messages:
                    for msg_id, data in entries:
                        await semaphore.acquire()

                        async def _run(mid=msg_id, d=data):
                            try:
                                await process_task(d)
                            except Exception:
                                logger.exception(f"Unhandled error processing {mid}")
                            finally:
                                await r.xack(TASK_STREAM, CONSUMER_GROUP, mid)
                                semaphore.release()

                        asyncio.create_task(_run())

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker loop error, reconnecting in 2s")
                await asyncio.sleep(2)
    finally:
        await r.aclose()
        await engine_db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
