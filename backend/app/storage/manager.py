from __future__ import annotations
from app.config import settings
from app.storage.base import StorageBackend
from app.storage.local import LocalStorageBackend


_backend: StorageBackend | None = None


def get_storage() -> StorageBackend:
    global _backend
    if _backend is None:
        if settings.storage_backend == "minio":
            from app.storage.minio_backend import MinioStorageBackend
            _backend = MinioStorageBackend(
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                bucket=settings.minio_bucket,
                secure=settings.minio_secure,
            )
        else:
            _backend = LocalStorageBackend(root=settings.storage_local_root)
    return _backend
