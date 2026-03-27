from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://vp:vp_secret@localhost:5432/videoprocess"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Storage
    storage_backend: str = "local"  # "local" or "minio"
    storage_local_root: str = "/tmp/vp_storage"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "videoprocess"
    minio_secure: bool = False

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    cors_origins: list[str] = ["*"]
    exo_watchdog_url: str = "http://exo-watchdog.constructure-monitor.svc.cluster.local:8000"

    # Shared retrieval services
    embedding_gateway_url: str = "http://embedding-gateway.constructure-infra.svc.cluster.local:8080"
    qdrant_url: str = "http://qdrant.constructure-infra.svc.cluster.local:6333"
    material_qdrant_collection: str = "videoprocess_material_clips"
    material_lighthouse_url: str = ""
    material_univtg_url: str = ""

    # Video worker features
    video_use_gpu: bool = False
    video_use_videotoolbox: bool = False

    model_config = {"env_prefix": "", "case_sensitive": False}


settings = Settings()
