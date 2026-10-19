from functools import lru_cache
from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support and strict Pydantic v2 typing."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = Field(default="Rivult Background Task Engine", description="Service name")
    app_version: str = Field(default="1.0.0", description="API version")
    environment: str = Field(default="development", description="Runtime environment")
    debug: bool = Field(default=False, description="Debug mode")
    host: str = Field(default="0.0.0.0", description="FastAPI host")
    port: int = Field(default=8084, description="FastAPI listening port")
    cors_origins: List[str] = Field(default=["*"], description="Allowed CORS origins")

    # Celery & Broker Settings
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0",
        description="Celery broker URL (Redis or RabbitMQ)",
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/1",
        description="Persistent Celery result backend URL",
    )
    celery_task_always_eager: bool = Field(
        default=False,
        description="Execute tasks synchronously in-process (ideal for dev/testing without Redis)",
    )
    celery_worker_concurrency: int = Field(
        default=4,
        description="Number of concurrent worker processes/threads",
    )
    celery_worker_prefetch_multiplier: int = Field(
        default=1,
        description="Worker prefetch multiplier to avoid task hoarding",
    )
    celery_task_acks_late: bool = Field(
        default=True,
        description="Acknowledge tasks only after completion to prevent loss on worker crash",
    )
    celery_result_expires: int = Field(
        default=86400,
        description="Task result retention in seconds (default 24h)",
    )
    celery_task_time_limit: int = Field(
        default=1800,
        description="Hard task time limit in seconds (30m)",
    )
    celery_task_soft_time_limit: int = Field(
        default=1500,
        description="Soft task time limit in seconds (25m)",
    )

    # Logging
    log_level: str = Field(default="INFO", description="Log level: DEBUG, INFO, WARNING, ERROR")
    log_json_format: bool = Field(default=True, description="Output structured JSON logs")


@lru_cache()
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
