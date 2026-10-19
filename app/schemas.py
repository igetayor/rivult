import re
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class TaskState(str, Enum):
    """Normalized Celery & engine task execution states."""
    PENDING = "PENDING"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"


class AggregationWindow(str, Enum):
    """Allowed aggregation time windows."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ReportType(str, Enum):
    """Predefined analytics report templates."""
    EXECUTIVE_SUMMARY = "executive_summary"
    FINANCIAL_AUDIT = "financial_audit"
    USER_ENGAGEMENT = "user_engagement"
    SYSTEM_HEALTH = "system_health"


class ReportFormat(str, Enum):
    """Report export formats."""
    PDF = "pdf"
    CSV = "csv"
    JSON = "json"
    XLSX = "xlsx"


class TaskPriority(str, Enum):
    """Task priority queue routing."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class ImageOperation(str, Enum):
    """Supported batch image processing operations."""
    RESIZE = "resize"
    THUMBNAIL = "thumbnail"
    COMPRESS = "compress"
    WATERMARK = "watermark"


class ImageFormat(str, Enum):
    """Target image output formats."""
    WEBP = "webp"
    JPEG = "jpeg"
    PNG = "png"
    AVIF = "avif"


# -------------------------------------------------------------------------
# Request Schemas (Strict Pydantic v2: extra="forbid", stripped strings)
# -------------------------------------------------------------------------

class AggregationTaskRequest(BaseModel):
    """Strict schema for heavy data aggregation dispatch."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "dataset_id": "analytics_q3_metrics",
                "dimensions": ["region", "subscription_tier", "device_type"],
                "metrics": ["revenue", "dau", "churn_rate", "avg_latency_ms"],
                "start_date": "2026-01-01",
                "end_date": "2026-03-31",
                "aggregation_window": "daily",
                "include_outliers": False,
                "confidence_threshold": 0.95,
            }
        },
    )

    dataset_id: str = Field(
        ...,
        min_length=3,
        max_length=64,
        description="Unique identifier of dataset to aggregate",
    )
    dimensions: List[str] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Grouping dimensions for aggregation",
    )
    metrics: List[str] = Field(
        ...,
        min_length=1,
        max_length=20,
        description="Quantitative metrics to calculate",
    )
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: str = Field(..., description="End date (YYYY-MM-DD)")
    aggregation_window: AggregationWindow = Field(
        default=AggregationWindow.DAILY,
        description="Aggregation time bucket",
    )
    include_outliers: bool = Field(
        default=False,
        description="Whether to include extreme statistical anomalies",
    )
    confidence_threshold: float = Field(
        default=0.95,
        ge=0.5,
        le=0.999,
        description="Confidence interval threshold for anomaly filtering",
    )

    @field_validator("dataset_id")
    @classmethod
    def validate_dataset_id(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9_\-]+$", v):
            raise ValueError("dataset_id must contain only alphanumeric characters, dashes, or underscores")
        return v

    @field_validator("dimensions", "metrics")
    @classmethod
    def validate_non_empty_items(cls, v: List[str]) -> List[str]:
        cleaned = [item.strip() for item in v if item.strip()]
        if not cleaned:
            raise ValueError("List must contain at least one non-empty string identifier")
        return cleaned

    @model_validator(mode="after")
    def validate_date_range(self) -> "AggregationTaskRequest":
        try:
            start = date.fromisoformat(self.start_date)
            end = date.fromisoformat(self.end_date)
        except ValueError as exc:
            raise ValueError(f"Dates must be in strict ISO format YYYY-MM-DD: {exc}") from exc

        if start > end:
            raise ValueError(f"start_date ({start}) cannot be after end_date ({end})")
        return self


class ReportGenerationRequest(BaseModel):
    """Strict schema for batch report generation dispatch."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "report_type": "executive_summary",
                "format": "pdf",
                "recipient_emails": ["analytics-team@example.com", "cfo@example.com"],
                "filters": {"region": "EMEA", "min_spend": 1000},
                "priority": "high",
                "notify_on_completion": True,
            }
        },
    )

    report_type: ReportType = Field(..., description="Type of analytical report to generate")
    format: ReportFormat = Field(default=ReportFormat.PDF, description="Output file format")
    recipient_emails: List[str] = Field(
        ...,
        min_length=1,
        max_length=20,
        description="Recipient email addresses for completed report",
    )
    filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Custom filtering parameters applied to report generation",
    )
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL,
        description="Execution priority level",
    )
    notify_on_completion: bool = Field(
        default=True,
        description="Send email notification when task finishes",
    )

    @field_validator("recipient_emails")
    @classmethod
    def validate_emails(cls, v: List[str]) -> List[str]:
        email_regex = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
        cleaned = []
        for email in v:
            stripped = email.strip()
            if not email_regex.match(stripped):
                raise ValueError(f"Invalid email address format: {email}")
            cleaned.append(stripped)
        return cleaned


class ImagePipelineRequest(BaseModel):
    """Strict schema for batch image transformation pipelines."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "batch_id": "batch_media_2026_01",
                "source_urls": [
                    "https://assets.example.com/raw/img01.png",
                    "https://assets.example.com/raw/img02.png",
                ],
                "operations": ["resize", "compress", "thumbnail"],
                "target_format": "webp",
                "quality": 85,
                "watermark_text": "Rivult Engine",
            }
        },
    )

    batch_id: str = Field(..., min_length=3, max_length=64, description="Unique batch batch identifier")
    source_urls: List[str] = Field(..., min_length=1, max_length=50, description="List of source image URLs")
    operations: List[ImageOperation] = Field(..., min_length=1, description="List of transform operations")
    target_format: ImageFormat = Field(default=ImageFormat.WEBP, description="Target output format")
    quality: int = Field(default=85, ge=10, le=100, description="Compression quality 10-100")
    watermark_text: Optional[str] = Field(default=None, max_length=64, description="Optional watermark")

    @field_validator("source_urls")
    @classmethod
    def validate_urls(cls, v: List[str]) -> List[str]:
        for url in v:
            if not (url.startswith("http://") or url.startswith("https://")):
                raise ValueError(f"Invalid URL schema: {url}")
        return v


# -------------------------------------------------------------------------
# Response Schemas
# -------------------------------------------------------------------------

class TaskDispatchResponse(BaseModel):
    """Instant 202 response returned immediately upon task dispatch."""

    task_id: str = Field(..., description="Unique Celery task identifier")
    task_name: str = Field(..., description="Name of the dispatched task")
    status: TaskState = Field(default=TaskState.PENDING, description="Initial task status")
    dispatched_at: datetime = Field(..., description="UTC timestamp of dispatch")
    correlation_id: str = Field(..., description="Correlation ID for distributed tracing")
    status_url: str = Field(..., description="API URL to poll task status")


class TaskProgressData(BaseModel):
    """Detailed progress tracking payload."""
    current_step: str = Field(..., description="Current executing phase")
    percent: int = Field(default=0, ge=0, le=100, description="Completion percentage (0-100)")
    processed_items: int = Field(default=0, ge=0)
    total_items: int = Field(default=0, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TaskStatusResponse(BaseModel):
    """Comprehensive status of a background task."""

    task_id: str = Field(..., description="Unique Celery task identifier")
    task_name: Optional[str] = Field(default=None, description="Task registered function name")
    status: TaskState = Field(..., description="Current lifecycle state")
    progress: Optional[TaskProgressData] = Field(default=None, description="In-flight progress metadata")
    result: Optional[Any] = Field(default=None, description="Final return value if completed successfully")
    error: Optional[str] = Field(default=None, description="Error message if task failed")
    retries: int = Field(default=0, description="Number of retry attempts executed")
    execution_time_seconds: Optional[float] = Field(default=None, description="Duration in seconds")
    worker: Optional[str] = Field(default=None, description="Worker hostname executing task")
    created_at: Optional[str] = Field(default=None, description="Task creation timestamp")
    completed_at: Optional[str] = Field(default=None, description="Task completion timestamp")


class TaskRevokeResponse(BaseModel):
    """Result of revoking or cancelling a task."""
    task_id: str
    status: str
    message: str


class WorkerHealthResponse(BaseModel):
    """Worker pool status and broker connectivity check."""
    status: str
    broker: str
    result_backend: str
    active_workers: int
    worker_names: List[str]
    concurrency_per_worker: int


class EngineMetricsResponse(BaseModel):
    """Live engine orchestration statistics."""
    total_tasks_dispatched: int
    active_tasks: int
    completed_tasks: int
    failed_tasks: int
    retried_tasks: int
    uptime_seconds: float
