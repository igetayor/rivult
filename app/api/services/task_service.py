"""
TaskService — business logic for Celery task dispatch, status inspection,
and engine telemetry. Decoupled from FastAPI — can be called by CLI tools too.
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from celery.result import AsyncResult

from app.celery_app import celery_app
from app.config import get_settings
from app.logger import logger
from app.schemas import (
    AggregationTaskRequest,
    ImagePipelineRequest,
    ReportGenerationRequest,
    TaskDispatchResponse,
    TaskState,
    TaskStatusResponse,
    TaskProgressData,
    WorkerHealthResponse,
    EngineMetricsResponse,
)
from app.tasks.aggregation import heavy_data_aggregation
from app.tasks.image_pipeline import image_transformation_pipeline
from app.tasks.reports import batch_report_generation

settings = get_settings()

# In-memory registry for live dashboard preview
_dispatched_registry: List[Dict[str, Any]] = []
_engine_stats = {
    "total_dispatched": 0,
    "completed": 0,
    "failed": 0,
    "retried": 0,
    "start_time": time.time(),
}


def _record_dispatch(task_id: str, task_name: str, correlation_id: str, summary: Dict[str, Any]) -> None:
    _dispatched_registry.insert(0, {
        "task_id": task_id,
        "task_name": task_name,
        "correlation_id": correlation_id,
        "status": TaskState.PENDING.value,
        "dispatched_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
    })
    if len(_dispatched_registry) > 100:
        _dispatched_registry.pop()
    _engine_stats["total_dispatched"] += 1


class TaskService:
    # ------------------------------------------------------------------ #
    #  Dispatch Operations                                                 #
    # ------------------------------------------------------------------ #
    def dispatch_aggregation(self, req: AggregationTaskRequest) -> TaskDispatchResponse:
        correlation_id = f"agg_{uuid.uuid4().hex[:10]}"
        task = heavy_data_aggregation.apply_async(
            kwargs=req.model_dump(),
            countdown=0,
        )
        _record_dispatch(task.id, "heavy_data_aggregation", correlation_id, req.model_dump())
        return TaskDispatchResponse(task_id=task.id, correlation_id=correlation_id, status=TaskState.PENDING)

    def dispatch_image_pipeline(self, req: ImagePipelineRequest) -> TaskDispatchResponse:
        correlation_id = f"img_{uuid.uuid4().hex[:10]}"
        task = image_transformation_pipeline.apply_async(kwargs=req.model_dump())
        _record_dispatch(task.id, "image_transformation_pipeline", correlation_id, req.model_dump())
        return TaskDispatchResponse(task_id=task.id, correlation_id=correlation_id, status=TaskState.PENDING)

    def dispatch_reports(self, req: ReportGenerationRequest) -> TaskDispatchResponse:
        correlation_id = f"rpt_{uuid.uuid4().hex[:10]}"
        task = batch_report_generation.apply_async(kwargs=req.model_dump())
        _record_dispatch(task.id, "batch_report_generation", correlation_id, req.model_dump())
        return TaskDispatchResponse(task_id=task.id, correlation_id=correlation_id, status=TaskState.PENDING)

    # ------------------------------------------------------------------ #
    #  Status & Introspection                                              #
    # ------------------------------------------------------------------ #
    def get_task_status(self, task_id: str) -> TaskStatusResponse:
        result = AsyncResult(task_id, app=celery_app)
        progress = None
        if result.info and isinstance(result.info, dict):
            progress = TaskProgressData(**result.info) if "current" in result.info else None
        return TaskStatusResponse(
            task_id=task_id,
            state=result.state,
            result=result.result if result.ready() else None,
            progress=progress,
        )

    def revoke_task(self, task_id: str) -> None:
        celery_app.control.revoke(task_id, terminate=True)

    def list_tasks(self, limit: int = 20) -> List[Dict[str, Any]]:
        return _dispatched_registry[:limit]

    def get_worker_health(self) -> WorkerHealthResponse:
        inspect = celery_app.control.inspect(timeout=1.0)
        active = inspect.active() or {}
        return WorkerHealthResponse(
            workers_online=len(active),
            active_tasks=sum(len(v) for v in active.values()),
            broker_url=settings.celery_broker_url,
        )

    def get_engine_metrics(self) -> EngineMetricsResponse:
        uptime = round(time.time() - _engine_stats["start_time"], 2)
        return EngineMetricsResponse(
            total_dispatched=_engine_stats["total_dispatched"],
            completed=_engine_stats["completed"],
            failed=_engine_stats["failed"],
            retried=_engine_stats["retried"],
            uptime_seconds=uptime,
        )
