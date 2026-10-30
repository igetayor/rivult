"""
TaskService — business logic for Celery task dispatch, status inspection,
and engine telemetry. Decoupled from FastAPI — can be called by CLI tools too.
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from celery.result import AsyncResult

from app.celery_app import celery_app
from app.config import get_settings
from app.logger import logger
from app.schemas import (
    AggregationTaskRequest,
    EngineMetricsResponse,
    ImagePipelineRequest,
    ReportGenerationRequest,
    TaskDispatchResponse,
    TaskProgressData,
    TaskRevokeResponse,
    TaskState,
    TaskStatusResponse,
    WorkerHealthResponse,
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
    def dispatch_aggregation(self, req: AggregationTaskRequest, correlation_id: Optional[str] = None) -> TaskDispatchResponse:
        corr_id = correlation_id or f"agg_{uuid.uuid4().hex[:10]}"
        task = heavy_data_aggregation.apply_async(
            args=[req.model_dump()],
            kwargs={"correlation_id": corr_id},
        )
        task_name = "tasks.heavy_data_aggregation"
        _record_dispatch(task.id, task_name, corr_id, {"dataset_id": req.dataset_id, "window": req.aggregation_window.value})
        logger.info(f"Dispatched aggregation task {task.id}", extra={"task_id": task.id, "correlation_id": corr_id})
        return TaskDispatchResponse(
            task_id=task.id,
            task_name=task_name,
            status=TaskState.PENDING,
            dispatched_at=datetime.now(timezone.utc),
            correlation_id=corr_id,
            status_url=f"/api/v1/tasks/{task.id}",
        )

    def dispatch_image_pipeline(self, req: ImagePipelineRequest, correlation_id: Optional[str] = None) -> TaskDispatchResponse:
        corr_id = correlation_id or f"img_{uuid.uuid4().hex[:10]}"
        task = image_transformation_pipeline.apply_async(
            args=[req.model_dump()],
            kwargs={"correlation_id": corr_id},
        )
        task_name = "tasks.image_transformation_pipeline"
        _record_dispatch(task.id, task_name, corr_id, {"batch_id": req.batch_id, "count": len(req.source_urls)})
        logger.info(f"Dispatched image pipeline {task.id}", extra={"task_id": task.id, "correlation_id": corr_id})
        return TaskDispatchResponse(
            task_id=task.id,
            task_name=task_name,
            status=TaskState.PENDING,
            dispatched_at=datetime.now(timezone.utc),
            correlation_id=corr_id,
            status_url=f"/api/v1/tasks/{task.id}",
        )

    def dispatch_reports(self, req: ReportGenerationRequest, correlation_id: Optional[str] = None) -> TaskDispatchResponse:
        corr_id = correlation_id or f"rpt_{uuid.uuid4().hex[:10]}"
        task = batch_report_generation.apply_async(
            args=[req.model_dump()],
            kwargs={"correlation_id": corr_id},
        )
        task_name = "tasks.batch_report_generation"
        _record_dispatch(task.id, task_name, corr_id, {"report_type": req.report_type.value, "format": req.format.value})
        logger.info(f"Dispatched report task {task.id}", extra={"task_id": task.id, "correlation_id": corr_id})
        return TaskDispatchResponse(
            task_id=task.id,
            task_name=task_name,
            status=TaskState.PENDING,
            dispatched_at=datetime.now(timezone.utc),
            correlation_id=corr_id,
            status_url=f"/api/v1/tasks/{task.id}",
        )

    # ------------------------------------------------------------------ #
    #  Status & Introspection                                              #
    # ------------------------------------------------------------------ #
    def get_task_status(self, task_id: str) -> TaskStatusResponse:
        async_result = AsyncResult(task_id, app=celery_app)
        state = async_result.state
        progress: Optional[TaskProgressData] = None
        result_data = None
        error_msg = None

        if state == "PROGRESS":
            info = async_result.info or {}
            progress = TaskProgressData(
                current_step=info.get("current_step", "EXECUTING"),
                percent=info.get("percent", 0),
                processed_items=info.get("processed_items", 0),
                total_items=info.get("total_items", 0),
                metadata=info,
            )
        elif state == "SUCCESS":
            result_data = async_result.result
        elif state == "FAILURE":
            error_msg = str(async_result.result)

        try:
            task_state = TaskState(state)
        except ValueError:
            task_state = TaskState.PENDING

        retries_val = getattr(async_result, "retries", 0)
        return TaskStatusResponse(
            task_id=task_id,
            status=task_state,
            progress=progress,
            result=result_data,
            error=error_msg,
            retries=int(retries_val) if retries_val is not None else 0,
        )

    def revoke_task(self, task_id: str) -> TaskRevokeResponse:
        celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        logger.warning(f"Task {task_id} revoked by operator request", extra={"task_id": task_id})
        return TaskRevokeResponse(
            task_id=task_id,
            status="REVOKED",
            message=f"Task {task_id} successfully revoked",
        )

    def list_tasks(self, limit: int = 20) -> List[Dict[str, Any]]:
        updated_list = []
        for entry in _dispatched_registry[:limit]:
            async_result = AsyncResult(entry["task_id"], app=celery_app)
            item = dict(entry)
            item["status"] = async_result.state
            if async_result.state == "SUCCESS":
                item["completed"] = True
            updated_list.append(item)
        return updated_list

    def get_worker_health(self) -> WorkerHealthResponse:
        active_worker_names = []
        try:
            inspector = celery_app.control.inspect(timeout=1.0)
            ping_res = inspector.ping() if inspector else None
            if ping_res:
                active_worker_names = list(ping_res.keys())
        except Exception:
            active_worker_names = []

        return WorkerHealthResponse(
            status="HEALTHY" if active_worker_names or settings.celery_task_always_eager else "STANDALONE_READY",
            broker=settings.celery_broker_url,
            result_backend=settings.celery_result_backend,
            active_workers=len(active_worker_names),
            worker_names=active_worker_names,
            concurrency_per_worker=settings.celery_worker_concurrency,
        )

    def get_engine_metrics(self) -> EngineMetricsResponse:
        uptime = round(time.time() - _engine_stats["start_time"], 2)
        active = sum(1 for t in _dispatched_registry if AsyncResult(t["task_id"], app=celery_app).state in ("PENDING", "PROGRESS", "STARTED"))
        completed = sum(1 for t in _dispatched_registry if AsyncResult(t["task_id"], app=celery_app).state == "SUCCESS")
        failed = sum(1 for t in _dispatched_registry if AsyncResult(t["task_id"], app=celery_app).state == "FAILURE")

        return EngineMetricsResponse(
            total_tasks_dispatched=_engine_stats["total_dispatched"],
            active_tasks=active,
            completed_tasks=completed,
            failed_tasks=failed,
            retried_tasks=_engine_stats["retried"],
            uptime_seconds=uptime,
        )
