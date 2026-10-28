import time
from datetime import datetime, timezone
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Request, status
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
router = APIRouter(prefix="/api/v1", tags=["Tasks"])

# In-memory tracking of dispatched tasks for listing/dashboard preview
_dispatched_tasks_registry: List[Dict[str, Any]] = []
_engine_stats = {
    "total_dispatched": 0,
    "completed": 0,
    "failed": 0,
    "retried": 0,
    "start_time": time.time(),
}


def _record_dispatch(task_id: str, task_name: str, correlation_id: str, payload_summary: Dict[str, Any]) -> None:
    """Records task metadata for listing and live frontend feeds."""
    entry = {
        "task_id": task_id,
        "task_name": task_name,
        "correlation_id": correlation_id,
        "status": TaskState.PENDING.value,
        "dispatched_at": datetime.now(timezone.utc).isoformat(),
        "summary": payload_summary,
    }
    _dispatched_tasks_registry.insert(0, entry)
    if len(_dispatched_tasks_registry) > 100:
        _dispatched_tasks_registry.pop()
    _engine_stats["total_dispatched"] += 1


@router.post(
    "/tasks/aggregation",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch heavy multi-dimensional data aggregation task",
)
async def dispatch_aggregation_task(
    payload: AggregationTaskRequest,
    request: Request,
) -> TaskDispatchResponse:
    """
    Asynchronously dispatches a heavy data aggregation job.
    Returns task ID immediately without waiting for execution.
    """
    correlation_id = getattr(request.state, 
    "correlation_id", "unknown")

    async_result = heavy_data_aggregation.apply_async(
        args=[payload.model_dump()],
        kwargs={"correlation_id": correlation_id},
    )

    _record_dispatch(
        task_id=async_result.id,
        task_name="tasks.heavy_data_aggregation",
        correlation_id=correlation_id,
        payload_summary={"dataset_id": payload.dataset_id, "window": payload.aggregation_window.value},
    )

    logger.info(
        f"Dispatched aggregation task {async_result.id} for dataset {payload.dataset_id}",
        extra={"task_id": async_result.id, "correlation_id": correlation_id},
    )

    return TaskDispatchResponse(
        task_id=async_result.id,
        task_name="tasks.heavy_data_aggregation",
        status=TaskState.PENDING,
        dispatched_at=datetime.now(timezone.utc),
        correlation_id=correlation_id,
        status_url=f"/api/v1/tasks/{async_result.id}",
    )


@router.post(
    "/tasks/report",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch batch analytical report generation",
)
async def dispatch_report_task(
    payload: ReportGenerationRequest,
    request: Request,
) -> TaskDispatchResponse:
    """
    Asynchronously dispatches an analytical report generation task.
    Returns task ID immediately.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")

    async_result = batch_report_generation.apply_async(
        args=[payload.model_dump()],
        kwargs={"correlation_id": correlation_id},
    )

    _record_dispatch(
        task_id=async_result.id,
        task_name="tasks.batch_report_generation",
        correlation_id=correlation_id,
        payload_summary={"report_type": payload.report_type.value, "format": payload.format.value},
    )

    logger.info(
        f"Dispatched report task {async_result.id} [{payload.report_type.value}]",
        extra={"task_id": async_result.id, "correlation_id": correlation_id},
    )

    return TaskDispatchResponse(
        task_id=async_result.id,
        task_name="tasks.batch_report_generation",
        status=TaskState.PENDING,
        dispatched_at=datetime.now(timezone.utc),
        correlation_id=correlation_id,
        status_url=f"/api/v1/tasks/{async_result.id}",
    )


@router.post(
    "/tasks/image-pipeline",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch batch image asset transformation pipeline",
)
async def dispatch_image_pipeline(
    payload: ImagePipelineRequest,
    request: Request,
) -> TaskDispatchResponse:
    """
    Asynchronously dispatches a batch image transformation pipeline.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")

    async_result = image_transformation_pipeline.apply_async(
        args=[payload.model_dump()],
        kwargs={"correlation_id": correlation_id},
    )

    _record_dispatch(
        task_id=async_result.id,
        task_name="tasks.image_transformation_pipeline",
        correlation_id=correlation_id,
        payload_summary={"batch_id": payload.batch_id, "count": len(payload.source_urls)},
    )

    return TaskDispatchResponse(
        task_id=async_result.id,
        task_name="tasks.image_transformation_pipeline",
        status=TaskState.PENDING,
        dispatched_at=datetime.now(timezone.utc),
        correlation_id=correlation_id,
        status_url=f"/api/v1/tasks/{async_result.id}",
    )


@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    summary="Retrieve current execution status and persistent results",
)
async def get_task_status(task_id: str) -> TaskStatusResponse:
    """
    Polls the persistent result backend for task state, progress, or completed results.
    """
    async_result = AsyncResult(task_id, app=celery_app)

    state = async_result.state
    progress: TaskProgressData | None = None
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

    # Normalize state to TaskState enum
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


@router.post(
    "/tasks/{task_id}/revoke",
    response_model=TaskRevokeResponse,
    summary="Revoke or terminate an active or pending task",
)
async def revoke_task(task_id: str) -> TaskRevokeResponse:
    """
    Revokes the task from Celery broker/workers and terminates execution if running.
    """
    try:
        celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        logger.warning(f"Task {task_id} revoked by operator request", extra={"task_id": task_id})
        return TaskRevokeResponse(
            task_id=task_id,
            status="REVOKED",
            message=f"Task {task_id} successfully revoked",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to revoke task {task_id}: {str(exc)}",
        )


@router.get(
    "/tasks",
    summary="List recently dispatched tasks",
)
async def list_dispatched_tasks(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Returns list of recently dispatched tasks with current updated statuses.
    """
    updated_list = []
    for entry in _dispatched_tasks_registry[:limit]:
        async_result = AsyncResult(entry["task_id"], app=celery_app)
        item = dict(entry)
        item["status"] = async_result.state
        if async_result.state == "SUCCESS":
            item["completed"] = True
        updated_list.append(item)
    return updated_list


@router.get(
    "/workers/health",
    response_model=WorkerHealthResponse,
    summary="Worker pool status and broker connectivity",
)
async def get_worker_health() -> WorkerHealthResponse:
    """
    Inspects registered Celery workers and broker reachability.
    """
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


@router.get(
    "/metrics",
    response_model=EngineMetricsResponse,
    summary="Live orchestration telemetry metrics",
)
async def get_engine_metrics() -> EngineMetricsResponse:
    """
    Returns live counters on dispatched tasks and orchestration runtime.
    """
    uptime = time.time() - _engine_stats["start_time"]
    active = sum(1 for t in _dispatched_tasks_registry if AsyncResult(t["task_id"], app=celery_app).state in ("PENDING", "PROGRESS", "STARTED"))
    completed = sum(1 for t in _dispatched_tasks_registry if AsyncResult(t["task_id"], app=celery_app).state == "SUCCESS")
    failed = sum(1 for t in _dispatched_tasks_registry if AsyncResult(t["task_id"], app=celery_app).state == "FAILURE")

    return EngineMetricsResponse(
        total_tasks_dispatched=_engine_stats["total_dispatched"],
        active_tasks=active,
        completed_tasks=completed,
        failed_tasks=failed,
        retried_tasks=_engine_stats["retried"],
        uptime_seconds=round(uptime, 2),
    )
