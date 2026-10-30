"""
TaskController — FastAPI route handlers for the Rivult Celery task engine.

Thin HTTP adapter: parse → delegate to TaskService → return response.
Zero Celery imports, zero task logic, zero registry manipulation here.
"""

from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Request, status

from app.schemas import (
    AggregationTaskRequest,
    EngineMetricsResponse,
    ImagePipelineRequest,
    ReportGenerationRequest,
    TaskDispatchResponse,
    TaskRevokeResponse,
    TaskStatusResponse,
    WorkerHealthResponse,
)
from app.api.services.task_service import TaskService

router = APIRouter(prefix="/api/v1", tags=["Tasks"])
_svc = TaskService()


@router.post(
    "/tasks/aggregation",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch a heavy data aggregation task to Celery workers",
)
async def dispatch_aggregation(payload: AggregationTaskRequest, request: Request) -> TaskDispatchResponse:
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    return _svc.dispatch_aggregation(payload, correlation_id=correlation_id)


@router.post(
    "/tasks/image-pipeline",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch an image transformation pipeline task",
)
async def dispatch_image_pipeline(payload: ImagePipelineRequest, request: Request) -> TaskDispatchResponse:
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    return _svc.dispatch_image_pipeline(payload, correlation_id=correlation_id)


@router.post(
    "/tasks/report",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch a batch report generation task",
    include_in_schema=False,
)
@router.post(
    "/tasks/reports",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch a batch report generation task",
)
async def dispatch_reports(payload: ReportGenerationRequest, request: Request) -> TaskDispatchResponse:
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    return _svc.dispatch_reports(payload, correlation_id=correlation_id)


@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    summary="Poll status and progress of a dispatched task",
)
async def get_task_status(task_id: str) -> TaskStatusResponse:
    return _svc.get_task_status(task_id)


@router.delete(
    "/tasks/{task_id}",
    response_model=TaskRevokeResponse,
    summary="Revoke and terminate a running or pending task",
)
@router.post(
    "/tasks/{task_id}/revoke",
    response_model=TaskRevokeResponse,
    summary="Revoke and terminate a running or pending task via POST",
    include_in_schema=False,
)
async def revoke_task(task_id: str) -> TaskRevokeResponse:
    return _svc.revoke_task(task_id)


@router.get(
    "/tasks",
    summary="List recently dispatched tasks from in-memory registry",
)
async def list_tasks(limit: int = 20) -> List[Dict[str, Any]]:
    return _svc.list_tasks(limit=limit)


@router.get(
    "/workers/health",
    response_model=WorkerHealthResponse,
    summary="Check Celery worker pool health and active task count",
)
async def worker_health() -> WorkerHealthResponse:
    return _svc.get_worker_health()


@router.get(
    "/metrics",
    response_model=EngineMetricsResponse,
    summary="Engine-level telemetry: throughput, uptime, error rates",
)
async def engine_metrics() -> EngineMetricsResponse:
    return _svc.get_engine_metrics()
