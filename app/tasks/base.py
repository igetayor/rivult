import time
import logging
from typing import Any, Dict, Optional
from celery import Task
from app.logger import logger


class TransientUpstreamError(Exception):
    """Raised when a temporary network, I/O, or resource lock failure occurs."""
    pass


class BaseTask(Task):
    """Base Celery task providing structured logging, telemetry, and retry handling."""

    abstract = True

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        correlation_id = kwargs.get("correlation_id", "unknown")
        logger.info(
            f"Starting task {self.name} [{self.request.id}]",
            extra={
                "task_id": self.request.id,
                "task_name": self.name,
                "correlation_id": correlation_id,
            },
        )
        try:
            result = super().__call__(*args, **kwargs)
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            logger.info(
                f"Completed task {self.name} [{self.request.id}] in {duration_ms}ms",
                extra={
                    "task_id": self.request.id,
                    "task_name": self.name,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            return result
        except Exception as exc:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            logger.error(
                f"Task {self.name} [{self.request.id}] failed after {duration_ms}ms: {exc}",
                exc_info=True,
                extra={
                    "task_id": self.request.id,
                    "task_name": self.name,
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                },
            )
            raise

    def on_retry(self, exc: Exception, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        """Callback invoked whenever Celery initiates an automatic retry."""
        logger.warning(
            f"Task {self.name} [{task_id}] retrying (attempt {self.request.retries}/{self.max_retries}): {exc}",
            extra={
                "task_id": task_id,
                "task_name": self.name,
                "retries": self.request.retries,
            },
        )
        super().on_retry(exc, task_id, args, kwargs, einfo)

    def on_failure(self, exc: Exception, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        """Callback invoked when task exhausts all retry attempts or encounters fatal error."""
        logger.error(
            f"Task {self.name} [{task_id}] exhausted retries or failed fatally: {exc}",
            extra={
                "task_id": task_id,
                "task_name": self.name,
            },
        )
        super().on_failure(exc, task_id, args, kwargs, einfo)
