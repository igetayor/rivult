import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from app.logger import logger


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Ensures every HTTP request carries an X-Correlation-ID for distributed tracing,
    records execution latency, and produces structured access logs.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID") or f"corr_{uuid.uuid4().hex}"
        request.state.correlation_id = correlation_id

        start_time = time.perf_counter()

        logger.info(
            f"Incoming {request.method} {request.url.path}",
            extra={
                "method": request.method,
                "path": request.url.path,
                "correlation_id": correlation_id,
            },
        )

        response = await call_next(request)

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        response.headers["X-Correlation-ID"] = correlation_id
        response.headers["X-Process-Time-Ms"] = str(duration_ms)

        logger.info(
            f"Completed {request.method} {request.url.path} with status {response.status_code} in {duration_ms}ms",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "correlation_id": correlation_id,
                "duration_ms": duration_ms,
            },
        )

        return response
