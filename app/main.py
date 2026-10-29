from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api.middleware import CorrelationIdMiddleware
from app.api.controllers.task_controller import router as task_router
from app.config import get_settings
from app.logger import logger

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle management."""
    logger.info(
        f"Starting {settings.app_name} v{settings.app_version} on {settings.host}:{settings.port}",
        extra={
            "app_name": settings.app_name,
            "version": settings.app_version,
            "broker": settings.celery_broker_url,
            "result_backend": settings.celery_result_backend,
            "eager_mode": settings.celery_task_always_eager,
        },
    )
    yield
    logger.info(f"Shutting down {settings.app_name}")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Scalable background task orchestration engine using Python, FastAPI, and Celery backed by Redis.",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Correlation ID and latency tracking middleware
app.add_middleware(CorrelationIdMiddleware)

# Cross-Origin Resource Sharing (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(task_router)

# Mount frontend web artifacts
web_dir = Path(__file__).resolve().parent.parent / "web"
if web_dir.exists():
    app.mount("/static", StaticFiles(directory=str(web_dir)), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    async def serve_index():
        index_file = web_dir / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return HTMLResponse("<h1>Rivult Engine UI Loaded</h1>")

    @app.get("/style.css", include_in_schema=False)
    async def serve_style():
        style_file = web_dir / "style.css"
        if style_file.exists():
            return FileResponse(style_file, media_type="text/css")
        return HTMLResponse("", status_code=404)


@app.get("/healthz", tags=["System"])
async def health_check():
    """Liveness probe endpoint."""
    return {"status": "ok", "service": "rivult", "version": settings.app_version}
