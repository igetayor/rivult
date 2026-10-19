from celery import Celery
from app.config import get_settings

settings = get_settings()

celery_app = Celery("rivult")

# Configure Celery with strict production-grade parameters
celery_app.conf.update(
    broker_url=settings.celery_broker_url,
    result_backend=settings.celery_result_backend,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,

    # Task tracking and persistent result backend settings
    task_track_started=True,
    task_send_sent_event=True,
    result_expires=settings.celery_result_expires,
    result_persistent=True,

    # Worker concurrency and prefetch controls
    worker_concurrency=settings.celery_worker_concurrency,
    worker_prefetch_multiplier=settings.celery_worker_prefetch_multiplier,
    task_acks_late=settings.celery_task_acks_late,
    task_reject_on_worker_lost=True,

    # Time limits to prevent worker hangs
    task_time_limit=settings.celery_task_time_limit,
    task_soft_time_limit=settings.celery_task_soft_time_limit,

    # Eager mode toggle (for in-memory local testing or dev fallback)
    task_always_eager=settings.celery_task_always_eager,
    task_eager_propagates=True,

    # Dedicated priority queues and task routing
    task_queues={
        "default": {"exchange": "default", "routing_key": "default"},
        "data_heavy": {"exchange": "data_heavy", "routing_key": "data_heavy"},
        "reports": {"exchange": "reports", "routing_key": "reports"},
        "media": {"exchange": "media", "routing_key": "media"},
    },
    task_default_queue="default",
    task_routes={
        "tasks.heavy_data_aggregation": {"queue": "data_heavy"},
        "tasks.batch_report_generation": {"queue": "reports"},
        "tasks.image_transformation_pipeline": {"queue": "media"},
    },
)

# Auto-discover task modules
celery_app.autodiscover_tasks(["app.tasks.aggregation", "app.tasks.reports", "app.tasks.image_pipeline"])
