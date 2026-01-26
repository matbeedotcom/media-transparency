"""Celery worker configuration for MITDS.

Handles background task execution for data ingestion,
detection analysis, and report generation.
"""

from celery import Celery
from celery.schedules import crontab

from .config import get_settings

settings = get_settings()

# Create Celery app
app = Celery(
    "mitds",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

# Celery configuration
app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Task execution
    task_acks_late=True,  # Acknowledge tasks after completion
    task_reject_on_worker_lost=True,  # Requeue tasks if worker dies
    task_time_limit=3600,  # 1 hour max per task
    task_soft_time_limit=3300,  # Soft limit at 55 minutes
    # Worker settings
    worker_prefetch_multiplier=1,  # Fetch one task at a time
    worker_concurrency=4,  # 4 concurrent workers
    # Result backend
    result_expires=86400,  # Results expire after 24 hours
    # Task routing
    task_routes={
        "mitds.ingestion.*": {"queue": "ingestion"},
        "mitds.detection.*": {"queue": "detection"},
        "mitds.reporting.*": {"queue": "reporting"},
    },
    # Retry settings
    task_default_retry_delay=60,  # 1 minute between retries
    task_max_retries=3,
)

# Beat schedule for periodic tasks
app.conf.beat_schedule = {
    # IRS 990 weekly ingestion (Sunday at 2 AM UTC)
    "ingest-irs990-weekly": {
        "task": "mitds.ingestion.tasks.ingest_irs990",
        "schedule": crontab(hour=2, minute=0, day_of_week=0),
        "options": {"queue": "ingestion"},
    },
    # CRA charities weekly ingestion (Sunday at 4 AM UTC)
    "ingest-cra-weekly": {
        "task": "mitds.ingestion.tasks.ingest_cra",
        "schedule": crontab(hour=4, minute=0, day_of_week=0),
        "options": {"queue": "ingestion"},
    },
    # OpenCorporates weekly ingestion (Monday at 2 AM UTC)
    "ingest-opencorporates-weekly": {
        "task": "mitds.ingestion.tasks.ingest_opencorporates",
        "schedule": crontab(hour=2, minute=0, day_of_week=1),
        "options": {"queue": "ingestion"},
        "kwargs": {"enabled": settings.enable_opencorporates_ingestion},
    },
    # Meta Ad Library daily ingestion (every day at 6 AM UTC)
    "ingest-meta-ads-daily": {
        "task": "mitds.ingestion.tasks.ingest_meta_ads",
        "schedule": crontab(hour=6, minute=0),
        "options": {"queue": "ingestion"},
        "kwargs": {"enabled": settings.enable_meta_ads_ingestion},
    },
    # Entity resolution daily (every day at 8 AM UTC)
    "run-entity-resolution": {
        "task": "mitds.resolution.tasks.run_matching",
        "schedule": crontab(hour=8, minute=0),
        "options": {"queue": "detection"},
    },
    # Data quality metrics daily (every day at 10 AM UTC)
    "calculate-quality-metrics": {
        "task": "mitds.quality.tasks.calculate_metrics",
        "schedule": crontab(hour=10, minute=0),
        "options": {"queue": "detection"},
    },
}


# Autodiscover tasks in the mitds package
app.autodiscover_tasks(
    [
        "mitds.ingestion.tasks",
        "mitds.resolution.tasks",
        "mitds.detection.tasks",
        "mitds.reporting.tasks",
        "mitds.quality.tasks",
    ],
    force=True,
)


@app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    print(f"Request: {self.request!r}")
    return {"status": "ok", "worker": self.request.hostname}
