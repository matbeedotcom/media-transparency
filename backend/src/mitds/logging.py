"""Structured logging configuration for MITDS.

Provides JSON-formatted logs for production and human-readable
logs for development.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Any

import json

from .config import get_settings


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        # Add common fields from record
        if record.funcName:
            log_data["function"] = record.funcName
        if record.lineno:
            log_data["line"] = record.lineno

        return json.dumps(log_data, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable log formatter for development."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging() -> None:
    """Configure logging based on settings."""
    settings = get_settings()

    # Set log level
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Create handler
    handler = logging.StreamHandler(sys.stdout)

    # Choose formatter based on environment
    if settings.log_format == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(TextFormatter())

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = [handler]

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("neo4j").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)

    # Log startup
    logger = get_logger(__name__)
    logger.info(
        "Logging initialized",
        extra={
            "environment": settings.environment,
            "log_level": settings.log_level,
            "log_format": settings.log_format,
        },
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds context to all log messages."""

    def process(
        self, msg: str, kwargs: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        """Process the logging message and add extra context."""
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def get_context_logger(name: str, **context: Any) -> LoggerAdapter:
    """Get a logger with additional context.

    Args:
        name: Logger name
        **context: Context fields to add to all log messages

    Returns:
        Logger adapter with context

    Usage:
        logger = get_context_logger(__name__, source="irs990", run_id="abc123")
        logger.info("Processing record")  # Includes source and run_id
    """
    return LoggerAdapter(get_logger(name), context)


# =========================
# Convenience functions
# =========================


def log_ingestion_start(source: str, run_id: str) -> None:
    """Log the start of an ingestion run."""
    logger = get_logger("mitds.ingestion")
    logger.info(
        f"Starting ingestion for {source}",
        extra={"source": source, "run_id": run_id, "event": "ingestion_start"},
    )


def log_ingestion_complete(
    source: str, run_id: str, records_processed: int, duration_seconds: float
) -> None:
    """Log the completion of an ingestion run."""
    logger = get_logger("mitds.ingestion")
    logger.info(
        f"Completed ingestion for {source}",
        extra={
            "source": source,
            "run_id": run_id,
            "records_processed": records_processed,
            "duration_seconds": duration_seconds,
            "event": "ingestion_complete",
        },
    )


def log_ingestion_error(source: str, run_id: str, error: str) -> None:
    """Log an ingestion error."""
    logger = get_logger("mitds.ingestion")
    logger.error(
        f"Ingestion error for {source}: {error}",
        extra={
            "source": source,
            "run_id": run_id,
            "error": error,
            "event": "ingestion_error",
        },
    )


def log_detection_result(
    detection_type: str, finding_id: str, score: float, flagged: bool
) -> None:
    """Log a detection result."""
    logger = get_logger("mitds.detection")
    logger.info(
        f"Detection result: {detection_type}",
        extra={
            "detection_type": detection_type,
            "finding_id": finding_id,
            "score": score,
            "flagged": flagged,
            "event": "detection_result",
        },
    )
