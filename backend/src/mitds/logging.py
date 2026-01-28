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
    logging.getLogger("pdfminer").setLevel(logging.WARNING)
    logging.getLogger("pdfplumber").setLevel(logging.WARNING)

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


def log_ingestion_record(
    source: str,
    run_id: str,
    record_id: str,
    action: str,
    entity_type: str | None = None,
) -> None:
    """Log processing of an individual record.

    Args:
        source: Data source identifier
        run_id: Ingestion run identifier
        record_id: Record identifier
        action: Action taken (created, updated, skipped)
        entity_type: Type of entity processed
    """
    logger = get_logger("mitds.ingestion")
    logger.debug(
        f"Processed record {record_id}",
        extra={
            "source": source,
            "run_id": run_id,
            "record_id": record_id,
            "action": action,
            "entity_type": entity_type,
            "event": "record_processed",
        },
    )


def log_ingestion_batch(
    source: str,
    run_id: str,
    batch_number: int,
    records_in_batch: int,
    total_processed: int,
) -> None:
    """Log completion of a batch within an ingestion run.

    Args:
        source: Data source identifier
        run_id: Ingestion run identifier
        batch_number: Batch sequence number
        records_in_batch: Records processed in this batch
        total_processed: Total records processed so far
    """
    logger = get_logger("mitds.ingestion")
    logger.info(
        f"Completed batch {batch_number} for {source}",
        extra={
            "source": source,
            "run_id": run_id,
            "batch_number": batch_number,
            "records_in_batch": records_in_batch,
            "total_processed": total_processed,
            "event": "batch_complete",
        },
    )


def log_ingestion_progress(
    source: str,
    run_id: str,
    progress_percent: float,
    records_processed: int,
    records_total: int | None = None,
) -> None:
    """Log ingestion progress update.

    Args:
        source: Data source identifier
        run_id: Ingestion run identifier
        progress_percent: Percentage complete (0-100)
        records_processed: Records processed so far
        records_total: Total records expected (if known)
    """
    logger = get_logger("mitds.ingestion")
    logger.info(
        f"Ingestion progress for {source}: {progress_percent:.1f}%",
        extra={
            "source": source,
            "run_id": run_id,
            "progress_percent": progress_percent,
            "records_processed": records_processed,
            "records_total": records_total,
            "event": "ingestion_progress",
        },
    )


def log_data_quality(
    source: str,
    run_id: str,
    dimension: str,
    score: float,
    passed: bool,
    details: dict[str, Any] | None = None,
) -> None:
    """Log data quality measurement.

    Args:
        source: Data source identifier
        run_id: Ingestion run identifier
        dimension: Quality dimension measured
        score: Quality score (0-1)
        passed: Whether threshold was met
        details: Additional measurement details
    """
    logger = get_logger("mitds.quality")
    level = logging.INFO if passed else logging.WARNING
    logger.log(
        level,
        f"Quality {dimension} for {source}: {score:.2%}",
        extra={
            "source": source,
            "run_id": run_id,
            "dimension": dimension,
            "score": score,
            "passed": passed,
            "details": details,
            "event": "quality_measurement",
        },
    )


def log_api_request(
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    user_id: str | None = None,
    request_id: str | None = None,
) -> None:
    """Log an API request.

    Args:
        method: HTTP method
        path: Request path
        status_code: Response status code
        duration_ms: Request duration in milliseconds
        user_id: Authenticated user ID
        request_id: Request correlation ID
    """
    logger = get_logger("mitds.api")
    logger.info(
        f"{method} {path} - {status_code}",
        extra={
            "method": method,
            "path": path,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "user_id": user_id,
            "request_id": request_id,
            "event": "api_request",
        },
    )


def log_graph_operation(
    operation: str,
    node_count: int | None = None,
    relationship_count: int | None = None,
    duration_ms: float | None = None,
) -> None:
    """Log a graph database operation.

    Args:
        operation: Operation type (create, query, update)
        node_count: Number of nodes affected
        relationship_count: Number of relationships affected
        duration_ms: Operation duration in milliseconds
    """
    logger = get_logger("mitds.graph")
    logger.debug(
        f"Graph operation: {operation}",
        extra={
            "operation": operation,
            "node_count": node_count,
            "relationship_count": relationship_count,
            "duration_ms": duration_ms,
            "event": "graph_operation",
        },
    )


def log_resolution_event(
    strategy: str,
    source_entity: str,
    matched_entity: str | None,
    confidence: float,
    is_match: bool,
) -> None:
    """Log an entity resolution event.

    Args:
        strategy: Resolution strategy used
        source_entity: Entity being resolved
        matched_entity: Matched entity ID (if found)
        confidence: Match confidence score
        is_match: Whether it was considered a match
    """
    logger = get_logger("mitds.resolution")
    logger.debug(
        f"Resolution {strategy}: {source_entity} -> {matched_entity or 'no match'}",
        extra={
            "strategy": strategy,
            "source_entity": source_entity,
            "matched_entity": matched_entity,
            "confidence": confidence,
            "is_match": is_match,
            "event": "entity_resolution",
        },
    )
