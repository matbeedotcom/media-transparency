"""Abstract base class for data ingesters.

All data source connectors must implement this interface
to ensure consistent behavior and retry logic.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, AsyncIterator, Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel

from ..logging import get_context_logger, log_ingestion_start, log_ingestion_complete, log_ingestion_error

# Type variable for ingested record type
T = TypeVar("T", bound=BaseModel)


class IngestionConfig(BaseModel):
    """Configuration for an ingestion run."""

    incremental: bool = True
    limit: int | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    target_entities: list[str] | None = None
    extra_params: dict[str, Any] = {}


class IngestionResult(BaseModel):
    """Result of an ingestion run."""

    run_id: UUID
    source: str
    status: str  # "completed", "failed", "partial"
    started_at: datetime
    completed_at: datetime | None = None
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    duplicates_found: int = 0
    errors: list[dict[str, Any]] = []
    log_output: str = ""

    @property
    def duration_seconds(self) -> float | None:
        """Calculate run duration in seconds."""
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class BaseIngester(ABC, Generic[T]):
    """Abstract base class for data source ingesters.

    Provides common functionality:
    - Retry logic with exponential backoff
    - Progress tracking
    - Error handling and logging
    - Incremental vs full refresh support
    """

    def __init__(self, source_name: str):
        """Initialize the ingester.

        Args:
            source_name: Name of the data source (e.g., 'irs990', 'cra')
        """
        self.source_name = source_name
        self.run_id: UUID | None = None
        self._logger = None

    @property
    def logger(self):
        """Get a logger with run context."""
        if self._logger is None or self.run_id is None:
            self._logger = get_context_logger(
                f"mitds.ingestion.{self.source_name}",
                source=self.source_name,
            )
        return self._logger

    @abstractmethod
    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[T]:
        """Fetch records from the data source.

        Args:
            config: Ingestion configuration

        Yields:
            Parsed records of type T
        """
        ...

    @abstractmethod
    async def process_record(self, record: T) -> dict[str, Any]:
        """Process a single record.

        Args:
            record: The record to process

        Returns:
            Processing result with status and details
        """
        ...

    @abstractmethod
    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync.

        Returns:
            Last sync timestamp or None if never synced
        """
        ...

    @abstractmethod
    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the timestamp of a successful sync.

        Args:
            timestamp: Sync completion timestamp
        """
        ...

    async def run(
        self, config: IngestionConfig | None = None, run_id: UUID | None = None
    ) -> IngestionResult:
        """Run the ingestion process.

        Args:
            config: Optional ingestion configuration
            run_id: Optional run ID (from API layer). If not provided, a new UUID is generated.

        Returns:
            Ingestion result with statistics
        """
        from .run_log import start_capture, finish_capture, RunLogHandler

        if config is None:
            config = IngestionConfig()

        self.run_id = run_id or uuid4()
        run_id_str = str(self.run_id)

        # Set up per-run log capture
        start_capture(run_id_str)
        handler = RunLogHandler(run_id_str)
        handler.setFormatter(logging.Formatter("%(message)s"))
        source_logger = logging.getLogger(f"mitds.ingestion.{self.source_name}")
        source_logger.addHandler(handler)

        result = IngestionResult(
            run_id=self.run_id,
            source=self.source_name,
            status="running",
            started_at=datetime.utcnow(),
        )

        try:
            log_ingestion_start(self.source_name, run_id_str)
            self.logger.info(
                "Starting ingestion",
                extra={"config": config.model_dump(), "run_id": run_id_str},
            )

            try:
                # If incremental, get last sync time
                if config.incremental:
                    last_sync = await self.get_last_sync_time()
                    if last_sync:
                        config.date_from = last_sync
                        self.logger.info(
                            f"Incremental sync from {last_sync.isoformat()}"
                        )

                # Process records
                async for record in self.fetch_records(config):
                    record_name = getattr(record, "name", None) or getattr(record, "corporation_name", None) or getattr(record, "id", "unknown")
                    try:
                        process_result = await self.process_record(record)
                        result.records_processed += 1

                        if process_result.get("created"):
                            result.records_created += 1
                            action = "created"
                        elif process_result.get("updated"):
                            result.records_updated += 1
                            action = "updated"
                        elif process_result.get("duplicate"):
                            result.duplicates_found += 1
                            action = "skipped (duplicate)"
                        else:
                            action = "processed"

                        entity_id = process_result.get("entity_id", "")
                        self.logger.info(
                            f"[{result.records_processed}] {action}: {record_name}"
                            + (f" (entity={entity_id})" if entity_id else "")
                        )

                        # Progress update every 100 records
                        if result.records_processed % 100 == 0:
                            self.logger.info(
                                f"Progress: {result.records_processed} processed, "
                                f"{result.records_created} created, "
                                f"{result.records_updated} updated, "
                                f"{result.duplicates_found} duplicates, "
                                f"{len(result.errors)} errors"
                            )

                        # Check limit
                        if config.limit and result.records_processed >= config.limit:
                            self.logger.info(f"Reached limit of {config.limit} records")
                            break

                    except Exception as e:
                        result.records_processed += 1
                        error_info = {
                            "record_id": getattr(record, "id", None),
                            "error": str(e),
                            "error_type": type(e).__name__,
                        }
                        result.errors.append(error_info)
                        self.logger.warning(
                            f"[{result.records_processed}] FAILED: {record_name} — {e}",
                            extra=error_info,
                        )

                        # Continue processing other records
                        continue

                # Summary
                self.logger.info(
                    f"Ingestion complete: {result.records_processed} processed, "
                    f"{result.records_created} created, {result.records_updated} updated, "
                    f"{result.duplicates_found} duplicates, {len(result.errors)} errors"
                )

                # Mark as complete
                result.status = "completed" if not result.errors else "partial"
                result.completed_at = datetime.utcnow()

                # Save sync time
                if result.status in ("completed", "partial"):
                    await self.save_sync_time(result.started_at)

                log_ingestion_complete(
                    self.source_name,
                    run_id_str,
                    result.records_processed,
                    result.duration_seconds or 0,
                )

            except Exception as e:
                result.status = "failed"
                result.completed_at = datetime.utcnow()
                result.errors.append({
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "fatal": True,
                })
                log_ingestion_error(self.source_name, run_id_str, str(e))
                self.logger.exception("Ingestion failed")

        finally:
            # Detach handler and flush captured logs
            source_logger.removeHandler(handler)
            result.log_output = finish_capture(run_id_str)

        return result


class RetryConfig(BaseModel):
    """Configuration for retry behavior."""

    max_retries: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0


async def with_retry(
    func,
    config: RetryConfig | None = None,
    logger=None,
):
    """Execute a function with exponential backoff retry.

    Args:
        func: Async function to execute
        config: Retry configuration
        logger: Optional logger for retry messages

    Returns:
        Function result

    Raises:
        Exception: If all retries are exhausted
    """
    import asyncio

    if config is None:
        config = RetryConfig()

    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await func()
        except Exception as e:
            last_exception = e

            if attempt < config.max_retries:
                delay = min(
                    config.base_delay * (config.exponential_base ** attempt),
                    config.max_delay,
                )

                if logger:
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s"
                    )

                await asyncio.sleep(delay)
            else:
                if logger:
                    logger.error(
                        f"All {config.max_retries + 1} attempts failed"
                    )

    raise last_exception
