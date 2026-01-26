"""Abstract base class for data ingesters.

All data source connectors must implement this interface
to ensure consistent behavior and retry logic.
"""

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

    async def run(self, config: IngestionConfig | None = None) -> IngestionResult:
        """Run the ingestion process.

        Args:
            config: Optional ingestion configuration

        Returns:
            Ingestion result with statistics
        """
        if config is None:
            config = IngestionConfig()

        self.run_id = uuid4()
        result = IngestionResult(
            run_id=self.run_id,
            source=self.source_name,
            status="running",
            started_at=datetime.utcnow(),
        )

        log_ingestion_start(self.source_name, str(self.run_id))
        self.logger.info(
            "Starting ingestion",
            extra={"config": config.model_dump(), "run_id": str(self.run_id)},
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
                try:
                    process_result = await self.process_record(record)
                    result.records_processed += 1

                    if process_result.get("created"):
                        result.records_created += 1
                    elif process_result.get("updated"):
                        result.records_updated += 1
                    elif process_result.get("duplicate"):
                        result.duplicates_found += 1

                    # Check limit
                    if config.limit and result.records_processed >= config.limit:
                        self.logger.info(f"Reached limit of {config.limit} records")
                        break

                except Exception as e:
                    error_info = {
                        "record_id": getattr(record, "id", None),
                        "error": str(e),
                        "error_type": type(e).__name__,
                    }
                    result.errors.append(error_info)
                    self.logger.warning(
                        f"Error processing record: {e}",
                        extra=error_info,
                    )

                    # Continue processing other records
                    continue

            # Mark as complete
            result.status = "completed" if not result.errors else "partial"
            result.completed_at = datetime.utcnow()

            # Save sync time
            if result.status in ("completed", "partial"):
                await self.save_sync_time(result.started_at)

            log_ingestion_complete(
                self.source_name,
                str(self.run_id),
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
            log_ingestion_error(self.source_name, str(self.run_id), str(e))
            self.logger.exception("Ingestion failed")

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
