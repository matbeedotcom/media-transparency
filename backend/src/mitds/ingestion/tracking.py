"""Ingestion run tracking service for MITDS.

Tracks the status and progress of data ingestion runs.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from ..db import get_db_session
from ..logging import get_logger

logger = get_logger(__name__)


class IngestionRun:
    """Represents a single ingestion run."""

    def __init__(
        self,
        run_id: UUID,
        source: str,
        started_at: datetime,
        status: str = "running",
    ):
        self.run_id = run_id
        self.source = source
        self.started_at = started_at
        self.completed_at: datetime | None = None
        self.status = status
        self.records_processed = 0
        self.records_created = 0
        self.records_updated = 0
        self.duplicates_found = 0
        self.errors: list[dict[str, Any]] = []


class IngestionTracker:
    """Service for tracking ingestion runs."""

    async def start_run(self, source: str) -> IngestionRun:
        """Start a new ingestion run.

        Args:
            source: Data source name

        Returns:
            New IngestionRun instance
        """
        run = IngestionRun(
            run_id=uuid4(),
            source=source,
            started_at=datetime.utcnow(),
            status="running",
        )

        async with get_db_session() as session:
            await session.execute(
                insert("ingestion_runs").values(
                    id=run.run_id,
                    source=run.source,
                    started_at=run.started_at,
                    status=run.status,
                )
            )

        logger.info(
            f"Started ingestion run {run.run_id} for {source}",
            extra={"run_id": str(run.run_id), "source": source},
        )

        return run

    async def update_progress(
        self,
        run_id: UUID,
        records_processed: int,
        records_created: int = 0,
        records_updated: int = 0,
        duplicates_found: int = 0,
    ) -> None:
        """Update progress for an ingestion run.

        Args:
            run_id: Run ID
            records_processed: Total records processed
            records_created: Records created
            records_updated: Records updated
            duplicates_found: Duplicate records found
        """
        async with get_db_session() as session:
            await session.execute(
                update("ingestion_runs")
                .where("ingestion_runs.id" == run_id)
                .values(
                    records_processed=records_processed,
                    records_created=records_created,
                    records_updated=records_updated,
                    duplicates_found=duplicates_found,
                )
            )

    async def complete_run(
        self,
        run_id: UUID,
        status: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        duplicates_found: int,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        """Mark an ingestion run as complete.

        Args:
            run_id: Run ID
            status: Final status ('completed', 'partial', 'failed')
            records_processed: Total records processed
            records_created: Records created
            records_updated: Records updated
            duplicates_found: Duplicate records found
            errors: List of error details
        """
        async with get_db_session() as session:
            await session.execute(
                update("ingestion_runs")
                .where("ingestion_runs.id" == run_id)
                .values(
                    status=status,
                    completed_at=datetime.utcnow(),
                    records_processed=records_processed,
                    records_created=records_created,
                    records_updated=records_updated,
                    duplicates_found=duplicates_found,
                    errors=errors or [],
                )
            )

        logger.info(
            f"Completed ingestion run {run_id} with status {status}",
            extra={
                "run_id": str(run_id),
                "status": status,
                "records_processed": records_processed,
            },
        )

    async def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        """Get details for an ingestion run.

        Args:
            run_id: Run ID

        Returns:
            Run details or None if not found
        """
        async with get_db_session() as session:
            result = await session.execute(
                select("ingestion_runs").where("ingestion_runs.id" == run_id)
            )
            row = result.fetchone()

            if row:
                return {
                    "id": row.id,
                    "source": row.source,
                    "started_at": row.started_at,
                    "completed_at": row.completed_at,
                    "status": row.status,
                    "records_processed": row.records_processed,
                    "records_created": row.records_created,
                    "records_updated": row.records_updated,
                    "duplicates_found": row.duplicates_found,
                    "errors": row.errors,
                }

            return None

    async def get_last_successful_run(self, source: str) -> dict[str, Any] | None:
        """Get the last successful ingestion run for a source.

        Args:
            source: Data source name

        Returns:
            Run details or None if no successful runs
        """
        async with get_db_session() as session:
            result = await session.execute(
                select("ingestion_runs")
                .where("ingestion_runs.source" == source)
                .where("ingestion_runs.status".in_(["completed", "partial"]))
                .order_by("ingestion_runs.completed_at".desc())
                .limit(1)
            )
            row = result.fetchone()

            if row:
                return {
                    "id": row.id,
                    "source": row.source,
                    "started_at": row.started_at,
                    "completed_at": row.completed_at,
                    "status": row.status,
                    "records_processed": row.records_processed,
                }

            return None

    async def get_source_status(self, source: str) -> dict[str, Any]:
        """Get the current status for a data source.

        Args:
            source: Data source name

        Returns:
            Source status including last run info
        """
        last_run = await self.get_last_successful_run(source)

        if not last_run:
            return {
                "source": source,
                "status": "never_run",
                "last_successful_run": None,
                "records_count": 0,
            }

        # Determine health status based on last run age
        completed_at = last_run.get("completed_at")
        if completed_at:
            age_hours = (datetime.utcnow() - completed_at).total_seconds() / 3600

            # Sources should run at least weekly
            if age_hours > 168:  # 7 days
                status = "stale"
            else:
                status = "healthy"
        else:
            status = "unknown"

        return {
            "source": source,
            "status": status,
            "last_successful_run": completed_at,
            "records_count": last_run.get("records_processed", 0),
        }


# Singleton instance
_tracker: IngestionTracker | None = None


def get_tracker() -> IngestionTracker:
    """Get the ingestion tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = IngestionTracker()
    return _tracker
