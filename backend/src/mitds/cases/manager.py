"""Case lifecycle management for the Case Intake System.

The CaseManager handles case creation, status updates, pause/resume,
and integration with the existing research engine.
"""

import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db_session
from ..research import (
    ResearchSessionConfig,
    ResearchSessionManager,
    get_session_manager,
)
from ..research.models import EntryPointType as ResearchEntryPointType
from .models import (
    Case,
    CaseConfig,
    CaseReport,
    CaseResponse,
    CaseStats,
    CaseStatus,
    CaseSummary,
    CreateCaseRequest,
    EntryPointType,
    EntityMatch,
    Evidence,
    MatchStatus,
)

logger = logging.getLogger(__name__)


class CaseManager:
    """Manages case lifecycle and integrates with research engine.

    The CaseManager is responsible for:
    - Creating cases from various entry points
    - Managing case status (pause, resume, complete)
    - Integrating with ResearchSessionManager for lead processing
    - Tracking case statistics
    """

    def __init__(self, session: AsyncSession | None = None):
        """Initialize the CaseManager.

        Args:
            session: Optional database session. If not provided, will use
                    the default async session.
        """
        self._session = session
        self._research_manager: ResearchSessionManager | None = None

    @property
    def research_manager(self) -> ResearchSessionManager:
        """Get the research session manager."""
        if self._research_manager is None:
            self._research_manager = get_session_manager()
        return self._research_manager

    async def _get_session(self) -> AsyncSession:
        """Get the database session."""
        if self._session is not None:
            return self._session
        return await get_db_session().__anext__()

    def _map_entry_point_type(self, case_type: EntryPointType) -> ResearchEntryPointType:
        """Map case entry point type to research entry point type."""
        mapping = {
            EntryPointType.META_AD: ResearchEntryPointType.META_ADS,
            EntryPointType.CORPORATION: ResearchEntryPointType.COMPANY,
            EntryPointType.URL: ResearchEntryPointType.COMPANY,  # Will extract entities
            EntryPointType.TEXT: ResearchEntryPointType.COMPANY,  # Will extract entities
        }
        return mapping.get(case_type, ResearchEntryPointType.COMPANY)

    def _case_config_to_research_config(self, config: CaseConfig) -> ResearchSessionConfig:
        """Convert case config to research session config."""
        return ResearchSessionConfig(
            max_depth=config.max_depth,
            max_entities=config.max_entities,
            max_relationships=config.max_relationships,
            jurisdictions=config.jurisdictions,
            min_confidence=config.min_confidence,
        )

    async def create_case(
        self,
        request: CreateCaseRequest,
        created_by: str | None = None,
    ) -> Case:
        """Create a new case.

        Args:
            request: Case creation request with entry point details
            created_by: Optional user identifier

        Returns:
            The created Case object
        """
        logger.info(
            f"Creating case '{request.name}' with entry point "
            f"{request.entry_point_type}={request.entry_point_value[:50]}..."
        )

        case = Case(
            id=uuid4(),
            name=request.name,
            description=request.description,
            entry_point_type=request.entry_point_type,
            entry_point_value=request.entry_point_value,
            config=request.config or CaseConfig(),
            status=CaseStatus.INITIALIZING,
            created_by=created_by,
        )

        # Store in database
        session = await self._get_session()
        await self._insert_case(session, case)

        logger.info(f"Created case {case.id}")
        return case

    async def _insert_case(self, session: AsyncSession, case: Case) -> None:
        """Insert a case into the database."""
        # This would use SQLAlchemy ORM models in production
        # For now, we'll use raw SQL
        await session.execute(
            """
            INSERT INTO cases (
                id, name, description, entry_point_type, entry_point_value,
                status, config, stats, research_session_id, created_at,
                updated_at, completed_at, created_by
            ) VALUES (
                :id, :name, :description, :entry_point_type, :entry_point_value,
                :status, :config, :stats, :research_session_id, :created_at,
                :updated_at, :completed_at, :created_by
            )
            """,
            {
                "id": str(case.id),
                "name": case.name,
                "description": case.description,
                "entry_point_type": case.entry_point_type,
                "entry_point_value": case.entry_point_value,
                "status": case.status,
                "config": case.config.model_dump(),
                "stats": case.stats.model_dump(),
                "research_session_id": str(case.research_session_id) if case.research_session_id else None,
                "created_at": case.created_at,
                "updated_at": case.updated_at,
                "completed_at": case.completed_at,
                "created_by": case.created_by,
            },
        )
        await session.commit()

    async def get_case(self, case_id: UUID) -> Case | None:
        """Get a case by ID.

        Args:
            case_id: The case ID

        Returns:
            The Case object, or None if not found
        """
        session = await self._get_session()
        result = await session.execute(
            """
            SELECT * FROM cases WHERE id = :id
            """,
            {"id": str(case_id)},
        )
        row = result.fetchone()
        if row is None:
            return None

        return self._row_to_case(row)

    def _row_to_case(self, row: Any) -> Case:
        """Convert a database row to a Case object."""
        return Case(
            id=UUID(row.id) if isinstance(row.id, str) else row.id,
            name=row.name,
            description=row.description,
            entry_point_type=EntryPointType(row.entry_point_type),
            entry_point_value=row.entry_point_value,
            status=CaseStatus(row.status),
            config=CaseConfig(**row.config) if row.config else CaseConfig(),
            stats=CaseStats(**row.stats) if row.stats else CaseStats(),
            research_session_id=UUID(row.research_session_id) if row.research_session_id else None,
            created_at=row.created_at,
            updated_at=row.updated_at,
            completed_at=row.completed_at,
            created_by=row.created_by,
        )

    async def list_cases(
        self,
        status: CaseStatus | None = None,
        created_by: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[CaseSummary], int]:
        """List cases with optional filters.

        Args:
            status: Filter by status
            created_by: Filter by creator
            limit: Maximum results to return
            offset: Offset for pagination

        Returns:
            Tuple of (list of CaseSummary, total count)
        """
        session = await self._get_session()

        # Build query
        query = "SELECT id, name, status, entry_point_type, created_at FROM cases WHERE 1=1"
        params: dict[str, Any] = {}

        if status:
            query += " AND status = :status"
            params["status"] = status.value
        if created_by:
            query += " AND created_by = :created_by"
            params["created_by"] = created_by

        # Get total count
        count_result = await session.execute(
            f"SELECT COUNT(*) FROM ({query}) AS subquery",
            params,
        )
        total = count_result.scalar() or 0

        # Get paginated results
        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        result = await session.execute(query, params)
        rows = result.fetchall()

        summaries = [
            CaseSummary(
                id=UUID(row.id) if isinstance(row.id, str) else row.id,
                name=row.name,
                status=CaseStatus(row.status),
                entry_point_type=EntryPointType(row.entry_point_type),
                entity_count=0,  # Would need a join to get this
                created_at=row.created_at,
            )
            for row in rows
        ]

        return summaries, total

    async def start_processing(self, case_id: UUID) -> Case:
        """Start processing a case.

        Creates a research session and begins autonomous lead processing.

        Args:
            case_id: The case to start processing

        Returns:
            Updated Case object
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")

        if case.status != CaseStatus.INITIALIZING:
            raise ValueError(f"Case {case_id} is not in INITIALIZING state")

        # Create research session
        research_config = self._case_config_to_research_config(case.config)
        research_session = await self.research_manager.create_session(
            name=f"Case: {case.name}",
            description=f"Research session for case {case.id}",
            entry_point_type=self._map_entry_point_type(case.entry_point_type),
            entry_point_value=case.entry_point_value,
            config=research_config,
        )

        # Update case status
        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET
                status = :status,
                research_session_id = :research_session_id,
                updated_at = :updated_at
            WHERE id = :id
            """,
            {
                "id": str(case_id),
                "status": CaseStatus.PROCESSING.value,
                "research_session_id": str(research_session.id),
                "updated_at": datetime.utcnow(),
            },
        )
        await session.commit()

        logger.info(f"Started processing case {case_id} with research session {research_session.id}")

        return await self.get_case(case_id)

    async def pause_case(self, case_id: UUID) -> Case:
        """Pause a case's processing.

        Args:
            case_id: The case to pause

        Returns:
            Updated Case object
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")

        if case.status != CaseStatus.PROCESSING:
            raise ValueError(f"Case {case_id} is not in PROCESSING state")

        # Pause the research session if exists
        if case.research_session_id:
            await self.research_manager.pause_session(case.research_session_id)

        # Update case status
        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id
            """,
            {
                "id": str(case_id),
                "status": CaseStatus.PAUSED.value,
                "updated_at": datetime.utcnow(),
            },
        )
        await session.commit()

        logger.info(f"Paused case {case_id}")
        return await self.get_case(case_id)

    async def resume_case(self, case_id: UUID) -> Case:
        """Resume a paused case.

        Args:
            case_id: The case to resume

        Returns:
            Updated Case object
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")

        if case.status != CaseStatus.PAUSED:
            raise ValueError(f"Case {case_id} is not in PAUSED state")

        # Resume the research session if exists
        if case.research_session_id:
            await self.research_manager.resume_session(case.research_session_id)

        # Update case status
        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id
            """,
            {
                "id": str(case_id),
                "status": CaseStatus.PROCESSING.value,
                "updated_at": datetime.utcnow(),
            },
        )
        await session.commit()

        logger.info(f"Resumed case {case_id}")
        return await self.get_case(case_id)

    async def complete_case(self, case_id: UUID) -> Case:
        """Mark a case as completed.

        Args:
            case_id: The case to complete

        Returns:
            Updated Case object
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")

        now = datetime.utcnow()
        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET
                status = :status,
                updated_at = :updated_at,
                completed_at = :completed_at
            WHERE id = :id
            """,
            {
                "id": str(case_id),
                "status": CaseStatus.COMPLETED.value,
                "updated_at": now,
                "completed_at": now,
            },
        )
        await session.commit()

        logger.info(f"Completed case {case_id}")
        return await self.get_case(case_id)

    async def fail_case(self, case_id: UUID, error_message: str) -> Case:
        """Mark a case as failed.

        Args:
            case_id: The case that failed
            error_message: Description of the failure

        Returns:
            Updated Case object
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")

        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id
            """,
            {
                "id": str(case_id),
                "status": CaseStatus.FAILED.value,
                "updated_at": datetime.utcnow(),
            },
        )
        await session.commit()

        logger.error(f"Case {case_id} failed: {error_message}")
        return await self.get_case(case_id)

    async def delete_case(self, case_id: UUID) -> bool:
        """Delete a case and all associated data.

        Args:
            case_id: The case to delete

        Returns:
            True if deleted, False if not found
        """
        case = await self.get_case(case_id)
        if case is None:
            return False

        session = await self._get_session()
        await session.execute(
            "DELETE FROM cases WHERE id = :id",
            {"id": str(case_id)},
        )
        await session.commit()

        logger.info(f"Deleted case {case_id}")
        return True

    async def update_stats(self, case_id: UUID, stats: CaseStats) -> None:
        """Update case statistics.

        Args:
            case_id: The case to update
            stats: New statistics
        """
        session = await self._get_session()
        await session.execute(
            """
            UPDATE cases SET stats = :stats, updated_at = :updated_at WHERE id = :id
            """,
            {
                "id": str(case_id),
                "stats": stats.model_dump(),
                "updated_at": datetime.utcnow(),
            },
        )
        await session.commit()

    async def get_pending_matches(
        self, case_id: UUID, limit: int = 20
    ) -> list[EntityMatch]:
        """Get pending entity matches for a case.

        Args:
            case_id: The case ID
            limit: Maximum matches to return

        Returns:
            List of pending EntityMatch objects
        """
        session = await self._get_session()
        result = await session.execute(
            """
            SELECT * FROM entity_matches
            WHERE case_id = :case_id AND status = :status
            ORDER BY confidence DESC
            LIMIT :limit
            """,
            {
                "case_id": str(case_id),
                "status": MatchStatus.PENDING.value,
                "limit": limit,
            },
        )
        rows = result.fetchall()

        return [self._row_to_entity_match(row) for row in rows]

    def _row_to_entity_match(self, row: Any) -> EntityMatch:
        """Convert a database row to an EntityMatch object."""
        from .models import MatchSignals

        return EntityMatch(
            id=UUID(row.id) if isinstance(row.id, str) else row.id,
            case_id=UUID(row.case_id) if isinstance(row.case_id, str) else row.case_id,
            source_entity_id=UUID(row.source_entity_id) if isinstance(row.source_entity_id, str) else row.source_entity_id,
            target_entity_id=UUID(row.target_entity_id) if isinstance(row.target_entity_id, str) else row.target_entity_id,
            confidence=row.confidence,
            match_signals=MatchSignals(**row.match_signals) if row.match_signals else MatchSignals(),
            status=MatchStatus(row.status),
            reviewed_by=row.reviewed_by,
            reviewed_at=row.reviewed_at,
            review_notes=row.review_notes,
            created_at=row.created_at,
        )


# Singleton instance
_case_manager: CaseManager | None = None


def get_case_manager() -> CaseManager:
    """Get the case manager singleton."""
    global _case_manager
    if _case_manager is None:
        _case_manager = CaseManager()
    return _case_manager
