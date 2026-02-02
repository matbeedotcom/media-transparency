"""Case lifecycle management for the Case Intake System.

The CaseManager handles case creation, status updates, pause/resume,
and integration with the existing research engine.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db_session, get_session_factory
from ..research import (
    ResearchSessionConfig,
    ResearchSessionManager,
    get_session_manager,
)
from ..research.models import EntryPointType as ResearchEntryPointType
from ..research.processor import LeadProcessor
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
    ProcessingDetails,
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

    @asynccontextmanager
    async def _get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a fresh database session as a context manager.
        
        Usage:
            async with self._get_session() as session:
                await session.execute(...)
        """
        async with get_db_session() as session:
            yield session

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
        async with self._get_session() as session:
            await self._insert_case(session, case)

        logger.info(f"Created case {case.id}")
        return case

    async def _insert_case(self, session: AsyncSession, case: Case) -> None:
        """Insert a case into the database."""
        # This would use SQLAlchemy ORM models in production
        # For now, we'll use raw SQL
        await session.execute(
            text("""
            INSERT INTO cases (
                id, name, description, entry_point_type, entry_point_value,
                status, config, stats, research_session_id, created_at,
                updated_at, completed_at, created_by
            ) VALUES (
                :id, :name, :description, :entry_point_type, :entry_point_value,
                :status, :config, :stats, :research_session_id, :created_at,
                :updated_at, :completed_at, :created_by
            )
            """),
            {
                "id": str(case.id),
                "name": case.name,
                "description": case.description,
                "entry_point_type": case.entry_point_type,
                "entry_point_value": case.entry_point_value,
                "status": case.status,
                "config": json.dumps(case.config.model_dump()),
                "stats": json.dumps(case.stats.model_dump()),
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
        async with self._get_session() as session:
            result = await session.execute(
                text("SELECT * FROM cases WHERE id = :id"),
                {"id": str(case_id)},
            )
            row = result.fetchone()
            if row is None:
                return None

            case = self._row_to_case(row)
            
            # If processing, fetch live stats from research session
            if case.status == CaseStatus.PROCESSING and case.research_session_id:
                case = await self._enrich_with_live_stats(case)
            
            return case
    
    async def _enrich_with_live_stats(self, case: Case) -> Case:
        """Enrich case with live stats from the research session.
        
        Args:
            case: The case to enrich
            
        Returns:
            Case with updated stats from the research session
        """
        if not case.research_session_id:
            return case
            
        try:
            research_session = await self.research_manager.get_session(case.research_session_id)
            if research_session and research_session.stats:
                # Map research session stats to case stats
                case.stats = CaseStats(
                    entity_count=research_session.stats.total_entities,
                    relationship_count=research_session.stats.total_relationships,
                    evidence_count=case.stats.evidence_count,  # Keep existing
                    pending_matches=case.stats.pending_matches,  # Keep existing
                    leads_processed=research_session.stats.leads_completed,
                    leads_pending=research_session.stats.leads_pending,
                )
        except Exception as e:
            logger.warning(f"Failed to fetch live stats for case {case.id}: {e}")
        
        return case

    def _row_to_case(self, row: Any) -> Case:
        """Convert a database row to a Case object."""
        # Handle UUID fields - asyncpg returns UUID objects, not strings
        case_id = UUID(str(row.id)) if not isinstance(row.id, UUID) else row.id
        research_id = None
        if row.research_session_id:
            research_id = UUID(str(row.research_session_id)) if not isinstance(row.research_session_id, UUID) else row.research_session_id
        
        return Case(
            id=case_id,
            name=row.name,
            description=row.description,
            entry_point_type=EntryPointType(row.entry_point_type),
            entry_point_value=row.entry_point_value,
            status=CaseStatus(row.status),
            config=CaseConfig(**row.config) if row.config else CaseConfig(),
            stats=CaseStats(**row.stats) if row.stats else CaseStats(),
            research_session_id=research_id,
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
        async with self._get_session() as session:
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
                text(f"SELECT COUNT(*) FROM ({query}) AS subquery"),
                params,
            )
            total = count_result.scalar() or 0

            # Get paginated results
            query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset

            result = await session.execute(text(query), params)
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
        async with self._get_session() as session:
            await session.execute(
                text("""
                UPDATE cases SET
                    status = :status,
                    research_session_id = :research_session_id,
                    updated_at = :updated_at
                WHERE id = :id
                """),
                {
                    "id": str(case_id),
                    "status": CaseStatus.PROCESSING.value,
                    "research_session_id": str(research_session.id),
                    "updated_at": datetime.utcnow(),
                },
            )

        logger.info(f"Started processing case {case_id} with research session {research_session.id}")

        # Start background processing task
        asyncio.create_task(
            self._run_case_processing(case_id, research_session.id)
        )

        return await self.get_case(case_id)

    async def _run_case_processing(self, case_id: UUID, session_id: UUID) -> None:
        """Background task to process a case.
        
        Args:
            case_id: The case being processed
            session_id: The research session ID
        """
        try:
            logger.info(f"Starting background processing for case {case_id}")
            processor = LeadProcessor(self.research_manager)
            
            # Process the session (this will run until completion or limits reached)
            stats = await processor.process_session(session_id)
            
            logger.info(f"Case {case_id} processing completed: {stats.total_entities} entities, {stats.total_relationships} relationships")
            
            # Mark case as completed
            await self.complete_case(case_id)
            
        except Exception as e:
            logger.error(f"Case {case_id} processing failed: {e}")
            await self.fail_case(case_id, str(e))

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
        async with self._get_session() as session:
            await session.execute(
                text("UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id"),
                {
                    "id": str(case_id),
                    "status": CaseStatus.PAUSED.value,
                    "updated_at": datetime.utcnow(),
                },
            )

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
        async with self._get_session() as session:
            await session.execute(
                text("UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id"),
                {
                    "id": str(case_id),
                    "status": CaseStatus.PROCESSING.value,
                    "updated_at": datetime.utcnow(),
                },
            )

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

        # Sync final stats from research session before completing
        if case.research_session_id:
            try:
                case = await self._enrich_with_live_stats(case)
                await self.update_stats(case_id, case.stats)
            except Exception as e:
                logger.warning(f"Failed to sync final stats for case {case_id}: {e}")

        now = datetime.utcnow()
        async with self._get_session() as session:
            await session.execute(
                text("""
                UPDATE cases SET
                    status = :status,
                    updated_at = :updated_at,
                    completed_at = :completed_at
                WHERE id = :id
                """),
                {
                    "id": str(case_id),
                    "status": CaseStatus.COMPLETED.value,
                    "updated_at": now,
                    "completed_at": now,
                },
            )

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

        async with self._get_session() as session:
            await session.execute(
                text("UPDATE cases SET status = :status, updated_at = :updated_at WHERE id = :id"),
                {
                    "id": str(case_id),
                    "status": CaseStatus.FAILED.value,
                    "updated_at": datetime.utcnow(),
                },
            )

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

        async with self._get_session() as session:
            await session.execute(
                text("DELETE FROM cases WHERE id = :id"),
                {"id": str(case_id)},
            )

        logger.info(f"Deleted case {case_id}")
        return True

    async def update_stats(self, case_id: UUID, stats: CaseStats) -> None:
        """Update case statistics.

        Args:
            case_id: The case to update
            stats: New statistics
        """
        async with self._get_session() as session:
            await session.execute(
                text("UPDATE cases SET stats = :stats, updated_at = :updated_at WHERE id = :id"),
                {
                    "id": str(case_id),
                    "stats": json.dumps(stats.model_dump()),
                    "updated_at": datetime.utcnow(),
                },
            )

    async def get_processing_details(self, case_id: UUID) -> ProcessingDetails:
        """Get detailed processing information for an active case.
        
        Args:
            case_id: The case ID
            
        Returns:
            ProcessingDetails with real-time processing information
        """
        case = await self.get_case(case_id)
        if case is None:
            raise ValueError(f"Case {case_id} not found")
        
        if case.status != CaseStatus.PROCESSING or not case.research_session_id:
            return ProcessingDetails(
                is_processing=False,
                current_phase="idle" if case.status != CaseStatus.COMPLETED else "completed",
            )
        
        try:
            from ..research.queue import get_queue_manager
            
            research_session = await self.research_manager.get_session(case.research_session_id)
            queue_manager = get_queue_manager()
            
            if not research_session:
                return ProcessingDetails(is_processing=True, current_phase="initializing")
            
            # Get queue stats
            queue_stats = await queue_manager.get_queue_stats(case.research_session_id)
            
            # Get recent entities (last 5 discovered)
            recent_entities_data = await self.research_manager.get_session_entities(
                case.research_session_id, limit=5
            )
            recent_entities = [e["name"] for e in recent_entities_data]
            
            # Get recent/current leads being processed
            async with self._get_session() as session:
                result = await session.execute(
                    text("""
                        SELECT target_identifier FROM lead_queue 
                        WHERE session_id = :session_id 
                        AND status IN ('pending', 'processing')
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 5
                    """),
                    {"session_id": str(case.research_session_id)},
                )
                rows = result.fetchall()
                recent_leads = [row.target_identifier for row in rows]
            
            # Calculate progress
            leads_total = queue_stats.total
            leads_completed = queue_stats.completed
            progress_percent = 0.0
            if leads_total > 0:
                progress_percent = min(100.0, (leads_completed / leads_total) * 100)
            
            # Determine current phase
            if queue_stats.pending > 0:
                current_phase = "processing_leads"
            elif leads_completed > 0:
                current_phase = "finalizing"
            else:
                current_phase = "initializing"
            
            # Calculate elapsed time
            elapsed_seconds = 0.0
            started_at = research_session.started_at
            if started_at:
                elapsed_seconds = (datetime.utcnow() - started_at).total_seconds()
            
            return ProcessingDetails(
                is_processing=True,
                current_phase=current_phase,
                progress_percent=progress_percent,
                leads_total=leads_total,
                leads_pending=queue_stats.pending,
                leads_completed=leads_completed,
                leads_failed=queue_stats.failed,
                leads_skipped=queue_stats.skipped,
                recent_entities=recent_entities,
                recent_leads=recent_leads,
                started_at=started_at,
                elapsed_seconds=elapsed_seconds,
            )
            
        except Exception as e:
            logger.warning(f"Failed to get processing details for case {case_id}: {e}")
            return ProcessingDetails(
                is_processing=True,
                current_phase="processing",
            )

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
        async with self._get_session() as session:
            result = await session.execute(
                text("""
                SELECT * FROM entity_matches
                WHERE case_id = :case_id AND status = :status
                ORDER BY confidence DESC
                LIMIT :limit
                """),
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
