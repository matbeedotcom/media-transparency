"""Entity match review queue.

Manages medium-confidence entity matches that require human approval
before being merged into the case network.
"""

import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_db_session
from ..models import (
    EntityMatch,
    EntityMatchResponse,
    EntitySummary,
    MatchSignals,
    MatchStatus,
)

logger = logging.getLogger(__name__)


class EntityMatchQueue:
    """Queue for reviewing entity matches.

    Manages the lifecycle of entity matches:
    - Create pending matches from resolution results
    - List pending matches for review
    - Approve/reject/defer matches
    - Track review history
    """

    def __init__(
        self,
        db_session: AsyncSession | None = None,
        neo4j_session: Any = None,
    ):
        """Initialize the queue.

        Args:
            db_session: Optional PostgreSQL database session
            neo4j_session: Optional Neo4j session for graph operations
        """
        self._db_session = db_session
        self._neo4j_session = neo4j_session

    async def _get_db_session(self) -> AsyncSession:
        """Get the database session."""
        if self._db_session is not None:
            return self._db_session
        async with get_db_session() as session:
            return session

    async def _get_neo4j_session(self):
        """Get the Neo4j session."""
        if self._neo4j_session is not None:
            return self._neo4j_session
        from ...db import get_neo4j_session
        return await get_neo4j_session().__anext__()

    async def create_match(
        self,
        case_id: UUID,
        source_entity_id: UUID,
        target_entity_id: UUID,
        confidence: float,
        match_signals: MatchSignals | dict[str, Any] | None = None,
    ) -> EntityMatch:
        """Create a new entity match for review.

        Args:
            case_id: The case this match belongs to
            source_entity_id: The source entity (e.g., Sponsor)
            target_entity_id: The target entity (e.g., Organization)
            confidence: Match confidence score
            match_signals: Signals that contributed to the confidence

        Returns:
            The created EntityMatch
        """
        # Normalize match_signals
        if match_signals is None:
            signals = MatchSignals()
        elif isinstance(match_signals, dict):
            signals = MatchSignals(**match_signals)
        else:
            signals = match_signals

        match = EntityMatch(
            id=uuid4(),
            case_id=case_id,
            source_entity_id=source_entity_id,
            target_entity_id=target_entity_id,
            confidence=confidence,
            match_signals=signals,
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        session = await self._get_db_session()

        # Get status value safely (could be enum or string)
        status_value = match.status.value if hasattr(match.status, 'value') else match.status

        await session.execute(
            """
            INSERT INTO entity_matches (
                id, case_id, source_entity_id, target_entity_id,
                confidence, match_signals, status, created_at
            ) VALUES (
                :id, :case_id, :source_entity_id, :target_entity_id,
                :confidence, :match_signals, :status, :created_at
            )
            """,
            {
                "id": str(match.id),
                "case_id": str(match.case_id),
                "source_entity_id": str(match.source_entity_id),
                "target_entity_id": str(match.target_entity_id),
                "confidence": match.confidence,
                "match_signals": match.match_signals.model_dump(),
                "status": status_value,
                "created_at": match.created_at,
            },
        )
        await session.commit()

        logger.info(
            f"Created entity match {match.id} for case {case_id} "
            f"(confidence: {confidence:.2f})"
        )
        return match

    async def get_pending(
        self,
        case_id: UUID,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[EntityMatch], int]:
        """Get pending matches for a case.

        Args:
            case_id: The case ID
            limit: Maximum matches to return
            offset: Offset for pagination

        Returns:
            Tuple of (list of EntityMatch, total count)
        """
        session = await self._get_db_session()

        # Get total count
        count_result = await session.execute(
            """
            SELECT COUNT(*) FROM entity_matches
            WHERE case_id = :case_id AND status = :status
            """,
            {"case_id": str(case_id), "status": MatchStatus.PENDING.value},
        )
        total = count_result.scalar() or 0

        # Get matches
        result = await session.execute(
            """
            SELECT * FROM entity_matches
            WHERE case_id = :case_id AND status = :status
            ORDER BY confidence DESC
            LIMIT :limit OFFSET :offset
            """,
            {
                "case_id": str(case_id),
                "status": MatchStatus.PENDING.value,
                "limit": limit,
                "offset": offset,
            },
        )
        rows = result.fetchall()

        matches = [self._row_to_match(row) for row in rows]
        return matches, total

    async def get_match(self, match_id: UUID) -> EntityMatch | None:
        """Get a single match by ID.

        Args:
            match_id: The match ID

        Returns:
            The EntityMatch or None if not found
        """
        session = await self._get_db_session()
        result = await session.execute(
            "SELECT * FROM entity_matches WHERE id = :id",
            {"id": str(match_id)},
        )
        row = result.fetchone()
        if row is None:
            return None
        return self._row_to_match(row)

    async def approve(
        self,
        match_id: UUID,
        reviewed_by: str,
        notes: str | None = None,
    ) -> EntityMatch:
        """Approve an entity match.

        This will create a SAME_AS relationship between the entities
        in the Neo4j graph.

        Args:
            match_id: The match to approve
            reviewed_by: User identifier
            notes: Optional reviewer notes

        Returns:
            Updated EntityMatch
        """
        match = await self.get_match(match_id)
        if match is None:
            raise ValueError(f"Match {match_id} not found")

        if match.status != MatchStatus.PENDING:
            raise ValueError(f"Match {match_id} is not pending (status: {match.status})")

        now = datetime.utcnow()
        session = await self._get_db_session()
        await session.execute(
            """
            UPDATE entity_matches SET
                status = :status,
                reviewed_by = :reviewed_by,
                reviewed_at = :reviewed_at,
                review_notes = :review_notes
            WHERE id = :id
            """,
            {
                "id": str(match_id),
                "status": MatchStatus.APPROVED.value,
                "reviewed_by": reviewed_by,
                "reviewed_at": now,
                "review_notes": notes,
            },
        )
        await session.commit()

        # Create SAME_AS relationship in Neo4j
        await self._create_same_as_relationship(
            match.source_entity_id,
            match.target_entity_id,
            match.confidence,
            reviewed_by,
        )

        logger.info(f"Approved match {match_id} by {reviewed_by}")
        return await self.get_match(match_id)

    async def reject(
        self,
        match_id: UUID,
        reviewed_by: str,
        notes: str | None = None,
    ) -> EntityMatch:
        """Reject an entity match.

        Args:
            match_id: The match to reject
            reviewed_by: User identifier
            notes: Optional reason for rejection

        Returns:
            Updated EntityMatch
        """
        match = await self.get_match(match_id)
        if match is None:
            raise ValueError(f"Match {match_id} not found")

        if match.status != MatchStatus.PENDING:
            raise ValueError(f"Match {match_id} is not pending (status: {match.status})")

        now = datetime.utcnow()
        session = await self._get_db_session()
        await session.execute(
            """
            UPDATE entity_matches SET
                status = :status,
                reviewed_by = :reviewed_by,
                reviewed_at = :reviewed_at,
                review_notes = :review_notes
            WHERE id = :id
            """,
            {
                "id": str(match_id),
                "status": MatchStatus.REJECTED.value,
                "reviewed_by": reviewed_by,
                "reviewed_at": now,
                "review_notes": notes,
            },
        )
        await session.commit()

        logger.info(f"Rejected match {match_id} by {reviewed_by}: {notes}")
        return await self.get_match(match_id)

    async def defer(
        self,
        match_id: UUID,
        reviewed_by: str,
        notes: str | None = None,
    ) -> EntityMatch:
        """Defer a match for later review.

        Args:
            match_id: The match to defer
            reviewed_by: User identifier
            notes: Optional notes

        Returns:
            Updated EntityMatch
        """
        match = await self.get_match(match_id)
        if match is None:
            raise ValueError(f"Match {match_id} not found")

        if match.status != MatchStatus.PENDING:
            raise ValueError(f"Match {match_id} is not pending (status: {match.status})")

        now = datetime.utcnow()
        session = await self._get_db_session()
        await session.execute(
            """
            UPDATE entity_matches SET
                status = :status,
                reviewed_by = :reviewed_by,
                reviewed_at = :reviewed_at,
                review_notes = :review_notes
            WHERE id = :id
            """,
            {
                "id": str(match_id),
                "status": MatchStatus.DEFERRED.value,
                "reviewed_by": reviewed_by,
                "reviewed_at": now,
                "review_notes": notes,
            },
        )
        await session.commit()

        logger.info(f"Deferred match {match_id} by {reviewed_by}")
        return await self.get_match(match_id)

    async def _create_same_as_relationship(
        self,
        source_id: UUID,
        target_id: UUID,
        confidence: float,
        approved_by: str,
    ) -> None:
        """Create a SAME_AS relationship in Neo4j."""
        query = """
        MATCH (s {id: $source_id})
        MATCH (t {id: $target_id})
        MERGE (s)-[r:SAME_AS]->(t)
        SET r.confidence = $confidence,
            r.approved_by = $approved_by,
            r.approved_at = datetime(),
            r.source = 'case_review'
        RETURN r
        """

        try:
            neo4j_session = await self._get_neo4j_session()
            await neo4j_session.run(
                query,
                {
                    "source_id": str(source_id),
                    "target_id": str(target_id),
                    "confidence": confidence,
                    "approved_by": approved_by,
                },
            )
            logger.info(f"Created SAME_AS relationship: {source_id} -> {target_id}")
        except Exception as e:
            logger.error(f"Failed to create SAME_AS relationship: {e}")
            raise

    async def get_match_with_entities(
        self, match_id: UUID
    ) -> EntityMatchResponse | None:
        """Get a match with full entity details for display.

        Args:
            match_id: The match ID

        Returns:
            EntityMatchResponse with entity summaries, or None if not found
        """
        match = await self.get_match(match_id)
        if match is None:
            return None

        # Get entity details from Neo4j
        source = await self._get_entity_summary(match.source_entity_id)
        target = await self._get_entity_summary(match.target_entity_id)

        if source is None or target is None:
            logger.warning(
                f"Could not load entity details for match {match_id}: "
                f"source={match.source_entity_id}, target={match.target_entity_id}"
            )
            # Create placeholder summaries
            source = source or EntitySummary(
                id=match.source_entity_id,
                name="Unknown",
                entity_type="unknown",
                jurisdiction=None,
                identifiers={},
            )
            target = target or EntitySummary(
                id=match.target_entity_id,
                name="Unknown",
                entity_type="unknown",
                jurisdiction=None,
                identifiers={},
            )

        return EntityMatchResponse(
            id=match.id,
            source_entity=source,
            target_entity=target,
            confidence=match.confidence,
            match_signals=match.match_signals,
            status=match.status,
            reviewed_by=match.reviewed_by,
            reviewed_at=match.reviewed_at,
            review_notes=match.review_notes,
        )

    async def _get_entity_summary(self, entity_id: UUID) -> EntitySummary | None:
        """Get entity summary from Neo4j."""
        query = """
        MATCH (e {id: $id})
        RETURN e.id as id, e.name as name, e.entity_type as entity_type,
               e.jurisdiction as jurisdiction,
               e.ein as ein, e.bn as bn, e.meta_page_id as meta_page_id
        """

        try:
            neo4j_session = await self._get_neo4j_session()
            result = await neo4j_session.run(query, {"id": str(entity_id)})
            record = await result.single()

            if record:
                row = dict(record)
                identifiers = {}
                if row.get("ein"):
                    identifiers["ein"] = row["ein"]
                if row.get("bn"):
                    identifiers["bn"] = row["bn"]
                if row.get("meta_page_id"):
                    identifiers["meta_page_id"] = row["meta_page_id"]

                return EntitySummary(
                    id=entity_id,
                    name=row.get("name", "Unknown"),
                    entity_type=row.get("entity_type", "unknown"),
                    jurisdiction=row.get("jurisdiction"),
                    identifiers=identifiers,
                )
        except Exception as e:
            logger.warning(f"Failed to get entity summary: {e}")

        return None

    def _row_to_match(self, row: Any) -> EntityMatch:
        """Convert a database row to an EntityMatch."""
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
_queue: EntityMatchQueue | None = None


def get_match_queue() -> EntityMatchQueue:
    """Get the entity match queue singleton."""
    global _queue
    if _queue is None:
        _queue = EntityMatchQueue()
    return _queue
