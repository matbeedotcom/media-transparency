"""Research session manager for MITDS.

Manages research investigation sessions from creation through
completion, including state tracking and statistics.
"""

import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from ..db import get_db_session
from ..logging import get_context_logger
from .models import (
    CreateSessionRequest,
    EntryPointType,
    ResearchSession,
    ResearchSessionConfig,
    SessionStats,
    SessionStatus,
)

logger = get_context_logger(__name__)


class ResearchSessionManager:
    """Manages research session lifecycle.

    Provides methods for:
    - Creating new research sessions
    - Starting, pausing, and resuming sessions
    - Tracking session statistics
    - Retrieving session data
    """

    async def create_session(
        self,
        name: str,
        entry_point_type: EntryPointType,
        entry_point_value: str,
        config: ResearchSessionConfig | None = None,
        description: str | None = None,
        created_by: str | None = None,
    ) -> ResearchSession:
        """Create a new research session.

        Args:
            name: User-provided name for the session
            entry_point_type: Type of entry point (meta_ads, ein, bn, etc.)
            entry_point_value: The initial search/identifier
            config: Session configuration (defaults applied if None)
            description: Optional description
            created_by: User identifier

        Returns:
            Created ResearchSession
        """
        now = datetime.utcnow()
        session_id = uuid4()

        if config is None:
            config = ResearchSessionConfig()

        session = ResearchSession(
            id=session_id,
            name=name,
            description=description,
            entry_point_type=entry_point_type,
            entry_point_value=entry_point_value,
            status=SessionStatus.INITIALIZING,
            config=config,
            stats=SessionStats(),
            created_at=now,
            updated_at=now,
            created_by=created_by,
        )

        await self._store_session(session)

        logger.info(
            f"Created research session {session_id}: {name}",
            extra={"session_id": str(session_id), "entry_point": entry_point_value},
        )

        return session

    async def get_session(self, session_id: UUID) -> ResearchSession | None:
        """Get a session by ID.

        Args:
            session_id: Session UUID

        Returns:
            ResearchSession or None if not found
        """
        async with get_db_session() as db:
            query = text("""
                SELECT * FROM research_sessions
                WHERE id = :session_id
            """)

            result = await db.execute(query, {"session_id": str(session_id)})
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_session(row)

    async def list_sessions(
        self,
        status: SessionStatus | None = None,
        created_by: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[ResearchSession]:
        """List research sessions.

        Args:
            status: Filter by status
            created_by: Filter by creator
            limit: Maximum sessions to return
            offset: Offset for pagination

        Returns:
            List of ResearchSession objects
        """
        async with get_db_session() as db:
            filters = ["1=1"]
            params: dict[str, Any] = {"limit": limit, "offset": offset}

            if status:
                filters.append("status = :status")
                params["status"] = status.value
            if created_by:
                filters.append("created_by = :created_by")
                params["created_by"] = created_by

            where_clause = " AND ".join(filters)

            query = text(f"""
                SELECT * FROM research_sessions
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """)

            result = await db.execute(query, params)
            rows = result.fetchall()

            return [self._row_to_session(row) for row in rows]

    async def start_session(self, session_id: UUID) -> ResearchSession | None:
        """Start a session (mark as running).

        Args:
            session_id: Session UUID

        Returns:
            Updated session or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE research_sessions
                SET status = :status,
                    started_at = COALESCE(started_at, :started_at),
                    updated_at = :updated_at
                WHERE id = :session_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "status": SessionStatus.RUNNING.value,
                    "started_at": now,
                    "updated_at": now,
                },
            )
            row = result.fetchone()

            if not row:
                return None

            logger.info(f"Started research session {session_id}")
            return self._row_to_session(row)

    async def pause_session(self, session_id: UUID) -> ResearchSession | None:
        """Pause a running session.

        Args:
            session_id: Session UUID

        Returns:
            Updated session or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE research_sessions
                SET status = :status,
                    paused_at = :paused_at,
                    updated_at = :updated_at
                WHERE id = :session_id AND status = 'running'
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "status": SessionStatus.PAUSED.value,
                    "paused_at": now,
                    "updated_at": now,
                },
            )
            row = result.fetchone()

            if not row:
                return None

            logger.info(f"Paused research session {session_id}")
            return self._row_to_session(row)

    async def resume_session(self, session_id: UUID) -> ResearchSession | None:
        """Resume a paused session.

        Args:
            session_id: Session UUID

        Returns:
            Updated session or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE research_sessions
                SET status = :status,
                    paused_at = NULL,
                    updated_at = :updated_at
                WHERE id = :session_id AND status = 'paused'
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "status": SessionStatus.RUNNING.value,
                    "updated_at": now,
                },
            )
            row = result.fetchone()

            if not row:
                return None

            logger.info(f"Resumed research session {session_id}")
            return self._row_to_session(row)

    async def complete_session(
        self,
        session_id: UUID,
        status: SessionStatus = SessionStatus.COMPLETED,
    ) -> ResearchSession | None:
        """Mark a session as completed or failed.

        Args:
            session_id: Session UUID
            status: Final status (completed or failed)

        Returns:
            Updated session or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE research_sessions
                SET status = :status,
                    completed_at = :completed_at,
                    updated_at = :updated_at
                WHERE id = :session_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "status": status.value,
                    "completed_at": now,
                    "updated_at": now,
                },
            )
            row = result.fetchone()

            if not row:
                return None

            logger.info(f"Completed research session {session_id} with status {status.value}")
            return self._row_to_session(row)

    async def update_stats(
        self,
        session_id: UUID,
        stats: SessionStats,
    ) -> None:
        """Update session statistics.

        Args:
            session_id: Session UUID
            stats: Updated statistics
        """
        async with get_db_session() as db:
            query = text("""
                UPDATE research_sessions
                SET stats = :stats,
                    updated_at = :updated_at
                WHERE id = :session_id
            """)

            await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "stats": json.dumps(stats.model_dump()),
                    "updated_at": datetime.utcnow(),
                },
            )

    async def set_entry_point_entity(
        self,
        session_id: UUID,
        entity_id: UUID,
    ) -> None:
        """Set the resolved entry point entity.

        Args:
            session_id: Session UUID
            entity_id: Resolved entity UUID
        """
        async with get_db_session() as db:
            query = text("""
                UPDATE research_sessions
                SET entry_point_entity_id = :entity_id,
                    updated_at = :updated_at
                WHERE id = :session_id
            """)

            await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "entity_id": str(entity_id),
                    "updated_at": datetime.utcnow(),
                },
            )

    async def add_session_entity(
        self,
        session_id: UUID,
        entity_id: UUID,
        depth: int,
        relevance_score: float = 1.0,
        lead_id: UUID | None = None,
    ) -> None:
        """Add an entity to a session's discovered entities.

        Args:
            session_id: Session UUID
            entity_id: Discovered entity UUID
            depth: Hops from entry point
            relevance_score: Computed relevance (0.0-1.0)
            lead_id: Lead that discovered this entity
        """
        async with get_db_session() as db:
            query = text("""
                INSERT INTO session_entities (
                    session_id, entity_id, added_at, added_via_lead_id,
                    depth, relevance_score
                ) VALUES (
                    :session_id, :entity_id, :added_at, :lead_id,
                    :depth, :relevance_score
                )
                ON CONFLICT (session_id, entity_id) DO NOTHING
            """)

            await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "entity_id": str(entity_id),
                    "added_at": datetime.utcnow(),
                    "lead_id": str(lead_id) if lead_id else None,
                    "depth": depth,
                    "relevance_score": relevance_score,
                },
            )

    async def add_session_relationship(
        self,
        session_id: UUID,
        relationship_id: UUID,
        lead_id: UUID | None = None,
    ) -> None:
        """Add a relationship to a session's discovered relationships.

        Args:
            session_id: Session UUID
            relationship_id: Discovered relationship UUID
            lead_id: Lead that discovered this relationship
        """
        async with get_db_session() as db:
            query = text("""
                INSERT INTO session_relationships (
                    session_id, relationship_id, added_at, added_via_lead_id
                ) VALUES (
                    :session_id, :relationship_id, :added_at, :lead_id
                )
                ON CONFLICT (session_id, relationship_id) DO NOTHING
            """)

            await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "relationship_id": str(relationship_id),
                    "added_at": datetime.utcnow(),
                    "lead_id": str(lead_id) if lead_id else None,
                },
            )

    async def get_session_entities(
        self,
        session_id: UUID,
        depth: int | None = None,
        entity_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Get entities discovered in a session.

        Args:
            session_id: Session UUID
            depth: Filter by depth
            entity_type: Filter by entity type
            limit: Maximum to return
            offset: Pagination offset

        Returns:
            List of entity summaries
        """
        async with get_db_session() as db:
            filters = ["se.session_id = :session_id"]
            params: dict[str, Any] = {
                "session_id": str(session_id),
                "limit": limit,
                "offset": offset,
            }

            if depth is not None:
                filters.append("se.depth = :depth")
                params["depth"] = depth
            if entity_type:
                filters.append("e.entity_type = :entity_type")
                params["entity_type"] = entity_type

            where_clause = " AND ".join(filters)

            query = text(f"""
                SELECT e.id, e.name, e.entity_type,
                       se.depth, se.relevance_score, se.added_at
                FROM session_entities se
                JOIN entities e ON e.id = se.entity_id
                WHERE {where_clause}
                ORDER BY se.depth ASC, se.relevance_score DESC
                LIMIT :limit OFFSET :offset
            """)

            result = await db.execute(query, params)
            rows = result.fetchall()

            return [
                {
                    "id": row.id,
                    "name": row.name,
                    "entity_type": row.entity_type,
                    "depth": row.depth,
                    "relevance_score": row.relevance_score,
                    "added_at": row.added_at,
                }
                for row in rows
            ]

    async def get_session_entity_count(self, session_id: UUID) -> int:
        """Get count of entities in a session.

        Args:
            session_id: Session UUID

        Returns:
            Entity count
        """
        async with get_db_session() as db:
            query = text("""
                SELECT COUNT(*) as count
                FROM session_entities
                WHERE session_id = :session_id
            """)

            result = await db.execute(query, {"session_id": str(session_id)})
            row = result.fetchone()
            return row.count if row else 0

    async def get_session_relationship_count(self, session_id: UUID) -> int:
        """Get count of relationships in a session.

        Args:
            session_id: Session UUID

        Returns:
            Relationship count
        """
        async with get_db_session() as db:
            query = text("""
                SELECT COUNT(*) as count
                FROM session_relationships
                WHERE session_id = :session_id
            """)

            result = await db.execute(query, {"session_id": str(session_id)})
            row = result.fetchone()
            return row.count if row else 0

    async def delete_session(self, session_id: UUID) -> bool:
        """Delete a session and all associated data.

        Args:
            session_id: Session UUID

        Returns:
            True if deleted, False if not found
        """
        async with get_db_session() as db:
            # Delete in order of dependencies
            await db.execute(
                text("DELETE FROM session_relationships WHERE session_id = :id"),
                {"id": str(session_id)},
            )
            await db.execute(
                text("DELETE FROM session_entities WHERE session_id = :id"),
                {"id": str(session_id)},
            )
            await db.execute(
                text("DELETE FROM lead_queue WHERE session_id = :id"),
                {"id": str(session_id)},
            )

            result = await db.execute(
                text("DELETE FROM research_sessions WHERE id = :id RETURNING id"),
                {"id": str(session_id)},
            )
            row = result.fetchone()

            if row:
                logger.info(f"Deleted research session {session_id}")
                return True
            return False

    async def _store_session(self, session: ResearchSession) -> None:
        """Store a session in the database."""
        async with get_db_session() as db:
            query = text("""
                INSERT INTO research_sessions (
                    id, name, description,
                    entry_point_type, entry_point_value, entry_point_entity_id,
                    status, config, stats,
                    created_at, updated_at, started_at, completed_at, paused_at,
                    created_by
                ) VALUES (
                    :id, :name, :description,
                    :entry_point_type, :entry_point_value, :entry_point_entity_id,
                    :status, :config, :stats,
                    :created_at, :updated_at, :started_at, :completed_at, :paused_at,
                    :created_by
                )
            """)

            await db.execute(
                query,
                {
                    "id": str(session.id),
                    "name": session.name,
                    "description": session.description,
                    "entry_point_type": session.entry_point_type.value if isinstance(session.entry_point_type, EntryPointType) else session.entry_point_type,
                    "entry_point_value": session.entry_point_value,
                    "entry_point_entity_id": str(session.entry_point_entity_id) if session.entry_point_entity_id else None,
                    "status": session.status.value if isinstance(session.status, SessionStatus) else session.status,
                    "config": json.dumps(session.config.model_dump()),
                    "stats": json.dumps(session.stats.model_dump()),
                    "created_at": session.created_at,
                    "updated_at": session.updated_at,
                    "started_at": session.started_at,
                    "completed_at": session.completed_at,
                    "paused_at": session.paused_at,
                    "created_by": session.created_by,
                },
            )

    def _row_to_session(self, row) -> ResearchSession:
        """Convert database row to ResearchSession."""
        config_data = row.config
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        stats_data = row.stats
        if isinstance(stats_data, str):
            stats_data = json.loads(stats_data)

        return ResearchSession(
            id=UUID(row.id) if isinstance(row.id, str) else row.id,
            name=row.name,
            description=row.description,
            entry_point_type=EntryPointType(row.entry_point_type),
            entry_point_value=row.entry_point_value,
            entry_point_entity_id=UUID(row.entry_point_entity_id) if row.entry_point_entity_id else None,
            status=SessionStatus(row.status),
            config=ResearchSessionConfig(**config_data) if config_data else ResearchSessionConfig(),
            stats=SessionStats(**stats_data) if stats_data else SessionStats(),
            created_at=row.created_at,
            updated_at=row.updated_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            paused_at=row.paused_at,
            created_by=row.created_by,
        )


# Singleton instance
_manager: ResearchSessionManager | None = None


def get_session_manager() -> ResearchSessionManager:
    """Get the session manager singleton."""
    global _manager
    if _manager is None:
        _manager = ResearchSessionManager()
    return _manager
