"""Lead queue manager for MITDS research.

Manages the priority queue of leads to investigate during
a research session.
"""

import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from ..db import get_db_session
from ..logging import get_context_logger
from .models import (
    IdentifierType,
    Lead,
    LeadStatus,
    LeadType,
    QueuedLead,
    QueueStats,
)

logger = get_context_logger(__name__)


class LeadQueueManager:
    """Manages the priority queue of leads.

    Provides methods for:
    - Adding leads to the queue
    - Retrieving leads by priority
    - Updating lead status
    - Queue statistics
    """

    async def enqueue(
        self,
        session_id: UUID,
        leads: list[Lead],
        source_entity_id: UUID | None = None,
        depth: int = 0,
    ) -> int:
        """Add leads to the queue.

        Deduplicates against existing leads in the session.

        Args:
            session_id: Research session UUID
            leads: List of leads to add
            source_entity_id: Entity that generated these leads
            depth: Hops from entry point

        Returns:
            Number of leads actually added (after deduplication)
        """
        if not leads:
            return 0

        added = 0
        now = datetime.utcnow()

        async with get_db_session() as db:
            for lead in leads:
                # Check for duplicate (same target identifier in this session)
                check_query = text("""
                    SELECT id FROM lead_queue
                    WHERE session_id = :session_id
                    AND target_identifier = :target_identifier
                    AND target_identifier_type = :target_identifier_type
                    AND status NOT IN ('completed', 'skipped', 'failed')
                """)

                result = await db.execute(
                    check_query,
                    {
                        "session_id": str(session_id),
                        "target_identifier": lead.target_identifier,
                        "target_identifier_type": lead.target_identifier_type.value if isinstance(lead.target_identifier_type, IdentifierType) else lead.target_identifier_type,
                    },
                )

                if result.fetchone():
                    # Already exists, skip
                    continue

                # Insert new lead
                insert_query = text("""
                    INSERT INTO lead_queue (
                        id, session_id, source_entity_id,
                        lead_type, target_identifier, target_identifier_type,
                        priority, confidence, depth, status,
                        context, created_at
                    ) VALUES (
                        :id, :session_id, :source_entity_id,
                        :lead_type, :target_identifier, :target_identifier_type,
                        :priority, :confidence, :depth, :status,
                        :context, :created_at
                    )
                """)

                await db.execute(
                    insert_query,
                    {
                        "id": str(lead.id),
                        "session_id": str(session_id),
                        "source_entity_id": str(source_entity_id) if source_entity_id else None,
                        "lead_type": lead.lead_type.value if isinstance(lead.lead_type, LeadType) else lead.lead_type,
                        "target_identifier": lead.target_identifier,
                        "target_identifier_type": lead.target_identifier_type.value if isinstance(lead.target_identifier_type, IdentifierType) else lead.target_identifier_type,
                        "priority": lead.priority,
                        "confidence": lead.confidence,
                        "depth": depth,
                        "status": LeadStatus.PENDING.value,
                        "context": json.dumps(lead.context),
                        "created_at": now,
                    },
                )
                added += 1

        if added > 0:
            logger.debug(
                f"Enqueued {added} leads for session {session_id} at depth {depth}"
            )

        return added

    async def enqueue_single(
        self,
        session_id: UUID,
        lead: Lead,
        source_entity_id: UUID | None = None,
        depth: int = 0,
    ) -> bool:
        """Add a single lead to the queue.

        Args:
            session_id: Research session UUID
            lead: Lead to add
            source_entity_id: Entity that generated this lead
            depth: Hops from entry point

        Returns:
            True if added, False if duplicate
        """
        result = await self.enqueue(session_id, [lead], source_entity_id, depth)
        return result > 0

    async def dequeue(
        self,
        session_id: UUID,
        batch_size: int = 10,
    ) -> list[QueuedLead]:
        """Get next batch of leads to process.

        Orders by priority ASC, confidence DESC, created_at ASC.
        Marks retrieved leads as IN_PROGRESS.

        Args:
            session_id: Research session UUID
            batch_size: Maximum leads to retrieve

        Returns:
            List of leads to process
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            # Select and update in one transaction
            select_query = text("""
                SELECT * FROM lead_queue
                WHERE session_id = :session_id
                AND status = 'pending'
                ORDER BY priority ASC, confidence DESC, created_at ASC
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
            """)

            result = await db.execute(
                select_query,
                {"session_id": str(session_id), "batch_size": batch_size},
            )
            rows = result.fetchall()

            if not rows:
                return []

            leads = []
            lead_ids = []

            for row in rows:
                leads.append(self._row_to_lead(row))
                lead_ids.append(str(row.id))

            # Mark as in_progress
            if lead_ids:
                update_query = text("""
                    UPDATE lead_queue
                    SET status = 'in_progress'
                    WHERE id = ANY(:ids)
                """)
                await db.execute(update_query, {"ids": lead_ids})

            return leads

    async def get_lead(self, lead_id: UUID) -> QueuedLead | None:
        """Get a specific lead by ID.

        Args:
            lead_id: Lead UUID

        Returns:
            QueuedLead or None if not found
        """
        async with get_db_session() as db:
            query = text("""
                SELECT * FROM lead_queue
                WHERE id = :lead_id
            """)

            result = await db.execute(query, {"lead_id": str(lead_id)})
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_lead(row)

    async def complete_lead(
        self,
        lead_id: UUID,
        result: dict[str, Any] | None = None,
    ) -> QueuedLead | None:
        """Mark a lead as completed.

        Args:
            lead_id: Lead UUID
            result: Processing result summary

        Returns:
            Updated lead or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE lead_queue
                SET status = 'completed',
                    processed_at = :processed_at,
                    result = :result
                WHERE id = :lead_id
                RETURNING *
            """)

            db_result = await db.execute(
                query,
                {
                    "lead_id": str(lead_id),
                    "processed_at": now,
                    "result": json.dumps(result) if result else None,
                },
            )
            row = db_result.fetchone()

            if not row:
                return None

            return self._row_to_lead(row)

    async def fail_lead(
        self,
        lead_id: UUID,
        error_message: str,
    ) -> QueuedLead | None:
        """Mark a lead as failed.

        Args:
            lead_id: Lead UUID
            error_message: Error description

        Returns:
            Updated lead or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE lead_queue
                SET status = 'failed',
                    processed_at = :processed_at,
                    result = :result
                WHERE id = :lead_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "lead_id": str(lead_id),
                    "processed_at": now,
                    "result": json.dumps({"error": error_message}),
                },
            )
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_lead(row)

    async def skip_lead(
        self,
        lead_id: UUID,
        reason: str,
    ) -> QueuedLead | None:
        """Mark a lead as skipped.

        Args:
            lead_id: Lead UUID
            reason: Skip reason

        Returns:
            Updated lead or None if not found
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE lead_queue
                SET status = 'skipped',
                    processed_at = :processed_at,
                    skip_reason = :reason
                WHERE id = :lead_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "lead_id": str(lead_id),
                    "processed_at": now,
                    "reason": reason,
                },
            )
            row = result.fetchone()

            if not row:
                return None

            logger.debug(f"Skipped lead {lead_id}: {reason}")
            return self._row_to_lead(row)

    async def requeue_lead(
        self,
        lead_id: UUID,
        new_priority: int | None = None,
    ) -> QueuedLead | None:
        """Requeue a failed or skipped lead.

        Args:
            lead_id: Lead UUID
            new_priority: Optionally change priority

        Returns:
            Updated lead or None if not found
        """
        async with get_db_session() as db:
            if new_priority is not None:
                query = text("""
                    UPDATE lead_queue
                    SET status = 'pending',
                        processed_at = NULL,
                        result = NULL,
                        skip_reason = NULL,
                        priority = :priority
                    WHERE id = :lead_id
                    RETURNING *
                """)
                params = {"lead_id": str(lead_id), "priority": new_priority}
            else:
                query = text("""
                    UPDATE lead_queue
                    SET status = 'pending',
                        processed_at = NULL,
                        result = NULL,
                        skip_reason = NULL
                    WHERE id = :lead_id
                    RETURNING *
                """)
                params = {"lead_id": str(lead_id)}

            result = await db.execute(query, params)
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_lead(row)

    async def set_priority(
        self,
        lead_id: UUID,
        priority: int,
    ) -> QueuedLead | None:
        """Change a lead's priority.

        Args:
            lead_id: Lead UUID
            priority: New priority (1=highest, 5=lowest)

        Returns:
            Updated lead or None if not found
        """
        if not 1 <= priority <= 5:
            raise ValueError("Priority must be between 1 and 5")

        async with get_db_session() as db:
            query = text("""
                UPDATE lead_queue
                SET priority = :priority
                WHERE id = :lead_id AND status = 'pending'
                RETURNING *
            """)

            result = await db.execute(
                query,
                {"lead_id": str(lead_id), "priority": priority},
            )
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_lead(row)

    async def get_pending_leads(
        self,
        session_id: UUID,
        lead_type: LeadType | None = None,
        min_confidence: float | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[QueuedLead]:
        """Get pending leads for a session.

        Args:
            session_id: Research session UUID
            lead_type: Filter by lead type
            min_confidence: Filter by minimum confidence
            limit: Maximum to return
            offset: Pagination offset

        Returns:
            List of pending leads
        """
        async with get_db_session() as db:
            filters = ["session_id = :session_id", "status = 'pending'"]
            params: dict[str, Any] = {
                "session_id": str(session_id),
                "limit": limit,
                "offset": offset,
            }

            if lead_type:
                filters.append("lead_type = :lead_type")
                params["lead_type"] = lead_type.value
            if min_confidence is not None:
                filters.append("confidence >= :min_confidence")
                params["min_confidence"] = min_confidence

            where_clause = " AND ".join(filters)

            query = text(f"""
                SELECT * FROM lead_queue
                WHERE {where_clause}
                ORDER BY priority ASC, confidence DESC, created_at ASC
                LIMIT :limit OFFSET :offset
            """)

            result = await db.execute(query, params)
            rows = result.fetchall()

            return [self._row_to_lead(row) for row in rows]

    async def get_queue_stats(self, session_id: UUID) -> QueueStats:
        """Get statistics for a session's lead queue.

        Args:
            session_id: Research session UUID

        Returns:
            Queue statistics
        """
        async with get_db_session() as db:
            # Status counts
            status_query = text("""
                SELECT status, COUNT(*) as count
                FROM lead_queue
                WHERE session_id = :session_id
                GROUP BY status
            """)
            status_result = await db.execute(
                status_query, {"session_id": str(session_id)}
            )
            status_counts = {row.status: row.count for row in status_result.fetchall()}

            # Type counts (pending only)
            type_query = text("""
                SELECT lead_type, COUNT(*) as count
                FROM lead_queue
                WHERE session_id = :session_id AND status = 'pending'
                GROUP BY lead_type
            """)
            type_result = await db.execute(
                type_query, {"session_id": str(session_id)}
            )
            type_counts = {row.lead_type: row.count for row in type_result.fetchall()}

            # Priority counts (pending only)
            priority_query = text("""
                SELECT priority, COUNT(*) as count
                FROM lead_queue
                WHERE session_id = :session_id AND status = 'pending'
                GROUP BY priority
            """)
            priority_result = await db.execute(
                priority_query, {"session_id": str(session_id)}
            )
            priority_counts = {row.priority: row.count for row in priority_result.fetchall()}

            # Average confidence (pending only)
            avg_query = text("""
                SELECT AVG(confidence) as avg_conf
                FROM lead_queue
                WHERE session_id = :session_id AND status = 'pending'
            """)
            avg_result = await db.execute(avg_query, {"session_id": str(session_id)})
            avg_row = avg_result.fetchone()
            avg_confidence = float(avg_row.avg_conf or 0)

            total = sum(status_counts.values())

            return QueueStats(
                total=total,
                pending=status_counts.get("pending", 0),
                in_progress=status_counts.get("in_progress", 0),
                completed=status_counts.get("completed", 0),
                skipped=status_counts.get("skipped", 0),
                failed=status_counts.get("failed", 0),
                by_type=type_counts,
                by_priority=priority_counts,
                average_confidence=avg_confidence,
            )

    async def skip_leads_at_max_depth(
        self,
        session_id: UUID,
        max_depth: int,
    ) -> int:
        """Skip all pending leads at or beyond max depth.

        Args:
            session_id: Research session UUID
            max_depth: Maximum allowed depth

        Returns:
            Number of leads skipped
        """
        async with get_db_session() as db:
            query = text("""
                UPDATE lead_queue
                SET status = 'skipped',
                    skip_reason = 'max_depth_reached',
                    processed_at = :processed_at
                WHERE session_id = :session_id
                AND status = 'pending'
                AND depth >= :max_depth
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "max_depth": max_depth,
                    "processed_at": datetime.utcnow(),
                },
            )

            # Get row count from result
            skipped = result.rowcount
            if skipped > 0:
                logger.info(
                    f"Skipped {skipped} leads at max depth {max_depth} for session {session_id}"
                )
            return skipped

    async def skip_leads_below_confidence(
        self,
        session_id: UUID,
        min_confidence: float,
    ) -> int:
        """Skip all pending leads below confidence threshold.

        Args:
            session_id: Research session UUID
            min_confidence: Minimum required confidence

        Returns:
            Number of leads skipped
        """
        async with get_db_session() as db:
            query = text("""
                UPDATE lead_queue
                SET status = 'skipped',
                    skip_reason = 'below_confidence_threshold',
                    processed_at = :processed_at
                WHERE session_id = :session_id
                AND status = 'pending'
                AND confidence < :min_confidence
            """)

            result = await db.execute(
                query,
                {
                    "session_id": str(session_id),
                    "min_confidence": min_confidence,
                    "processed_at": datetime.utcnow(),
                },
            )

            skipped = result.rowcount
            if skipped > 0:
                logger.info(
                    f"Skipped {skipped} leads below confidence {min_confidence} for session {session_id}"
                )
            return skipped

    def _row_to_lead(self, row) -> QueuedLead:
        """Convert database row to QueuedLead."""
        context_data = row.context
        if isinstance(context_data, str):
            context_data = json.loads(context_data)

        result_data = row.result if hasattr(row, 'result') else None
        if isinstance(result_data, str):
            result_data = json.loads(result_data)

        return QueuedLead(
            id=UUID(row.id) if isinstance(row.id, str) else row.id,
            session_id=UUID(row.session_id) if isinstance(row.session_id, str) else row.session_id,
            source_entity_id=UUID(row.source_entity_id) if row.source_entity_id else None,
            lead_type=LeadType(row.lead_type),
            target_identifier=row.target_identifier,
            target_identifier_type=IdentifierType(row.target_identifier_type),
            priority=row.priority,
            confidence=row.confidence,
            depth=row.depth,
            status=LeadStatus(row.status),
            context=context_data or {},
            created_at=row.created_at,
            processed_at=row.processed_at if hasattr(row, 'processed_at') else None,
            result=result_data,
            skip_reason=row.skip_reason if hasattr(row, 'skip_reason') else None,
        )


# Singleton instance
_manager: LeadQueueManager | None = None


def get_queue_manager() -> LeadQueueManager:
    """Get the queue manager singleton."""
    global _manager
    if _manager is None:
        _manager = LeadQueueManager()
    return _manager
