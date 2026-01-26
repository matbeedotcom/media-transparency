"""Human-in-the-loop entity reconciliation for MITDS.

Provides a queue-based system for managing entity matches that
require human review:
- Low confidence matches (< 0.9)
- Conflicting matches
- New entities needing categorization

Analysts can approve, reject, or manually merge entities.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import get_db_session
from ..logging import get_context_logger
from .matcher import MatchCandidate, MatchResult, MatchStrategy

logger = get_context_logger(__name__)


# =========================
# Data Models
# =========================


class ReconciliationStatus(str, Enum):
    """Status of a reconciliation task."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    APPROVED = "approved"
    REJECTED = "rejected"
    MERGED = "merged"
    SKIPPED = "skipped"


class ReconciliationPriority(str, Enum):
    """Priority levels for reconciliation tasks."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ReconciliationTask(BaseModel):
    """A task in the reconciliation queue."""

    id: UUID
    created_at: datetime
    updated_at: datetime
    status: ReconciliationStatus = ReconciliationStatus.PENDING
    priority: ReconciliationPriority = ReconciliationPriority.MEDIUM

    # Match information
    source_entity_id: UUID
    source_entity_name: str
    source_entity_type: str
    candidate_entity_id: UUID
    candidate_entity_name: str
    candidate_entity_type: str

    # Match details
    match_strategy: MatchStrategy
    match_confidence: float
    match_details: dict[str, Any] = Field(default_factory=dict)

    # Review information
    assigned_to: str | None = None
    reviewed_by: str | None = None
    reviewed_at: datetime | None = None
    review_notes: str | None = None
    resolution: str | None = None  # "same_entity", "different", "merge_left", "merge_right"

    # Additional context
    context: dict[str, Any] = Field(default_factory=dict)


class ReconciliationStats(BaseModel):
    """Statistics about the reconciliation queue."""

    total_pending: int = 0
    total_in_progress: int = 0
    total_completed: int = 0
    total_approved: int = 0
    total_rejected: int = 0
    total_merged: int = 0
    avg_confidence: float = 0.0
    by_priority: dict[str, int] = Field(default_factory=dict)
    by_strategy: dict[str, int] = Field(default_factory=dict)


# =========================
# Reconciliation Queue
# =========================


class ReconciliationQueue:
    """Queue for managing entity reconciliation tasks.

    Provides methods for:
    - Adding matches that need review
    - Assigning tasks to analysts
    - Processing review decisions
    - Tracking statistics
    """

    def __init__(
        self,
        confidence_threshold: float = 0.9,
        auto_approve_threshold: float = 0.98,
    ):
        """Initialize the queue.

        Args:
            confidence_threshold: Below this, tasks go to queue
            auto_approve_threshold: Above this, auto-approve
        """
        self.confidence_threshold = confidence_threshold
        self.auto_approve_threshold = auto_approve_threshold

    async def add_match(
        self,
        match: MatchResult,
        priority: ReconciliationPriority | None = None,
        context: dict[str, Any] | None = None,
    ) -> ReconciliationTask | None:
        """Add a match to the reconciliation queue if needed.

        High-confidence matches are auto-approved.
        Low-confidence matches are queued for review.

        Args:
            match: Match result to potentially queue
            priority: Override auto-calculated priority
            context: Additional context for reviewers

        Returns:
            ReconciliationTask if queued, None if auto-approved
        """
        # Auto-approve very high confidence matches
        if match.confidence >= self.auto_approve_threshold:
            logger.info(
                f"Auto-approved match: {match.source.name} -> {match.target.name} "
                f"(confidence: {match.confidence:.2f})"
            )
            await self._record_auto_approval(match)
            return None

        # Calculate priority if not provided
        if priority is None:
            priority = self._calculate_priority(match)

        # Create task
        now = datetime.utcnow()
        task = ReconciliationTask(
            id=uuid4(),
            created_at=now,
            updated_at=now,
            status=ReconciliationStatus.PENDING,
            priority=priority,
            source_entity_id=match.source.entity_id,
            source_entity_name=match.source.name,
            source_entity_type=match.source.entity_type,
            candidate_entity_id=match.target.entity_id,
            candidate_entity_name=match.target.name,
            candidate_entity_type=match.target.entity_type,
            match_strategy=match.strategy,
            match_confidence=match.confidence,
            match_details=match.match_details,
            context=context or {},
        )

        # Store in database
        await self._store_task(task)

        logger.info(
            f"Queued for review: {match.source.name} -> {match.target.name} "
            f"(confidence: {match.confidence:.2f}, priority: {priority.value})"
        )

        return task

    async def add_batch(
        self,
        matches: list[MatchResult],
        context: dict[str, Any] | None = None,
    ) -> list[ReconciliationTask]:
        """Add multiple matches to the queue.

        Args:
            matches: List of match results
            context: Shared context for all tasks

        Returns:
            List of created tasks (excludes auto-approved)
        """
        tasks = []
        for match in matches:
            task = await self.add_match(match, context=context)
            if task:
                tasks.append(task)
        return tasks

    async def get_pending_tasks(
        self,
        limit: int = 50,
        priority: ReconciliationPriority | None = None,
        strategy: MatchStrategy | None = None,
        assigned_to: str | None = None,
    ) -> list[ReconciliationTask]:
        """Get pending reconciliation tasks.

        Args:
            limit: Maximum tasks to return
            priority: Filter by priority
            strategy: Filter by match strategy
            assigned_to: Filter by assignee

        Returns:
            List of pending tasks
        """
        async with get_db_session() as db:
            filters = ["status IN ('pending', 'in_progress')"]
            params: dict[str, Any] = {"limit": limit}

            if priority:
                filters.append("priority = :priority")
                params["priority"] = priority.value
            if strategy:
                filters.append("match_strategy = :strategy")
                params["strategy"] = strategy.value
            if assigned_to:
                filters.append("assigned_to = :assigned_to")
                params["assigned_to"] = assigned_to

            where_clause = " AND ".join(filters)

            query = text(f"""
                SELECT * FROM reconciliation_tasks
                WHERE {where_clause}
                ORDER BY
                    CASE priority
                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                    END,
                    created_at ASC
                LIMIT :limit
            """)

            result = await db.execute(query, params)
            rows = result.fetchall()

            return [self._row_to_task(row) for row in rows]

    async def get_task(self, task_id: UUID) -> ReconciliationTask | None:
        """Get a specific task by ID.

        Args:
            task_id: Task ID

        Returns:
            Task or None if not found
        """
        async with get_db_session() as db:
            query = text("""
                SELECT * FROM reconciliation_tasks
                WHERE id = :task_id
            """)

            result = await db.execute(query, {"task_id": str(task_id)})
            row = result.fetchone()

            if not row:
                return None

            return self._row_to_task(row)

    async def assign_task(
        self,
        task_id: UUID,
        assignee: str,
    ) -> ReconciliationTask | None:
        """Assign a task to an analyst.

        Args:
            task_id: Task ID
            assignee: Analyst username/ID

        Returns:
            Updated task or None if not found
        """
        async with get_db_session() as db:
            query = text("""
                UPDATE reconciliation_tasks
                SET assigned_to = :assignee,
                    status = 'in_progress',
                    updated_at = :updated_at
                WHERE id = :task_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "task_id": str(task_id),
                    "assignee": assignee,
                    "updated_at": datetime.utcnow(),
                },
            )
            row = result.fetchone()
            await db.commit()

            if not row:
                return None

            return self._row_to_task(row)

    async def resolve_task(
        self,
        task_id: UUID,
        resolution: str,
        reviewer: str,
        notes: str | None = None,
    ) -> ReconciliationTask | None:
        """Resolve a reconciliation task.

        Args:
            task_id: Task ID
            resolution: Resolution type ("same_entity", "different", "merge_left", "merge_right")
            reviewer: Reviewer username/ID
            notes: Optional review notes

        Returns:
            Updated task or None if not found
        """
        # Map resolution to status
        status_map = {
            "same_entity": ReconciliationStatus.APPROVED,
            "different": ReconciliationStatus.REJECTED,
            "merge_left": ReconciliationStatus.MERGED,
            "merge_right": ReconciliationStatus.MERGED,
            "skip": ReconciliationStatus.SKIPPED,
        }

        status = status_map.get(resolution, ReconciliationStatus.REJECTED)

        async with get_db_session() as db:
            now = datetime.utcnow()

            query = text("""
                UPDATE reconciliation_tasks
                SET status = :status,
                    resolution = :resolution,
                    reviewed_by = :reviewer,
                    reviewed_at = :reviewed_at,
                    review_notes = :notes,
                    updated_at = :updated_at
                WHERE id = :task_id
                RETURNING *
            """)

            result = await db.execute(
                query,
                {
                    "task_id": str(task_id),
                    "status": status.value,
                    "resolution": resolution,
                    "reviewer": reviewer,
                    "reviewed_at": now,
                    "notes": notes,
                    "updated_at": now,
                },
            )
            row = result.fetchone()
            await db.commit()

            if not row:
                return None

            task = self._row_to_task(row)

            # Apply resolution
            await self._apply_resolution(task)

            logger.info(
                f"Resolved task {task_id}: {resolution} by {reviewer}"
            )

            return task

    async def get_stats(self) -> ReconciliationStats:
        """Get statistics about the reconciliation queue.

        Returns:
            Queue statistics
        """
        async with get_db_session() as db:
            # Get counts by status
            status_query = text("""
                SELECT status, COUNT(*) as count
                FROM reconciliation_tasks
                GROUP BY status
            """)
            status_result = await db.execute(status_query)
            status_counts = {row.status: row.count for row in status_result.fetchall()}

            # Get counts by priority
            priority_query = text("""
                SELECT priority, COUNT(*) as count
                FROM reconciliation_tasks
                WHERE status IN ('pending', 'in_progress')
                GROUP BY priority
            """)
            priority_result = await db.execute(priority_query)
            priority_counts = {row.priority: row.count for row in priority_result.fetchall()}

            # Get counts by strategy
            strategy_query = text("""
                SELECT match_strategy, COUNT(*) as count
                FROM reconciliation_tasks
                WHERE status IN ('pending', 'in_progress')
                GROUP BY match_strategy
            """)
            strategy_result = await db.execute(strategy_query)
            strategy_counts = {row.match_strategy: row.count for row in strategy_result.fetchall()}

            # Get average confidence of pending tasks
            avg_query = text("""
                SELECT AVG(match_confidence) as avg_conf
                FROM reconciliation_tasks
                WHERE status IN ('pending', 'in_progress')
            """)
            avg_result = await db.execute(avg_query)
            avg_row = avg_result.fetchone()
            avg_confidence = float(avg_row.avg_conf or 0)

            return ReconciliationStats(
                total_pending=status_counts.get("pending", 0),
                total_in_progress=status_counts.get("in_progress", 0),
                total_completed=sum(
                    status_counts.get(s, 0)
                    for s in ["approved", "rejected", "merged", "skipped"]
                ),
                total_approved=status_counts.get("approved", 0),
                total_rejected=status_counts.get("rejected", 0),
                total_merged=status_counts.get("merged", 0),
                avg_confidence=avg_confidence,
                by_priority=priority_counts,
                by_strategy=strategy_counts,
            )

    def _calculate_priority(self, match: MatchResult) -> ReconciliationPriority:
        """Calculate priority based on match characteristics.

        Higher priority for:
        - Lower confidence matches
        - Deterministic strategy with conflicts
        - High-value entities (more relationships)
        """
        confidence = match.confidence

        if confidence < 0.5:
            return ReconciliationPriority.CRITICAL
        elif confidence < 0.7:
            return ReconciliationPriority.HIGH
        elif confidence < 0.85:
            return ReconciliationPriority.MEDIUM
        else:
            return ReconciliationPriority.LOW

    async def _store_task(self, task: ReconciliationTask):
        """Store a task in the database."""
        async with get_db_session() as db:
            query = text("""
                INSERT INTO reconciliation_tasks (
                    id, created_at, updated_at, status, priority,
                    source_entity_id, source_entity_name, source_entity_type,
                    candidate_entity_id, candidate_entity_name, candidate_entity_type,
                    match_strategy, match_confidence, match_details, context
                ) VALUES (
                    :id, :created_at, :updated_at, :status, :priority,
                    :source_entity_id, :source_entity_name, :source_entity_type,
                    :candidate_entity_id, :candidate_entity_name, :candidate_entity_type,
                    :match_strategy, :match_confidence, :match_details, :context
                )
            """)

            import json
            await db.execute(
                query,
                {
                    "id": str(task.id),
                    "created_at": task.created_at,
                    "updated_at": task.updated_at,
                    "status": task.status.value,
                    "priority": task.priority.value,
                    "source_entity_id": str(task.source_entity_id),
                    "source_entity_name": task.source_entity_name,
                    "source_entity_type": task.source_entity_type,
                    "candidate_entity_id": str(task.candidate_entity_id),
                    "candidate_entity_name": task.candidate_entity_name,
                    "candidate_entity_type": task.candidate_entity_type,
                    "match_strategy": task.match_strategy.value,
                    "match_confidence": task.match_confidence,
                    "match_details": json.dumps(task.match_details),
                    "context": json.dumps(task.context),
                },
            )
            await db.commit()

    async def _record_auto_approval(self, match: MatchResult):
        """Record an auto-approved match for audit purposes."""
        async with get_db_session() as db:
            query = text("""
                INSERT INTO entity_merges (
                    id, source_entity_id, target_entity_id,
                    merge_type, confidence, approved_by,
                    approved_at, match_strategy, match_details
                ) VALUES (
                    :id, :source_id, :target_id,
                    'auto_approved', :confidence, 'system',
                    :approved_at, :strategy, :details
                )
            """)

            import json
            await db.execute(
                query,
                {
                    "id": str(uuid4()),
                    "source_id": str(match.source.entity_id),
                    "target_id": str(match.target.entity_id),
                    "confidence": match.confidence,
                    "approved_at": datetime.utcnow(),
                    "strategy": match.strategy.value,
                    "details": json.dumps(match.match_details),
                },
            )
            await db.commit()

    async def _apply_resolution(self, task: ReconciliationTask):
        """Apply the resolution (merge entities if needed)."""
        if task.resolution == "same_entity":
            # Record as confirmed match
            await self._record_confirmed_match(task)
        elif task.resolution in ("merge_left", "merge_right"):
            # Perform entity merge
            if task.resolution == "merge_left":
                # Keep source, merge target into source
                await self._merge_entities(
                    keep_id=task.source_entity_id,
                    merge_id=task.candidate_entity_id,
                    reviewer=task.reviewed_by,
                )
            else:
                # Keep target, merge source into target
                await self._merge_entities(
                    keep_id=task.candidate_entity_id,
                    merge_id=task.source_entity_id,
                    reviewer=task.reviewed_by,
                )
        elif task.resolution == "different":
            # Record as non-match to prevent future matching
            await self._record_non_match(task)

    async def _record_confirmed_match(self, task: ReconciliationTask):
        """Record a confirmed match relationship."""
        async with get_db_session() as db:
            query = text("""
                INSERT INTO entity_merges (
                    id, source_entity_id, target_entity_id,
                    merge_type, confidence, approved_by,
                    approved_at, match_strategy, notes
                ) VALUES (
                    :id, :source_id, :target_id,
                    'confirmed_same', :confidence, :reviewer,
                    :approved_at, :strategy, :notes
                )
            """)

            await db.execute(
                query,
                {
                    "id": str(uuid4()),
                    "source_id": str(task.source_entity_id),
                    "target_id": str(task.candidate_entity_id),
                    "confidence": task.match_confidence,
                    "reviewer": task.reviewed_by,
                    "approved_at": task.reviewed_at,
                    "strategy": task.match_strategy.value,
                    "notes": task.review_notes,
                },
            )
            await db.commit()

    async def _record_non_match(self, task: ReconciliationTask):
        """Record entities as confirmed different."""
        async with get_db_session() as db:
            query = text("""
                INSERT INTO entity_non_matches (
                    id, entity_id_1, entity_id_2,
                    confirmed_by, confirmed_at, notes
                ) VALUES (
                    :id, :id_1, :id_2,
                    :reviewer, :confirmed_at, :notes
                )
            """)

            await db.execute(
                query,
                {
                    "id": str(uuid4()),
                    "id_1": str(task.source_entity_id),
                    "id_2": str(task.candidate_entity_id),
                    "reviewer": task.reviewed_by,
                    "confirmed_at": task.reviewed_at,
                    "notes": task.review_notes,
                },
            )
            await db.commit()

    async def _merge_entities(
        self,
        keep_id: UUID,
        merge_id: UUID,
        reviewer: str | None,
    ):
        """Merge two entities, keeping one and redirecting the other.

        This updates all relationships to point to the kept entity
        and marks the merged entity as merged.
        """
        async with get_db_session() as db:
            now = datetime.utcnow()

            # Update relationships to point to kept entity
            update_rels_query = text("""
                UPDATE relationships
                SET from_entity_id = :keep_id
                WHERE from_entity_id = :merge_id
            """)
            await db.execute(
                update_rels_query,
                {"keep_id": str(keep_id), "merge_id": str(merge_id)},
            )

            update_rels_query2 = text("""
                UPDATE relationships
                SET to_entity_id = :keep_id
                WHERE to_entity_id = :merge_id
            """)
            await db.execute(
                update_rels_query2,
                {"keep_id": str(keep_id), "merge_id": str(merge_id)},
            )

            # Mark merged entity
            mark_merged_query = text("""
                UPDATE entities
                SET status = 'MERGED',
                    merged_into = :keep_id,
                    merged_at = :merged_at,
                    merged_by = :reviewer
                WHERE id = :merge_id
            """)
            await db.execute(
                mark_merged_query,
                {
                    "keep_id": str(keep_id),
                    "merge_id": str(merge_id),
                    "merged_at": now,
                    "reviewer": reviewer,
                },
            )

            # Record merge
            record_query = text("""
                INSERT INTO entity_merges (
                    id, source_entity_id, target_entity_id,
                    merge_type, approved_by, approved_at
                ) VALUES (
                    :id, :source_id, :target_id,
                    'merged', :reviewer, :merged_at
                )
            """)
            await db.execute(
                record_query,
                {
                    "id": str(uuid4()),
                    "source_id": str(merge_id),
                    "target_id": str(keep_id),
                    "reviewer": reviewer,
                    "merged_at": now,
                },
            )

            await db.commit()

            logger.info(
                f"Merged entity {merge_id} into {keep_id} by {reviewer}"
            )

    def _row_to_task(self, row) -> ReconciliationTask:
        """Convert database row to ReconciliationTask."""
        import json

        return ReconciliationTask(
            id=UUID(row.id) if isinstance(row.id, str) else row.id,
            created_at=row.created_at,
            updated_at=row.updated_at,
            status=ReconciliationStatus(row.status),
            priority=ReconciliationPriority(row.priority),
            source_entity_id=UUID(row.source_entity_id) if isinstance(row.source_entity_id, str) else row.source_entity_id,
            source_entity_name=row.source_entity_name,
            source_entity_type=row.source_entity_type,
            candidate_entity_id=UUID(row.candidate_entity_id) if isinstance(row.candidate_entity_id, str) else row.candidate_entity_id,
            candidate_entity_name=row.candidate_entity_name,
            candidate_entity_type=row.candidate_entity_type,
            match_strategy=MatchStrategy(row.match_strategy),
            match_confidence=row.match_confidence,
            match_details=json.loads(row.match_details) if isinstance(row.match_details, str) else (row.match_details or {}),
            assigned_to=row.assigned_to,
            reviewed_by=row.reviewed_by,
            reviewed_at=row.reviewed_at,
            review_notes=row.review_notes,
            resolution=row.resolution,
            context=json.loads(row.context) if isinstance(row.context, str) else (row.context or {}),
        )


# =========================
# Convenience Functions
# =========================


async def get_reconciliation_queue() -> ReconciliationQueue:
    """Get the default reconciliation queue instance."""
    return ReconciliationQueue()


async def queue_low_confidence_matches(
    matches: list[MatchResult],
    confidence_threshold: float = 0.9,
) -> list[ReconciliationTask]:
    """Queue matches below confidence threshold for review.

    Args:
        matches: List of match results
        confidence_threshold: Threshold below which to queue

    Returns:
        List of created tasks
    """
    queue = ReconciliationQueue(confidence_threshold=confidence_threshold)

    tasks = []
    for match in matches:
        if match.confidence < confidence_threshold:
            task = await queue.add_match(match)
            if task:
                tasks.append(task)

    return tasks
