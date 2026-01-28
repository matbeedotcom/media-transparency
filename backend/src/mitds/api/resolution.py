"""Entity resolution API endpoints for MITDS."""

from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import PaginatedResponse
from .auth import CurrentUser, OptionalUser

router = APIRouter(prefix="/resolution")


# =========================
# Response Models
# =========================


class CandidateResponse(BaseModel):
    """Resolution candidate response."""

    id: UUID
    status: str
    priority: str
    source_entity_id: UUID
    source_entity_name: str
    source_entity_type: str
    candidate_entity_id: UUID
    candidate_entity_name: str
    candidate_entity_type: str
    match_strategy: str
    match_confidence: float
    match_details: dict[str, Any] = {}
    created_at: str
    assigned_to: str | None = None


class ResolutionStatsResponse(BaseModel):
    """Resolution statistics response."""

    total_pending: int
    total_in_progress: int
    total_completed: int
    total_approved: int
    total_rejected: int
    total_merged: int
    avg_confidence: float
    by_priority: dict[str, int]
    by_strategy: dict[str, int]


# =========================
# List Candidates
# =========================


@router.get("/candidates")
async def list_candidates(
    status: str = Query("pending", pattern="^(pending|in_progress|all)$"),
    priority: str | None = Query(None),
    strategy: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: OptionalUser = None,
):
    """List pending resolution candidates with pagination."""
    from ..resolution.reconcile import (
        ReconciliationQueue,
        ReconciliationPriority,
    )
    from ..resolution.matcher import MatchStrategy

    queue = ReconciliationQueue()

    priority_filter = None
    if priority:
        try:
            priority_filter = ReconciliationPriority(priority)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid priority: {priority}")

    strategy_filter = None
    if strategy:
        try:
            strategy_filter = MatchStrategy(strategy)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid strategy: {strategy}")

    tasks = await queue.get_pending_tasks(
        limit=limit,
        priority=priority_filter,
        strategy=strategy_filter,
    )

    candidates = [
        CandidateResponse(
            id=task.id,
            status=task.status.value,
            priority=task.priority.value,
            source_entity_id=task.source_entity_id,
            source_entity_name=task.source_entity_name,
            source_entity_type=task.source_entity_type,
            candidate_entity_id=task.candidate_entity_id,
            candidate_entity_name=task.candidate_entity_name,
            candidate_entity_type=task.candidate_entity_type,
            match_strategy=task.match_strategy.value,
            match_confidence=task.match_confidence,
            match_details=task.match_details,
            created_at=task.created_at.isoformat(),
            assigned_to=task.assigned_to,
        )
        for task in tasks
    ]

    return PaginatedResponse(
        results=candidates,
        total=len(candidates),
        limit=limit,
        offset=offset,
    )


# =========================
# Merge Candidate
# =========================


@router.post("/candidates/{candidate_id}/merge")
async def merge_candidate(
    candidate_id: UUID,
    user: OptionalUser = None,
):
    """Approve and merge a resolution candidate."""
    from ..resolution.reconcile import ReconciliationQueue

    queue = ReconciliationQueue()
    task = await queue.get_task(candidate_id)

    if not task:
        raise HTTPException(status_code=404, detail="Candidate not found")

    reviewer = user.id if user else "anonymous"
    resolved = await queue.resolve_task(
        task_id=candidate_id,
        resolution="same_entity",
        reviewer=reviewer,
        notes="Approved via API",
    )

    if not resolved:
        raise HTTPException(status_code=500, detail="Failed to resolve task")

    return {
        "id": str(resolved.id),
        "status": resolved.status.value,
        "resolution": "same_entity",
        "reviewer": reviewer,
    }


# =========================
# Reject Candidate
# =========================


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(
    candidate_id: UUID,
    user: OptionalUser = None,
):
    """Reject a resolution candidate."""
    from ..resolution.reconcile import ReconciliationQueue

    queue = ReconciliationQueue()
    task = await queue.get_task(candidate_id)

    if not task:
        raise HTTPException(status_code=404, detail="Candidate not found")

    reviewer = user.id if user else "anonymous"
    resolved = await queue.resolve_task(
        task_id=candidate_id,
        resolution="different",
        reviewer=reviewer,
        notes="Rejected via API",
    )

    if not resolved:
        raise HTTPException(status_code=500, detail="Failed to resolve task")

    return {
        "id": str(resolved.id),
        "status": resolved.status.value,
        "resolution": "different",
        "reviewer": reviewer,
    }


# =========================
# Trigger Resolution Run
# =========================


@router.post("/trigger")
async def trigger_resolution(
    entity_type: str = Query("all", pattern="^(Organization|Person|Outlet|all)$"),
    dry_run: bool = Query(False),
    user: OptionalUser = None,
):
    """Trigger a resolution run."""
    from ..resolution.resolver import EntityResolver

    entity_types = (
        ["Organization", "Person", "Outlet"]
        if entity_type == "all"
        else [entity_type]
    )

    resolver = EntityResolver()
    total_candidates = 0

    for etype in entity_types:
        duplicates = await resolver.find_duplicates(etype)
        total_candidates += len(duplicates)

    return {
        "status": "completed" if not dry_run else "dry_run",
        "entity_types": entity_types,
        "candidates_found": total_candidates,
        "dry_run": dry_run,
    }


# =========================
# Resolution Stats
# =========================


@router.get("/stats")
async def resolution_stats(
    user: OptionalUser = None,
):
    """Get resolution queue statistics."""
    from ..resolution.reconcile import ReconciliationQueue

    queue = ReconciliationQueue()
    stats = await queue.get_stats()

    return ResolutionStatsResponse(
        total_pending=stats.total_pending,
        total_in_progress=stats.total_in_progress,
        total_completed=stats.total_completed,
        total_approved=stats.total_approved,
        total_rejected=stats.total_rejected,
        total_merged=stats.total_merged,
        avg_confidence=stats.avg_confidence,
        by_priority=stats.by_priority,
        by_strategy=stats.by_strategy,
    )
