"""API endpoints for research investigations.

Provides REST API for managing research sessions that
"follow the leads" through entity networks.
"""

from typing import Any
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db
from ..research import (
    CreateSessionRequest,
    EntryPointType,
    LeadStatus,
    LeadSummary,
    LeadType,
    QueueStats,
    ResearchSession,
    ResearchSessionConfig,
    SessionGraph,
    SessionResponse,
    SessionStats,
    SessionStatus,
    get_processor,
    get_queue_manager,
    get_session_manager,
)

router = APIRouter(prefix="/research", tags=["research"])


# =============================================================================
# Request/Response Models
# =============================================================================


class SessionListResponse(BaseModel):
    """Response with list of sessions."""

    sessions: list[SessionResponse]
    total: int


class EntitySummaryResponse(BaseModel):
    """Summary of an entity in a session."""

    id: str
    name: str
    entity_type: str
    depth: int
    relevance_score: float


class EntitiesListResponse(BaseModel):
    """Response with list of entities."""

    entities: list[EntitySummaryResponse]
    total: int


class LeadsListResponse(BaseModel):
    """Response with list of leads."""

    leads: list[LeadSummary]
    total: int
    stats: QueueStats


class SkipLeadRequest(BaseModel):
    """Request to skip a lead."""

    reason: str = Field(..., min_length=1)


class PrioritizeLeadRequest(BaseModel):
    """Request to change lead priority."""

    priority: int = Field(..., ge=1, le=5)


# =============================================================================
# Session Endpoints
# =============================================================================


@router.post("/sessions", response_model=SessionResponse)
async def create_session(
    request: CreateSessionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Create and optionally start a new research session.

    The session is created in INITIALIZING state. Use the
    /sessions/{id}/start endpoint to begin processing, or
    pass `start=true` query parameter.
    """
    manager = get_session_manager()

    session = await manager.create_session(
        name=request.name,
        description=request.description,
        entry_point_type=request.entry_point_type,
        entry_point_value=request.entry_point_value,
        config=request.config,
    )

    return _session_to_response(session)


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    status: SessionStatus | None = None,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List research sessions with optional filtering."""
    manager = get_session_manager()

    sessions = await manager.list_sessions(
        status=status,
        limit=limit,
        offset=offset,
    )

    return SessionListResponse(
        sessions=[_session_to_response(s) for s in sessions],
        total=len(sessions),  # TODO: Get actual total count
    )


@router.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get details for a specific session."""
    manager = get_session_manager()

    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return _session_to_response(session)


@router.post("/sessions/{session_id}/start", response_model=SessionResponse)
async def start_session(
    session_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Start processing a session.

    This creates the initial lead from the entry point and
    marks the session as RUNNING.
    """
    manager = get_session_manager()
    processor = get_processor()

    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if session.status not in [SessionStatus.INITIALIZING, SessionStatus.PAUSED]:
        raise HTTPException(
            status_code=400,
            detail=f"Session cannot be started (status: {session.status})",
        )

    # Start in background
    background_tasks.add_task(processor.process_session, session_id)

    # Update status
    session = await manager.start_session(session_id)
    return _session_to_response(session)


@router.post("/sessions/{session_id}/pause", response_model=SessionResponse)
async def pause_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Pause a running session."""
    manager = get_session_manager()

    session = await manager.pause_session(session_id)
    if not session:
        raise HTTPException(
            status_code=400,
            detail="Session cannot be paused (not running?)",
        )

    return _session_to_response(session)


@router.post("/sessions/{session_id}/resume", response_model=SessionResponse)
async def resume_session(
    session_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Resume a paused session."""
    manager = get_session_manager()
    processor = get_processor()

    session = await manager.resume_session(session_id)
    if not session:
        raise HTTPException(
            status_code=400,
            detail="Session cannot be resumed (not paused?)",
        )

    # Continue processing in background
    background_tasks.add_task(processor.process_session, session_id)

    return _session_to_response(session)


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Delete a session and all its data."""
    manager = get_session_manager()

    deleted = await manager.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")

    return {"deleted": True, "session_id": str(session_id)}


# =============================================================================
# Session Data Endpoints
# =============================================================================


@router.get("/sessions/{session_id}/entities", response_model=EntitiesListResponse)
async def get_session_entities(
    session_id: UUID,
    depth: int | None = None,
    entity_type: str | None = None,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get entities discovered in a session."""
    manager = get_session_manager()

    # Verify session exists
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    entities = await manager.get_session_entities(
        session_id,
        depth=depth,
        entity_type=entity_type,
        limit=limit,
        offset=offset,
    )

    total = await manager.get_session_entity_count(session_id)

    return EntitiesListResponse(
        entities=[
            EntitySummaryResponse(
                id=str(e["id"]),
                name=e["name"],
                entity_type=e["entity_type"],
                depth=e["depth"],
                relevance_score=e["relevance_score"],
            )
            for e in entities
        ],
        total=total,
    )


@router.get("/sessions/{session_id}/graph", response_model=SessionGraph)
async def get_session_graph(
    session_id: UUID,
    max_nodes: int = Query(default=200, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Get session discoveries as a graph for visualization."""
    manager = get_session_manager()

    # Verify session exists
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Get entities
    entities = await manager.get_session_entities(session_id, limit=max_nodes)

    # Build nodes
    nodes = [
        {
            "id": str(e["id"]),
            "label": e["name"],
            "type": e["entity_type"],
            "properties": {
                "depth": e["depth"],
                "relevance_score": e["relevance_score"],
            },
        }
        for e in entities
    ]

    # TODO: Add edges from relationships
    edges = []

    return SessionGraph(nodes=nodes, edges=edges)


# =============================================================================
# Lead Queue Endpoints
# =============================================================================


@router.get("/sessions/{session_id}/leads", response_model=LeadsListResponse)
async def get_session_leads(
    session_id: UUID,
    status: LeadStatus | None = None,
    lead_type: LeadType | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get leads in a session's queue."""
    manager = get_session_manager()
    queue = get_queue_manager()

    # Verify session exists
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Get leads (currently only supports pending filter)
    if status == LeadStatus.PENDING or status is None:
        leads = await queue.get_pending_leads(
            session_id,
            lead_type=lead_type,
            limit=limit,
            offset=offset,
        )
    else:
        leads = []

    stats = await queue.get_queue_stats(session_id)

    return LeadsListResponse(
        leads=[
            LeadSummary(
                id=lead.id,
                lead_type=lead.lead_type,
                target_identifier=lead.target_identifier,
                target_identifier_type=lead.target_identifier_type,
                priority=lead.priority,
                confidence=lead.confidence,
                depth=lead.depth,
                status=lead.status,
                created_at=lead.created_at,
                processed_at=lead.processed_at,
            )
            for lead in leads
        ],
        total=stats.total,
        stats=stats,
    )


@router.post("/sessions/{session_id}/leads/{lead_id}/skip")
async def skip_lead(
    session_id: UUID,
    lead_id: UUID,
    request: SkipLeadRequest,
    db: AsyncSession = Depends(get_db),
):
    """Manually skip a pending lead."""
    queue = get_queue_manager()

    lead = await queue.skip_lead(lead_id, request.reason)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    return {"skipped": True, "lead_id": str(lead_id), "reason": request.reason}


@router.post("/sessions/{session_id}/leads/{lead_id}/prioritize")
async def prioritize_lead(
    session_id: UUID,
    lead_id: UUID,
    request: PrioritizeLeadRequest,
    db: AsyncSession = Depends(get_db),
):
    """Change a lead's priority."""
    queue = get_queue_manager()

    lead = await queue.set_priority(lead_id, request.priority)
    if not lead:
        raise HTTPException(
            status_code=404,
            detail="Lead not found or not in pending status",
        )

    return {
        "updated": True,
        "lead_id": str(lead_id),
        "new_priority": request.priority,
    }


@router.post("/sessions/{session_id}/leads/{lead_id}/requeue")
async def requeue_lead(
    session_id: UUID,
    lead_id: UUID,
    priority: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Requeue a failed or skipped lead."""
    queue = get_queue_manager()

    lead = await queue.requeue_lead(lead_id, new_priority=priority)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    return {"requeued": True, "lead_id": str(lead_id)}


# =============================================================================
# Helper Functions
# =============================================================================


def _session_to_response(session: ResearchSession) -> SessionResponse:
    """Convert ResearchSession to SessionResponse."""
    return SessionResponse(
        id=session.id,
        name=session.name,
        description=session.description,
        entry_point_type=session.entry_point_type,
        entry_point_value=session.entry_point_value,
        status=session.status,
        stats=session.stats,
        created_at=session.created_at,
        updated_at=session.updated_at,
        started_at=session.started_at,
        completed_at=session.completed_at,
    )
