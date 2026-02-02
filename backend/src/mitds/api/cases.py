"""API endpoints for Case Intake System.

Provides REST endpoints for case management, entity match review,
and report generation.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel

from ..cases.manager import CaseManager, get_case_manager
from ..cases.models import (
    Case,
    CaseResponse,
    CaseStatus,
    CaseSummary,
    CreateCaseRequest,
    EntityMatchResponse,
    MatchStatus,
    ProcessingDetails,
)
from ..cases.reports.generator import ReportGenerator, get_report_generator
from ..cases.reports.templates import export_report
from ..cases.review.queue import EntityMatchQueue, get_match_queue

router = APIRouter(prefix="/cases", tags=["cases"])


# =============================================================================
# Case Management Endpoints
# =============================================================================


class CaseListResponse(BaseModel):
    """Response for case list endpoint."""

    items: list[CaseSummary]
    total: int


@router.get("", response_model=CaseListResponse)
async def list_cases(
    status: CaseStatus | None = None,
    created_by: str | None = None,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    manager: CaseManager = Depends(get_case_manager),
) -> CaseListResponse:
    """List cases with optional filters."""
    items, total = await manager.list_cases(
        status=status,
        created_by=created_by,
        limit=limit,
        offset=offset,
    )
    return CaseListResponse(items=items, total=total)


@router.post("", response_model=CaseResponse, status_code=201)
async def create_case(
    request: CreateCaseRequest,
    manager: CaseManager = Depends(get_case_manager),
) -> CaseResponse:
    """Create a new case from an entry point."""
    case = await manager.create_case(request)
    return _case_to_response(case)


@router.get("/{case_id}", response_model=CaseResponse)
async def get_case(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> CaseResponse:
    """Get case details."""
    case = await manager.get_case(case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")
    return _case_to_response(case)


@router.delete("/{case_id}", status_code=204)
async def delete_case(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> None:
    """Delete a case."""
    deleted = await manager.delete_case(case_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Case not found")


@router.post("/{case_id}/start", response_model=CaseResponse)
async def start_case(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> CaseResponse:
    """Start processing a case."""
    try:
        case = await manager.start_processing(case_id)
        return _case_to_response(case)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{case_id}/pause", response_model=CaseResponse)
async def pause_case(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> CaseResponse:
    """Pause case processing."""
    try:
        case = await manager.pause_case(case_id)
        return _case_to_response(case)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{case_id}/resume", response_model=CaseResponse)
async def resume_case(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> CaseResponse:
    """Resume case processing."""
    try:
        case = await manager.resume_case(case_id)
        return _case_to_response(case)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{case_id}/processing", response_model=ProcessingDetails)
async def get_case_processing(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
) -> ProcessingDetails:
    """Get detailed processing information for an active case.
    
    Returns real-time processing statistics including:
    - Current processing phase
    - Progress percentage
    - Lead counts (pending, completed, failed)
    - Recently discovered entities
    - Current leads being processed
    """
    try:
        return await manager.get_processing_details(case_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# =============================================================================
# Report Endpoints
# =============================================================================


@router.get("/{case_id}/report")
async def get_case_report(
    case_id: UUID,
    format: str = Query(default="json", pattern="^(json|markdown|md)$"),
    manager: CaseManager = Depends(get_case_manager),
    generator: ReportGenerator = Depends(get_report_generator),
) -> Response:
    """Get case report in specified format."""
    case = await manager.get_case(case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")

    # Get or generate report
    report = await generator.get_report(case_id)
    if report is None:
        # Generate report on demand
        report = await generator.generate(case)

    # Export in requested format
    content = export_report(report, format)

    if format == "json":
        return Response(content=content, media_type="application/json")
    else:
        return Response(content=content, media_type="text/markdown")


@router.post("/{case_id}/report")
async def generate_case_report(
    case_id: UUID,
    manager: CaseManager = Depends(get_case_manager),
    generator: ReportGenerator = Depends(get_report_generator),
) -> Response:
    """Generate or regenerate case report."""
    case = await manager.get_case(case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")

    report = await generator.generate(case)
    content = export_report(report, "json")
    return Response(content=content, media_type="application/json")


# =============================================================================
# Entity Match Review Endpoints
# =============================================================================


class MatchListResponse(BaseModel):
    """Response for match list endpoint."""

    items: list[EntityMatchResponse]
    pending_count: int


@router.get("/{case_id}/matches", response_model=MatchListResponse)
async def list_case_matches(
    case_id: UUID,
    status: MatchStatus = MatchStatus.PENDING,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    queue: EntityMatchQueue = Depends(get_match_queue),
) -> MatchListResponse:
    """List entity matches for a case."""
    matches, total = await queue.get_pending(case_id, limit=limit, offset=offset)

    # Get full details for each match
    items = []
    for match in matches:
        response = await queue.get_match_with_entities(match.id)
        if response:
            items.append(response)

    return MatchListResponse(items=items, pending_count=total)


class ReviewRequest(BaseModel):
    """Request for match review actions."""

    notes: str | None = None


@router.post("/matches/{match_id}/approve", response_model=EntityMatchResponse)
async def approve_match(
    match_id: UUID,
    request: ReviewRequest | None = None,
    reviewed_by: str = Query(default="anonymous"),
    queue: EntityMatchQueue = Depends(get_match_queue),
) -> EntityMatchResponse:
    """Approve an entity match."""
    try:
        notes = request.notes if request else None
        await queue.approve(match_id, reviewed_by, notes)
        response = await queue.get_match_with_entities(match_id)
        if response is None:
            raise HTTPException(status_code=404, detail="Match not found")
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/matches/{match_id}/reject", response_model=EntityMatchResponse)
async def reject_match(
    match_id: UUID,
    request: ReviewRequest | None = None,
    reviewed_by: str = Query(default="anonymous"),
    queue: EntityMatchQueue = Depends(get_match_queue),
) -> EntityMatchResponse:
    """Reject an entity match."""
    try:
        notes = request.notes if request else None
        await queue.reject(match_id, reviewed_by, notes)
        response = await queue.get_match_with_entities(match_id)
        if response is None:
            raise HTTPException(status_code=404, detail="Match not found")
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/matches/{match_id}/defer", response_model=EntityMatchResponse)
async def defer_match(
    match_id: UUID,
    request: ReviewRequest | None = None,
    reviewed_by: str = Query(default="anonymous"),
    queue: EntityMatchQueue = Depends(get_match_queue),
) -> EntityMatchResponse:
    """Defer an entity match for later review."""
    try:
        notes = request.notes if request else None
        await queue.defer(match_id, reviewed_by, notes)
        response = await queue.get_match_with_entities(match_id)
        if response is None:
            raise HTTPException(status_code=404, detail="Match not found")
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# Helper Functions
# =============================================================================


def _case_to_response(case: Case) -> CaseResponse:
    """Convert a Case to CaseResponse."""
    return CaseResponse(
        id=case.id,
        name=case.name,
        description=case.description,
        entry_point_type=case.entry_point_type,
        entry_point_value=case.entry_point_value,
        status=case.status,
        config=case.config,
        stats=case.stats,
        research_session_id=case.research_session_id,
        created_at=case.created_at,
        updated_at=case.updated_at,
        completed_at=case.completed_at,
    )
