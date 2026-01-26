"""Detection API endpoints for MITDS.

Provides endpoints for:
- Temporal coordination analysis
- Composite coordination scoring
- Detection result explanation
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from . import NotFoundError
from .auth import CurrentUser, OptionalUser
from ..db import get_db_session, get_neo4j_session
from ..detection.temporal import (
    TemporalCoordinationDetector,
    TemporalCoordinationResult,
    TimingEvent,
    BurstDetectionResult,
    LeadLagResult,
    SynchronizationResult,
)
from ..logging import get_context_logger

logger = get_context_logger(__name__)

router = APIRouter(prefix="/detection")


# =========================
# Request/Response Models
# =========================


class TemporalAnalysisRequest(BaseModel):
    """Request for temporal coordination analysis."""

    entity_ids: list[UUID] = Field(..., description="Entity IDs to analyze")
    start_date: datetime = Field(..., description="Start of analysis window")
    end_date: datetime = Field(..., description="End of analysis window")
    event_types: list[str] | None = Field(
        None,
        description="Event types to include (default: all)",
    )
    exclude_hard_negatives: bool = Field(
        True,
        description="Filter out legitimate coordination (breaking news, etc.)",
    )
    async_mode: bool = Field(
        False,
        description="Run analysis asynchronously and return job ID",
    )


class TemporalAnalysisResponse(BaseModel):
    """Response for temporal coordination analysis."""

    analysis_id: str
    status: str = "completed"
    analyzed_at: datetime
    time_range_start: datetime
    time_range_end: datetime
    entity_count: int
    event_count: int

    # Results
    coordination_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    is_coordinated: bool
    explanation: str

    # Detailed results
    bursts: list[dict[str, Any]] = Field(default_factory=list)
    lead_lag_pairs: list[dict[str, Any]] = Field(default_factory=list)
    synchronized_groups: list[dict[str, Any]] = Field(default_factory=list)
    hard_negatives_filtered: int = 0


class CompositeScoreRequest(BaseModel):
    """Request for composite coordination score."""

    entity_ids: list[UUID] = Field(..., description="Entity IDs to analyze")
    weights: dict[str, float] | None = Field(
        None,
        description="Custom weights for score components",
    )
    include_temporal: bool = True
    include_funding: bool = True
    include_infrastructure: bool = True


class CompositeScoreResponse(BaseModel):
    """Response for composite coordination score."""

    finding_id: str
    overall_score: float = Field(ge=0.0, le=1.0)
    signal_breakdown: dict[str, float]
    flagged: bool
    confidence_band: dict[str, float]
    entities_analyzed: int
    explanation: str


class FindingExplanation(BaseModel):
    """Detailed explanation of a detection finding."""

    finding_id: str
    finding_type: str
    entity_ids: list[str]
    score: float
    confidence: float
    why_flagged: str
    evidence_summary: list[dict[str, Any]]
    hard_negatives_checked: list[dict[str, Any]]
    recommendations: list[str]


# =========================
# Temporal Coordination
# =========================


async def _run_temporal_analysis(
    job_id: str,
    request: TemporalAnalysisRequest,
) -> dict[str, Any]:
    """Run temporal analysis and store results."""
    from sqlalchemy import text

    try:
        # Fetch events for the specified entities and time range
        events = await _fetch_timing_events(
            entity_ids=request.entity_ids,
            start_date=request.start_date,
            end_date=request.end_date,
            event_types=request.event_types,
        )

        # Run temporal coordination detection
        detector = TemporalCoordinationDetector()
        result = await detector.detect_coordination(
            events=events,
            entity_ids=[str(eid) for eid in request.entity_ids],
            exclude_hard_negatives=request.exclude_hard_negatives,
        )

        # Store result
        async with get_db_session() as db:
            store_query = text("""
                UPDATE jobs
                SET status = 'completed',
                    completed_at = :completed_at,
                    result = :result
                WHERE id = :job_id
            """)
            await db.execute(
                store_query,
                {
                    "job_id": job_id,
                    "completed_at": datetime.utcnow(),
                    "result": result.model_dump_json(),
                },
            )
            await db.commit()

        return result.model_dump()

    except Exception as e:
        logger.exception(f"Temporal analysis failed for job {job_id}")

        async with get_db_session() as db:
            error_query = text("""
                UPDATE jobs
                SET status = 'failed',
                    completed_at = :completed_at,
                    error = :error
                WHERE id = :job_id
            """)
            await db.execute(
                error_query,
                {
                    "job_id": job_id,
                    "completed_at": datetime.utcnow(),
                    "error": str(e),
                },
            )
            await db.commit()

        raise


async def _fetch_timing_events(
    entity_ids: list[UUID],
    start_date: datetime,
    end_date: datetime,
    event_types: list[str] | None = None,
) -> list[TimingEvent]:
    """Fetch timing events from the database."""
    from sqlalchemy import text

    async with get_db_session() as db:
        # Build event type filter
        type_filter = ""
        if event_types:
            type_list = ", ".join(f"'{t}'" for t in event_types)
            type_filter = f"AND event_type IN ({type_list})"

        # Convert UUIDs to strings for query
        entity_id_strs = [str(eid) for eid in entity_ids]
        entity_list = ", ".join(f"'{eid}'" for eid in entity_id_strs)

        query = text(f"""
            SELECT
                entity_id,
                event_time,
                event_type,
                metadata
            FROM events
            WHERE entity_id IN ({entity_list})
            AND event_time BETWEEN :start_date AND :end_date
            {type_filter}
            ORDER BY event_time
        """)

        result = await db.execute(
            query,
            {"start_date": start_date, "end_date": end_date},
        )
        rows = result.fetchall()

        events = []
        for row in rows:
            events.append(TimingEvent(
                entity_id=str(row.entity_id),
                timestamp=row.event_time,
                event_type=row.event_type,
                metadata=row.metadata or {},
            ))

        return events


@router.post("/temporal-coordination")
async def analyze_temporal_coordination(
    request: TemporalAnalysisRequest,
    background_tasks: BackgroundTasks,
    user: CurrentUser = None,
) -> TemporalAnalysisResponse | dict[str, Any]:
    """Analyze temporal coordination between entities.

    Detects patterns indicating coordinated timing:
    - Publication bursts
    - Lead-lag relationships
    - Synchronized timing distributions

    Args:
        request: Analysis parameters
        background_tasks: FastAPI background tasks

    Returns:
        Analysis results or job ID for async mode
    """
    if len(request.entity_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least 2 entities required for coordination analysis",
        )

    if request.end_date <= request.start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date must be after start_date",
        )

    # For async mode, create a job and return job ID
    if request.async_mode:
        from sqlalchemy import text

        job_id = str(uuid4())

        async with get_db_session() as db:
            create_job = text("""
                INSERT INTO jobs (id, job_type, status, created_at, metadata)
                VALUES (:id, 'temporal_analysis', 'pending', :created_at, :metadata)
            """)
            await db.execute(
                create_job,
                {
                    "id": job_id,
                    "created_at": datetime.utcnow(),
                    "metadata": {
                        "entity_count": len(request.entity_ids),
                        "time_range": f"{request.start_date} to {request.end_date}",
                    },
                },
            )
            await db.commit()

        # Start background task
        background_tasks.add_task(_run_temporal_analysis, job_id, request)

        return {
            "job_id": job_id,
            "status": "pending",
            "status_url": f"/api/v1/jobs/{job_id}",
            "message": "Temporal analysis started",
        }

    # Synchronous mode - run analysis directly
    events = await _fetch_timing_events(
        entity_ids=request.entity_ids,
        start_date=request.start_date,
        end_date=request.end_date,
        event_types=request.event_types,
    )

    # Calculate hard negatives filtered count
    original_count = len(events)

    detector = TemporalCoordinationDetector()
    result = await detector.detect_coordination(
        events=events,
        entity_ids=[str(eid) for eid in request.entity_ids],
        exclude_hard_negatives=request.exclude_hard_negatives,
    )

    hard_negatives_filtered = original_count - result.event_count if request.exclude_hard_negatives else 0

    return TemporalAnalysisResponse(
        analysis_id=result.analysis_id,
        status="completed",
        analyzed_at=result.analyzed_at,
        time_range_start=result.time_range_start,
        time_range_end=result.time_range_end,
        entity_count=result.entity_count,
        event_count=result.event_count,
        coordination_score=result.coordination_score,
        confidence=result.confidence,
        is_coordinated=result.is_coordinated,
        explanation=result.explanation,
        bursts=[b.model_dump() for b in result.bursts],
        lead_lag_pairs=[p.model_dump() for p in result.lead_lag_pairs],
        synchronized_groups=[g.model_dump() for g in result.synchronized_groups],
        hard_negatives_filtered=hard_negatives_filtered,
    )


# =========================
# Composite Score
# =========================


@router.post("/composite-score")
async def calculate_composite_score(
    request: CompositeScoreRequest,
    user: CurrentUser = None,
) -> CompositeScoreResponse:
    """Calculate composite coordination score combining all signals.

    Combines multiple detection signals:
    - Temporal coordination (timing patterns)
    - Funding relationships (shared funders)
    - Infrastructure sharing (technical overlap)

    The composite score requires at least 2 independent signals
    to flag potential coordination (no single signal triggers alone).

    Args:
        request: Score calculation parameters

    Returns:
        Composite score with breakdown by signal type
    """
    if len(request.entity_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least 2 entities required for coordination scoring",
        )

    finding_id = str(uuid4())
    signal_scores = {}
    confidence_values = []

    # Default weights
    weights = request.weights or {
        "temporal": 0.4,
        "funding": 0.4,
        "infrastructure": 0.2,
    }

    # Calculate temporal score if requested
    if request.include_temporal:
        try:
            # Get recent events for temporal analysis
            end_date = datetime.utcnow()
            start_date = end_date.replace(year=end_date.year - 1)

            events = await _fetch_timing_events(
                entity_ids=request.entity_ids,
                start_date=start_date,
                end_date=end_date,
            )

            if events:
                detector = TemporalCoordinationDetector()
                result = await detector.detect_coordination(
                    events=events,
                    entity_ids=[str(eid) for eid in request.entity_ids],
                )
                signal_scores["temporal"] = result.coordination_score
                confidence_values.append(result.confidence)
            else:
                signal_scores["temporal"] = 0.0

        except Exception as e:
            logger.warning(f"Temporal scoring failed: {e}")
            signal_scores["temporal"] = 0.0

    # Calculate funding score if requested
    if request.include_funding:
        try:
            from ..detection.funding import FundingClusterDetector

            detector = FundingClusterDetector(min_shared_funders=1)
            shared_funders = await detector.find_shared_funders(
                entity_ids=request.entity_ids,
                min_recipients=2,
            )

            if shared_funders:
                # Score based on number of shared funders and concentration
                max_concentration = max(sf.funding_concentration for sf in shared_funders)
                signal_scores["funding"] = min(1.0, len(shared_funders) * 0.2 + max_concentration * 0.5)
                confidence_values.append(0.9)  # High confidence for funding data
            else:
                signal_scores["funding"] = 0.0

        except Exception as e:
            logger.warning(f"Funding scoring failed: {e}")
            signal_scores["funding"] = 0.0

    # Calculate infrastructure score if requested
    if request.include_infrastructure:
        # Placeholder - infrastructure detection will be implemented in Phase 6
        signal_scores["infrastructure"] = 0.0

    # Calculate weighted composite score
    total_weight = sum(weights.get(k, 0) for k in signal_scores if signal_scores[k] > 0)

    if total_weight > 0:
        overall_score = sum(
            signal_scores[k] * weights.get(k, 0)
            for k in signal_scores
        ) / total_weight
    else:
        overall_score = 0.0

    # Calculate confidence band
    avg_confidence = sum(confidence_values) / len(confidence_values) if confidence_values else 0.5
    confidence_margin = (1 - avg_confidence) * 0.2

    # Determine if flagged (requires at least 2 signals above threshold)
    significant_signals = sum(1 for score in signal_scores.values() if score > 0.3)
    flagged = overall_score > 0.5 and significant_signals >= 2

    # Generate explanation
    active_signals = [k for k, v in signal_scores.items() if v > 0.3]
    if flagged:
        explanation = (
            f"Coordination indicators detected across {significant_signals} signals "
            f"({', '.join(active_signals)}). Overall score: {overall_score:.2f}."
        )
    elif overall_score > 0.3:
        explanation = (
            f"Some coordination indicators present but insufficient for flagging. "
            f"Score: {overall_score:.2f}. Active signals: {', '.join(active_signals) or 'none'}."
        )
    else:
        explanation = "No significant coordination indicators detected."

    return CompositeScoreResponse(
        finding_id=finding_id,
        overall_score=overall_score,
        signal_breakdown={
            "temporal_score": signal_scores.get("temporal", 0.0),
            "funding_score": signal_scores.get("funding", 0.0),
            "infrastructure_score": signal_scores.get("infrastructure", 0.0),
        },
        flagged=flagged,
        confidence_band={
            "lower": max(0.0, overall_score - confidence_margin),
            "upper": min(1.0, overall_score + confidence_margin),
        },
        entities_analyzed=len(request.entity_ids),
        explanation=explanation,
    )


# =========================
# Explain Finding
# =========================


@router.get("/explain/{finding_id}")
async def explain_finding(
    finding_id: UUID,
    user: CurrentUser = None,
) -> FindingExplanation:
    """Get detailed explanation for a detection finding.

    Provides:
    - Why the finding was flagged
    - Evidence breakdown with source links
    - Hard negatives that were checked
    - Recommendations for further investigation

    Args:
        finding_id: ID of the finding to explain

    Returns:
        Detailed finding explanation
    """
    from sqlalchemy import text

    async with get_db_session() as db:
        query = text("""
            SELECT
                id,
                finding_type,
                entity_ids,
                score,
                confidence,
                metadata,
                created_at
            FROM detection_findings
            WHERE id = :finding_id
        """)

        result = await db.execute(query, {"finding_id": str(finding_id)})
        finding = result.fetchone()

        if not finding:
            raise NotFoundError("Finding", finding_id)

        metadata = finding.metadata or {}

        # Build why_flagged explanation
        if finding.score >= 0.7:
            why_flagged = (
                f"Strong coordination indicators detected (score: {finding.score:.2f}). "
                f"Multiple independent signals suggest coordinated activity."
            )
        elif finding.score >= 0.5:
            why_flagged = (
                f"Moderate coordination indicators (score: {finding.score:.2f}). "
                f"Patterns warrant further investigation."
            )
        else:
            why_flagged = (
                f"Weak indicators (score: {finding.score:.2f}). "
                f"Included for completeness but low likelihood of coordination."
            )

        # Build evidence summary
        evidence_summary = metadata.get("evidence", [])

        # Build hard negatives checked
        hard_negatives_checked = metadata.get("hard_negatives", [])

        # Build recommendations
        recommendations = []
        if finding.score >= 0.5:
            recommendations.append("Review funding relationships for additional connections")
            recommendations.append("Check for shared personnel or board members")
            recommendations.append("Examine content similarity during burst periods")
        if finding.score >= 0.7:
            recommendations.append("Consider adding to monitoring watchlist")
            recommendations.append("Generate detailed structural risk report")

        return FindingExplanation(
            finding_id=str(finding.id),
            finding_type=finding.finding_type,
            entity_ids=finding.entity_ids or [],
            score=finding.score,
            confidence=finding.confidence,
            why_flagged=why_flagged,
            evidence_summary=evidence_summary,
            hard_negatives_checked=hard_negatives_checked,
            recommendations=recommendations,
        )
