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


class FundingClusterRequest(BaseModel):
    """Request for funding cluster detection."""

    entity_type: str | None = Field(None, description="Filter by entity type (Organization, Person, Outlet)")
    fiscal_year: int | None = Field(None, description="Filter by fiscal year")
    min_shared_funders: int = Field(2, ge=1, description="Minimum shared funders to form a cluster")
    limit: int = Field(50, ge=1, le=500, description="Maximum clusters to return")


class FundingClusterResponse(BaseModel):
    """Response for funding cluster detection."""

    clusters: list[dict[str, Any]] = Field(default_factory=list)
    total_clusters: int = 0
    explanation: str = ""


class InfrastructureSharingRequest(BaseModel):
    """Request for infrastructure sharing detection."""

    entity_ids: list[UUID] | None = Field(None, description="Entity IDs to resolve to domains")
    domains: list[str] | None = Field(None, description="Domain strings to scan directly")
    min_score: float = Field(1.0, ge=0.0, description="Minimum match score to include")


class InfrastructureSharingResponse(BaseModel):
    """Response for infrastructure sharing detection."""

    profiles: list[dict[str, Any]] = Field(default_factory=list)
    matches: list[dict[str, Any]] = Field(default_factory=list)
    total_matches: int = 0
    domains_scanned: int = 0
    errors: list[str] = Field(default_factory=list)
    explanation: str = ""


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
    adjusted_score: float = Field(0.0, ge=0.0, le=1.0)
    signal_breakdown: dict[str, float]
    category_breakdown: dict[str, float] = Field(default_factory=dict)
    flagged: bool
    flag_reason: str | None = None
    confidence_band: dict[str, float]
    entities_analyzed: int
    explanation: str
    validation_passed: bool = False
    validation_messages: list[str] = Field(default_factory=list)


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
    """Fetch timing events from the database.

    Uses correct column names from the events table schema:
    - entity_ids (UUID ARRAY) — array overlap with target entities
    - occurred_at (DateTime) — event timestamp
    - properties (JSONB) — event metadata
    """
    from sqlalchemy import text

    async with get_db_session() as db:
        # Build query with parameterized bindings (no string interpolation)
        entity_id_strs = [str(eid) for eid in entity_ids]

        if event_types:
            query = text("""
                SELECT
                    entity_ids,
                    occurred_at,
                    event_type,
                    properties
                FROM events
                WHERE entity_ids && :entity_ids
                AND occurred_at BETWEEN :start_date AND :end_date
                AND event_type = ANY(:event_types)
                ORDER BY occurred_at
            """)
            params = {
                "entity_ids": entity_id_strs,
                "start_date": start_date,
                "end_date": end_date,
                "event_types": event_types,
            }
        else:
            query = text("""
                SELECT
                    entity_ids,
                    occurred_at,
                    event_type,
                    properties
                FROM events
                WHERE entity_ids && :entity_ids
                AND occurred_at BETWEEN :start_date AND :end_date
                ORDER BY occurred_at
            """)
            params = {
                "entity_ids": entity_id_strs,
                "start_date": start_date,
                "end_date": end_date,
            }

        result = await db.execute(query, params)
        rows = result.fetchall()

        events = []
        for row in rows:
            # Each event may belong to multiple entities; emit one
            # TimingEvent per entity that matches our target list
            row_entity_ids = [str(eid) for eid in (row.entity_ids or [])]
            target_set = set(entity_id_strs)
            matched_entities = [eid for eid in row_entity_ids if eid in target_set]

            for eid in matched_entities:
                events.append(TimingEvent(
                    entity_id=eid,
                    timestamp=row.occurred_at,
                    event_type=row.event_type,
                    metadata=row.properties or {},
                ))

        return events


@router.post("/temporal-coordination")
async def analyze_temporal_coordination(
    request: TemporalAnalysisRequest,
    background_tasks: BackgroundTasks,
    user: OptionalUser = None,
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
# Funding Clusters
# =========================


@router.post("/funding-clusters")
async def detect_funding_clusters(
    request: FundingClusterRequest,
    user: OptionalUser = None,
) -> FundingClusterResponse:
    """Detect funding clusters among entities.

    Identifies groups of entities sharing common funders.
    Returns clusters scored by funding concentration with evidence references.
    """
    from ..detection.funding import FundingClusterDetector

    try:
        detector = FundingClusterDetector(
            min_shared_funders=request.min_shared_funders,
        )
        results = await detector.detect_clusters(
            entity_type=request.entity_type,
            fiscal_year=request.fiscal_year,
            limit=request.limit,
        )

        clusters = []
        for r in results:
            clusters.append({
                "cluster_id": r.cluster_id,
                "shared_funder": r.shared_funder.model_dump() if hasattr(r.shared_funder, "model_dump") else {"name": str(r.shared_funder)},
                "members": [m.model_dump() if hasattr(m, "model_dump") else {"name": str(m)} for m in r.members],
                "total_funding": r.total_funding,
                "funding_by_member": r.funding_by_member,
                "fiscal_years": r.fiscal_years,
                "score": r.score,
                "evidence_summary": r.evidence_summary,
            })

        total = len(clusters)
        if total == 0:
            explanation = "No funding clusters found matching the specified criteria."
        elif total == 1:
            explanation = f"Found 1 funding cluster with shared funders."
        else:
            explanation = (
                f"Found {total} funding clusters. "
                f"Highest scoring cluster has a score of {clusters[0]['score']:.2f}."
            )

        return FundingClusterResponse(
            clusters=clusters,
            total_clusters=total,
            explanation=explanation,
        )

    except Exception as e:
        logger.exception("Funding cluster detection failed")
        raise HTTPException(status_code=500, detail=f"Funding cluster detection failed: {e}")


# =========================
# Infrastructure Sharing
# =========================


@router.post("/infrastructure-sharing")
async def detect_infrastructure_sharing(
    request: InfrastructureSharingRequest,
    user: OptionalUser = None,
) -> InfrastructureSharingResponse:
    """Detect shared infrastructure between domains.

    Scans domain infrastructure (DNS, WHOIS, hosting, analytics, SSL)
    and identifies pairwise matches. Accepts entity IDs (resolves to domains)
    or domain strings directly.
    """
    from ..detection.infra import InfrastructureDetector

    domains: list[str] = list(request.domains or [])
    errors: list[str] = []

    # Resolve entity IDs to domains via Neo4j
    if request.entity_ids:
        try:
            async with get_neo4j_session() as neo4j:
                for entity_id in request.entity_ids:
                    result = await neo4j.run(
                        """
                        MATCH (n {id: $entity_id})
                        WHERE n:Outlet OR n:Organization
                        RETURN n.domain AS domain, n.domains AS domains, n.name AS name
                        """,
                        entity_id=str(entity_id),
                    )
                    record = await result.single()
                    if record:
                        if record["domain"]:
                            domains.append(record["domain"])
                        elif record["domains"]:
                            domains.extend(record["domains"])
                        else:
                            errors.append(f"Entity {entity_id} ({record['name']}) has no domain property")
                    else:
                        errors.append(f"Entity {entity_id} not found in graph")
        except Exception as e:
            logger.warning(f"Failed to resolve entity domains: {e}")
            errors.append(f"Domain resolution failed: {e}")

    if not domains:
        raise HTTPException(
            status_code=400,
            detail="No domains to scan. Provide domains directly or entity_ids with domain properties.",
        )

    if len(domains) < 2:
        raise HTTPException(
            status_code=400,
            detail=f"At least 2 domains required for infrastructure comparison (found {len(domains)}).",
        )

    # Run infrastructure detection
    detector = InfrastructureDetector()
    try:
        matches = await detector.find_shared_infrastructure(
            domains=domains,
            min_score=request.min_score,
        )

        # Build profiles (scan results are internal to the detector;
        # we report domain-level summaries)
        profiles = []
        for domain in domains:
            try:
                profile = await detector.analyze_domain(domain)
                profiles.append({
                    "domain": domain,
                    "dns": {
                        "nameservers": profile.dns.nameservers if profile.dns else [],
                        "a_records": profile.dns.a_records if profile.dns else [],
                    } if profile.dns else None,
                    "whois": {
                        "registrar": profile.whois.registrar if profile.whois else None,
                        "registrant_org": profile.whois.registrant_org if profile.whois else None,
                    } if profile.whois else None,
                    "hosting": [
                        {"ip": h.ip_address, "provider": h.hosting_provider, "asn": h.asn}
                        for h in (profile.hosting or [])
                    ],
                    "analytics": {
                        "google_analytics": profile.analytics.google_analytics_ids if profile.analytics else [],
                        "google_tag_manager": profile.analytics.google_tag_manager_ids if profile.analytics else [],
                    } if profile.analytics else None,
                    "ssl": {
                        "issuer": profile.ssl.issuer if profile.ssl else None,
                        "san_count": len(profile.ssl.subject_alt_names) if profile.ssl else 0,
                    } if profile.ssl else None,
                })
            except Exception as e:
                profiles.append({"domain": domain, "error": str(e)})
                errors.append(f"Failed to profile {domain}: {e}")

        match_dicts = []
        for m in matches:
            match_dicts.append({
                "domain_a": m.domain_a,
                "domain_b": m.domain_b,
                "signals": [
                    {
                        "signal_type": s.signal_type.value,
                        "value": s.value,
                        "weight": s.weight,
                        "description": s.description,
                    }
                    for s in m.signals
                ],
                "total_score": m.total_score,
                "confidence": m.confidence,
            })

        total = len(match_dicts)
        if total == 0:
            explanation = f"No shared infrastructure detected above min_score {request.min_score} across {len(domains)} domains."
        else:
            top_score = match_dicts[0]["total_score"]
            explanation = (
                f"Found {total} infrastructure sharing match{'es' if total > 1 else ''} "
                f"across {len(domains)} domains. Highest score: {top_score:.1f}."
            )

        return InfrastructureSharingResponse(
            profiles=profiles,
            matches=match_dicts,
            total_matches=total,
            domains_scanned=len(domains),
            errors=errors,
            explanation=explanation,
        )

    except Exception as e:
        logger.exception("Infrastructure detection failed")
        raise HTTPException(status_code=500, detail=f"Infrastructure detection failed: {e}")
    finally:
        await detector.close()


# =========================
# Composite Score
# =========================


@router.post("/composite-score")
async def calculate_composite_score_endpoint(
    request: CompositeScoreRequest,
    user: OptionalUser = None,
) -> CompositeScoreResponse:
    """Calculate composite coordination score combining all signals.

    Uses CompositeScoreCalculator with correlation-aware weighting.
    Requires at least 2 independent signals from 2 different categories
    to flag potential coordination (no single signal triggers alone).

    Args:
        request: Score calculation parameters

    Returns:
        Composite score with breakdown by signal type
    """
    from ..detection.composite import (
        CompositeScoreCalculator,
        DetectedSignal,
        SignalType,
    )

    if len(request.entity_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least 2 entities required for coordination scoring",
        )

    finding_id = str(uuid4())
    signals: list[DetectedSignal] = []
    signal_scores: dict[str, float] = {}
    partial_failures: list[str] = []

    entity_uuids = [eid for eid in request.entity_ids]

    # Collect temporal signal
    if request.include_temporal:
        try:
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
                if result.coordination_score > 0:
                    signals.append(DetectedSignal(
                        signal_type=SignalType.TEMPORAL_COORDINATION,
                        strength=result.coordination_score,
                        confidence=result.confidence,
                        entity_ids=entity_uuids,
                        metadata={"event_count": result.event_count},
                    ))
            else:
                signal_scores["temporal"] = 0.0
        except Exception as e:
            logger.warning(f"Temporal scoring failed: {e}")
            signal_scores["temporal"] = 0.0
            partial_failures.append(f"Temporal analysis failed: {e}")

    # Collect funding signal
    if request.include_funding:
        try:
            from ..detection.funding import FundingClusterDetector

            detector = FundingClusterDetector(min_shared_funders=1)
            shared_funders = await detector.find_shared_funders(
                entity_ids=request.entity_ids,
                min_recipients=2,
            )

            if shared_funders:
                max_concentration = max(sf.funding_concentration for sf in shared_funders)
                funding_strength = min(1.0, len(shared_funders) * 0.2 + max_concentration * 0.5)
                signal_scores["funding"] = funding_strength
                signals.append(DetectedSignal(
                    signal_type=SignalType.SHARED_FUNDER,
                    strength=funding_strength,
                    confidence=0.9,
                    entity_ids=entity_uuids,
                    metadata={"shared_funders": len(shared_funders)},
                ))
            else:
                signal_scores["funding"] = 0.0
        except Exception as e:
            logger.warning(f"Funding scoring failed: {e}")
            signal_scores["funding"] = 0.0
            partial_failures.append(f"Funding analysis failed: {e}")

    # Collect infrastructure signal
    if request.include_infrastructure:
        try:
            from ..detection.infra import InfrastructureDetector

            # Resolve entity IDs to domains
            domains: list[str] = []
            async with get_neo4j_session() as neo4j:
                for entity_id in request.entity_ids:
                    result = await neo4j.run(
                        """
                        MATCH (n {id: $entity_id})
                        WHERE n:Outlet OR n:Organization
                        RETURN n.domain AS domain, n.domains AS domains
                        """,
                        entity_id=str(entity_id),
                    )
                    record = await result.single()
                    if record:
                        if record["domain"]:
                            domains.append(record["domain"])
                        elif record["domains"]:
                            domains.extend(record["domains"])

            if len(domains) >= 2:
                infra_detector = InfrastructureDetector()
                try:
                    matches = await infra_detector.find_shared_infrastructure(
                        domains=domains,
                        min_score=1.0,
                    )
                    if matches:
                        top_score = matches[0].total_score
                        infra_strength = min(1.0, top_score / 10.0)
                        signal_scores["infrastructure"] = infra_strength
                        signals.append(DetectedSignal(
                            signal_type=SignalType.INFRASTRUCTURE_SHARING,
                            strength=infra_strength,
                            confidence=matches[0].confidence,
                            entity_ids=entity_uuids,
                            metadata={"matches": len(matches), "top_score": top_score},
                        ))
                    else:
                        signal_scores["infrastructure"] = 0.0
                finally:
                    await infra_detector.close()
            else:
                signal_scores["infrastructure"] = 0.0
        except Exception as e:
            logger.warning(f"Infrastructure scoring failed: {e}")
            signal_scores["infrastructure"] = 0.0
            partial_failures.append(f"Infrastructure analysis failed: {e}")

    # Use real CompositeScoreCalculator
    calculator = CompositeScoreCalculator()
    composite = calculator.calculate(signals)

    # Build explanation
    if composite.is_flagged:
        explanation = composite.flag_reason or "Coordination flagged."
    elif composite.adjusted_score > 0.3:
        explanation = (
            f"Some coordination indicators present but insufficient for flagging. "
            f"Adjusted score: {composite.adjusted_score:.2f}."
        )
    else:
        explanation = "No significant coordination indicators detected."

    if partial_failures:
        explanation += f" Note: {len(partial_failures)} detector(s) had errors: {'; '.join(partial_failures)}"

    # Persist finding to detection_findings table
    try:
        from sqlalchemy import text as sql_text

        async with get_db_session() as db:
            import json
            await db.execute(
                sql_text("""
                    INSERT INTO detection_findings
                        (id, finding_type, entity_ids, score, confidence, flagged, metadata, created_by)
                    VALUES
                        (:id, 'composite', :entity_ids, :score, :confidence, :flagged, :metadata, :created_by)
                """),
                {
                    "id": finding_id,
                    "entity_ids": [str(eid) for eid in request.entity_ids],
                    "score": composite.adjusted_score,
                    "confidence": composite.confidence_band.point_estimate,
                    "flagged": composite.is_flagged,
                    "metadata": json.dumps(composite.to_dict()),
                    "created_by": "api",
                },
            )
            await db.commit()
    except Exception as e:
        logger.warning(f"Failed to persist composite finding: {e}")

    return CompositeScoreResponse(
        finding_id=finding_id,
        overall_score=composite.raw_score,
        adjusted_score=composite.adjusted_score,
        signal_breakdown={
            "temporal_score": signal_scores.get("temporal", 0.0),
            "funding_score": signal_scores.get("funding", 0.0),
            "infrastructure_score": signal_scores.get("infrastructure", 0.0),
        },
        category_breakdown=composite.category_breakdown,
        flagged=composite.is_flagged,
        flag_reason=composite.flag_reason,
        confidence_band={
            "lower": composite.confidence_band.lower_bound,
            "upper": composite.confidence_band.upper_bound,
        },
        entities_analyzed=len(request.entity_ids),
        explanation=explanation,
        validation_passed=composite.validation_passed,
        validation_messages=composite.validation_messages,
    )


# =========================
# Explain Finding
# =========================


@router.get("/explain/{finding_id}")
async def explain_finding(
    finding_id: UUID,
    user: OptionalUser = None,
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
