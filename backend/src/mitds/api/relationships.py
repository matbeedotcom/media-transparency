"""Relationship API endpoints for MITDS."""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from . import NotFoundError
from .auth import OptionalUser
from ..detection.funding import (
    FundingClusterDetector,
    FundingClusterResult,
    SharedFunderResult,
)
from ..graph.queries import (
    EntityNode,
    FundingPath,
    PathResult,
    find_all_paths,
    find_connecting_entities,
    find_path_between,
    find_shared_funders,
    get_entity_network,
    get_funding_paths,
    get_funding_recipients,
    get_funding_sources,
)
from ..graph.temporal import (
    detect_changes_between,
    get_graph_at_time,
    get_relationship_timeline,
    RelationshipTimeline,
)
from ..models.relationships import RelationType

router = APIRouter(prefix="/relationships")


# =========================
# Response Models
# =========================


class PathResponse(BaseModel):
    """Path between two entities."""

    source: EntityNode
    target: EntityNode
    hops: int
    path_nodes: list[EntityNode] = Field(default_factory=list)
    path_edges: list[dict[str, Any]] = Field(default_factory=list)


class FundingClusterResponse(BaseModel):
    """Funding cluster response."""

    cluster_id: str
    shared_funder: EntityNode
    members: list[EntityNode]
    total_funding: float
    score: float
    confidence: float
    evidence_summary: str


class SharedFunderResponse(BaseModel):
    """Shared funder response."""

    funder: EntityNode
    recipients: list[EntityNode]
    shared_count: int
    total_funding: float
    funding_concentration: float


# =========================
# Find Paths
# =========================


@router.get("/path")
async def find_paths(
    from_id: UUID,
    to_id: UUID,
    max_hops: int = Query(3, ge=1, le=5),
    rel_types: str | None = Query(None, description="Comma-separated relationship types"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Find shortest path between two entities.

    Args:
        from_id: Source entity ID
        to_id: Target entity ID
        max_hops: Maximum path length
        rel_types: Filter by relationship types (comma-separated)

    Returns:
        Path with nodes and edges
    """
    # Parse relationship types
    types = None
    if rel_types:
        try:
            types = [RelationType(rt.strip()) for rt in rel_types.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    path = await find_path_between(
        source_id=from_id,
        target_id=to_id,
        max_hops=max_hops,
        rel_types=types,
    )

    if not path:
        return {
            "path_found": False,
            "from_entity": {"id": str(from_id)},
            "to_entity": {"id": str(to_id)},
            "paths": [],
        }

    return {
        "path_found": True,
        "from_entity": path.funder.model_dump(),
        "to_entity": path.recipient.model_dump(),
        "hops": path.hops,
        "intermediaries": [n.model_dump() for n in path.intermediaries],
        "relationships": [r.model_dump() for r in path.relationships],
    }


# =========================
# Find All Paths
# =========================


@router.get("/paths/all")
async def find_all_entity_paths(
    from_id: UUID,
    to_id: UUID,
    max_hops: int = Query(5, ge=1, le=6),
    rel_types: str | None = Query(None, description="Comma-separated relationship types"),
    limit: int = Query(10, ge=1, le=50),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Find all paths between two entities.

    Unlike /path which returns only the shortest path, this endpoint
    returns all distinct paths up to max_hops, revealing different
    relationship types and intermediate connections.

    Args:
        from_id: Source entity ID
        to_id: Target entity ID
        max_hops: Maximum path length
        rel_types: Filter by relationship types (comma-separated)
        limit: Maximum number of paths to return

    Returns:
        All paths with nodes and edges
    """
    # Parse relationship types
    types = None
    if rel_types:
        try:
            types = [RelationType(rt.strip()) for rt in rel_types.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    paths = await find_all_paths(
        source_id=from_id,
        target_id=to_id,
        max_hops=max_hops,
        rel_types=types,
        limit=limit,
    )

    return {
        "from_entity_id": str(from_id),
        "to_entity_id": str(to_id),
        "paths_found": len(paths),
        "paths": [
            {
                "source": p.source.model_dump(),
                "target": p.target.model_dump(),
                "path_length": p.path_length,
                "path_types": p.path_types,
                "nodes": [n.model_dump() for n in p.nodes],
                "relationships": [r.model_dump() for r in p.relationships],
            }
            for p in paths
        ],
    }


# =========================
# Find Connecting Entities
# =========================


@router.get("/connecting-entities")
async def find_entity_connectors(
    entity_ids: str = Query(..., description="Comma-separated entity IDs to find connections between"),
    max_hops: int = Query(3, ge=1, le=5),
    rel_types: str | None = Query(None, description="Comma-separated relationship types"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Find entities that connect multiple given entities.

    Discovers hidden connections like shared directors,
    board members, or intermediate organizations.

    Args:
        entity_ids: Comma-separated entity IDs (at least 2)
        max_hops: Maximum distance from any given entity
        rel_types: Filter by relationship types

    Returns:
        List of connecting entities ordered by connectivity
    """
    # Parse entity IDs
    try:
        parsed_ids = [UUID(id.strip()) for id in entity_ids.split(",")]
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid entity ID: {e}"
        )

    if len(parsed_ids) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least 2 entity IDs required"
        )

    # Parse relationship types
    types = None
    if rel_types:
        try:
            types = [RelationType(rt.strip()) for rt in rel_types.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    connectors = await find_connecting_entities(
        entity_ids=parsed_ids,
        max_hops=max_hops,
        rel_types=types,
    )

    return {
        "queried_entities": [str(eid) for eid in parsed_ids],
        "connecting_entities": [c.model_dump() for c in connectors],
        "total_connectors": len(connectors),
    }


# =========================
# Temporal Graph Queries
# =========================


@router.get("/graph-at-time/{entity_id}")
async def get_entity_graph_at_time(
    entity_id: UUID,
    as_of: datetime = Query(..., description="Point in time for the snapshot"),
    depth: int = Query(1, ge=1, le=3),
    rel_types: str | None = Query(None, description="Comma-separated relationship types"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get the state of relationships around an entity at a specific time.

    Returns only relationships that were valid at the given point in time,
    based on their valid_from and valid_to properties.

    Args:
        entity_id: Central entity ID
        as_of: Point in time for the snapshot (ISO format)
        depth: How many hops to traverse
        rel_types: Filter by relationship types

    Returns:
        Entity snapshot with valid relationships at that time
    """
    # Parse relationship types
    types = None
    if rel_types:
        try:
            types = [RelationType(rt.strip()) for rt in rel_types.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    snapshot = await get_graph_at_time(
        entity_id=entity_id,
        as_of=as_of,
        depth=depth,
        rel_types=types,
    )

    return {
        "entity": snapshot.entity.model_dump(),
        "as_of": as_of.isoformat(),
        "relationships": [
            {
                "id": str(r.id) if r.id else None,
                "rel_type": r.rel_type,
                "source": r.source.model_dump(),
                "target": r.target.model_dump(),
                "valid_from": r.valid_from.isoformat() if r.valid_from else None,
                "valid_to": r.valid_to.isoformat() if r.valid_to else None,
                "is_current": r.is_current,
            }
            for r in snapshot.relationships
        ],
        "incoming_count": snapshot.incoming_count,
        "outgoing_count": snapshot.outgoing_count,
    }


@router.get("/changes")
async def get_relationship_changes(
    entity_id: UUID,
    from_date: datetime = Query(..., description="Start of comparison period"),
    to_date: datetime = Query(..., description="End of comparison period"),
    rel_types: str | None = Query(None, description="Comma-separated relationship types"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Detect changes in relationships between two time points.

    Compares the graph state at from_date with to_date and identifies
    new relationships, ended relationships, and modifications.

    Args:
        entity_id: Central entity ID
        from_date: Start of comparison period
        to_date: End of comparison period
        rel_types: Filter by relationship types

    Returns:
        Graph diff with detected changes
    """
    if to_date <= from_date:
        raise HTTPException(
            status_code=400,
            detail="to_date must be after from_date"
        )

    # Parse relationship types
    types = None
    if rel_types:
        try:
            types = [RelationType(rt.strip()) for rt in rel_types.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    diff = await detect_changes_between(
        entity_id=entity_id,
        from_date=from_date,
        to_date=to_date,
        rel_types=types,
    )

    def serialize_rel(r):
        return {
            "id": str(r.id) if r.id else None,
            "rel_type": r.rel_type,
            "source": r.source.model_dump(),
            "target": r.target.model_dump(),
            "valid_from": r.valid_from.isoformat() if r.valid_from else None,
            "valid_to": r.valid_to.isoformat() if r.valid_to else None,
        }

    return {
        "entity_id": str(entity_id),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "added_relationships": [serialize_rel(r) for r in diff.added_relationships],
        "removed_relationships": [serialize_rel(r) for r in diff.removed_relationships],
        "modified_relationships": [
            {
                "change_type": c.change_type,
                "relationship": serialize_rel(c.relationship),
                "previous_state": c.previous_state,
                "new_state": c.new_state,
            }
            for c in diff.modified_relationships
        ],
        "summary": {
            "total_added": len(diff.added_relationships),
            "total_removed": len(diff.removed_relationships),
            "total_modified": len(diff.modified_relationships),
        },
    }


@router.get("/timeline")
async def get_entity_relationship_timeline(
    source_id: UUID,
    target_id: UUID,
    rel_type: str | None = Query(None, description="Filter by relationship type"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get the full timeline of relationships between two entities.

    Shows all historical periods when a relationship existed,
    useful for understanding the evolution of connections.

    Args:
        source_id: Source entity ID
        target_id: Target entity ID
        rel_type: Filter by relationship type

    Returns:
        Timeline with all historical relationship periods
    """
    # Parse relationship type
    type_filter = None
    if rel_type:
        try:
            type_filter = RelationType(rel_type)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {e}"
            )

    timeline = await get_relationship_timeline(
        source_id=source_id,
        target_id=target_id,
        rel_type=type_filter,
    )

    return {
        "source": timeline.source.model_dump(),
        "target": timeline.target.model_dump(),
        "periods": [
            {
                "id": str(p.id) if p.id else None,
                "rel_type": p.rel_type,
                "valid_from": p.valid_from.isoformat() if p.valid_from else None,
                "valid_to": p.valid_to.isoformat() if p.valid_to else None,
                "is_current": p.is_current,
                "properties": p.properties,
            }
            for p in timeline.periods
        ],
        "total_duration_days": timeline.total_duration_days,
        "is_currently_active": timeline.is_currently_active,
        "period_count": len(timeline.periods),
    }


# =========================
# Get Funding Paths
# =========================


@router.get("/funding-paths/{funder_id}")
async def get_funder_paths(
    funder_id: UUID,
    max_hops: int = Query(3, ge=1, le=5),
    min_amount: float | None = Query(None, description="Minimum funding amount"),
    fiscal_year: int | None = Query(None, description="Filter by fiscal year"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get all funding paths from a funder.

    Returns paths showing how funding flows from a funder
    to various recipients, including through intermediaries.
    """
    paths = await get_funding_paths(
        funder_id=funder_id,
        max_hops=max_hops,
        min_amount=min_amount,
        fiscal_year=fiscal_year,
    )

    return {
        "funder_id": str(funder_id),
        "paths": [
            {
                "recipient": p.recipient.model_dump(),
                "hops": p.hops,
                "total_amount": p.total_amount,
                "intermediaries": [n.model_dump() for n in p.intermediaries],
                "relationships": [r.model_dump() for r in p.relationships],
            }
            for p in paths
        ],
        "total_paths": len(paths),
    }


# =========================
# Get Funding Recipients
# =========================


@router.get("/funding-recipients/{funder_id}")
async def get_funder_recipients(
    funder_id: UUID,
    fiscal_year: int | None = Query(None, description="Filter by fiscal year"),
    min_amount: float | None = Query(None, description="Minimum funding amount"),
    limit: int = Query(50, ge=1, le=200),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get direct funding recipients of an entity.

    Returns all entities that have received funding directly
    from the specified funder.
    """
    recipients = await get_funding_recipients(
        funder_id=funder_id,
        fiscal_year=fiscal_year,
        min_amount=min_amount,
        limit=limit,
    )

    total_funding = sum(amt or 0 for _, amt in recipients)

    return {
        "funder_id": str(funder_id),
        "recipients": [
            {
                "entity": entity.model_dump(),
                "amount": amount,
            }
            for entity, amount in recipients
        ],
        "total_recipients": len(recipients),
        "total_funding": total_funding,
    }


# =========================
# Get Funding Sources
# =========================


@router.get("/funding-sources/{recipient_id}")
async def get_recipient_funders(
    recipient_id: UUID,
    fiscal_year: int | None = Query(None, description="Filter by fiscal year"),
    min_amount: float | None = Query(None, description="Minimum funding amount"),
    limit: int = Query(50, ge=1, le=200),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get funding sources of an entity.

    Returns all entities that have provided funding directly
    to the specified recipient.
    """
    funders = await get_funding_sources(
        recipient_id=recipient_id,
        fiscal_year=fiscal_year,
        min_amount=min_amount,
        limit=limit,
    )

    total_received = sum(amt or 0 for _, amt in funders)

    return {
        "recipient_id": str(recipient_id),
        "funders": [
            {
                "entity": entity.model_dump(),
                "amount": amount,
            }
            for entity, amount in funders
        ],
        "total_funders": len(funders),
        "total_received": total_received,
    }


# =========================
# Get Funding Clusters
# =========================


@router.get("/funding-clusters")
async def get_funding_clusters(
    min_shared_funders: int = Query(2, ge=1, le=10),
    min_cluster_size: int = Query(2, ge=2, le=20),
    entity_type: str | None = Query(None, description="Filter by entity type (OUTLET, ORGANIZATION)"),
    fiscal_year: int | None = Query(None, description="Filter by fiscal year"),
    limit: int = Query(20, ge=1, le=50),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get funding clusters (entities sharing common funders).

    Identifies groups of entities that receive funding from
    the same sources, which may indicate coordinated influence.

    Args:
        min_shared_funders: Minimum funders that must be shared
        min_cluster_size: Minimum entities in a cluster
        entity_type: Filter by entity type
        fiscal_year: Filter by fiscal year
        limit: Maximum clusters to return
    """
    detector = FundingClusterDetector(
        min_shared_funders=min_shared_funders,
        min_cluster_size=min_cluster_size,
    )

    clusters = await detector.detect_clusters(
        entity_type=entity_type,
        fiscal_year=fiscal_year,
        limit=limit,
    )

    return {
        "clusters": [
            FundingClusterResponse(
                cluster_id=c.cluster_id,
                shared_funder=c.shared_funder,
                members=c.members,
                total_funding=c.total_funding,
                score=c.score,
                confidence=c.confidence,
                evidence_summary=c.evidence_summary,
            ).model_dump()
            for c in clusters
        ],
        "total_clusters": len(clusters),
        "parameters": {
            "min_shared_funders": min_shared_funders,
            "min_cluster_size": min_cluster_size,
            "entity_type": entity_type,
            "fiscal_year": fiscal_year,
        },
    }


# =========================
# Get Shared Funders
# =========================


@router.get("/shared-funders")
async def get_shared_funders_endpoint(
    entity_ids: str | None = Query(None, description="Comma-separated entity IDs"),
    min_recipients: int = Query(2, ge=2, le=20),
    fiscal_year: int | None = Query(None, description="Filter by fiscal year"),
    limit: int = Query(20, ge=1, le=50),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Find funders shared by multiple entities.

    Identifies funders that provide funding to multiple
    entities in the dataset.

    Args:
        entity_ids: Specific entities to check (comma-separated UUIDs)
        min_recipients: Minimum recipients to include a funder
        fiscal_year: Filter by fiscal year
        limit: Maximum results
    """
    # Parse entity IDs
    parsed_ids = None
    if entity_ids:
        try:
            parsed_ids = [UUID(id.strip()) for id in entity_ids.split(",")]
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid entity ID: {e}"
            )

    detector = FundingClusterDetector()
    shared = await detector.find_shared_funders(
        entity_ids=parsed_ids,
        min_recipients=min_recipients,
        fiscal_year=fiscal_year,
        limit=limit,
    )

    return {
        "shared_funders": [
            SharedFunderResponse(
                funder=sf.funder,
                recipients=sf.recipients,
                shared_count=sf.shared_count,
                total_funding=sf.total_funding,
                funding_concentration=sf.funding_concentration,
            ).model_dump()
            for sf in shared
        ],
        "total_funders": len(shared),
        "parameters": {
            "entity_ids": entity_ids,
            "min_recipients": min_recipients,
            "fiscal_year": fiscal_year,
        },
    }


# =========================
# Get Shared Infrastructure
# =========================


class SharedInfraResponse(BaseModel):
    """Shared infrastructure match response."""

    domain_a: str
    domain_b: str
    total_score: float
    confidence: float
    signals: list[dict[str, Any]]
    sharing_category: str | None = None


@router.get("/shared-infrastructure")
async def get_shared_infrastructure(
    outlet_ids: str | None = Query(None, description="Comma-separated outlet IDs"),
    domains: str | None = Query(None, description="Comma-separated domains to analyze"),
    min_score: float = Query(1.0, ge=0.0, le=10.0, description="Minimum score threshold"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get shared infrastructure relationships between outlets.

    Identifies outlets that share technical infrastructure
    (hosting, analytics, CDN) even when organizational links
    are hidden. Detects signals like:
    - Same Google Analytics/GTM IDs
    - Same hosting provider or IP address
    - Same SSL certificate SANs
    - Same AdSense publisher IDs
    - Shared nameservers or registrar

    Args:
        outlet_ids: Comma-separated outlet IDs to check
        domains: Comma-separated domains to analyze
        min_score: Minimum infrastructure sharing score to include

    Returns:
        List of shared infrastructure matches with signals
    """
    from ..detection.infra import InfrastructureDetector

    # Parse domains
    domain_list: list[str] = []
    if domains:
        domain_list = [d.strip().lower() for d in domains.split(",") if d.strip()]

    if not domain_list:
        return {
            "matches": [],
            "total_matches": 0,
            "message": "Provide domains parameter with comma-separated domain names",
            "parameters": {
                "outlet_ids": outlet_ids,
                "domains": domains,
                "min_score": min_score,
            },
        }

    # Detect shared infrastructure
    detector = InfrastructureDetector()
    try:
        matches = await detector.find_shared_infrastructure(
            domains=domain_list,
            min_score=min_score,
        )
    finally:
        await detector.close()

    return {
        "matches": [
            SharedInfraResponse(
                domain_a=m.domain_a,
                domain_b=m.domain_b,
                total_score=m.total_score,
                confidence=m.confidence,
                signals=[
                    {
                        "type": s.signal_type.value,
                        "value": s.value,
                        "weight": s.weight,
                        "description": s.description,
                    }
                    for s in m.signals
                ],
                sharing_category=next(
                    (s.signal_type.value.replace("same_", "")
                     for s in m.signals if s.weight >= 3.0),
                    "infrastructure"
                ),
            ).model_dump()
            for m in matches
        ],
        "total_matches": len(matches),
        "analyzed_domains": domain_list,
        "parameters": {
            "outlet_ids": outlet_ids,
            "domains": domains,
            "min_score": min_score,
        },
    }


@router.post("/shared-infrastructure/analyze")
async def analyze_domain_infrastructure(
    domain: str = Query(..., description="Domain to analyze"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Analyze infrastructure for a single domain.

    Performs full infrastructure detection including:
    - DNS lookup (nameservers, A records)
    - WHOIS data (registrar, registration dates)
    - Hosting provider detection via IP/ASN
    - Analytics tag detection (GA, GTM, FB Pixel, AdSense)
    - SSL certificate analysis

    Args:
        domain: Domain name to analyze (e.g., example.com)

    Returns:
        Complete infrastructure profile for the domain
    """
    from ..detection.infra import InfrastructureDetector

    detector = InfrastructureDetector()
    try:
        profile = await detector.analyze_domain(domain)
    finally:
        await detector.close()

    # Build response
    result: dict[str, Any] = {
        "domain": profile.domain,
        "scanned_at": profile.scanned_at.isoformat(),
    }

    # DNS info
    if profile.dns:
        result["dns"] = {
            "nameservers": profile.dns.nameservers,
            "a_records": profile.dns.a_records,
            "aaaa_records": profile.dns.aaaa_records,
            "mx_records": profile.dns.mx_records,
            "error": profile.dns.error,
        }

    # WHOIS info
    if profile.whois:
        result["whois"] = {
            "registrar": profile.whois.registrar,
            "registration_date": profile.whois.registration_date.isoformat()
                if profile.whois.registration_date else None,
            "expiry_date": profile.whois.expiry_date.isoformat()
                if profile.whois.expiry_date else None,
            "nameservers": profile.whois.nameservers,
            "registrant_org": profile.whois.registrant_org,
            "registrant_country": profile.whois.registrant_country,
            "error": profile.whois.error,
        }

    # Hosting info
    if profile.hosting:
        result["hosting"] = [
            {
                "ip_address": h.ip_address,
                "asn": h.asn,
                "asn_name": h.asn_name,
                "hosting_provider": h.hosting_provider,
                "cdn_provider": h.cdn_provider,
                "country": h.country,
                "is_shared_hosting": h.is_shared_hosting,
            }
            for h in profile.hosting
        ]

    # Analytics info
    if profile.analytics:
        result["analytics"] = {
            "google_analytics_ids": profile.analytics.google_analytics_ids,
            "google_tag_manager_ids": profile.analytics.google_tag_manager_ids,
            "facebook_pixel_ids": profile.analytics.facebook_pixel_ids,
            "adsense_ids": profile.analytics.adsense_ids,
            "cms_detected": profile.analytics.cms_detected,
            "technologies": profile.analytics.technologies,
            "error": profile.analytics.error,
        }

    # SSL info
    if profile.ssl:
        result["ssl"] = {
            "issuer": profile.ssl.issuer,
            "subject_alt_names": profile.ssl.subject_alt_names,
            "valid_from": profile.ssl.valid_from.isoformat()
                if profile.ssl.valid_from else None,
            "valid_until": profile.ssl.valid_until.isoformat()
                if profile.ssl.valid_until else None,
            "fingerprint": profile.ssl.fingerprint,
            "error": profile.ssl.error,
        }

    return result
