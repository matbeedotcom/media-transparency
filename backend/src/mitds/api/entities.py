"""Entity API endpoints for MITDS."""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from . import NotFoundError, PaginatedResponse
from .auth import CurrentUser, OptionalUser
from ..db import get_neo4j_session
from ..graph.queries import get_entity_relationships as graph_get_relationships
from ..graph.queries import get_entity_stats
from ..graph.queries import find_board_interlocks_for_entity
from ..models.base import EntityType, EntitySummary
from ..models.relationships import RelationType

router = APIRouter(prefix="/entities")


# =========================
# Response Models
# =========================


class EntityResponse(BaseModel):
    """Full entity response."""

    id: UUID
    entity_type: str
    name: str
    confidence: float
    created_at: datetime
    updated_at: datetime
    aliases: list[str] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)


class RelationshipResponse(BaseModel):
    """Relationship response."""

    id: UUID | None
    rel_type: str
    source_entity: EntitySummary
    target_entity: EntitySummary
    confidence: float
    properties: dict[str, Any] = Field(default_factory=dict)


class EvidenceResponse(BaseModel):
    """Evidence response."""

    id: UUID
    evidence_type: str
    source_url: str
    source_archive_url: str | None
    retrieved_at: datetime
    extraction_confidence: float


# =========================
# Search Entities
# =========================


@router.get("")
async def search_entities(
    q: str | None = Query(None, description="Search query"),
    type: EntityType | None = Query(None, description="Filter by entity type"),
    jurisdiction: str | None = Query(None, description="Filter by jurisdiction"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: OptionalUser = None,
) -> PaginatedResponse:
    """Search for entities by name, alias, or identifier.

    Supports searching across:
    - Organization names and aliases
    - Person names
    - Outlet names and domains
    - EIN, BN, and other identifiers
    """
    async with get_neo4j_session() as session:
        # Build search query
        type_filter = ""
        if type:
            type_filter = f"AND e.entity_type = '{type.value}'"

        jurisdiction_filter = ""
        if jurisdiction:
            jurisdiction_filter = f"AND e.jurisdiction = '{jurisdiction}'"

        if q:
            # Search with text matching
            search_query = f"""
            MATCH (e)
            WHERE (e:Organization OR e:Person OR e:Outlet OR e:Sponsor)
            AND (
                toLower(e.name) CONTAINS toLower($search_term)
                OR any(alias IN coalesce(e.aliases, []) WHERE toLower(alias) CONTAINS toLower($search_term))
                OR e.ein = $search_term
                OR e.bn = $search_term
            )
            {type_filter}
            {jurisdiction_filter}
            RETURN e
            ORDER BY e.confidence DESC, e.name
            SKIP $offset
            LIMIT $limit
            """

            count_query = f"""
            MATCH (e)
            WHERE (e:Organization OR e:Person OR e:Outlet OR e:Sponsor)
            AND (
                toLower(e.name) CONTAINS toLower($search_term)
                OR any(alias IN coalesce(e.aliases, []) WHERE toLower(alias) CONTAINS toLower($search_term))
                OR e.ein = $search_term
                OR e.bn = $search_term
            )
            {type_filter}
            {jurisdiction_filter}
            RETURN count(e) as total
            """

            result = await session.run(
                search_query, search_term=q, offset=offset, limit=limit
            )
            count_result = await session.run(count_query, search_term=q)
        else:
            # List all entities
            list_query = f"""
            MATCH (e)
            WHERE (e:Organization OR e:Person OR e:Outlet OR e:Sponsor)
            {type_filter.replace('AND', 'AND' if type_filter else '')}
            {jurisdiction_filter.replace('AND', 'AND' if jurisdiction_filter else '')}
            RETURN e
            ORDER BY e.name
            SKIP $offset
            LIMIT $limit
            """

            count_query = f"""
            MATCH (e)
            WHERE (e:Organization OR e:Person OR e:Outlet OR e:Sponsor)
            {type_filter.replace('AND', 'AND' if type_filter else '')}
            {jurisdiction_filter.replace('AND', 'AND' if jurisdiction_filter else '')}
            RETURN count(e) as total
            """

            result = await session.run(list_query, offset=offset, limit=limit)
            count_result = await session.run(count_query)

        records = await result.data()
        count_record = await count_result.single()
        total = count_record["total"] if count_record else 0

        entities = []
        for record in records:
            entity_data = dict(record["e"])
            # Convert Neo4j DateTime to string if needed
            created_at = entity_data.get("created_at")
            if hasattr(created_at, 'to_native'):
                created_at = created_at.to_native().isoformat()
            elif hasattr(created_at, 'isoformat'):
                created_at = created_at.isoformat()

            entities.append({
                "id": entity_data.get("id"),
                "entity_type": entity_data.get("entity_type"),
                "name": entity_data.get("name"),
                "confidence": entity_data.get("confidence", 1.0),
                "created_at": created_at,
            })

        return PaginatedResponse(
            results=entities,
            total=total,
            limit=limit,
            offset=offset,
        )


# =========================
# Get Entity
# =========================


@router.get("/{entity_id}")
async def get_entity(
    entity_id: UUID,
    user: OptionalUser = None,
) -> EntityResponse:
    """Get an entity by ID with full details."""
    async with get_neo4j_session() as session:
        query = """
        MATCH (e {id: $entity_id})
        RETURN e
        """
        result = await session.run(query, entity_id=str(entity_id))
        record = await result.single()

        if not record:
            raise NotFoundError("Entity", entity_id)

        entity_data = dict(record["e"])

        # Parse timestamps - handle neo4j.time.DateTime, str, and None
        created_at = entity_data.get("created_at")
        if hasattr(created_at, 'to_native'):
            created_at = created_at.to_native()
        elif isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        elif created_at is None:
            created_at = datetime.utcnow()

        updated_at = entity_data.get("updated_at")
        if hasattr(updated_at, 'to_native'):
            updated_at = updated_at.to_native()
        elif isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        elif updated_at is None:
            updated_at = created_at

        # Extract known fields
        known_fields = {
            "id", "entity_type", "name", "confidence", "created_at",
            "updated_at", "aliases"
        }
        properties = {
            k: v for k, v in entity_data.items()
            if k not in known_fields and v is not None
        }

        return EntityResponse(
            id=UUID(entity_data["id"]),
            entity_type=entity_data.get("entity_type", "UNKNOWN"),
            name=entity_data.get("name", "Unknown"),
            confidence=entity_data.get("confidence", 1.0),
            created_at=created_at,
            updated_at=updated_at,
            aliases=entity_data.get("aliases", []),
            properties=properties,
        )


# =========================
# Get Entity Relationships
# =========================


@router.get("/{entity_id}/relationships")
async def get_entity_relationships(
    entity_id: UUID,
    rel_type: str | None = Query(None, description="Filter by relationship type"),
    direction: str = Query("both", pattern="^(in|out|both)$"),
    as_of: str | None = Query(None, description="Point-in-time query (ISO 8601)"),
    limit: int = Query(50, ge=1, le=200),
    user: OptionalUser = None,
):
    """Get relationships for an entity.

    Returns relationships with the related entity summary for each.
    """
    async with get_neo4j_session() as session:
        # Build direction pattern
        if direction == "out":
            pattern = "(entity)-[r]->(related)"
        elif direction == "in":
            pattern = "(entity)<-[r]-(related)"
        else:
            pattern = "(entity)-[r]-(related)"

        # Build type filter
        type_filter = ""
        if rel_type:
            type_filter = f"AND type(r) = '{rel_type}'"

        # Build temporal filter
        time_filter = ""
        if as_of:
            try:
                point_in_time = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
                time_filter = f"""
                AND (r.valid_from IS NULL OR r.valid_from <= '{point_in_time.isoformat()}')
                AND (r.valid_to IS NULL OR r.valid_to > '{point_in_time.isoformat()}')
                """
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid as_of format. Use ISO 8601."
                )

        query = f"""
        MATCH {pattern}
        WHERE entity.id = $entity_id
        {type_filter}
        {time_filter}
        WITH r, startNode(r) as src, endNode(r) as tgt
        RETURN
            r.id as rel_id,
            type(r) as rel_type,
            src.id as source_id,
            src.name as source_name,
            src.entity_type as source_type,
            tgt.id as target_id,
            tgt.name as target_name,
            tgt.entity_type as target_type,
            r.confidence as confidence,
            properties(r) as rel_props
        LIMIT {limit}
        """

        result = await session.run(query, entity_id=str(entity_id))
        records = await result.data()

        relationships = []
        for record in records:
            rel_props = record.get("rel_props", {})
            # Remove internal Neo4j props from rel_props
            for key in ("id", "confidence"):
                rel_props.pop(key, None)

            # Parse relationship ID - may not be a valid UUID
            rel_id = None
            if record.get("rel_id"):
                try:
                    rel_id = UUID(str(record["rel_id"]))
                except ValueError:
                    rel_id = None

            relationships.append(
                RelationshipResponse(
                    id=rel_id,
                    rel_type=record.get("rel_type", ""),
                    source_entity=EntitySummary(
                        id=UUID(record["source_id"]),
                        entity_type=EntityType(record.get("source_type", "ORGANIZATION")),
                        name=record.get("source_name", "Unknown"),
                    ),
                    target_entity=EntitySummary(
                        id=UUID(record["target_id"]),
                        entity_type=EntityType(record.get("target_type", "ORGANIZATION")),
                        name=record.get("target_name", "Unknown"),
                    ),
                    confidence=record.get("confidence") or 1.0,
                    properties=rel_props,
                )
            )

        return {
            "relationships": relationships,
            "total": len(relationships),
        }


# =========================
# Get Entity Evidence
# =========================


@router.get("/{entity_id}/evidence")
async def get_entity_evidence(
    entity_id: UUID,
    limit: int = Query(20, ge=1, le=100),
    user: OptionalUser = None,
):
    """Get evidence for an entity.

    Returns all evidence records associated with this entity.
    """
    # Evidence is stored in PostgreSQL, linked via source_ids on the entity
    from sqlalchemy import text
    from ..db import get_db_session

    async with get_db_session() as db:
        # First get entity's source_ids from Neo4j
        async with get_neo4j_session() as neo_session:
            query = """
            MATCH (e {id: $entity_id})
            RETURN e.source_ids as source_ids
            """
            result = await neo_session.run(query, entity_id=str(entity_id))
            record = await result.single()

            if not record:
                raise NotFoundError("Entity", entity_id)

            source_ids = record.get("source_ids", [])

        if not source_ids:
            return {"evidence": []}

        # Query evidence from PostgreSQL
        evidence_query = text("""
            SELECT id, evidence_type, source_url, source_archive_url,
                   retrieved_at, extraction_confidence
            FROM evidence
            WHERE id = ANY(:evidence_ids)
            ORDER BY retrieved_at DESC
            LIMIT :limit
        """)

        # Parse source_ids to get evidence UUIDs
        evidence_ids = []
        for source_ref in source_ids:
            if isinstance(source_ref, dict) and source_ref.get("evidence_id"):
                evidence_ids.append(source_ref["evidence_id"])

        if not evidence_ids:
            return {"evidence": []}

        result = await db.execute(
            evidence_query,
            {"evidence_ids": evidence_ids, "limit": limit}
        )
        rows = result.fetchall()

        evidence_list = []
        for row in rows:
            evidence_list.append(
                EvidenceResponse(
                    id=row.id,
                    evidence_type=row.evidence_type,
                    source_url=row.source_url,
                    source_archive_url=row.source_archive_url,
                    retrieved_at=row.retrieved_at,
                    extraction_confidence=row.extraction_confidence,
                )
            )

        return {"evidence": evidence_list}


# =========================
# Get Entity Stats
# =========================


@router.get("/{entity_id}/stats")
async def get_entity_statistics(
    entity_id: UUID,
    user: OptionalUser = None,
):
    """Get statistics for an entity.

    Returns counts of relationships, funders, recipients, etc.
    """
    stats = await get_entity_stats(entity_id)

    if not stats:
        raise NotFoundError("Entity", entity_id)

    return stats


# =========================
# Get Board Interlocks
# =========================


@router.get("/{entity_id}/board-interlocks")
async def get_board_interlocks(
    entity_id: UUID,
    user: OptionalUser = None,
):
    """Get board interlocks for an organization.

    Returns directors of this entity who also serve on other boards,
    along with those other organizations.
    """
    interlocks = await find_board_interlocks_for_entity(entity_id)

    return {
        "interlocks": [
            {
                "director": {
                    "id": str(item["director"].id),
                    "entity_type": item["director"].entity_type,
                    "name": item["director"].name,
                },
                "organizations": [
                    {
                        "id": str(org.id),
                        "entity_type": org.entity_type,
                        "name": org.name,
                    }
                    for org in item["organizations"]
                ],
            }
            for item in interlocks
        ],
        "total": len(interlocks),
    }
