"""Temporal graph operations for MITDS.

Provides time-aware graph queries:
- Time-sliced views (graph state at a point in time)
- Historical change detection
- Relationship timeline analysis
"""

from datetime import datetime, date, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ..db import get_neo4j_session
from ..logging import get_context_logger
from ..models.relationships import RelationType
from .queries import EntityNode, RelationshipEdge, _parse_entity_node

logger = get_context_logger(__name__)


# =========================
# Data Models
# =========================


class TemporalRelationship(BaseModel):
    """Relationship with temporal validity information."""

    id: UUID | None = None
    rel_type: str
    source: EntityNode
    target: EntityNode
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    is_current: bool = True
    properties: dict[str, Any] = Field(default_factory=dict)


class RelationshipChange(BaseModel):
    """A detected change in relationships."""

    change_type: str  # "added", "removed", "modified"
    relationship: TemporalRelationship
    detected_at: datetime
    previous_state: dict[str, Any] | None = None
    new_state: dict[str, Any] | None = None


class EntitySnapshot(BaseModel):
    """State of an entity at a point in time."""

    entity: EntityNode
    as_of: datetime
    relationships: list[TemporalRelationship] = Field(default_factory=list)
    incoming_count: int = 0
    outgoing_count: int = 0


class GraphDiff(BaseModel):
    """Difference between two time points in the graph."""

    from_date: datetime
    to_date: datetime
    added_relationships: list[TemporalRelationship] = Field(default_factory=list)
    removed_relationships: list[TemporalRelationship] = Field(default_factory=list)
    modified_relationships: list[RelationshipChange] = Field(default_factory=list)
    added_entities: list[EntityNode] = Field(default_factory=list)
    removed_entities: list[EntityNode] = Field(default_factory=list)


class RelationshipTimeline(BaseModel):
    """Timeline of a relationship between two entities."""

    source: EntityNode
    target: EntityNode
    periods: list[TemporalRelationship] = Field(default_factory=list)
    total_duration_days: int | None = None
    is_currently_active: bool = False


# =========================
# Time-Sliced Views
# =========================


async def get_graph_at_time(
    entity_id: UUID,
    as_of: datetime,
    depth: int = 1,
    rel_types: list[RelationType] | None = None,
) -> EntitySnapshot:
    """Get the state of the graph around an entity at a specific time.

    Filters relationships to only include those valid at the given time.
    A relationship is valid if:
    - valid_from is NULL or <= as_of
    - valid_to is NULL or > as_of

    Args:
        entity_id: Central entity ID
        as_of: Point in time for the snapshot
        depth: How many hops to traverse
        rel_types: Filter by relationship types

    Returns:
        EntitySnapshot with entity and valid relationships
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        as_of_str = as_of.isoformat()

        query = f"""
        MATCH (center {{id: $entity_id}})
        OPTIONAL MATCH (center)-[r{type_filter}]-(related)
        WHERE (r.valid_from IS NULL OR r.valid_from <= $as_of)
          AND (r.valid_to IS NULL OR r.valid_to > $as_of)
        WITH center, r, related, type(r) as rel_type,
             CASE WHEN startNode(r).id = center.id THEN 'out' ELSE 'in' END as direction
        RETURN center,
               collect({{
                   rel: r,
                   rel_type: rel_type,
                   related: related,
                   direction: direction
               }}) as relationships
        """

        result = await session.run(
            query,
            entity_id=str(entity_id),
            as_of=as_of_str,
        )
        record = await result.single()

        if not record or not record.get("center"):
            return EntitySnapshot(
                entity=EntityNode(
                    id=entity_id,
                    entity_type="UNKNOWN",
                    name="Not Found",
                ),
                as_of=as_of,
            )

        center_node = _parse_entity_node(record["center"])

        relationships = []
        incoming = 0
        outgoing = 0

        for rel_data in record.get("relationships", []):
            if not rel_data.get("rel") or not rel_data.get("related"):
                continue

            rel_props = dict(rel_data["rel"])
            related_node = _parse_entity_node(rel_data["related"])

            # Determine source and target based on direction
            if rel_data["direction"] == "out":
                source = center_node
                target = related_node
                outgoing += 1
            else:
                source = related_node
                target = center_node
                incoming += 1

            # Parse validity dates
            valid_from = None
            valid_to = None
            if rel_props.get("valid_from"):
                try:
                    valid_from = datetime.fromisoformat(rel_props["valid_from"])
                except (ValueError, TypeError):
                    pass
            if rel_props.get("valid_to"):
                try:
                    valid_to = datetime.fromisoformat(rel_props["valid_to"])
                except (ValueError, TypeError):
                    pass

            relationships.append(TemporalRelationship(
                id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                rel_type=rel_data["rel_type"],
                source=source,
                target=target,
                valid_from=valid_from,
                valid_to=valid_to,
                is_current=valid_to is None,
                properties=rel_props,
            ))

        return EntitySnapshot(
            entity=center_node,
            as_of=as_of,
            relationships=relationships,
            incoming_count=incoming,
            outgoing_count=outgoing,
        )


async def get_entity_at_time(
    entity_id: UUID,
    as_of: datetime,
) -> EntityNode | None:
    """Get an entity's state at a specific time.

    Args:
        entity_id: Entity ID
        as_of: Point in time

    Returns:
        EntityNode or None if not found/not existing at that time
    """
    async with get_neo4j_session() as session:
        as_of_str = as_of.isoformat()

        query = """
        MATCH (e {id: $entity_id})
        WHERE (e.created_at IS NULL OR e.created_at <= $as_of)
        RETURN e
        """

        result = await session.run(
            query,
            entity_id=str(entity_id),
            as_of=as_of_str,
        )
        record = await result.single()

        if not record or not record.get("e"):
            return None

        return _parse_entity_node(record["e"])


# =========================
# Historical Change Detection
# =========================


async def detect_changes_between(
    entity_id: UUID,
    from_date: datetime,
    to_date: datetime,
    rel_types: list[RelationType] | None = None,
) -> GraphDiff:
    """Detect changes in relationships between two time points.

    Compares the graph state at from_date with to_date and identifies:
    - New relationships that started
    - Relationships that ended
    - Relationships with modified properties

    Args:
        entity_id: Central entity ID to analyze
        from_date: Start of comparison period
        to_date: End of comparison period
        rel_types: Filter by relationship types

    Returns:
        GraphDiff with detected changes
    """
    # Get snapshots at both points
    from_snapshot = await get_graph_at_time(entity_id, from_date, rel_types=rel_types)
    to_snapshot = await get_graph_at_time(entity_id, to_date, rel_types=rel_types)

    # Index relationships by a composite key for comparison
    def rel_key(rel: TemporalRelationship) -> str:
        return f"{rel.source.id}:{rel.target.id}:{rel.rel_type}"

    from_rels = {rel_key(r): r for r in from_snapshot.relationships}
    to_rels = {rel_key(r): r for r in to_snapshot.relationships}

    added = []
    removed = []
    modified = []

    # Find added relationships
    for key, rel in to_rels.items():
        if key not in from_rels:
            added.append(rel)
        else:
            # Check for modifications
            old_rel = from_rels[key]
            if old_rel.properties != rel.properties:
                modified.append(RelationshipChange(
                    change_type="modified",
                    relationship=rel,
                    detected_at=to_date,
                    previous_state=old_rel.properties,
                    new_state=rel.properties,
                ))

    # Find removed relationships
    for key, rel in from_rels.items():
        if key not in to_rels:
            removed.append(rel)

    return GraphDiff(
        from_date=from_date,
        to_date=to_date,
        added_relationships=added,
        removed_relationships=removed,
        modified_relationships=modified,
    )


async def get_relationship_changes(
    entity_id: UUID,
    since: datetime,
    rel_types: list[RelationType] | None = None,
) -> list[RelationshipChange]:
    """Get all relationship changes for an entity since a given time.

    Args:
        entity_id: Entity ID to check
        since: Start time for change detection
        rel_types: Filter by relationship types

    Returns:
        List of detected changes ordered by time
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        since_str = since.isoformat()

        # Query for relationships that changed (started or ended) since the given time
        query = f"""
        MATCH (center {{id: $entity_id}})-[r{type_filter}]-(related)
        WHERE (r.valid_from IS NOT NULL AND r.valid_from >= $since)
           OR (r.valid_to IS NOT NULL AND r.valid_to >= $since)
           OR (r.updated_at IS NOT NULL AND r.updated_at >= $since)
        WITH r, related, type(r) as rel_type,
             CASE WHEN startNode(r).id = $entity_id THEN startNode(r) ELSE endNode(r) END as source_node,
             CASE WHEN startNode(r).id = $entity_id THEN endNode(r) ELSE startNode(r) END as target_node
        RETURN r, related, rel_type, source_node, target_node
        ORDER BY COALESCE(r.valid_from, r.updated_at, r.created_at) DESC
        """

        result = await session.run(
            query,
            entity_id=str(entity_id),
            since=since_str,
        )
        records = await result.data()

        changes = []
        for record in records:
            rel_props = dict(record["r"]) if record.get("r") else {}

            source_node = _parse_entity_node(record["source_node"])
            target_node = _parse_entity_node(record["target_node"])

            # Determine change type
            valid_from = None
            valid_to = None
            if rel_props.get("valid_from"):
                try:
                    valid_from = datetime.fromisoformat(rel_props["valid_from"])
                except (ValueError, TypeError):
                    pass
            if rel_props.get("valid_to"):
                try:
                    valid_to = datetime.fromisoformat(rel_props["valid_to"])
                except (ValueError, TypeError):
                    pass

            if valid_from and valid_from >= since:
                change_type = "added"
                detected_at = valid_from
            elif valid_to and valid_to >= since:
                change_type = "removed"
                detected_at = valid_to
            else:
                change_type = "modified"
                detected_at = datetime.utcnow()

            changes.append(RelationshipChange(
                change_type=change_type,
                relationship=TemporalRelationship(
                    id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                    rel_type=record["rel_type"],
                    source=source_node,
                    target=target_node,
                    valid_from=valid_from,
                    valid_to=valid_to,
                    is_current=valid_to is None,
                    properties=rel_props,
                ),
                detected_at=detected_at,
            ))

        return changes


async def get_relationship_timeline(
    source_id: UUID,
    target_id: UUID,
    rel_type: RelationType | None = None,
) -> RelationshipTimeline:
    """Get the full timeline of relationships between two entities.

    Shows all historical periods when a relationship existed,
    useful for understanding the evolution of connections.

    Args:
        source_id: Source entity ID
        target_id: Target entity ID
        rel_type: Filter by relationship type

    Returns:
        RelationshipTimeline with all historical periods
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_type:
            type_filter = f":{rel_type.value}"

        query = f"""
        MATCH (source {{id: $source_id}})-[r{type_filter}]-(target {{id: $target_id}})
        WITH source, target, r, type(r) as rel_type
        ORDER BY COALESCE(r.valid_from, r.created_at) ASC
        RETURN source, target,
               collect({{
                   rel: r,
                   rel_type: rel_type
               }}) as relationships
        """

        result = await session.run(
            query,
            source_id=str(source_id),
            target_id=str(target_id),
        )
        record = await result.single()

        if not record:
            # No relationship found, return empty timeline
            async with get_neo4j_session() as session2:
                source_query = "MATCH (n {id: $id}) RETURN n"
                source_result = await session2.run(source_query, id=str(source_id))
                source_rec = await source_result.single()

                target_query = "MATCH (n {id: $id}) RETURN n"
                target_result = await session2.run(target_query, id=str(target_id))
                target_rec = await target_result.single()

                source_node = _parse_entity_node(source_rec["n"]) if source_rec else EntityNode(
                    id=source_id, entity_type="UNKNOWN", name="Unknown"
                )
                target_node = _parse_entity_node(target_rec["n"]) if target_rec else EntityNode(
                    id=target_id, entity_type="UNKNOWN", name="Unknown"
                )

            return RelationshipTimeline(
                source=source_node,
                target=target_node,
            )

        source_node = _parse_entity_node(record["source"])
        target_node = _parse_entity_node(record["target"])

        periods = []
        total_days = 0
        is_current = False

        for rel_data in record.get("relationships", []):
            rel_props = dict(rel_data["rel"]) if rel_data.get("rel") else {}

            valid_from = None
            valid_to = None
            if rel_props.get("valid_from"):
                try:
                    valid_from = datetime.fromisoformat(rel_props["valid_from"])
                except (ValueError, TypeError):
                    pass
            if rel_props.get("valid_to"):
                try:
                    valid_to = datetime.fromisoformat(rel_props["valid_to"])
                except (ValueError, TypeError):
                    pass

            # Calculate duration
            if valid_from:
                end = valid_to or datetime.utcnow()
                duration = (end - valid_from).days
                total_days += duration

            if valid_to is None:
                is_current = True

            periods.append(TemporalRelationship(
                id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                rel_type=rel_data["rel_type"],
                source=source_node,
                target=target_node,
                valid_from=valid_from,
                valid_to=valid_to,
                is_current=valid_to is None,
                properties=rel_props,
            ))

        return RelationshipTimeline(
            source=source_node,
            target=target_node,
            periods=periods,
            total_duration_days=total_days if total_days > 0 else None,
            is_currently_active=is_current,
        )


# =========================
# Aggregate Historical Queries
# =========================


async def get_entity_history(
    entity_id: UUID,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    interval_days: int = 30,
) -> list[EntitySnapshot]:
    """Get snapshots of an entity at regular intervals.

    Useful for visualizing how an entity's relationships
    evolved over time.

    Args:
        entity_id: Entity ID
        start_date: Start of history (default: entity creation)
        end_date: End of history (default: now)
        interval_days: Days between snapshots

    Returns:
        List of EntitySnapshots at each interval
    """
    if end_date is None:
        end_date = datetime.utcnow()

    if start_date is None:
        # Try to get entity creation date
        async with get_neo4j_session() as session:
            query = "MATCH (e {id: $id}) RETURN e.created_at as created"
            result = await session.run(query, id=str(entity_id))
            record = await result.single()

            if record and record.get("created"):
                try:
                    start_date = datetime.fromisoformat(record["created"])
                except (ValueError, TypeError):
                    start_date = end_date - timedelta(days=365)
            else:
                start_date = end_date - timedelta(days=365)

    snapshots = []
    current = start_date

    while current <= end_date:
        snapshot = await get_graph_at_time(entity_id, current)
        snapshots.append(snapshot)
        current += timedelta(days=interval_days)

    return snapshots


async def find_relationship_patterns(
    entity_ids: list[UUID],
    lookback_days: int = 365,
) -> dict[str, Any]:
    """Analyze relationship patterns across multiple entities.

    Looks for:
    - Common relationship timing (relationships created around same time)
    - Shared intermediaries that appeared/disappeared together
    - Cyclical patterns

    Args:
        entity_ids: Entities to analyze
        lookback_days: How far back to look

    Returns:
        Dictionary of detected patterns
    """
    if len(entity_ids) < 2:
        return {"patterns": [], "message": "Need at least 2 entities"}

    since = datetime.utcnow() - timedelta(days=lookback_days)

    # Collect all changes for all entities
    all_changes: list[tuple[UUID, RelationshipChange]] = []

    for eid in entity_ids:
        changes = await get_relationship_changes(eid, since)
        for change in changes:
            all_changes.append((eid, change))

    # Group changes by time windows (30-day buckets)
    time_buckets: dict[str, list[tuple[UUID, RelationshipChange]]] = {}

    for eid, change in all_changes:
        bucket_key = change.detected_at.strftime("%Y-%m")
        if bucket_key not in time_buckets:
            time_buckets[bucket_key] = []
        time_buckets[bucket_key].append((eid, change))

    # Find coordinated changes (multiple entities changing in same bucket)
    coordinated_changes = []
    for bucket, changes in time_buckets.items():
        entities_in_bucket = set(eid for eid, _ in changes)
        if len(entities_in_bucket) >= 2:
            coordinated_changes.append({
                "period": bucket,
                "entities_affected": list(str(eid) for eid in entities_in_bucket),
                "change_count": len(changes),
                "changes": [
                    {
                        "entity_id": str(eid),
                        "change_type": c.change_type,
                        "relationship_type": c.relationship.rel_type,
                    }
                    for eid, c in changes
                ],
            })

    # Find shared relationship targets
    shared_targets: dict[str, list[str]] = {}
    for eid, change in all_changes:
        target_id = str(change.relationship.target.id)
        if target_id not in shared_targets:
            shared_targets[target_id] = []
        if str(eid) not in shared_targets[target_id]:
            shared_targets[target_id].append(str(eid))

    shared_connections = [
        {
            "target_id": target_id,
            "target_name": next(
                (c.relationship.target.name for _, c in all_changes
                 if str(c.relationship.target.id) == target_id),
                "Unknown"
            ),
            "connected_entities": entities,
        }
        for target_id, entities in shared_targets.items()
        if len(entities) >= 2
    ]

    return {
        "coordinated_changes": coordinated_changes,
        "shared_connections": shared_connections,
        "total_changes_analyzed": len(all_changes),
        "analysis_period": {
            "start": since.isoformat(),
            "end": datetime.utcnow().isoformat(),
        },
    }
