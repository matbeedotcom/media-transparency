"""Graph query operations for Neo4j.

Provides query functions for:
- Funding path traversal
- Entity relationship queries
- Multi-hop path finding

Performance Notes:
- All queries are designed to use Neo4j indexes on id, name, and external IDs
- Variable-length paths use LIMIT to prevent explosion
- Aggregations use DISTINCT to reduce memory usage
- Temporal filters should use indexed properties (valid_from, valid_to, fiscal_year)
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ..db import get_neo4j_session
from ..logging import get_context_logger, log_graph_operation
from ..models.relationships import RelationType

logger = get_context_logger(__name__)


# Query timeout in seconds
QUERY_TIMEOUT = 30

# Maximum results for variable-length path queries
MAX_PATH_RESULTS = 100


class EntityNode(BaseModel):
    """Entity node from graph query."""

    id: UUID
    entity_type: str
    name: str
    properties: dict[str, Any] = Field(default_factory=dict)


class RelationshipEdge(BaseModel):
    """Relationship edge from graph query."""

    id: UUID | None = None
    rel_type: str
    source_id: UUID
    target_id: UUID
    properties: dict[str, Any] = Field(default_factory=dict)


class FundingPath(BaseModel):
    """A funding path from funder to recipient."""

    funder: EntityNode
    recipient: EntityNode
    intermediaries: list[EntityNode] = Field(default_factory=list)
    relationships: list[RelationshipEdge] = Field(default_factory=list)
    total_amount: float | None = None
    hops: int = 1


class FundingCluster(BaseModel):
    """A cluster of entities sharing common funders."""

    shared_funder: EntityNode
    recipients: list[EntityNode]
    total_funding: float | None = None
    fiscal_years: list[int] = Field(default_factory=list)


async def get_funding_paths(
    funder_id: UUID,
    max_hops: int = 3,
    min_amount: float | None = None,
    fiscal_year: int | None = None,
) -> list[FundingPath]:
    """Find all funding paths from a funder.

    Args:
        funder_id: ID of the funding entity
        max_hops: Maximum path length
        min_amount: Minimum funding amount to include
        fiscal_year: Filter by fiscal year

    Returns:
        List of funding paths
    """
    async with get_neo4j_session() as session:
        # Build query
        amount_filter = ""
        if min_amount is not None:
            amount_filter = f"AND ALL(r IN rels WHERE r.amount >= {min_amount})"

        year_filter = ""
        if fiscal_year is not None:
            year_filter = f"AND ANY(r IN rels WHERE r.fiscal_year = {fiscal_year})"

        query = f"""
        MATCH path = (funder {{id: $funder_id}})<-[:FUNDED_BY*1..{max_hops}]-(recipient)
        WITH path, funder, recipient, [r IN relationships(path) | r] as rels
        WHERE true {amount_filter} {year_filter}
        RETURN
            funder,
            recipient,
            [n IN nodes(path)[1..-1] | n] as intermediaries,
            rels,
            REDUCE(total = 0.0, r IN rels | total + COALESCE(r.amount, 0)) as total_amount,
            length(path) as hops
        ORDER BY total_amount DESC
        LIMIT 100
        """

        result = await session.run(query, funder_id=str(funder_id))
        records = await result.data()

        paths = []
        for record in records:
            funder_node = _parse_entity_node(record["funder"])
            recipient_node = _parse_entity_node(record["recipient"])
            intermediaries = [
                _parse_entity_node(n) for n in record.get("intermediaries", [])
            ]
            relationships = [
                _parse_relationship_edge(r) for r in record.get("rels", [])
            ]

            paths.append(
                FundingPath(
                    funder=funder_node,
                    recipient=recipient_node,
                    intermediaries=intermediaries,
                    relationships=relationships,
                    total_amount=record.get("total_amount"),
                    hops=record.get("hops", 1),
                )
            )

        return paths


async def get_funding_recipients(
    funder_id: UUID,
    fiscal_year: int | None = None,
    min_amount: float | None = None,
    limit: int = 100,
) -> list[tuple[EntityNode, float | None]]:
    """Get all direct funding recipients of an entity.

    Args:
        funder_id: ID of the funding entity
        fiscal_year: Filter by fiscal year
        min_amount: Minimum funding amount
        limit: Maximum results

    Returns:
        List of (recipient, amount) tuples
    """
    async with get_neo4j_session() as session:
        filters = []
        if fiscal_year:
            filters.append(f"r.fiscal_year = {fiscal_year}")
        if min_amount is not None:
            filters.append(f"r.amount >= {min_amount}")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = f"""
        MATCH (recipient)-[r:FUNDED_BY]->(funder {{id: $funder_id}})
        {where_clause}
        RETURN recipient, r.amount as amount
        ORDER BY r.amount DESC
        LIMIT {limit}
        """

        result = await session.run(query, funder_id=str(funder_id))
        records = await result.data()

        return [
            (_parse_entity_node(record["recipient"]), record.get("amount"))
            for record in records
        ]


async def get_funding_sources(
    recipient_id: UUID,
    fiscal_year: int | None = None,
    min_amount: float | None = None,
    limit: int = 100,
) -> list[tuple[EntityNode, float | None]]:
    """Get all direct funding sources of an entity.

    Args:
        recipient_id: ID of the recipient entity
        fiscal_year: Filter by fiscal year
        min_amount: Minimum funding amount
        limit: Maximum results

    Returns:
        List of (funder, amount) tuples
    """
    async with get_neo4j_session() as session:
        filters = []
        if fiscal_year:
            filters.append(f"r.fiscal_year = {fiscal_year}")
        if min_amount is not None:
            filters.append(f"r.amount >= {min_amount}")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = f"""
        MATCH (recipient {{id: $recipient_id}})-[r:FUNDED_BY]->(funder)
        {where_clause}
        RETURN funder, r.amount as amount
        ORDER BY r.amount DESC
        LIMIT {limit}
        """

        result = await session.run(query, recipient_id=str(recipient_id))
        records = await result.data()

        return [
            (_parse_entity_node(record["funder"]), record.get("amount"))
            for record in records
        ]


async def find_shared_funders(
    entity_ids: list[UUID],
    min_shared: int = 2,
) -> list[FundingCluster]:
    """Find funders shared by multiple entities.

    Args:
        entity_ids: List of entity IDs to check
        min_shared: Minimum number of entities sharing a funder

    Returns:
        List of funding clusters
    """
    if len(entity_ids) < 2:
        return []

    async with get_neo4j_session() as session:
        query = """
        UNWIND $entity_ids as eid
        MATCH (entity {id: eid})-[r:FUNDED_BY]->(funder)
        WITH funder, collect(DISTINCT entity) as recipients,
             collect(r.amount) as amounts,
             collect(DISTINCT r.fiscal_year) as years
        WHERE size(recipients) >= $min_shared
        RETURN funder, recipients,
               REDUCE(total = 0.0, a IN amounts | total + COALESCE(a, 0)) as total_funding,
               years
        ORDER BY size(recipients) DESC, total_funding DESC
        """

        result = await session.run(
            query,
            entity_ids=[str(eid) for eid in entity_ids],
            min_shared=min_shared,
        )
        records = await result.data()

        clusters = []
        for record in records:
            funder_node = _parse_entity_node(record["funder"])
            recipients = [
                _parse_entity_node(r) for r in record.get("recipients", [])
            ]
            years = [y for y in record.get("years", []) if y is not None]

            clusters.append(
                FundingCluster(
                    shared_funder=funder_node,
                    recipients=recipients,
                    total_funding=record.get("total_funding"),
                    fiscal_years=sorted(years),
                )
            )

        return clusters


class BoardInterlockCluster(BaseModel):
    """A cluster of organizations sharing a common director."""

    shared_director: EntityNode
    organizations: list[EntityNode]
    org_count: int


async def find_shared_directors(
    entity_ids: list[UUID],
    min_shared: int = 2,
) -> list[BoardInterlockCluster]:
    """Find directors shared by multiple entities (board interlocks).

    Args:
        entity_ids: List of organization entity IDs to check
        min_shared: Minimum number of organizations sharing a director

    Returns:
        List of board interlock clusters
    """
    if len(entity_ids) < 2:
        return []

    async with get_neo4j_session() as session:
        query = """
        UNWIND $entity_ids as eid
        MATCH (org {id: eid})<-[:DIRECTOR_OF]-(person:Person)
        WITH person, collect(DISTINCT org) as orgs
        WHERE size(orgs) >= $min_shared
        RETURN person, orgs
        ORDER BY size(orgs) DESC
        """

        result = await session.run(
            query,
            entity_ids=[str(eid) for eid in entity_ids],
            min_shared=min_shared,
        )
        records = await result.data()

        clusters = []
        for record in records:
            director_node = _parse_entity_node(record["person"])
            org_nodes = [
                _parse_entity_node(o) for o in record.get("orgs", [])
            ]

            clusters.append(
                BoardInterlockCluster(
                    shared_director=director_node,
                    organizations=org_nodes,
                    org_count=len(org_nodes),
                )
            )

        return clusters


async def find_board_interlocks_for_entity(
    entity_id: UUID,
) -> list[dict[str, Any]]:
    """Find board interlocks for a specific entity.

    Returns directors of this entity who also serve on other boards,
    along with those other organizations.

    Args:
        entity_id: ID of the organization entity

    Returns:
        List of dicts with 'director' and 'organizations' keys
    """
    async with get_neo4j_session() as session:
        query = """
        MATCH (org {id: $entity_id})<-[:DIRECTOR_OF]-(person:Person)-[:DIRECTOR_OF]->(other:Organization)
        WHERE other.id <> $entity_id
        WITH person, collect(DISTINCT other) as other_orgs
        RETURN person, other_orgs
        ORDER BY size(other_orgs) DESC
        """

        result = await session.run(query, entity_id=str(entity_id))
        records = await result.data()

        interlocks = []
        for record in records:
            director_node = _parse_entity_node(record["person"])
            org_nodes = [
                _parse_entity_node(o) for o in record.get("other_orgs", [])
            ]

            interlocks.append({
                "director": director_node,
                "organizations": org_nodes,
            })

        return interlocks


async def get_entity_relationships(
    entity_id: UUID,
    rel_types: list[RelationType] | None = None,
    direction: str = "both",  # "in", "out", "both"
    limit: int = 100,
) -> list[tuple[EntityNode, RelationshipEdge]]:
    """Get all relationships for an entity.

    Args:
        entity_id: ID of the entity
        rel_types: Filter by relationship types
        direction: Relationship direction
        limit: Maximum results

    Returns:
        List of (related_entity, relationship) tuples
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        if direction == "out":
            pattern = f"(entity)-[r{type_filter}]->(related)"
        elif direction == "in":
            pattern = f"(entity)<-[r{type_filter}]-(related)"
        else:
            pattern = f"(entity)-[r{type_filter}]-(related)"

        query = f"""
        MATCH {pattern}
        WHERE entity.id = $entity_id
        RETURN related, r, type(r) as rel_type,
               startNode(r).id as source_id, endNode(r).id as target_id
        LIMIT {limit}
        """

        result = await session.run(query, entity_id=str(entity_id))
        records = await result.data()

        relationships = []
        for record in records:
            related_node = _parse_entity_node(record["related"])

            rel_props = dict(record["r"]) if record.get("r") else {}
            edge = RelationshipEdge(
                id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                rel_type=record.get("rel_type", ""),
                source_id=UUID(record["source_id"]),
                target_id=UUID(record["target_id"]),
                properties=rel_props,
            )

            relationships.append((related_node, edge))

        return relationships


async def find_path_between(
    source_id: UUID,
    target_id: UUID,
    max_hops: int = 5,
    rel_types: list[RelationType] | None = None,
) -> list[FundingPath] | None:
    """Find shortest path between two entities.

    Args:
        source_id: Starting entity ID
        target_id: Ending entity ID
        max_hops: Maximum path length
        rel_types: Filter by relationship types

    Returns:
        Shortest path if found, None otherwise
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        query = f"""
        MATCH path = shortestPath(
            (source {{id: $source_id}})-[{type_filter}*1..{max_hops}]-(target {{id: $target_id}})
        )
        RETURN
            source,
            target,
            [n IN nodes(path)[1..-1] | n] as intermediaries,
            [r IN relationships(path) | r] as rels,
            length(path) as hops
        LIMIT 1
        """

        result = await session.run(
            query,
            source_id=str(source_id),
            target_id=str(target_id),
        )
        record = await result.single()

        if not record:
            return None

        source_node = _parse_entity_node(record["source"])
        target_node = _parse_entity_node(record["target"])
        intermediaries = [
            _parse_entity_node(n) for n in record.get("intermediaries", [])
        ]
        relationships = [
            _parse_relationship_edge(r) for r in record.get("rels", [])
        ]

        return FundingPath(
            funder=source_node,
            recipient=target_node,
            intermediaries=intermediaries,
            relationships=relationships,
            hops=record.get("hops", 1),
        )


class PathResult(BaseModel):
    """Result of a path finding query."""

    source: EntityNode
    target: EntityNode
    nodes: list[EntityNode]
    relationships: list[RelationshipEdge]
    path_length: int
    path_types: list[str]  # Relationship types in order


async def find_all_paths(
    source_id: UUID,
    target_id: UUID,
    max_hops: int = 5,
    rel_types: list[RelationType] | None = None,
    limit: int = 10,
) -> list[PathResult]:
    """Find all paths between two entities up to max_hops.

    Unlike find_path_between which returns only the shortest path,
    this returns all distinct paths which may reveal different
    relationship types and intermediate connections.

    Args:
        source_id: Starting entity ID
        target_id: Ending entity ID
        max_hops: Maximum path length
        rel_types: Filter by relationship types
        limit: Maximum number of paths to return

    Returns:
        List of all paths found
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        query = f"""
        MATCH path = (source {{id: $source_id}})-[{type_filter}*1..{max_hops}]-(target {{id: $target_id}})
        WITH path,
             nodes(path) as path_nodes,
             relationships(path) as path_rels,
             length(path) as path_length
        RETURN
            path_nodes,
            path_rels,
            path_length,
            [r IN path_rels | type(r)] as path_types
        ORDER BY path_length ASC
        LIMIT {limit}
        """

        result = await session.run(
            query,
            source_id=str(source_id),
            target_id=str(target_id),
        )
        records = await result.data()

        paths = []
        for record in records:
            path_nodes = [_parse_entity_node(n) for n in record["path_nodes"]]

            if len(path_nodes) < 2:
                continue

            # Parse relationships with proper direction
            path_rels = []
            for i, rel in enumerate(record.get("path_rels", [])):
                rel_props = dict(rel) if rel else {}
                # Determine source and target from path nodes
                source = path_nodes[i].id if i < len(path_nodes) else UUID(int=0)
                target = path_nodes[i + 1].id if i + 1 < len(path_nodes) else UUID(int=0)

                path_rels.append(RelationshipEdge(
                    id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                    rel_type=record["path_types"][i] if i < len(record["path_types"]) else "UNKNOWN",
                    source_id=source,
                    target_id=target,
                    properties=rel_props,
                ))

            paths.append(PathResult(
                source=path_nodes[0],
                target=path_nodes[-1],
                nodes=path_nodes,
                relationships=path_rels,
                path_length=record["path_length"],
                path_types=record.get("path_types", []),
            ))

        return paths


async def find_connecting_entities(
    entity_ids: list[UUID],
    max_hops: int = 3,
    rel_types: list[RelationType] | None = None,
) -> list[EntityNode]:
    """Find entities that connect multiple given entities.

    Useful for discovering hidden connections like shared directors
    or intermediate organizations.

    Args:
        entity_ids: List of entity IDs to find connections between
        max_hops: Maximum distance from any given entity
        rel_types: Filter by relationship types

    Returns:
        List of connecting entities ordered by connectivity
    """
    if len(entity_ids) < 2:
        return []

    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        query = f"""
        UNWIND $entity_ids as eid
        MATCH (start {{id: eid}})
        MATCH (start)-[{type_filter}*1..{max_hops}]-(connector)
        WHERE NOT connector.id IN $entity_ids
        WITH connector, count(DISTINCT start) as connections
        WHERE connections >= 2
        RETURN DISTINCT connector, connections
        ORDER BY connections DESC
        LIMIT 50
        """

        result = await session.run(
            query,
            entity_ids=[str(eid) for eid in entity_ids],
        )
        records = await result.data()

        return [_parse_entity_node(record["connector"]) for record in records]


async def get_entity_network(
    entity_id: UUID,
    depth: int = 2,
    rel_types: list[RelationType] | None = None,
    limit_per_hop: int = 20,
) -> tuple[list[EntityNode], list[RelationshipEdge]]:
    """Get the network of entities around a central entity.

    Returns all entities within `depth` hops and their relationships,
    useful for visualizing an entity's network.

    Args:
        entity_id: Central entity ID
        depth: How many hops to traverse
        rel_types: Filter by relationship types
        limit_per_hop: Maximum entities per hop level

    Returns:
        Tuple of (nodes, relationships) in the network
    """
    async with get_neo4j_session() as session:
        type_filter = ""
        if rel_types:
            types = "|".join(rt.value for rt in rel_types)
            type_filter = f":{types}"

        query = f"""
        MATCH (center {{id: $entity_id}})
        CALL {{
            WITH center
            MATCH path = (center)-[{type_filter}*1..{depth}]-(connected)
            RETURN connected, relationships(path) as rels
            LIMIT {limit_per_hop * depth}
        }}
        WITH collect(DISTINCT connected) as all_nodes,
             collect(rels) as all_rels_nested
        UNWIND all_nodes as node
        WITH node, all_rels_nested
        UNWIND all_rels_nested as rels
        UNWIND rels as rel
        WITH collect(DISTINCT node) as nodes, collect(DISTINCT rel) as relationships
        RETURN nodes, relationships
        """

        result = await session.run(query, entity_id=str(entity_id))
        record = await result.single()

        if not record:
            return [], []

        nodes = [_parse_entity_node(n) for n in record.get("nodes", [])]

        relationships = []
        for rel in record.get("relationships", []):
            rel_props = dict(rel) if rel else {}
            relationships.append(RelationshipEdge(
                id=UUID(rel_props.get("id")) if rel_props.get("id") else None,
                rel_type=rel_props.pop("type", "UNKNOWN") if "type" in rel_props else "UNKNOWN",
                source_id=UUID(int=0),  # Will be populated by Neo4j
                target_id=UUID(int=0),
                properties=rel_props,
            ))

        return nodes, relationships


async def get_entity_stats(entity_id: UUID) -> dict[str, Any]:
    """Get statistics for an entity.

    Args:
        entity_id: ID of the entity

    Returns:
        Dictionary of statistics
    """
    async with get_neo4j_session() as session:
        query = """
        MATCH (e {id: $entity_id})
        OPTIONAL MATCH (e)-[r_out]->()
        OPTIONAL MATCH (e)<-[r_in]-()
        OPTIONAL MATCH (e)-[:FUNDED_BY]->(funder)
        OPTIONAL MATCH (recipient)-[:FUNDED_BY]->(e)
        OPTIONAL MATCH (e)<-[:DIRECTOR_OF]-(director)
        OPTIONAL MATCH (e)<-[:EMPLOYED_BY]-(employee)
        RETURN
            e.entity_type as entity_type,
            e.name as name,
            count(DISTINCT r_out) as outgoing_relationships,
            count(DISTINCT r_in) as incoming_relationships,
            count(DISTINCT funder) as funder_count,
            count(DISTINCT recipient) as recipient_count,
            count(DISTINCT director) as director_count,
            count(DISTINCT employee) as employee_count
        """

        result = await session.run(query, entity_id=str(entity_id))
        record = await result.single()

        if not record:
            return {}

        return {
            "entity_type": record.get("entity_type"),
            "name": record.get("name"),
            "outgoing_relationships": record.get("outgoing_relationships", 0),
            "incoming_relationships": record.get("incoming_relationships", 0),
            "funder_count": record.get("funder_count", 0),
            "recipient_count": record.get("recipient_count", 0),
            "director_count": record.get("director_count", 0),
            "employee_count": record.get("employee_count", 0),
        }


def _parse_entity_node(node_data: Any) -> EntityNode:
    """Parse Neo4j node data into EntityNode."""
    if node_data is None:
        raise ValueError("Node data is None")

    # Handle different node data formats
    if isinstance(node_data, dict):
        props = dict(node_data)
    else:
        # Neo4j Node object
        props = dict(node_data)

    entity_id = props.pop("id", None)
    if entity_id is None:
        raise ValueError("Node missing id property")

    entity_type = props.pop("entity_type", "UNKNOWN")
    name = props.pop("name", "Unknown")

    return EntityNode(
        id=UUID(entity_id) if isinstance(entity_id, str) else entity_id,
        entity_type=entity_type,
        name=name,
        properties=props,
    )


def _parse_relationship_edge(rel_data: Any) -> RelationshipEdge:
    """Parse Neo4j relationship data into RelationshipEdge."""
    if rel_data is None:
        raise ValueError("Relationship data is None")

    if isinstance(rel_data, dict):
        props = dict(rel_data)
        rel_type = props.pop("type", "UNKNOWN")
        rel_id = props.pop("id", None)
        source_id = props.pop("source_id", None)
        target_id = props.pop("target_id", None)
    else:
        # Neo4j Relationship object
        props = dict(rel_data)
        rel_type = type(rel_data).__name__
        rel_id = props.pop("id", None)
        source_id = None
        target_id = None

    return RelationshipEdge(
        id=UUID(rel_id) if rel_id else None,
        rel_type=rel_type,
        source_id=UUID(source_id) if source_id else UUID(int=0),
        target_id=UUID(target_id) if target_id else UUID(int=0),
        properties=props,
    )


class GraphQueries:
    """Client for executing arbitrary Cypher queries against Neo4j.

    Provides a simple interface for running custom queries, primarily
    used by the report generator for dynamic queries.
    """

    async def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and return results as dictionaries.

        Args:
            query: Cypher query string
            params: Query parameters

        Returns:
            List of result records as dictionaries
        """
        async with get_neo4j_session() as session:
            result = await session.run(query, **(params or {}))
            records = await result.data()
            return records or []
