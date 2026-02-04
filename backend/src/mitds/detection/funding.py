"""Funding cluster detection for MITDS.

Detects groups of outlets/organizations that share common funders,
indicating potential coordinated influence.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ..db import get_neo4j_session
from ..graph.queries import EntityNode, FundingCluster
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class FundingClusterResult(BaseModel):
    """Result of funding cluster detection."""

    cluster_id: str
    shared_funder: EntityNode
    members: list[EntityNode]
    total_funding: float
    funding_by_member: dict[str, float] = Field(default_factory=dict)
    fiscal_years: list[int] = Field(default_factory=list)
    score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_summary: str = ""


class SharedFunderResult(BaseModel):
    """Result of shared funder identification."""

    funder: EntityNode
    recipients: list[EntityNode]
    shared_count: int
    total_funding: float
    funding_concentration: float  # % of funder's total going to these recipients
    years_active: int


class FundingClusterDetector:
    """Detector for funding clusters and shared funders.

    Identifies coordinated funding patterns:
    1. Entities sharing multiple common funders
    2. Funders with concentrated recipient portfolios
    3. Funding timing patterns
    """

    def __init__(
        self,
        min_shared_funders: int = 2,
        min_cluster_size: int = 2,
        min_funding_amount: float = 0.0,
    ):
        """Initialize the detector.

        Args:
            min_shared_funders: Minimum shared funders to form cluster
            min_cluster_size: Minimum entities in a cluster
            min_funding_amount: Minimum funding to consider
        """
        self.min_shared_funders = min_shared_funders
        self.min_cluster_size = min_cluster_size
        self.min_funding_amount = min_funding_amount

    async def detect_clusters(
        self,
        entity_type: str | None = None,
        fiscal_year: int | None = None,
        jurisdiction: str | None = None,
        limit: int = 50,
    ) -> list[FundingClusterResult]:
        """Detect funding clusters.

        Args:
            entity_type: Filter by entity type (OUTLET, ORGANIZATION)
            fiscal_year: Filter by fiscal year
            jurisdiction: Filter by jurisdiction (e.g., 'CA' for Canada)
            limit: Maximum clusters to return

        Returns:
            List of detected funding clusters
        """
        async with get_neo4j_session() as session:
            # Build WHERE clause conditions for recipient filtering
            recipient_conditions = []
            if entity_type:
                recipient_conditions.append(f"recipient:{entity_type}")
            if fiscal_year:
                recipient_conditions.append(f"r.fiscal_year = {fiscal_year}")
            if jurisdiction:
                # Use STARTS WITH to match both 'CA' and 'CA-ON', 'CA-BC', etc.
                recipient_conditions.append(f"recipient.jurisdiction STARTS WITH '{jurisdiction}'")

            recipient_where = ""
            if recipient_conditions:
                recipient_where = "WHERE " + " AND ".join(recipient_conditions)

            # Memory-efficient approach: start from funders with multiple recipients
            # This avoids the expensive cartesian product of finding all recipient pairs
            query = f"""
            // First, find funders that have multiple recipients (potential cluster centers)
            MATCH (funder)<-[r:FUNDED_BY]-(recipient)
            {recipient_where}
            WITH funder, count(DISTINCT recipient) as recipient_count
            WHERE recipient_count >= 2
            
            // Limit funders to avoid memory issues
            ORDER BY recipient_count DESC
            LIMIT 200
            
            // Now get the actual recipients for each funder
            MATCH (recipient)-[r:FUNDED_BY]->(funder)
            {recipient_where}
            
            RETURN funder, collect(DISTINCT recipient) as recipients
            ORDER BY size(recipients) DESC
            LIMIT {limit * 2}
            """

            result = await session.run(query)
            records = await result.data()

            # Build clusters from funder-recipient data
            # Find recipients that share multiple funders
            clusters = self._build_clusters_from_funders(records)

            # Calculate scores and return top clusters
            scored_clusters = []
            for cluster in clusters[:limit]:
                score = self._calculate_cluster_score(cluster)
                cluster.score = score
                cluster.confidence = min(score + 0.2, 1.0)
                cluster.evidence_summary = self._generate_evidence_summary(cluster)
                scored_clusters.append(cluster)

            return sorted(scored_clusters, key=lambda c: c.score, reverse=True)

    async def find_shared_funders(
        self,
        entity_ids: list[UUID] | None = None,
        min_recipients: int = 2,
        fiscal_year: int | None = None,
        limit: int = 50,
    ) -> list[SharedFunderResult]:
        """Find funders shared by multiple entities.

        Args:
            entity_ids: Specific entities to check (None for all)
            min_recipients: Minimum recipients to include funder
            fiscal_year: Filter by fiscal year
            limit: Maximum results

        Returns:
            List of shared funders with their recipients
        """
        async with get_neo4j_session() as session:
            # Build WHERE clause conditions
            where_conditions = []
            if entity_ids:
                ids_str = "[" + ",".join(f"'{str(id)}'" for id in entity_ids) + "]"
                where_conditions.append(f"recipient.id IN {ids_str}")
            if fiscal_year:
                where_conditions.append(f"r.fiscal_year = {fiscal_year}")

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
            // Find all funders and their recipients
            MATCH (recipient)-[r:FUNDED_BY]->(funder)
            {where_clause}

            // Group by funder
            WITH funder,
                 collect(DISTINCT recipient) as recipients,
                 sum(r.amount) as total_funding,
                 collect(DISTINCT r.fiscal_year) as years

            WHERE size(recipients) >= {min_recipients}

            // Get total funding from this funder
            OPTIONAL MATCH (any_recipient)-[all_funding:FUNDED_BY]->(funder)
            WITH funder, recipients, total_funding, years,
                 sum(all_funding.amount) as funder_total

            RETURN
                funder,
                recipients,
                size(recipients) as recipient_count,
                total_funding,
                CASE WHEN funder_total > 0
                     THEN total_funding / funder_total
                     ELSE 0 END as concentration,
                size([y IN years WHERE y IS NOT NULL]) as years_active
            ORDER BY recipient_count DESC, total_funding DESC
            LIMIT {limit}
            """

            result = await session.run(query)
            records = await result.data()

            shared_funders = []
            for record in records:
                funder_node = self._parse_entity_node(record["funder"])
                recipients = [
                    self._parse_entity_node(r) for r in record.get("recipients", [])
                ]

                shared_funders.append(
                    SharedFunderResult(
                        funder=funder_node,
                        recipients=recipients,
                        shared_count=record.get("recipient_count", 0),
                        total_funding=record.get("total_funding") or 0.0,
                        funding_concentration=record.get("concentration") or 0.0,
                        years_active=record.get("years_active") or 0,
                    )
                )

            return shared_funders

    async def get_funding_network(
        self,
        entity_id: UUID,
        max_hops: int = 2,
        min_amount: float | None = None,
    ) -> dict[str, Any]:
        """Get the funding network around an entity.

        Args:
            entity_id: Central entity ID
            max_hops: Maximum relationship hops
            min_amount: Minimum funding amount to include

        Returns:
            Network data with nodes and edges
        """
        async with get_neo4j_session() as session:
            amount_filter = ""
            if min_amount is not None:
                amount_filter = f"WHERE ALL(r IN rels WHERE r.amount >= {min_amount})"

            query = f"""
            // Find all connected funding relationships
            MATCH path = (center {{id: $entity_id}})-[:FUNDED_BY*1..{max_hops}]-(connected)
            WITH path, [r IN relationships(path) | r] as rels
            {amount_filter}

            // Collect unique nodes and relationships
            UNWIND nodes(path) as node
            WITH collect(DISTINCT node) as all_nodes, rels

            UNWIND rels as rel
            WITH all_nodes, collect(DISTINCT rel) as all_rels

            RETURN all_nodes, all_rels
            """

            result = await session.run(query, entity_id=str(entity_id))
            record = await result.single()

            if not record:
                return {"nodes": [], "edges": []}

            nodes = []
            for node in record.get("all_nodes", []):
                node_data = dict(node)
                nodes.append({
                    "id": node_data.get("id"),
                    "name": node_data.get("name"),
                    "type": node_data.get("entity_type"),
                })

            edges = []
            for rel in record.get("all_rels", []):
                rel_data = dict(rel)
                edges.append({
                    "source": rel_data.get("source_id"),
                    "target": rel_data.get("target_id"),
                    "type": "FUNDED_BY",
                    "amount": rel_data.get("amount"),
                    "fiscal_year": rel_data.get("fiscal_year"),
                })

            return {"nodes": nodes, "edges": edges}

    def _group_into_clusters(
        self, records: list[dict]
    ) -> list[FundingClusterResult]:
        """Group query results into distinct clusters."""
        # Use union-find to group entities
        parent = {}

        def find(x):
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Build clusters from pairs
        entity_info = {}
        funder_info = {}

        for record in records:
            recipient = record.get("recipient")
            other = record.get("other")
            shared_funders = record.get("shared_funders", [])

            if recipient and other:
                recipient_id = recipient.get("id")
                other_id = other.get("id")

                if recipient_id and other_id:
                    union(recipient_id, other_id)
                    entity_info[recipient_id] = recipient
                    entity_info[other_id] = other

                    for funder in shared_funders:
                        funder_id = funder.get("id")
                        if funder_id:
                            funder_info[funder_id] = funder

        # Group by cluster root
        clusters_by_root = {}
        for entity_id in entity_info:
            root = find(entity_id)
            if root not in clusters_by_root:
                clusters_by_root[root] = set()
            clusters_by_root[root].add(entity_id)

        # Build cluster results
        clusters = []
        cluster_num = 0

        for root, member_ids in clusters_by_root.items():
            if len(member_ids) < self.min_cluster_size:
                continue

            members = [
                self._parse_entity_node(entity_info[mid])
                for mid in member_ids
                if mid in entity_info
            ]

            # Find shared funders for this cluster
            # (simplified - in practice would need more complex logic)
            if funder_info:
                first_funder_id = next(iter(funder_info))
                shared_funder = self._parse_entity_node(funder_info[first_funder_id])
            else:
                continue

            cluster_num += 1
            clusters.append(
                FundingClusterResult(
                    cluster_id=f"cluster_{cluster_num}",
                    shared_funder=shared_funder,
                    members=members,
                    total_funding=0.0,  # Would be calculated from relationships
                    score=0.0,
                    confidence=0.0,
                )
            )

        return clusters

    def _build_clusters_from_funders(
        self, records: list[dict]
    ) -> list[FundingClusterResult]:
        """Build clusters from funder-centric query results.

        This approach creates clusters where each cluster is centered around
        a funder that funds multiple recipients.
        """
        clusters = []
        cluster_num = 0

        for record in records:
            funder_node = record.get("funder")
            recipients = record.get("recipients", [])

            if not funder_node or len(recipients) < self.min_cluster_size:
                continue

            # Parse funder
            funder = self._parse_entity_node(funder_node)

            # Parse recipients
            members = [
                self._parse_entity_node(r)
                for r in recipients
                if r is not None
            ]

            if len(members) < self.min_cluster_size:
                continue

            cluster_num += 1
            clusters.append(
                FundingClusterResult(
                    cluster_id=f"cluster_{cluster_num}",
                    shared_funder=funder,
                    members=members,
                    total_funding=0.0,  # Could be calculated from relationships
                    score=0.0,
                    confidence=0.0,
                )
            )

        return clusters

    def _calculate_cluster_score(self, cluster: FundingClusterResult) -> float:
        """Calculate a coordination score for a cluster."""
        # Factors:
        # 1. Number of members (more = higher score)
        # 2. Funding concentration
        # 3. Temporal overlap

        member_score = min(len(cluster.members) / 10, 1.0) * 0.4

        # Funding concentration (if available)
        funding_score = 0.3 if cluster.total_funding > 0 else 0.0

        # Base score for having multiple shared funders
        shared_score = 0.3

        return member_score + funding_score + shared_score

    def _generate_evidence_summary(self, cluster: FundingClusterResult) -> str:
        """Generate a human-readable evidence summary."""
        member_names = [m.name for m in cluster.members[:5]]
        if len(cluster.members) > 5:
            member_names.append(f"and {len(cluster.members) - 5} others")

        return (
            f"Cluster of {len(cluster.members)} entities "
            f"({', '.join(member_names)}) "
            f"sharing funding from {cluster.shared_funder.name}. "
            f"Total funding: ${cluster.total_funding:,.2f}."
        )

    def _parse_entity_node(self, node_data: Any) -> EntityNode:
        """Parse node data into EntityNode."""
        if node_data is None:
            raise ValueError("Node data is None")

        if isinstance(node_data, dict):
            props = dict(node_data)
        else:
            props = dict(node_data)

        entity_id = props.pop("id", None)
        if entity_id is None:
            raise ValueError("Node missing id property")

        entity_type = props.pop("entity_type", "UNKNOWN")
        name = props.pop("name", "Unknown")

        # Convert Neo4j types to Python types
        cleaned_props = {}
        for key, value in props.items():
            if hasattr(value, 'to_native'):
                # Neo4j temporal types have to_native() method
                cleaned_props[key] = value.to_native()
            elif hasattr(value, 'isoformat'):
                # datetime-like objects
                cleaned_props[key] = value.isoformat()
            else:
                cleaned_props[key] = value

        return EntityNode(
            id=UUID(entity_id) if isinstance(entity_id, str) else entity_id,
            entity_type=entity_type,
            name=name,
            properties=cleaned_props,
        )


# Convenience functions
async def detect_funding_clusters(
    entity_type: str | None = None,
    fiscal_year: int | None = None,
    min_shared_funders: int = 2,
    limit: int = 50,
) -> list[FundingClusterResult]:
    """Detect funding clusters.

    Args:
        entity_type: Filter by entity type
        fiscal_year: Filter by fiscal year
        min_shared_funders: Minimum shared funders
        limit: Maximum results

    Returns:
        List of funding clusters
    """
    detector = FundingClusterDetector(min_shared_funders=min_shared_funders)
    return await detector.detect_clusters(
        entity_type=entity_type,
        fiscal_year=fiscal_year,
        limit=limit,
    )


async def find_shared_funders(
    entity_ids: list[UUID] | None = None,
    min_recipients: int = 2,
    fiscal_year: int | None = None,
    limit: int = 50,
) -> list[SharedFunderResult]:
    """Find shared funders.

    Args:
        entity_ids: Specific entities to check
        min_recipients: Minimum recipients
        fiscal_year: Filter by fiscal year
        limit: Maximum results

    Returns:
        List of shared funders
    """
    detector = FundingClusterDetector()
    return await detector.find_shared_funders(
        entity_ids=entity_ids,
        min_recipients=min_recipients,
        fiscal_year=fiscal_year,
        limit=limit,
    )
