"""Graph operations modules for MITDS.

Provides Neo4j graph building and querying functionality.
"""

from .builder import (
    GraphBuilder,
    NodeResult,
    RelationshipResult,
    get_graph_builder,
)
from .queries import (
    EntityNode,
    FundingCluster,
    FundingPath,
    RelationshipEdge,
    find_path_between,
    find_shared_funders,
    get_entity_relationships,
    get_entity_stats,
    get_funding_paths,
    get_funding_recipients,
    get_funding_sources,
)

__all__ = [
    # Builder
    "GraphBuilder",
    "NodeResult",
    "RelationshipResult",
    "get_graph_builder",
    # Queries
    "EntityNode",
    "FundingCluster",
    "FundingPath",
    "RelationshipEdge",
    "find_path_between",
    "find_shared_funders",
    "get_entity_relationships",
    "get_entity_stats",
    "get_funding_paths",
    "get_funding_recipients",
    "get_funding_sources",
]
