"""Political contribution lead extractor.

Extracts leads for corporate contributors to election third parties
by querying CONTRIBUTED_TO relationships in Neo4j.
"""

from typing import Any, AsyncIterator
from uuid import UUID

from ...db import get_neo4j_session
from ...logging import get_context_logger
from ..models import (
    IdentifierType,
    Lead,
    LeadType,
    ResearchSessionConfig,
)
from .base import BaseLeadExtractor

logger = get_context_logger(__name__)


class PoliticalContributionExtractor(BaseLeadExtractor):
    """Extracts political contribution leads.

    Discovers corporate contributors to election third parties
    by querying CONTRIBUTED_TO relationships in Neo4j.
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["elections_canada"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract political contribution leads from an organization.

        Queries Neo4j for CONTRIBUTED_TO relationships where the target
        entity is a third party, and generates leads for corporate contributors.

        Args:
            entity_id: Entity UUID (third party organization)
            entity_type: Entity type (should be ORGANIZATION)
            entity_name: Entity name
            entity_data: Entity metadata/properties
            config: Session configuration for filtering

        Yields:
            Lead objects for corporate contributors
        """
        try:
            async with get_neo4j_session() as session:
                # Find contributors to this third party
                contributors_query = """
                    MATCH (contributor)-[r:CONTRIBUTED_TO]->(tp:Organization)
                    WHERE tp.id = $entity_id OR tp.name = $entity_name
                      AND tp.is_election_third_party = true
                      AND (contributor:Organization OR contributor.entity_type = 'organization')
                    RETURN contributor.id as contributor_id,
                           contributor.name as contributor_name,
                           contributor.entity_type as contributor_type,
                           r.amount as amount,
                           r.contributor_class as contributor_class,
                           r.election_id as election_id,
                           r.jurisdiction as jurisdiction
                    ORDER BY r.amount DESC
                """

                result = await session.run(
                    contributors_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                async for record in result:
                    contributor_name = record.get("contributor_name")
                    if not contributor_name:
                        continue

                    # Use NAME identifier type for contributor identification
                    identifier = contributor_name
                    id_type = IdentifierType.NAME

                    # Calculate priority based on contribution amount
                    amount = record.get("amount")
                    if amount and amount > 50000:
                        priority = 1  # Major contributor
                    elif amount and amount > 10000:
                        priority = 2  # Significant contributor
                    else:
                        priority = 3  # Minor contributor

                    # Base confidence on contributor class
                    contributor_class = record.get("contributor_class", "")
                    confidence = 0.9 if contributor_class == "corporation" else 0.8

                    yield self.create_lead(
                        lead_type=LeadType.POLITICAL_CONTRIBUTION,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "contributed_to",
                            "third_party": entity_name,
                            "amount": amount,
                            "contributor_class": contributor_class,
                            "election_id": record.get("election_id"),
                            "jurisdiction": record.get("jurisdiction"),
                        },
                        source_relationship_type="CONTRIBUTED_TO",
                    )

        except Exception as e:
            logger.warning(
                f"PoliticalContributionExtractor: Failed to extract leads from {entity_name}: {e}"
            )
            return
