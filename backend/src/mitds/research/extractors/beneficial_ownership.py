"""Beneficial ownership lead extractor.

Extracts leads for corporations controlled by the same beneficial owner
by querying BENEFICIAL_OWNER_OF relationships in Neo4j.
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


class BeneficialOwnershipExtractor(BaseLeadExtractor):
    """Extracts beneficial ownership leads.

    Discovers other corporations controlled by the same beneficial owner
    by querying BENEFICIAL_OWNER_OF relationships in Neo4j.
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["beneficial_ownership"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract beneficial ownership leads from an organization.

        Queries Neo4j for BENEFICIAL_OWNER_OF relationships where the target
        is this organization, then finds other organizations controlled by the
        same beneficial owner.

        Args:
            entity_id: Entity UUID (corporation)
            entity_type: Entity type (should be ORGANIZATION)
            entity_name: Entity name
            entity_data: Entity metadata/properties
            config: Session configuration for filtering

        Yields:
            Lead objects for other corporations controlled by the same owner
        """
        try:
            async with get_neo4j_session() as session:
                # Find beneficial owners of this corporation
                owners_query = """
                    MATCH (person:Person)-[r:BENEFICIAL_OWNER_OF]->(corp:Organization)
                    WHERE corp.id = $entity_id OR corp.name = $entity_name
                    RETURN person.id as person_id,
                           person.name as person_name,
                           r.control_description as control_description
                """

                result = await session.run(
                    owners_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                owners = []
                async for record in result:
                    person_name = record.get("person_name")
                    if person_name:
                        owners.append({
                            "person_id": record.get("person_id"),
                            "person_name": person_name,
                            "control_description": record.get("control_description"),
                        })

                # For each beneficial owner, find other corporations they control
                for owner in owners:
                    person_id = owner["person_id"]
                    person_name = owner["person_name"]

                    if not person_id:
                        continue

                    # Find other corporations controlled by this person
                    other_corps_query = """
                        MATCH (person:Person)-[r:BENEFICIAL_OWNER_OF]->(other_corp:Organization)
                        WHERE person.id = $person_id
                          AND (other_corp.id <> $entity_id OR other_corp.name <> $entity_name)
                          AND other_corp.canada_corp_num IS NOT NULL
                        RETURN other_corp.id as corp_id,
                               other_corp.name as corp_name,
                               other_corp.canada_corp_num as corp_number,
                               r.control_description as control_description
                    """

                    other_result = await session.run(
                        other_corps_query,
                        person_id=str(person_id),
                        entity_id=str(entity_id),
                        entity_name=entity_name,
                    )

                    async for record in other_result:
                        corp_name = record.get("corp_name")
                        corp_number = record.get("corp_number")

                        if not corp_name or not corp_number:
                            continue

                        # Use CORP_NUMBER identifier type for corporation identification
                        identifier = corp_number
                        id_type = IdentifierType.CORP_NUMBER

                        # Priority based on control description
                        control_desc = record.get("control_description", "").lower()
                        if "majority" in control_desc or "controlling" in control_desc:
                            priority = 1  # High priority
                        elif "significant" in control_desc:
                            priority = 2
                        else:
                            priority = 3

                        confidence = 0.85

                        yield self.create_lead(
                            lead_type=LeadType.BENEFICIAL_OWNERSHIP,
                            target_identifier=identifier,
                            identifier_type=id_type,
                            priority=priority,
                            confidence=confidence,
                            context={
                                "relationship": "beneficial_owner_of",
                                "beneficial_owner": person_name,
                                "source_corporation": entity_name,
                                "target_corporation": corp_name,
                                "control_description": record.get("control_description"),
                            },
                            source_relationship_type="BENEFICIAL_OWNER_OF",
                        )

        except Exception as e:
            logger.warning(
                f"BeneficialOwnershipExtractor: Failed to extract leads from {entity_name}: {e}"
            )
            return
