"""Shared address lead extractor.

Extracts leads for organizations sharing the same registered office address
by querying the entities table in PostgreSQL.
"""

from typing import Any, AsyncIterator
from uuid import UUID

from ...db import get_db_session
from ...logging import get_context_logger
from ..models import (
    IdentifierType,
    Lead,
    LeadType,
    ResearchSessionConfig,
)
from .base import BaseLeadExtractor

logger = get_context_logger(__name__)


class SharedAddressExtractor(BaseLeadExtractor):
    """Extracts shared address leads.

    Discovers organizations sharing the same registered office address
    by querying the entities table in PostgreSQL.
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["postgresql"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract shared address leads from an organization.

        Queries PostgreSQL entities table for organizations sharing the same
        registered office address (city + postal_code + street).

        Args:
            entity_id: Entity UUID (organization)
            entity_type: Entity type (should be ORGANIZATION)
            entity_name: Entity name
            entity_data: Entity metadata/properties
            config: Session configuration for filtering

        Yields:
            Lead objects for co-located entities
        """
        try:
            async with get_db_session() as db:
                from sqlalchemy import text

                # Get the current entity's address from metadata
                entity_query = text("""
                    SELECT metadata->>'city' AS city,
                           metadata->>'postal_code' AS postal_code,
                           metadata->>'street' AS street,
                           metadata->>'registered_address' AS registered_address
                    FROM entities
                    WHERE id = :entity_id
                      AND entity_type = 'organization'
                """)

                result = await db.execute(entity_query, {"entity_id": str(entity_id)})
                entity_row = result.fetchone()

                if not entity_row:
                    return

                city = entity_row[0]
                postal_code = entity_row[1]
                street = entity_row[2]
                registered_address = entity_row[3]

                # Need at least city and postal_code to find matches
                if not city or not postal_code:
                    return

                # Query for other organizations sharing the same address
                # Match on city + postal_code (and street if available)
                shared_query = text("""
                    SELECT id, name, metadata
                    FROM entities
                    WHERE entity_type = 'organization'
                      AND id <> :entity_id
                      AND metadata->>'city' = :city
                      AND metadata->>'postal_code' = :postal_code
                """)

                params = {
                    "entity_id": str(entity_id),
                    "city": city,
                    "postal_code": postal_code,
                }

                # If street is available, also match on street
                if street:
                    shared_query = text("""
                        SELECT id, name, metadata
                        FROM entities
                        WHERE entity_type = 'organization'
                          AND id <> :entity_id
                          AND metadata->>'city' = :city
                          AND metadata->>'postal_code' = :postal_code
                          AND metadata->>'street' = :street
                    """)
                    params["street"] = street

                result = await db.execute(shared_query, params)

                for row in result.fetchall():
                    co_located_name = row[1]
                    if not co_located_name:
                        continue

                    # Use NAME identifier type for co-located entity identification
                    identifier = co_located_name
                    id_type = IdentifierType.NAME

                    # Priority based on exact address match
                    # If street matches, higher priority
                    priority = 2 if street else 3
                    confidence = 0.9 if street else 0.8

                    yield self.create_lead(
                        lead_type=LeadType.INFRASTRUCTURE,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "shared_address",
                            "source_entity": entity_name,
                            "shared_city": city,
                            "shared_postal_code": postal_code,
                            "shared_street": street if street else None,
                        },
                        source_relationship_type="SHARED_ADDRESS",
                    )

        except Exception as e:
            logger.warning(
                f"SharedAddressExtractor: Failed to extract leads from {entity_name}: {e}"
            )
            return
