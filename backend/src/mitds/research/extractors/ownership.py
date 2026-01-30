"""Ownership lead extractor.

Extracts ownership-related leads from SEC EDGAR, SEDAR,
and OpenCorporates data.
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


class OwnershipLeadExtractor(BaseLeadExtractor):
    """Extracts ownership-related leads.

    Discovers:
    - Beneficial owners (from SEC 13D/13G filings)
    - Parent companies (from OpenCorporates)
    - Subsidiaries
    - SEDAR early warning reports
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["sec_edgar", "sedar", "opencorporates"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract ownership leads from an organization.

        Queries Neo4j for existing OWNS relationships and
        PostgreSQL for ownership filings that haven't been
        fully processed.
        """
        # Extract from Neo4j relationships
        async for lead in self._extract_from_neo4j(
            entity_id, entity_name, entity_data, config
        ):
            if self.should_follow_lead(lead, config):
                yield lead

        # Extract from SEC EDGAR filings (if US jurisdiction)
        jurisdiction = entity_data.get("jurisdiction", "").upper()
        if jurisdiction in ["US", ""] or jurisdiction.startswith("US-"):
            async for lead in self._extract_from_edgar(
                entity_id, entity_name, entity_data, config
            ):
                if self.should_follow_lead(lead, config):
                    yield lead

        # Extract from SEDAR (if Canadian jurisdiction)
        if jurisdiction in ["CA", ""] or jurisdiction.startswith("CA-"):
            async for lead in self._extract_from_sedar(
                entity_id, entity_name, entity_data, config
            ):
                if self.should_follow_lead(lead, config):
                    yield lead

    async def _extract_from_neo4j(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from existing Neo4j OWNS relationships."""
        try:
            async with get_neo4j_session() as session:
                # Find entities that own this entity
                owners_query = """
                    MATCH (owner)-[r:OWNS]->(target)
                    WHERE target.id = $entity_id OR target.name = $entity_name
                    RETURN owner.id as owner_id,
                           owner.name as owner_name,
                           owner.entity_type as owner_type,
                           owner.sec_cik as cik,
                           owner.sedar_profile as sedar_profile,
                           owner.bn as bn,
                           r.ownership_percentage as percentage,
                           r.confidence as confidence
                """

                result = await session.run(
                    owners_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                async for record in result:
                    owner_name = record["owner_name"]
                    if not owner_name:
                        continue

                    # Determine identifier type
                    if record["cik"]:
                        id_type = IdentifierType.CIK
                        identifier = record["cik"]
                    elif record["sedar_profile"]:
                        id_type = IdentifierType.SEDAR_PROFILE
                        identifier = record["sedar_profile"]
                    elif record["bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = owner_name

                    # Calculate priority based on ownership percentage
                    percentage = record.get("percentage")
                    if percentage and percentage > 50:
                        priority = 1  # Majority owner
                    elif percentage and percentage > 10:
                        priority = 2  # Significant owner
                    else:
                        priority = 3  # Minor owner

                    priority = self.apply_priority_boost(
                        priority, LeadType.OWNERSHIP, config
                    )

                    confidence = record.get("confidence", 0.85)

                    yield self.create_lead(
                        lead_type=LeadType.OWNERSHIP,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "owner_of",
                            "owned_entity": entity_name,
                            "ownership_percentage": percentage,
                        },
                        source_relationship_type="OWNS",
                    )

                # Find entities owned by this entity
                owned_query = """
                    MATCH (source)-[r:OWNS]->(owned)
                    WHERE source.id = $entity_id OR source.name = $entity_name
                    RETURN owned.id as owned_id,
                           owned.name as owned_name,
                           owned.entity_type as owned_type,
                           owned.sec_cik as cik,
                           owned.sedar_profile as sedar_profile,
                           owned.bn as bn,
                           r.ownership_percentage as percentage,
                           r.confidence as confidence
                """

                result = await session.run(
                    owned_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                async for record in result:
                    owned_name = record["owned_name"]
                    if not owned_name:
                        continue

                    # Determine identifier type
                    if record["cik"]:
                        id_type = IdentifierType.CIK
                        identifier = record["cik"]
                    elif record["sedar_profile"]:
                        id_type = IdentifierType.SEDAR_PROFILE
                        identifier = record["sedar_profile"]
                    elif record["bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = owned_name

                    priority = self.apply_priority_boost(2, LeadType.OWNERSHIP, config)
                    confidence = record.get("confidence", 0.85)

                    yield self.create_lead(
                        lead_type=LeadType.OWNERSHIP,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "owned_by",
                            "owner_entity": entity_name,
                            "ownership_percentage": record.get("percentage"),
                        },
                        source_relationship_type="OWNS",
                    )

        except Exception as e:
            logger.warning(f"Neo4j ownership extraction failed for {entity_name}: {e}")

    async def _extract_from_edgar(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from SEC EDGAR filings.

        Looks for 13D/13G beneficial ownership filings
        that reference this company.
        """
        cik = entity_data.get("sec_cik") or entity_data.get("cik")
        if not cik:
            return

        try:
            async with get_neo4j_session() as session:
                # Query Neo4j for OWNS relationships from SEC EDGAR
                query = """
                    MATCH (owner)-[r:OWNS]->(target)
                    WHERE (target.id = $entity_id OR target.name = $entity_name)
                    AND r.source = 'sec_edgar'
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN owner.id as owner_id,
                           owner.name as owner_name,
                           owner.sec_cik as cik,
                           r.ownership_percentage as percentage,
                           r.confidence as confidence
                """

                result = await session.run(
                    query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    owner_name = record["owner_name"]
                    if not owner_name:
                        continue

                    filer_cik = record["cik"]
                    if filer_cik:
                        id_type = IdentifierType.CIK
                        identifier = filer_cik
                    else:
                        id_type = IdentifierType.NAME
                        identifier = owner_name

                    percentage = record.get("percentage")
                    if percentage:
                        percentage = float(percentage)
                    priority = 1 if percentage and percentage > 5 else 2
                    priority = self.apply_priority_boost(
                        priority, LeadType.OWNERSHIP, config
                    )

                    yield self.create_lead(
                        lead_type=LeadType.OWNERSHIP,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.85,
                        context={
                            "source": "sec_edgar",
                            "filing_type": "13D/13G",
                            "owned_entity": entity_name,
                            "ownership_percentage": percentage,
                        },
                        source_relationship_type="OWNS",
                    )

        except Exception as e:
            logger.warning(f"EDGAR ownership extraction failed for {entity_name}: {e}")

    async def _extract_from_sedar(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from SEDAR filings.

        Looks for early warning reports and insider filings.
        """
        try:
            async with get_neo4j_session() as session:
                # Query Neo4j for OWNS relationships from SEDAR
                query = """
                    MATCH (owner)-[r:OWNS]->(target)
                    WHERE (target.id = $entity_id OR target.name = $entity_name)
                    AND r.source = 'sedar'
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN owner.id as owner_id,
                           owner.name as owner_name,
                           owner.sedar_profile as sedar_profile,
                           owner.bn as bn,
                           r.ownership_percentage as percentage,
                           r.confidence as confidence
                """

                result = await session.run(
                    query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    owner_name = record["owner_name"]
                    if not owner_name:
                        continue

                    if record["sedar_profile"]:
                        id_type = IdentifierType.SEDAR_PROFILE
                        identifier = record["sedar_profile"]
                    elif record["bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = owner_name

                    percentage = record.get("percentage")
                    if percentage:
                        percentage = float(percentage)
                    priority = 1 if percentage and percentage > 10 else 2
                    priority = self.apply_priority_boost(
                        priority, LeadType.OWNERSHIP, config
                    )

                    yield self.create_lead(
                        lead_type=LeadType.OWNERSHIP,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.85,
                        context={
                            "source": "sedar",
                            "filing_type": "early_warning",
                            "owned_entity": entity_name,
                            "ownership_percentage": percentage,
                        },
                        source_relationship_type="OWNS",
                    )

        except Exception as e:
            logger.warning(f"SEDAR ownership extraction failed for {entity_name}: {e}")
