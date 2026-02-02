"""Funding lead extractor.

Extracts funding-related leads from IRS 990, CRA T3010,
and Elections Canada data.
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


class FundingLeadExtractor(BaseLeadExtractor):
    """Extracts funding-related leads.

    Discovers:
    - Grant recipients (from IRS 990 Schedule I)
    - Major donors (from IRS 990 Schedule B, CRA T3010)
    - Third-party election contributors
    - LittleSis funding relationships
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["irs990", "cra", "elections_canada", "littlesis"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract funding leads from an organization.

        Queries Neo4j for existing FUNDED_BY relationships and
        PostgreSQL for funding records.
        """
        # Extract from Neo4j relationships
        async for lead in self._extract_from_neo4j(
            entity_id, entity_name, entity_data, config
        ):
            if self.should_follow_lead(lead, config):
                yield lead

        # Extract from IRS 990 (if US jurisdiction or nonprofit)
        jurisdiction = entity_data.get("jurisdiction", "").upper()
        org_type = entity_data.get("org_type", "").lower()

        if jurisdiction in ["US", ""] or jurisdiction.startswith("US-") or org_type == "nonprofit":
            async for lead in self._extract_from_irs990(
                entity_id, entity_name, entity_data, config
            ):
                if self.should_follow_lead(lead, config):
                    yield lead

        # Extract from CRA (if Canadian jurisdiction)
        if jurisdiction in ["CA", ""] or jurisdiction.startswith("CA-"):
            async for lead in self._extract_from_cra(
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
        """Extract leads from existing Neo4j FUNDED_BY relationships."""
        try:
            async with get_neo4j_session() as session:
                # Find entities that fund this entity
                funders_query = """
                    MATCH (funder)-[r:FUNDED_BY]->(recipient)
                    WHERE recipient.id = $entity_id OR recipient.name = $entity_name
                    RETURN funder.id as funder_id,
                           funder.name as funder_name,
                           funder.entity_type as funder_type,
                           funder.ein as ein,
                           funder.bn as bn,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.confidence as confidence
                """

                result = await session.run(
                    funders_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                async for record in result:
                    funder_name = record["funder_name"]
                    if not funder_name:
                        continue

                    # Determine identifier type
                    if record["ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["ein"]
                    elif record["bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = funder_name

                    # Calculate priority based on funding amount
                    amount = record.get("amount")
                    if amount and amount > 100000:
                        priority = 1  # Major funder
                    elif amount and amount > 10000:
                        priority = 2  # Significant funder
                    else:
                        priority = 3  # Minor funder

                    priority = self.apply_priority_boost(
                        priority, LeadType.FUNDING, config
                    )

                    confidence = record.get("confidence", 0.85)

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "funder_of",
                            "funded_entity": entity_name,
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                        },
                        source_relationship_type="FUNDED_BY",
                    )

                # Find entities funded by this entity
                recipients_query = """
                    MATCH (source)-[r:FUNDED_BY]->(recipient)
                    WHERE source.id = $entity_id OR source.name = $entity_name
                    RETURN recipient.id as recipient_id,
                           recipient.name as recipient_name,
                           recipient.entity_type as recipient_type,
                           recipient.ein as ein,
                           recipient.bn as bn,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.grant_purpose as grant_purpose,
                           r.confidence as confidence
                """

                result = await session.run(
                    recipients_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                )

                async for record in result:
                    recipient_name = record["recipient_name"]
                    if not recipient_name:
                        continue

                    # Determine identifier type
                    if record["ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["ein"]
                    elif record["bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = recipient_name

                    amount = record.get("amount")
                    priority = self.apply_priority_boost(2, LeadType.FUNDING, config)
                    confidence = record.get("confidence", 0.85)

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=confidence,
                        context={
                            "relationship": "funded_by",
                            "funder_entity": entity_name,
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                            "grant_purpose": record.get("grant_purpose"),
                        },
                        source_relationship_type="FUNDED_BY",
                    )

        except Exception as e:
            logger.warning(f"Neo4j funding extraction failed for {entity_name}: {e}")

    async def _extract_from_irs990(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from IRS 990 filings.

        Looks for Schedule I (grants made) and Schedule B (donors).
        """
        try:
            async with get_neo4j_session() as session:
                # Query for grant recipients (entities this org funded)
                grants_query = """
                    MATCH (funder)-[r:FUNDED_BY]->(recipient)
                    WHERE (funder.id = $entity_id OR funder.name = $entity_name)
                    AND r.source IN ['irs990', 'irs_990']
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN recipient.id as recipient_id,
                           recipient.name as recipient_name,
                           recipient.ein as recipient_ein,
                           recipient.bn as recipient_bn,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.grant_purpose as grant_purpose,
                           r.confidence as confidence
                    ORDER BY r.amount DESC
                """

                result = await session.run(
                    grants_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    recipient_name = record["recipient_name"]
                    if not recipient_name:
                        continue

                    if record["recipient_ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["recipient_ein"]
                    elif record["recipient_bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["recipient_bn"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = recipient_name

                    amount = record.get("amount")
                    if amount:
                        amount = float(amount)

                    # Check funding amount threshold
                    if config.min_funding_amount and amount:
                        if amount < config.min_funding_amount:
                            continue

                    priority = 1 if amount and amount > 100000 else 2
                    priority = self.apply_priority_boost(
                        priority, LeadType.FUNDING, config
                    )

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.9,
                        context={
                            "source": "irs990_schedule_i",
                            "funder_entity": entity_name,
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                            "grant_purpose": record.get("grant_purpose"),
                        },
                        source_relationship_type="FUNDED_BY",
                    )

                # Query for funders of this entity
                funders_query = """
                    MATCH (funder)-[r:FUNDED_BY]->(recipient)
                    WHERE (recipient.id = $entity_id OR recipient.name = $entity_name)
                    AND r.source IN ['irs990', 'irs_990']
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN funder.id as funder_id,
                           funder.name as funder_name,
                           funder.ein as funder_ein,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.confidence as confidence
                    ORDER BY r.amount DESC
                """

                result = await session.run(
                    funders_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    funder_name = record["funder_name"]
                    if not funder_name:
                        continue

                    if record["funder_ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["funder_ein"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = funder_name

                    amount = record.get("amount")
                    if amount:
                        amount = float(amount)
                    priority = 1 if amount and amount > 100000 else 2
                    priority = self.apply_priority_boost(
                        priority, LeadType.FUNDING, config
                    )

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.9,
                        context={
                            "source": "irs990",
                            "funded_entity": entity_name,
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                        },
                        source_relationship_type="FUNDED_BY",
                    )

        except Exception as e:
            logger.warning(f"IRS 990 funding extraction failed for {entity_name}: {e}")

    async def _extract_from_cra(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract leads from CRA T3010 filings.

        Looks for qualified donees and major donors.
        """
        try:
            async with get_neo4j_session() as session:
                # Query for entities that fund this entity (via CRA)
                funders_query = """
                    MATCH (funder)-[r:FUNDED_BY]->(recipient)
                    WHERE (recipient.id = $entity_id OR recipient.name = $entity_name)
                    AND r.source IN ['cra', 'cra_t3010']
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN funder.id as related_id,
                           funder.name as related_name,
                           funder.bn as related_bn,
                           funder.ein as related_ein,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.confidence as confidence,
                           'funder' as relationship_direction
                """

                result = await session.run(
                    funders_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    related_name = record["related_name"]
                    if not related_name:
                        continue

                    if record["related_bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["related_bn"]
                    elif record["related_ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["related_ein"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = related_name

                    amount = record.get("amount")
                    if amount:
                        amount = float(amount)
                    priority = self.apply_priority_boost(2, LeadType.FUNDING, config)

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.85,
                        context={
                            "source": "cra_t3010",
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                            "funded_entity": entity_name,
                        },
                        source_relationship_type="FUNDED_BY",
                    )

                # Query for entities funded by this entity (via CRA)
                recipients_query = """
                    MATCH (funder)-[r:FUNDED_BY]->(recipient)
                    WHERE (funder.id = $entity_id OR funder.name = $entity_name)
                    AND r.source IN ['cra', 'cra_t3010']
                    AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                    RETURN recipient.id as related_id,
                           recipient.name as related_name,
                           recipient.bn as related_bn,
                           recipient.ein as related_ein,
                           r.amount as amount,
                           r.fiscal_year as fiscal_year,
                           r.confidence as confidence,
                           'recipient' as relationship_direction
                """

                result = await session.run(
                    recipients_query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    related_name = record["related_name"]
                    if not related_name:
                        continue

                    if record["related_bn"]:
                        id_type = IdentifierType.BN
                        identifier = record["related_bn"]
                    elif record["related_ein"]:
                        id_type = IdentifierType.EIN
                        identifier = record["related_ein"]
                    else:
                        id_type = IdentifierType.NAME
                        identifier = related_name

                    amount = record.get("amount")
                    if amount:
                        amount = float(amount)
                    priority = self.apply_priority_boost(2, LeadType.FUNDING, config)

                    yield self.create_lead(
                        lead_type=LeadType.FUNDING,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.85,
                        context={
                            "source": "cra_t3010",
                            "amount": amount,
                            "fiscal_year": record.get("fiscal_year"),
                            "funder_entity": entity_name,
                        },
                        source_relationship_type="FUNDED_BY",
                    )

        except Exception as e:
            logger.warning(f"CRA funding extraction failed for {entity_name}: {e}")


class CrossBorderFundingExtractor(BaseLeadExtractor):
    """Extracts cross-border funding leads.

    Specifically handles US→CA and CA→US funding flows
    from IRS 990 Schedule I foreign grants and CRA T3010
    gifts to qualified donees outside Canada.
    """

    @property
    def supported_entity_types(self) -> list[str]:
        return ["ORGANIZATION"]

    @property
    def lead_sources(self) -> list[str]:
        return ["irs990", "cra"]

    async def extract_leads(
        self,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> AsyncIterator[Lead]:
        """Extract cross-border funding leads."""
        # Only process if cross-border is enabled
        if "US" not in config.jurisdictions or "CA" not in config.jurisdictions:
            return

        jurisdiction = entity_data.get("jurisdiction", "").upper()

        try:
            async with get_neo4j_session() as session:
                # Find foreign grant recipients
                if jurisdiction in ["US", ""] or jurisdiction.startswith("US-"):
                    # US entity - look for Canadian recipients
                    query = """
                        MATCH (funder)-[r:FUNDED_BY]->(recipient)
                        WHERE (funder.id = $entity_id OR funder.name = $entity_name)
                        AND (recipient.jurisdiction STARTS WITH 'CA' OR r.address_country = 'CA')
                        AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                        RETURN recipient.id as related_id,
                               recipient.name as related_name,
                               recipient.bn as bn,
                               recipient.ein as ein,
                               recipient.jurisdiction as target_jurisdiction,
                               r.amount as amount,
                               r.confidence as confidence
                    """
                else:
                    # Canadian entity - look for US funders/recipients
                    query = """
                        MATCH (a)-[r:FUNDED_BY]->(b)
                        WHERE (a.id = $entity_id OR a.name = $entity_name OR b.id = $entity_id OR b.name = $entity_name)
                        AND (
                            (a.jurisdiction STARTS WITH 'US' AND (b.id = $entity_id OR b.name = $entity_name))
                            OR (b.jurisdiction STARTS WITH 'US' AND (a.id = $entity_id OR a.name = $entity_name))
                        )
                        AND (r.confidence IS NULL OR r.confidence >= $min_confidence)
                        WITH a, b, r,
                             CASE WHEN a.id = $entity_id OR a.name = $entity_name THEN b ELSE a END as related
                        RETURN related.id as related_id,
                               related.name as related_name,
                               related.bn as bn,
                               related.ein as ein,
                               related.jurisdiction as target_jurisdiction,
                               r.amount as amount,
                               r.confidence as confidence
                    """

                result = await session.run(
                    query,
                    entity_id=str(entity_id),
                    entity_name=entity_name,
                    min_confidence=config.min_confidence,
                )

                async for record in result:
                    name = record["related_name"]
                    if not name:
                        continue

                    bn = record.get("bn")
                    ein = record.get("ein")

                    if bn:
                        id_type = IdentifierType.BN
                        identifier = bn
                    elif ein:
                        id_type = IdentifierType.EIN
                        identifier = ein
                    else:
                        id_type = IdentifierType.NAME
                        identifier = name

                    amount = record.get("amount")
                    if amount:
                        amount = float(amount)
                    priority = 1  # Cross-border is always high priority

                    target_jurisdiction = record.get("target_jurisdiction", "")

                    yield self.create_lead(
                        lead_type=LeadType.CROSS_BORDER,
                        target_identifier=identifier,
                        identifier_type=id_type,
                        priority=priority,
                        confidence=record.get("confidence") or 0.8,
                        context={
                            "source_jurisdiction": jurisdiction,
                            "target_jurisdiction": target_jurisdiction,
                            "amount": amount,
                            "cross_border_flow": f"{jurisdiction}→{target_jurisdiction}",
                        },
                        source_relationship_type="FUNDED_BY",
                    )

        except Exception as e:
            logger.warning(f"Cross-border extraction failed for {entity_name}: {e}")
