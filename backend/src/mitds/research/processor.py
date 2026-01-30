"""Lead processor for MITDS research.

Orchestrates the processing of leads: resolving targets to entities,
triggering ingestion, extracting new leads, and updating the queue.
"""

import asyncio
from typing import Any
from uuid import UUID

from sqlalchemy import text

from ..db import get_db_session
from ..logging import get_context_logger
from ..resolution.matcher import HybridMatcher
from .extractors.base import BaseLeadExtractor
from .extractors.funding import CrossBorderFundingExtractor, FundingLeadExtractor
from .extractors.ownership import OwnershipLeadExtractor
from .models import (
    IdentifierType,
    Lead,
    LeadResult,
    LeadType,
    QueuedLead,
    ResearchSession,
    ResearchSessionConfig,
    SessionStats,
    SessionStatus,
    SingleIngestionResult,
)
from .queue import LeadQueueManager, get_queue_manager
from .session import ResearchSessionManager, get_session_manager

logger = get_context_logger(__name__)


class LeadProcessor:
    """Processes leads from the queue.

    Coordinates:
    - Entity resolution (matching leads to existing entities)
    - Targeted ingestion (fetching new entity data)
    - Lead extraction (discovering new leads from entities)
    - Queue management (enqueueing new leads, updating status)
    """

    def __init__(
        self,
        session_manager: ResearchSessionManager | None = None,
        queue_manager: LeadQueueManager | None = None,
        extractors: list[BaseLeadExtractor] | None = None,
    ):
        """Initialize the processor.

        Args:
            session_manager: Session manager (uses singleton if None)
            queue_manager: Queue manager (uses singleton if None)
            extractors: Lead extractors (uses defaults if None)
        """
        self.session_manager = session_manager or get_session_manager()
        self.queue_manager = queue_manager or get_queue_manager()

        # Initialize extractors
        if extractors is None:
            self.extractors = [
                OwnershipLeadExtractor(),
                FundingLeadExtractor(),
                CrossBorderFundingExtractor(),
            ]
        else:
            self.extractors = extractors

        # Matcher for entity resolution
        self.matcher = HybridMatcher()

    async def process_session(
        self,
        session_id: UUID,
        max_iterations: int | None = None,
    ) -> SessionStats:
        """Process a research session until completion or pause.

        Args:
            session_id: Session UUID
            max_iterations: Maximum processing iterations (None = until done)

        Returns:
            Final session statistics
        """
        session = await self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        if session.status not in [SessionStatus.RUNNING, SessionStatus.INITIALIZING]:
            raise ValueError(f"Session {session_id} is not in a runnable state")

        # Start the session if initializing
        if session.status == SessionStatus.INITIALIZING:
            # Create initial lead from entry point
            await self._create_entry_point_lead(session)
            session = await self.session_manager.start_session(session_id)

        iteration = 0
        while True:
            # Check if we should stop
            session = await self.session_manager.get_session(session_id)
            if not session or session.status != SessionStatus.RUNNING:
                break

            if max_iterations and iteration >= max_iterations:
                break

            # Check limits
            entity_count = await self.session_manager.get_session_entity_count(session_id)
            relationship_count = await self.session_manager.get_session_relationship_count(session_id)

            if entity_count >= session.config.max_entities:
                logger.info(f"Session {session_id}: Entity limit reached ({entity_count})")
                await self.session_manager.pause_session(session_id)
                break

            if relationship_count >= session.config.max_relationships:
                logger.info(f"Session {session_id}: Relationship limit reached ({relationship_count})")
                await self.session_manager.pause_session(session_id)
                break

            # Get next batch of leads
            leads = await self.queue_manager.dequeue(session_id, batch_size=5)
            if not leads:
                # No more leads - session complete
                await self.session_manager.complete_session(session_id)
                break

            # Process leads
            for lead in leads:
                try:
                    result = await self.process_lead(lead, session)

                    if result.success:
                        await self.queue_manager.complete_lead(
                            lead.id,
                            result={
                                "entity_id": str(result.entity_id) if result.entity_id else None,
                                "is_new": result.is_new_entity,
                                "relationships_created": result.relationships_created,
                                "new_leads_generated": result.new_leads_generated,
                            },
                        )
                    else:
                        await self.queue_manager.fail_lead(
                            lead.id,
                            error_message=result.error_message or "Unknown error",
                        )

                except Exception as e:
                    logger.error(f"Failed to process lead {lead.id}: {e}")
                    await self.queue_manager.fail_lead(lead.id, str(e))

            # Update session stats
            await self._update_session_stats(session_id)

            iteration += 1

            # Small delay to prevent overwhelming APIs
            await asyncio.sleep(0.1)

        # Get final stats
        session = await self.session_manager.get_session(session_id)
        return session.stats if session else SessionStats()

    async def process_lead(
        self,
        lead: QueuedLead,
        session: ResearchSession,
    ) -> LeadResult:
        """Process a single lead.

        1. Resolve target to existing entity or ingest new
        2. Actively ingest data to discover relationships
        3. Extract new leads from discovered relationships
        4. Enqueue new leads with depth+1

        Args:
            lead: Lead to process
            session: Research session

        Returns:
            Processing result
        """
        logger.debug(
            f"Processing lead: {lead.target_identifier} ({lead.lead_type})"
        )

        # Check depth limit
        if lead.depth >= session.config.max_depth:
            return LeadResult(
                lead_id=lead.id,
                success=False,
                error_message="Max depth reached",
            )

        # Step 1: Resolve or ingest the target entity
        entity_id, entity_type, entity_name, entity_data, is_new = await self._resolve_or_ingest(
            lead, session
        )

        if not entity_id:
            return LeadResult(
                lead_id=lead.id,
                success=False,
                error_message="Could not resolve or ingest entity",
            )

        # Step 2: Add entity to session
        await self.session_manager.add_session_entity(
            session.id,
            entity_id,
            depth=lead.depth,
            relevance_score=lead.confidence,
            lead_id=lead.id,
        )

        # Step 3: ACTIVELY INGEST DATA to discover and create relationships
        # This is the key step - we call ingestors even for existing entities
        # to fetch data from SEC EDGAR, SEDAR, etc. which creates relationships
        relationships_created = await self._ingest_for_relationships(
            entity_id=entity_id,
            entity_name=entity_name,
            entity_data=entity_data,
            config=session.config,
        )

        # Step 4: Extract new leads from this entity's relationships
        new_leads_count = 0
        if lead.depth + 1 < session.config.max_depth:
            new_leads_count = await self._extract_and_enqueue_leads(
                session_id=session.id,
                entity_id=entity_id,
                entity_type=entity_type,
                entity_name=entity_name,
                entity_data=entity_data,
                config=session.config,
                depth=lead.depth + 1,
            )

        return LeadResult(
            lead_id=lead.id,
            success=True,
            entity_id=entity_id,
            is_new_entity=is_new,
            relationships_created=relationships_created,
            new_leads_generated=new_leads_count,
        )

    async def _create_entry_point_lead(self, session: ResearchSession) -> None:
        """Create the initial lead from the session entry point."""
        # Map entry point type to lead type and identifier type
        entry_type_map = {
            "meta_ads": (LeadType.SPONSORSHIP, IdentifierType.NAME),
            "company": (LeadType.OWNERSHIP, IdentifierType.NAME),
            "ein": (LeadType.FUNDING, IdentifierType.EIN),
            "bn": (LeadType.FUNDING, IdentifierType.BN),
            "nonprofit": (LeadType.FUNDING, IdentifierType.NAME),
            "entity_id": (LeadType.OWNERSHIP, IdentifierType.ENTITY_ID),
        }

        entry_type = session.entry_point_type
        if isinstance(entry_type, str):
            lead_type, id_type = entry_type_map.get(entry_type, (LeadType.OWNERSHIP, IdentifierType.NAME))
        else:
            lead_type, id_type = entry_type_map.get(entry_type.value, (LeadType.OWNERSHIP, IdentifierType.NAME))

        initial_lead = Lead(
            lead_type=lead_type,
            target_identifier=session.entry_point_value,
            target_identifier_type=id_type,
            priority=1,  # Entry point is always highest priority
            confidence=1.0,
            context={"is_entry_point": True},
        )

        await self.queue_manager.enqueue_single(
            session.id,
            initial_lead,
            source_entity_id=None,
            depth=0,
        )

    async def _resolve_or_ingest(
        self,
        lead: QueuedLead,
        session: ResearchSession,
    ) -> tuple[UUID | None, str | None, str | None, dict[str, Any], bool]:
        """Resolve lead to entity, ingesting if needed.

        Returns:
            Tuple of (entity_id, entity_type, entity_name, entity_data, is_new)
            entity_data includes both metadata and external_ids for use in ingestion
        """
        # First, try to find existing entity
        entity = await self._find_existing_entity(lead)

        if entity:
            # Merge metadata with external_ids and jurisdiction for ingestion
            entity_data = {
                **(entity.get("metadata") or {}),
                "external_ids": entity.get("external_ids", {}),
                "jurisdiction": entity.get("jurisdiction", ""),
            }
            return (
                entity["id"],
                entity["entity_type"],
                entity["name"],
                entity_data,
                False,
            )

        # Not found - try to ingest from appropriate source
        result = await self._ingest_entity(lead, session.config)

        if result and result.entity_id:
            # Fetch the newly created entity data
            entity = await self._get_entity(result.entity_id)
            if entity:
                entity_data = {
                    **(entity.get("metadata") or {}),
                    "external_ids": entity.get("external_ids", {}),
                    "jurisdiction": entity.get("jurisdiction", ""),
                }
                return (
                    entity["id"],
                    entity["entity_type"],
                    entity["name"],
                    entity_data,
                    True,
                )

        return None, None, None, {}, False

    async def _find_existing_entity(
        self,
        lead: QueuedLead,
    ) -> dict[str, Any] | None:
        """Try to find an existing entity matching the lead."""
        async with get_db_session() as db:
            id_type = lead.target_identifier_type
            identifier = lead.target_identifier

            if id_type == IdentifierType.ENTITY_ID:
                # Direct entity ID lookup
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE id = :identifier
                """)
                params = {"identifier": identifier}

            elif id_type == IdentifierType.EIN:
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE external_ids->>'ein' = :identifier
                """)
                params = {"identifier": identifier}

            elif id_type == IdentifierType.BN:
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE external_ids->>'bn' = :identifier
                """)
                params = {"identifier": identifier}

            elif id_type == IdentifierType.CIK:
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE external_ids->>'sec_cik' = :identifier
                    OR external_ids->>'cik' = :identifier
                """)
                params = {"identifier": identifier}

            elif id_type == IdentifierType.SEDAR_PROFILE:
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE external_ids->>'sedar_profile' = :identifier
                """)
                params = {"identifier": identifier}

            elif id_type == IdentifierType.META_PAGE_ID:
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE external_ids->>'meta_page_id' = :identifier
                """)
                params = {"identifier": identifier}

            else:
                # Name-based lookup with fuzzy matching
                # Normalize the search term (remove trailing punctuation)
                normalized = identifier.strip().rstrip('.,;:')

                # Try exact match first, then fuzzy match
                query = text("""
                    SELECT id, name, entity_type, external_ids, metadata
                    FROM entities
                    WHERE LOWER(name) = LOWER(:identifier)
                       OR LOWER(TRIM(TRAILING '.' FROM TRIM(name))) = LOWER(:normalized)
                       OR LOWER(name) LIKE LOWER(:like_pattern)
                    ORDER BY
                        CASE WHEN LOWER(name) = LOWER(:identifier) THEN 0 ELSE 1 END,
                        LENGTH(name)
                    LIMIT 1
                """)
                params = {
                    "identifier": identifier,
                    "normalized": normalized,
                    "like_pattern": normalized + "%",
                }

            result = await db.execute(query, params)
            row = result.fetchone()

            if row:
                import json
                return {
                    "id": row.id if not isinstance(row.id, str) else UUID(row.id),
                    "name": row.name,
                    "entity_type": row.entity_type,
                    "external_ids": json.loads(row.external_ids) if isinstance(row.external_ids, str) else (row.external_ids or {}),
                    "metadata": json.loads(row.metadata) if isinstance(row.metadata, str) else (row.metadata or {}),
                }

        return None

    async def _get_entity(self, entity_id: UUID) -> dict[str, Any] | None:
        """Get entity by ID."""
        async with get_db_session() as db:
            query = text("""
                SELECT id, name, entity_type, external_ids, metadata, jurisdiction
                FROM entities
                WHERE id = :entity_id
            """)

            result = await db.execute(query, {"entity_id": str(entity_id)})
            row = result.fetchone()

            if row:
                import json
                return {
                    "id": row.id if not isinstance(row.id, str) else UUID(row.id),
                    "name": row.name,
                    "entity_type": row.entity_type,
                    "external_ids": json.loads(row.external_ids) if isinstance(row.external_ids, str) else (row.external_ids or {}),
                    "metadata": json.loads(row.metadata) if isinstance(row.metadata, str) else (row.metadata or {}),
                    "jurisdiction": row.jurisdiction,
                }

        return None

    async def _ingest_entity(
        self,
        lead: QueuedLead,
        _config: ResearchSessionConfig,  # noqa: ARG002
    ) -> SingleIngestionResult | None:
        """Ingest a new entity based on the lead.

        Calls the appropriate ingester's ingest_single() method based on
        the lead type and identifier type.
        """
        from ..ingestion.edgar import SECEDGARIngester
        from ..ingestion.meta_ads import MetaAdIngester
        from ..ingestion.sedar import SEDARIngester

        id_type = lead.target_identifier_type
        identifier = lead.target_identifier
        lead_type = lead.lead_type

        # Skip if we can't determine what to ingest
        if id_type == IdentifierType.NAME and len(identifier) < 3:
            return None

        logger.info(
            f"Attempting ingestion: {identifier} ({id_type}) via {lead_type}"
        )

        result = None

        # Determine which ingester(s) to use based on lead type and identifier type
        if lead_type == LeadType.SPONSORSHIP:
            # Meta Ads for sponsorship leads
            try:
                ingester = MetaAdIngester()
                meta_id_type = "name"
                if id_type == IdentifierType.META_PAGE_ID:
                    meta_id_type = "meta_page_id"
                result = await ingester.ingest_single(identifier, meta_id_type)
                await ingester.close()
            except Exception as e:
                logger.warning(f"Meta Ads ingestion failed: {e}")

        elif lead_type == LeadType.OWNERSHIP:
            # Try SEC EDGAR first (for US companies), then SEDAR (for Canadian)
            if id_type == IdentifierType.CIK:
                try:
                    ingester = SECEDGARIngester()
                    result = await ingester.ingest_single(identifier, "cik")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEC EDGAR ingestion failed: {e}")

            elif id_type == IdentifierType.SEDAR_PROFILE:
                try:
                    ingester = SEDARIngester()
                    result = await ingester.ingest_single(identifier, "sedar_profile")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEDAR ingestion failed: {e}")

            elif id_type == IdentifierType.BN:
                try:
                    ingester = SEDARIngester()
                    result = await ingester.ingest_single(identifier, "bn")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEDAR ingestion failed: {e}")

            elif id_type == IdentifierType.EIN:
                try:
                    ingester = SECEDGARIngester()
                    result = await ingester.ingest_single(identifier, "ein")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEC EDGAR ingestion failed: {e}")

            elif id_type == IdentifierType.NAME:
                # Try EDGAR first, then SEDAR
                try:
                    ingester = SECEDGARIngester()
                    result = await ingester.ingest_single(identifier, "name")
                    await ingester.close()
                except Exception as e:
                    logger.debug(f"SEC EDGAR by name failed: {e}")

                if not result or not result.entity_id:
                    try:
                        ingester = SEDARIngester()
                        result = await ingester.ingest_single(identifier, "name")
                        await ingester.close()
                    except Exception as e:
                        logger.debug(f"SEDAR by name failed: {e}")

        elif lead_type in (LeadType.FUNDING, LeadType.CROSS_BORDER):
            # For funding leads, try based on identifier type
            if id_type == IdentifierType.EIN:
                try:
                    ingester = SECEDGARIngester()
                    result = await ingester.ingest_single(identifier, "ein")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEC EDGAR by EIN failed: {e}")

            elif id_type == IdentifierType.BN:
                try:
                    ingester = SEDARIngester()
                    result = await ingester.ingest_single(identifier, "bn")
                    await ingester.close()
                except Exception as e:
                    logger.warning(f"SEDAR by BN failed: {e}")

            elif id_type == IdentifierType.NAME:
                # Try both ingesters for cross-border
                try:
                    ingester = SECEDGARIngester()
                    result = await ingester.ingest_single(identifier, "name")
                    await ingester.close()
                except Exception as e:
                    logger.debug(f"SEC EDGAR by name failed: {e}")

                if not result or not result.entity_id:
                    try:
                        ingester = SEDARIngester()
                        result = await ingester.ingest_single(identifier, "name")
                        await ingester.close()
                    except Exception as e:
                        logger.debug(f"SEDAR by name failed: {e}")

        # Convert ingestion result to research SingleIngestionResult if needed
        if result and result.entity_id:
            return SingleIngestionResult(
                entity_id=result.entity_id,
                entity_type=result.entity_type,
                entity_name=result.entity_name,
                is_new=result.is_new,
                relationships_created=[],  # Ingestion result uses int, we use list
            )

        return None

    async def _ingest_for_relationships(
        self,
        entity_id: UUID,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
    ) -> int:
        """Actively ingest data to discover and create relationships.

        This is the key "follow the leads" step. We call ingestors to fetch
        data from external sources (SEC EDGAR, SEDAR, Meta Ads, etc.) which
        will create OWNS, FUNDS, and other relationships in Neo4j.

        Args:
            entity_id: Entity UUID
            entity_name: Entity name for API queries
            entity_data: Entity metadata (may contain jurisdiction, identifiers)
            config: Session configuration

        Returns:
            Number of relationships created
        """
        from ..ingestion.edgar import SECEDGARIngester
        from ..ingestion.meta_ads import MetaAdIngester
        from ..ingestion.sedar import SEDARIngester

        logger.debug(f"Ingesting for relationships: {entity_name} (id={entity_id})")
        relationships_created = 0
        jurisdiction = entity_data.get("jurisdiction", "").upper()
        external_ids = entity_data.get("external_ids", {})

        # Get identifiers from entity data
        cik = external_ids.get("sec_cik") or external_ids.get("cik")
        sedar_profile = external_ids.get("sedar_profile")
        bn = external_ids.get("bn") or external_ids.get("business_number")

        # Determine which jurisdictions to query
        query_us = (
            jurisdiction in ["US", ""] or
            jurisdiction.startswith("US-") or
            cik is not None or
            "US" in config.jurisdictions
        )
        query_ca = (
            jurisdiction in ["CA", ""] or
            jurisdiction.startswith("CA-") or
            sedar_profile is not None or
            bn is not None or
            "CA" in config.jurisdictions
        )

        # Query SEC EDGAR for US entities
        if query_us:
            try:
                ingester = SECEDGARIngester()

                if cik:
                    # Query by CIK for ownership filings
                    logger.info(f"Fetching SEC EDGAR data for CIK {cik}")
                    result = await ingester.ingest_single(cik, "cik")
                    if result and result.relationships_created:
                        relationships_created += result.relationships_created
                else:
                    # Query by name
                    logger.info(f"Fetching SEC EDGAR data for {entity_name}")
                    result = await ingester.ingest_single(entity_name, "name")
                    if result and result.relationships_created:
                        relationships_created += result.relationships_created

                await ingester.close()
            except Exception as e:
                logger.debug(f"SEC EDGAR ingestion for relationships failed: {e}")

        # Query SEDAR for Canadian entities
        if query_ca:
            try:
                ingester = SEDARIngester()

                if sedar_profile:
                    logger.info(f"Fetching SEDAR data for profile {sedar_profile}")
                    result = await ingester.ingest_single(sedar_profile, "sedar_profile")
                    if result and result.relationships_created:
                        relationships_created += result.relationships_created
                elif bn:
                    logger.info(f"Fetching SEDAR data for BN {bn}")
                    result = await ingester.ingest_single(bn, "bn")
                    if result and result.relationships_created:
                        relationships_created += result.relationships_created
                else:
                    logger.info(f"Fetching SEDAR data for {entity_name}")
                    result = await ingester.ingest_single(entity_name, "name")
                    if result and result.relationships_created:
                        relationships_created += result.relationships_created

                await ingester.close()
            except Exception as e:
                logger.debug(f"SEDAR ingestion for relationships failed: {e}")

        # Query Meta Ads for ad sponsorship data
        try:
            ingester = MetaAdIngester()
            logger.info(f"Fetching Meta Ads data for {entity_name}")
            result = await ingester.ingest_single(entity_name, "name")
            if result and result.relationships_created:
                relationships_created += result.relationships_created
            await ingester.close()
        except Exception as e:
            logger.debug(f"Meta Ads ingestion for relationships failed: {e}")

        logger.info(
            f"Ingested data for {entity_name}: {relationships_created} relationships created"
        )
        return relationships_created

    async def _extract_and_enqueue_leads(
        self,
        session_id: UUID,
        entity_id: UUID,
        entity_type: str,
        entity_name: str,
        entity_data: dict[str, Any],
        config: ResearchSessionConfig,
        depth: int,
    ) -> int:
        """Extract leads from an entity and enqueue them.

        Returns:
            Number of new leads enqueued
        """
        total_enqueued = 0

        for extractor in self.extractors:
            if not extractor.supports_entity_type(entity_type):
                continue

            try:
                leads = []
                async for lead in extractor.extract_leads(
                    entity_id, entity_type, entity_name, entity_data, config
                ):
                    leads.append(lead)

                if leads:
                    enqueued = await self.queue_manager.enqueue(
                        session_id,
                        leads,
                        source_entity_id=entity_id,
                        depth=depth,
                    )
                    total_enqueued += enqueued
                    logger.debug(
                        f"Extractor {extractor.name}: Found {len(leads)} leads, "
                        f"enqueued {enqueued} for {entity_name}"
                    )

            except Exception as e:
                logger.warning(
                    f"Extractor {extractor.name} failed for {entity_name}: {e}"
                )

        return total_enqueued

    async def _update_session_stats(self, session_id: UUID) -> None:
        """Update session statistics from current data."""
        entity_count = await self.session_manager.get_session_entity_count(session_id)
        relationship_count = await self.session_manager.get_session_relationship_count(session_id)
        queue_stats = await self.queue_manager.get_queue_stats(session_id)

        stats = SessionStats(
            total_entities=entity_count,
            total_relationships=relationship_count,
            leads_total=queue_stats.total,
            leads_pending=queue_stats.pending,
            leads_completed=queue_stats.completed,
            leads_skipped=queue_stats.skipped,
            leads_failed=queue_stats.failed,
        )

        await self.session_manager.update_stats(session_id, stats)


# Singleton instance
_processor: LeadProcessor | None = None


def get_processor() -> LeadProcessor:
    """Get the lead processor singleton."""
    global _processor
    if _processor is None:
        _processor = LeadProcessor()
    return _processor
