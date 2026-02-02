"""Case report generator.

Produces ranked findings with evidence links, cross-border flags,
and unknown sections.
"""

import json
import logging
import math
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session_factory
from ...graph.queries import GraphQueries
from ..models import (
    AdMetadata,
    AdSummary,
    Case,
    CaseReport,
    CrossBorderFlag,
    EvidenceCitation,
    RankedEntity,
    RankedRelationship,
    ReportSummary,
    SimilarityLead,
    Unknown,
)

logger = logging.getLogger(__name__)


def _safe_uuid(value: Any) -> UUID | None:
    """Safely parse a value as UUID, returning None if invalid.
    
    Args:
        value: String or UUID value to parse
        
    Returns:
        UUID if valid, None otherwise
    """
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


def _safe_uuid_or_generate(value: Any) -> UUID:
    """Safely parse a value as UUID, generating a new one if invalid.
    
    Args:
        value: String or UUID value to parse
        
    Returns:
        UUID (parsed or newly generated)
    """
    result = _safe_uuid(value)
    return result if result else uuid4()


# Relationship type weights for significance scoring
RELATIONSHIP_WEIGHTS = {
    "FUNDED_BY": 1.5,
    "OWNS": 1.3,
    "SPONSORED_BY": 1.2,
    "DIRECTOR_OF": 1.0,
    "EMPLOYED_BY": 0.8,
    "SHARED_INFRA": 0.7,
}


class ReportGenerator:
    """Generates case reports with ranked findings.

    Scoring algorithms:
    - Entity relevance = depth_penalty * confidence * (1 + relationship_count * 0.1)
    - Relationship significance = confidence * log(amount + 1) * type_weight
    """

    # Limits for report sections
    MAX_ENTITIES = 20
    MAX_RELATIONSHIPS = 30

    def __init__(self, session: AsyncSession | None = None):
        """Initialize the generator.

        Args:
            session: Optional database session
        """
        self._session = session
        self._graph: GraphQueries | None = None

    async def _get_session(self) -> AsyncSession:
        """Get a fresh database session."""
        # Always create a fresh session to avoid transaction state issues
        factory = get_session_factory()
        return factory()

    @property
    def graph(self) -> GraphQueries:
        """Get the graph queries client."""
        if self._graph is None:
            self._graph = GraphQueries()
        return self._graph

    async def generate(self, case: Case) -> CaseReport:
        """Generate a report for a case.

        Args:
            case: The case to generate a report for

        Returns:
            Generated CaseReport
        """
        start_time = datetime.utcnow()
        logger.info(f"Generating report for case {case.id}")

        # Get session ID for graph queries
        session_id = case.research_session_id
        if session_id is None:
            logger.warning(f"Case {case.id} has no research session, generating empty report")
            return self._empty_report(case)

        # Fetch data from graph
        entities = await self._fetch_entities(session_id)
        relationships = await self._fetch_relationships(session_id)
        
        # Fetch ad details for enriching relationships
        ads = await self._fetch_ads(session_id)
        ad_map = {ad.get("meta_ad_id"): ad for ad in ads if ad.get("meta_ad_id")}

        # Rank entities
        ranked_entities = self._rank_entities(entities)[:self.MAX_ENTITIES]

        # Rank relationships and enrich with ad metadata
        ranked_relationships = self._rank_relationships(relationships, ad_map)[:self.MAX_RELATIONSHIPS]

        # Identify cross-border connections
        cross_border = await self._find_cross_border(session_id)

        # Identify unknowns
        unknowns = await self._find_unknowns(session_id)

        # Build evidence index from entities, relationships, and cross-border flags
        evidence_index = self._build_evidence_index(entities, ranked_relationships, cross_border)
        
        # Build ads summary
        ads_summary = self._build_ads_summary(ads) if ads else None
        
        # Generate similarity leads for further investigation
        similarity_leads = self._generate_similarity_leads(ads, entities)

        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()

        # Count pending matches
        pending_matches = await self._count_pending_matches(case.id)

        # Create summary
        # Handle entry_point_type whether it's an enum or already a string
        entry_type = case.entry_point_type.value if hasattr(case.entry_point_type, 'value') else case.entry_point_type
        summary = ReportSummary(
            entry_point=f"{entry_type}: {case.entry_point_value[:50]}",
            processing_time_seconds=processing_time,
            entity_count=len(entities),
            relationship_count=len(relationships),
            cross_border_count=len(cross_border),
            has_unresolved_matches=pending_matches > 0,
        )

        # Create report
        report = CaseReport(
            id=uuid4(),
            case_id=case.id,
            generated_at=end_time,
            summary=summary,
            top_entities=ranked_entities,
            top_relationships=ranked_relationships,
            cross_border_flags=cross_border,
            unknowns=unknowns,
            evidence_index=evidence_index,
            ads_summary=ads_summary,
            similarity_leads=similarity_leads,
        )

        # Store report
        await self._store_report(report)

        logger.info(
            f"Generated report for case {case.id}: "
            f"{len(ranked_entities)} entities, {len(ranked_relationships)} relationships"
        )
        return report

    def _empty_report(self, case: Case) -> CaseReport:
        """Create an empty report for a case without a research session."""
        entry_type = case.entry_point_type.value if hasattr(case.entry_point_type, 'value') else case.entry_point_type
        return CaseReport(
            id=uuid4(),
            case_id=case.id,
            generated_at=datetime.utcnow(),
            summary=ReportSummary(
                entry_point=f"{entry_type}: {case.entry_point_value[:50]}",
                processing_time_seconds=0.0,
                entity_count=0,
                relationship_count=0,
                cross_border_count=0,
                has_unresolved_matches=False,
            ),
            top_entities=[],
            top_relationships=[],
            cross_border_flags=[],
            unknowns=[],
            evidence_index=[],
        )

    async def _fetch_entities(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fetch entities discovered in the research session.
        
        Uses PostgreSQL session_entities table to get entity IDs linked to this
        session, then fetches entity details and relationship counts from Neo4j.
        """
        # First, get entity IDs and depths from PostgreSQL session_entities table
        entity_records = []
        session = None
        try:
            session = await self._get_session()
            result = await session.execute(
                text("""
                    SELECT entity_id, depth, relevance_score
                    FROM session_entities
                    WHERE session_id = :session_id
                    ORDER BY depth ASC, relevance_score DESC
                """),
                {"session_id": str(session_id)},
            )
            rows = result.fetchall()
            entity_records = [
                {"entity_id": str(row.entity_id), "depth": row.depth, "relevance_score": row.relevance_score}
                for row in rows
            ]
        except Exception as e:
            logger.warning(f"Failed to fetch session entities from PostgreSQL: {e}")
        finally:
            if session:
                await session.close()

        if not entity_records:
            # Fallback: try to find entities directly in Neo4j by looking for
            # Sponsor nodes or other entities created during this session
            return await self._fetch_entities_fallback(session_id)

        # Now fetch entity details from Neo4j
        entity_ids = [r["entity_id"] for r in entity_records]
        depth_map = {r["entity_id"]: r["depth"] for r in entity_records}
        
        # Note: Use COALESCE to avoid warnings about missing properties
        query = """
        MATCH (e)
        WHERE e.id IN $entity_ids
        OPTIONAL MATCH (e)-[r]-()
        WITH e, count(DISTINCT r) as rel_count
        RETURN e.id as id, e.name as name, 
               COALESCE(e.entity_type, 'unknown') as entity_type,
               e.jurisdiction as jurisdiction, 
               COALESCE(e.confidence, 0.8) as confidence,
               rel_count, 
               COALESCE(e.source_ids, []) as source_ids
        """

        try:
            results = await self.graph.execute(query, {"entity_ids": entity_ids})
            # Add depth from our PostgreSQL data
            for result in results or []:
                result["depth"] = depth_map.get(result.get("id"), 0)
            return results or []
        except Exception as e:
            logger.warning(f"Failed to fetch entity details from Neo4j: {e}")
            return []

    async def _fetch_entities_fallback(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fallback entity fetch when no session_entities records exist.
        
        This handles cases where entities were created but not properly linked
        to the session in PostgreSQL (e.g., via direct ingestion).
        
        Looks for Sponsor nodes created around the time of the research session.
        """
        # Get the case to find entry point info
        session = None
        case_created_at = None
        entry_point_value = None
        
        try:
            session = await self._get_session()
            # Find the case that uses this research session
            result = await session.execute(
                text("""
                    SELECT c.entry_point_value, c.created_at
                    FROM cases c
                    WHERE c.research_session_id = :session_id
                """),
                {"session_id": str(session_id)},
            )
            row = result.fetchone()
            if row:
                entry_point_value = row.entry_point_value
                case_created_at = row.created_at
        except Exception as e:
            logger.debug(f"Could not fetch case info for fallback: {e}")
        finally:
            if session:
                await session.close()

        if not entry_point_value:
            return []

        # Try to find Sponsor nodes that match the entry point
        # This handles Meta Ads cases where sponsors are created directly
        query = """
        MATCH (s:Sponsor)
        WHERE toLower(s.name) CONTAINS toLower($search_term)
        OPTIONAL MATCH (s)-[r]-()
        WITH s, count(DISTINCT r) as rel_count
        RETURN s.id as id, s.name as name, 'organization' as entity_type,
               s.jurisdiction as jurisdiction, s.confidence as confidence,
               rel_count, [] as source_ids
        LIMIT 50
        """

        try:
            # Use first few words of entry point as search term
            search_term = " ".join(entry_point_value.split()[:3])
            results = await self.graph.execute(query, {"search_term": search_term})
            # Add depth 0 for all fallback entities
            for result in results or []:
                result["depth"] = 0
            return results or []
        except Exception as e:
            logger.warning(f"Failed fallback entity fetch: {e}")
            return []

    async def _fetch_relationships(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fetch relationships discovered in the research session.
        
        Uses PostgreSQL session_relationships table to get relationship IDs,
        then fetches relationship details from Neo4j.
        """
        # First, get relationship IDs from PostgreSQL session_relationships table
        relationship_ids = []
        session = None
        try:
            session = await self._get_session()
            result = await session.execute(
                text("""
                    SELECT relationship_id
                    FROM session_relationships
                    WHERE session_id = :session_id
                """),
                {"session_id": str(session_id)},
            )
            rows = result.fetchall()
            relationship_ids = [str(row.relationship_id) for row in rows]
        except Exception as e:
            logger.warning(f"Failed to fetch session relationships from PostgreSQL: {e}")
        finally:
            if session:
                await session.close()

        if not relationship_ids:
            # Fallback: fetch relationships for entities in this session
            return await self._fetch_relationships_fallback(session_id)

        # Fetch relationship details from Neo4j by ID
        query = """
        MATCH (source)-[r]->(target)
        WHERE r.id IN $relationship_ids
        RETURN source.id as source_id, source.name as source_name,
               target.id as target_id, target.name as target_name,
               type(r) as rel_type, r.amount as amount, r.confidence as confidence,
               r.evidence_ids as evidence_ids
        """

        try:
            results = await self.graph.execute(query, {"relationship_ids": relationship_ids})
            return results or []
        except Exception as e:
            logger.warning(f"Failed to fetch relationship details from Neo4j: {e}")
            return []

    async def _fetch_relationships_fallback(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fallback relationship fetch based on entities in the session.
        
        When no session_relationships records exist, fetch relationships
        between entities that are linked to this session.
        """
        # Get entity IDs from session
        entities = await self._fetch_entities(session_id)
        if not entities:
            return []

        entity_ids = [e.get("id") for e in entities if e.get("id")]
        if not entity_ids:
            return []

        # Find relationships between these entities
        # Use COALESCE to avoid warnings about missing properties
        query = """
        MATCH (source)-[r]->(target)
        WHERE source.id IN $entity_ids OR target.id IN $entity_ids
        RETURN source.id as source_id, source.name as source_name,
               target.id as target_id, target.name as target_name,
               type(r) as rel_type, 
               r.amount as amount, 
               COALESCE(r.confidence, 0.8) as confidence,
               COALESCE(r.evidence_ids, []) as evidence_ids,
               source.meta_ad_id as source_meta_ad_id
        LIMIT 100
        """

        try:
            results = await self.graph.execute(query, {"entity_ids": entity_ids})
            return results or []
        except Exception as e:
            logger.warning(f"Failed fallback relationship fetch: {e}")
            return []

    def _rank_entities(self, entities: list[dict[str, Any]]) -> list[RankedEntity]:
        """Rank entities by relevance score.

        relevance = (1 / (depth + 1)) * confidence * (1 + rel_count * 0.1)
        """
        ranked = []

        for entity in entities:
            depth = entity.get("depth", 0) or 0
            confidence = entity.get("confidence", 0.8) or 0.8
            rel_count = entity.get("rel_count", 0) or 0

            # Calculate relevance score
            depth_penalty = 1.0 / (depth + 1)
            rel_bonus = 1.0 + rel_count * 0.1
            relevance = depth_penalty * confidence * rel_bonus

            ranked.append(RankedEntity(
                entity_id=_safe_uuid_or_generate(entity.get("id")),
                name=entity.get("name") or "Unknown",
                entity_type=entity.get("entity_type") or "unknown",
                relevance_score=relevance,
                depth=depth,
                key_relationships=[],  # Would need additional query
                jurisdiction=entity.get("jurisdiction"),
            ))

        # Sort by relevance (highest first)
        ranked.sort(key=lambda x: x.relevance_score, reverse=True)
        return ranked

    def _rank_relationships(
        self,
        relationships: list[dict[str, Any]],
        ad_map: dict[str, dict[str, Any]] | None = None,
    ) -> list[RankedRelationship]:
        """Rank relationships by significance score.

        significance = confidence * log(amount + 1) * type_weight
        
        Args:
            relationships: List of relationship dicts from Neo4j
            ad_map: Optional mapping of meta_ad_id -> ad details for enrichment
        """
        ranked = []
        ad_map = ad_map or {}

        for rel in relationships:
            rel_type = rel.get("rel_type") or "UNKNOWN"
            confidence = rel.get("confidence") or 0.8
            amount = rel.get("amount") or 0

            # Calculate significance
            type_weight = RELATIONSHIP_WEIGHTS.get(rel_type, 1.0)
            amount_factor = math.log(amount + 1) + 1  # +1 to ensure non-zero
            significance = confidence * amount_factor * type_weight

            evidence_ids = rel.get("evidence_ids") or []
            if isinstance(evidence_ids, str):
                evidence_ids = [evidence_ids]

            # Safely parse evidence IDs
            parsed_evidence_ids = []
            for eid in evidence_ids:
                parsed = _safe_uuid(eid)
                if parsed:
                    parsed_evidence_ids.append(parsed)

            # Build ad metadata if this is a SPONSORED_BY relationship
            ad_metadata = None
            if rel_type == "SPONSORED_BY":
                # Try to find ad details - source is typically the Ad node
                source_ad_id = rel.get("source_meta_ad_id") or rel.get("meta_ad_id")
                ad_data = ad_map.get(source_ad_id) if source_ad_id else None
                
                if ad_data:
                    # Parse target regions from delivery_by_region
                    target_regions = []
                    delivery_by_region = ad_data.get("delivery_by_region") or []
                    if isinstance(delivery_by_region, list):
                        for region in delivery_by_region:
                            if isinstance(region, dict):
                                target_regions.append(region.get("region", ""))
                            elif isinstance(region, str):
                                target_regions.append(region)
                    
                    ad_metadata = AdMetadata(
                        ad_id=ad_data.get("meta_ad_id"),
                        creative_body=ad_data.get("creative_body"),
                        creative_title=ad_data.get("creative_title"),
                        ad_snapshot_url=ad_data.get("ad_snapshot_url"),
                        impressions_lower=ad_data.get("impressions_lower"),
                        impressions_upper=ad_data.get("impressions_upper"),
                        spend_lower=ad_data.get("spend_lower"),
                        spend_upper=ad_data.get("spend_upper"),
                        currency=ad_data.get("currency") or "USD",
                        delivery_start=ad_data.get("ad_delivery_start_time"),
                        delivery_stop=ad_data.get("ad_delivery_stop_time"),
                        publisher_platforms=ad_data.get("publisher_platforms") or [],
                        target_regions=target_regions,
                        languages=ad_data.get("languages") or [],
                    )
                else:
                    # Create basic metadata from relationship data
                    ad_metadata = AdMetadata(
                        spend_lower=rel.get("spend_lower"),
                        spend_upper=rel.get("spend_upper"),
                        currency=rel.get("currency") or "USD",
                    )

            ranked.append(RankedRelationship(
                source_entity_id=_safe_uuid_or_generate(rel.get("source_id")),
                source_name=rel.get("source_name") or "Unknown",
                target_entity_id=_safe_uuid_or_generate(rel.get("target_id")),
                target_name=rel.get("target_name") or "Unknown",
                relationship_type=rel_type,
                significance_score=significance,
                amount=amount if amount > 0 else None,
                evidence_ids=parsed_evidence_ids,
                ad_metadata=ad_metadata,
            ))

        # Sort by significance (highest first)
        ranked.sort(key=lambda x: x.significance_score, reverse=True)
        return ranked

    async def _find_cross_border(self, session_id: UUID) -> list[CrossBorderFlag]:
        """Find US-CA cross-border connections among session entities."""
        # Get entities in this session first
        entities = await self._fetch_entities(session_id)
        if not entities:
            return []

        entity_ids = [e.get("id") for e in entities if e.get("id")]
        if not entity_ids:
            return []

        # Find cross-border relationships involving session entities
        query = """
        MATCH (us)-[r]->(ca)
        WHERE (us.id IN $entity_ids OR ca.id IN $entity_ids)
          AND us.jurisdiction = 'US' AND ca.jurisdiction = 'CA'
        RETURN us.id as us_id, us.name as us_name,
               ca.id as ca_id, ca.name as ca_name,
               type(r) as rel_type, r.amount as amount, r.evidence_ids as evidence_ids
        UNION
        MATCH (ca)-[r]->(us)
        WHERE (us.id IN $entity_ids OR ca.id IN $entity_ids)
          AND us.jurisdiction = 'US' AND ca.jurisdiction = 'CA'
        RETURN us.id as us_id, us.name as us_name,
               ca.id as ca_id, ca.name as ca_name,
               type(r) as rel_type, r.amount as amount, r.evidence_ids as evidence_ids
        """

        flags = []
        try:
            results = await self.graph.execute(query, {"entity_ids": entity_ids})
            for row in results or []:
                evidence_ids = row.get("evidence_ids") or []
                if isinstance(evidence_ids, str):
                    evidence_ids = [evidence_ids]

                # Safely parse evidence IDs
                parsed_evidence_ids = []
                for eid in evidence_ids:
                    parsed = _safe_uuid(eid)
                    if parsed:
                        parsed_evidence_ids.append(parsed)

                flags.append(CrossBorderFlag(
                    us_entity_id=_safe_uuid_or_generate(row.get("us_id")),
                    us_entity_name=row.get("us_name") or "Unknown",
                    ca_entity_id=_safe_uuid_or_generate(row.get("ca_id")),
                    ca_entity_name=row.get("ca_name") or "Unknown",
                    relationship_type=row.get("rel_type") or "UNKNOWN",
                    amount=row.get("amount"),
                    evidence_ids=parsed_evidence_ids,
                ))
        except Exception as e:
            logger.warning(f"Failed to find cross-border connections: {e}")

        return flags

    async def _find_unknowns(self, session_id: UUID) -> list[Unknown]:
        """Find entities that couldn't be fully traced.
        
        Looks for entities in the session that have trace_incomplete or
        no_sources_found flags set.
        """
        # Get entities in this session
        entities = await self._fetch_entities(session_id)
        if not entities:
            return []

        entity_ids = [e.get("id") for e in entities if e.get("id")]
        if not entity_ids:
            return []

        # Find entities with incomplete traces
        # Use COALESCE to avoid warnings when properties don't exist
        query = """
        MATCH (e)
        WHERE e.id IN $entity_ids
          AND (COALESCE(e.trace_incomplete, false) = true 
               OR COALESCE(e.no_sources_found, false) = true)
        RETURN e.name as name, 
               COALESCE(e.trace_reason, 'Not fully traced') as reason,
               COALESCE(e.attempted_sources, []) as sources
        """

        unknowns = []
        try:
            results = await self.graph.execute(query, {"entity_ids": entity_ids})
            for row in results or []:
                sources = row.get("sources") or []
                if isinstance(sources, str):
                    sources = [sources]

                unknowns.append(Unknown(
                    entity_name=row.get("name") or "Unknown",
                    reason=row.get("reason") or "Unknown reason",
                    attempted_sources=sources,
                ))
        except Exception as e:
            logger.warning(f"Failed to find unknowns: {e}")

        return unknowns

    def _build_evidence_index(
        self,
        entities: list[dict[str, Any]],
        relationships: list[RankedRelationship],
        cross_border: list[CrossBorderFlag],
    ) -> list[EvidenceCitation]:
        """Build evidence index from entities, relationships, and cross-border flags.
        
        Evidence is linked through:
        - Entity source_ids (evidence that created/discovered the entity)
        - Relationship evidence_ids (evidence supporting the relationship)
        
        Args:
            entities: Raw entity dicts with source_ids
            relationships: Ranked relationships with evidence_ids
            cross_border: Cross-border flags with evidence_ids
            
        Returns:
            Deduplicated list of evidence citations
        """
        evidence_map: dict[UUID, EvidenceCitation] = {}
        
        # Collect from entity source_ids
        for entity in entities:
            source_ids = entity.get("source_ids") or []
            if isinstance(source_ids, str):
                source_ids = [source_ids]
            
            entity_name = entity.get("name") or "Unknown"
            entity_type = entity.get("entity_type") or "unknown"
            
            for sid in source_ids:
                try:
                    eid = UUID(sid) if isinstance(sid, str) else sid
                    if eid and eid not in evidence_map:
                        evidence_map[eid] = EvidenceCitation(
                            evidence_id=eid,
                            source_type=f"entity:{entity_type}",
                            source_url=None,
                            retrieved_at=None,
                        )
                except (ValueError, TypeError):
                    # Invalid UUID, skip
                    continue
        
        # Collect from relationships
        for rel in relationships:
            for eid in rel.evidence_ids:
                if eid not in evidence_map:
                    evidence_map[eid] = EvidenceCitation(
                        evidence_id=eid,
                        source_type=f"relationship:{rel.relationship_type}",
                        source_url=None,
                        retrieved_at=None,
                    )
        
        # Collect from cross-border flags
        for flag in cross_border:
            for eid in flag.evidence_ids:
                if eid not in evidence_map:
                    evidence_map[eid] = EvidenceCitation(
                        evidence_id=eid,
                        source_type=f"cross_border:{flag.relationship_type}",
                        source_url=None,
                        retrieved_at=None,
                    )
        
        logger.info(f"Built evidence index with {len(evidence_map)} unique citations")
        return list(evidence_map.values())

    async def _fetch_ads(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fetch ad details from Neo4j for entities in this session.
        
        Returns detailed ad data for enriching relationships and building summaries.
        """
        # Get entities in this session
        entities = await self._fetch_entities(session_id)
        if not entities:
            return []

        entity_ids = [e.get("id") for e in entities if e.get("id")]
        if not entity_ids:
            return []

        # Find ads that are SPONSORED_BY entities in this session
        query = """
        MATCH (a:Ad)-[:SPONSORED_BY]->(s)
        WHERE s.id IN $entity_ids
        RETURN a.meta_ad_id as meta_ad_id, a.name as name,
               a.creative_body as creative_body, a.creative_title as creative_title,
               a.ad_snapshot_url as ad_snapshot_url,
               a.impressions_lower as impressions_lower, a.impressions_upper as impressions_upper,
               a.spend_lower as spend_lower, a.spend_upper as spend_upper,
               a.currency as currency, a.country as country,
               a.ad_delivery_start_time as ad_delivery_start_time,
               a.ad_delivery_stop_time as ad_delivery_stop_time,
               a.publisher_platforms as publisher_platforms,
               a.languages as languages,
               a.delivery_by_region as delivery_by_region,
               a.funding_entity as funding_entity,
               s.name as sponsor_name
        """

        try:
            results = await self.graph.execute(query, {"entity_ids": entity_ids})
            return results or []
        except Exception as e:
            logger.warning(f"Failed to fetch ads: {e}")
            return []

    def _build_ads_summary(self, ads: list[dict[str, Any]]) -> AdSummary:
        """Build aggregated summary of ad data.
        
        Args:
            ads: List of ad dicts from Neo4j
            
        Returns:
            AdSummary with aggregated statistics
        """
        if not ads:
            return AdSummary()

        total_spend_lower = 0.0
        total_spend_upper = 0.0
        total_impressions_lower = 0
        total_impressions_upper = 0
        currencies = set()
        platforms = set()
        countries = set()
        sponsors = set()
        dates = []
        creative_words: dict[str, int] = {}

        for ad in ads:
            # Accumulate spend
            if ad.get("spend_lower"):
                total_spend_lower += ad["spend_lower"]
            if ad.get("spend_upper"):
                total_spend_upper += ad["spend_upper"]

            # Accumulate impressions
            if ad.get("impressions_lower"):
                total_impressions_lower += ad["impressions_lower"]
            if ad.get("impressions_upper"):
                total_impressions_upper += ad["impressions_upper"]

            # Collect currencies
            if ad.get("currency"):
                currencies.add(ad["currency"])

            # Collect platforms
            ad_platforms = ad.get("publisher_platforms") or []
            if isinstance(ad_platforms, list):
                for p in ad_platforms:
                    if p:
                        platforms.add(p)

            # Collect countries
            if ad.get("country"):
                countries.add(ad["country"])

            # Collect sponsors
            if ad.get("sponsor_name"):
                sponsors.add(ad["sponsor_name"])
            if ad.get("funding_entity"):
                sponsors.add(ad["funding_entity"])

            # Collect dates
            if ad.get("ad_delivery_start_time"):
                dates.append(ad["ad_delivery_start_time"])
            if ad.get("ad_delivery_stop_time"):
                dates.append(ad["ad_delivery_stop_time"])

            # Extract words from creative content for themes
            creative_text = (ad.get("creative_body") or "") + " " + (ad.get("creative_title") or "")
            if creative_text.strip():
                # Simple word extraction - filter common words
                stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will", "would", "could", "should", "may", "might", "must", "shall", "can", "this", "that", "these", "those", "i", "you", "he", "she", "it", "we", "they", "what", "which", "who", "whom", "whose", "where", "when", "why", "how", "all", "each", "every", "both", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "just", "your", "our", "their", "its", "my", "his", "her"}
                words = creative_text.lower().split()
                for word in words:
                    # Clean punctuation
                    word = ''.join(c for c in word if c.isalnum())
                    if word and len(word) > 3 and word not in stop_words:
                        creative_words[word] = creative_words.get(word, 0) + 1

        # Get top themes (most common words)
        sorted_words = sorted(creative_words.items(), key=lambda x: x[1], reverse=True)
        top_themes = [word for word, count in sorted_words[:10] if count >= 2]

        # Determine date range
        date_range_start = min(dates) if dates else None
        date_range_end = max(dates) if dates else None

        return AdSummary(
            total_ads=len(ads),
            total_spend_lower=total_spend_lower if total_spend_lower > 0 else None,
            total_spend_upper=total_spend_upper if total_spend_upper > 0 else None,
            total_impressions_lower=total_impressions_lower if total_impressions_lower > 0 else None,
            total_impressions_upper=total_impressions_upper if total_impressions_upper > 0 else None,
            currencies=list(currencies),
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            publisher_platforms=list(platforms),
            target_countries=list(countries),
            top_creative_themes=top_themes,
            sponsors=list(sponsors),
        )

    def _generate_similarity_leads(
        self,
        ads: list[dict[str, Any]],
        entities: list[dict[str, Any]],
    ) -> list[SimilarityLead]:
        """Generate leads for further investigation based on ad patterns.
        
        Analyzes ad content, sponsors, and targeting to suggest related searches.
        
        Args:
            ads: List of ad dicts from Neo4j
            entities: List of entity dicts from the session
            
        Returns:
            List of SimilarityLead suggestions
        """
        leads: list[SimilarityLead] = []
        
        if not ads:
            return leads

        # Track patterns
        funding_entities: dict[str, list[str]] = {}  # funding_entity -> [ad_ids]
        platforms: dict[str, list[str]] = {}  # platform -> [ad_ids]
        regions: dict[str, list[str]] = {}  # region -> [ad_ids]
        keywords: dict[str, list[str]] = {}  # keyword -> [ad_ids]

        for ad in ads:
            ad_id = ad.get("meta_ad_id") or "unknown"
            
            # Track funding entities (different from page name)
            funding = ad.get("funding_entity")
            page_name = ad.get("name") or ad.get("sponsor_name")
            if funding and funding != page_name:
                if funding not in funding_entities:
                    funding_entities[funding] = []
                funding_entities[funding].append(ad_id)

            # Track platforms
            ad_platforms = ad.get("publisher_platforms") or []
            for p in ad_platforms:
                if p:
                    if p not in platforms:
                        platforms[p] = []
                    platforms[p].append(ad_id)

            # Track regions from delivery_by_region
            delivery_regions = ad.get("delivery_by_region") or []
            if isinstance(delivery_regions, list):
                for region in delivery_regions:
                    region_name = region.get("region") if isinstance(region, dict) else str(region)
                    if region_name:
                        if region_name not in regions:
                            regions[region_name] = []
                        regions[region_name].append(ad_id)

            # Extract significant keywords from content
            creative_text = (ad.get("creative_body") or "") + " " + (ad.get("creative_title") or "")
            if creative_text.strip():
                # Look for organization-like names (capitalized multi-word phrases)
                import re
                # Find capitalized phrases that might be organization names
                org_patterns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b', creative_text)
                for org in org_patterns:
                    if len(org) > 5:  # Skip short phrases
                        if org not in keywords:
                            keywords[org] = []
                        keywords[org].append(ad_id)

        # Generate leads from funding entities
        for funding, ad_ids in funding_entities.items():
            if len(ad_ids) >= 1:  # At least one ad with this funding entity
                leads.append(SimilarityLead(
                    lead_type="shared_funder",
                    description=f"'{funding}' appears as a funding entity/disclaimer on {len(ad_ids)} ad(s)",
                    target_value=funding,
                    confidence=0.85,
                    source_ads=ad_ids[:5],
                    suggested_search=funding,
                ))

        # Generate leads from organization mentions in content
        for org, ad_ids in keywords.items():
            if len(ad_ids) >= 2:  # Mentioned in at least 2 ads
                leads.append(SimilarityLead(
                    lead_type="mentioned_organization",
                    description=f"'{org}' mentioned in {len(ad_ids)} ad(s) - may be a related organization",
                    target_value=org,
                    confidence=0.6,
                    source_ads=ad_ids[:5],
                    suggested_search=org,
                ))

        # Generate leads from heavy region targeting
        for region, ad_ids in regions.items():
            if len(ad_ids) >= 3:  # Significant regional focus
                leads.append(SimilarityLead(
                    lead_type="regional_focus",
                    description=f"{len(ad_ids)} ad(s) heavily target '{region}' - search for other political ads in this region",
                    target_value=region,
                    confidence=0.5,
                    source_ads=ad_ids[:5],
                    suggested_search=None,  # Can't search by region in Meta Ad Library
                ))

        # Sort by confidence
        leads.sort(key=lambda x: x.confidence, reverse=True)
        
        return leads[:10]  # Limit to top 10 leads

    async def _count_pending_matches(self, case_id: UUID) -> int:
        """Count pending entity matches for a case."""
        session = None
        try:
            session = await self._get_session()
            result = await session.execute(
                text("""
                SELECT COUNT(*) FROM entity_matches
                WHERE case_id = :case_id AND status = 'pending'
                """),
                {"case_id": str(case_id)},
            )
            return result.scalar() or 0
        except Exception as e:
            logger.warning(f"Failed to count pending matches: {e}")
            return 0
        finally:
            if session:
                await session.close()

    async def _store_report(self, report: CaseReport) -> None:
        """Store a report in the database."""
        session = None
        try:
            session = await self._get_session()
            
            # Serialize new fields
            ads_summary_json = json.dumps(report.ads_summary.model_dump()) if report.ads_summary else None
            similarity_leads_json = json.dumps([l.model_dump() for l in report.similarity_leads])

            # Check if report exists (upsert)
            existing = await session.execute(
                text("SELECT id FROM case_reports WHERE case_id = :case_id"),
                {"case_id": str(report.case_id)},
            )
            if existing.fetchone():
                # Update existing report
                await session.execute(
                    text("""
                    UPDATE case_reports SET
                        generated_at = :generated_at,
                        report_version = report_version + 1,
                        summary = :summary,
                        top_entities = :top_entities,
                        top_relationships = :top_relationships,
                        cross_border_flags = :cross_border_flags,
                        unknowns = :unknowns,
                        evidence_index = :evidence_index,
                        ads_summary = :ads_summary,
                        similarity_leads = :similarity_leads
                    WHERE case_id = :case_id
                    """),
                    {
                        "case_id": str(report.case_id),
                        "generated_at": report.generated_at,
                        "summary": report.summary.model_dump(),
                        "top_entities": [e.model_dump() for e in report.top_entities],
                        "top_relationships": [r.model_dump() for r in report.top_relationships],
                        "cross_border_flags": [f.model_dump() for f in report.cross_border_flags],
                        "unknowns": [u.model_dump() for u in report.unknowns],
                        "evidence_index": [e.model_dump() for e in report.evidence_index],
                        "ads_summary": ads_summary_json,
                        "similarity_leads": similarity_leads_json,
                    },
                )
            else:
                # Insert new report
                await session.execute(
                    text("""
                    INSERT INTO case_reports (
                        id, case_id, generated_at, report_version, summary,
                        top_entities, top_relationships, cross_border_flags,
                        unknowns, evidence_index, ads_summary, similarity_leads
                    ) VALUES (
                        :id, :case_id, :generated_at, :report_version, :summary,
                        :top_entities, :top_relationships, :cross_border_flags,
                        :unknowns, :evidence_index, :ads_summary, :similarity_leads
                    )
                    """),
                    {
                        "id": str(report.id),
                        "case_id": str(report.case_id),
                        "generated_at": report.generated_at,
                        "report_version": report.report_version,
                        "summary": json.dumps(report.summary.model_dump()),
                        "top_entities": json.dumps([e.model_dump() for e in report.top_entities]),
                        "top_relationships": json.dumps([r.model_dump() for r in report.top_relationships]),
                        "cross_border_flags": json.dumps([f.model_dump() for f in report.cross_border_flags]),
                        "unknowns": json.dumps([u.model_dump() for u in report.unknowns]),
                        "evidence_index": json.dumps([e.model_dump() for e in report.evidence_index]),
                        "ads_summary": ads_summary_json,
                        "similarity_leads": similarity_leads_json,
                    },
                )

            await session.commit()
        except Exception as e:
            logger.warning(f"Failed to store report: {e}")
        finally:
            if session:
                await session.close()

    async def get_report(self, case_id: UUID) -> CaseReport | None:
        """Get an existing report for a case."""
        session = None
        try:
            session = await self._get_session()
            result = await session.execute(
                text("SELECT * FROM case_reports WHERE case_id = :case_id"),
                {"case_id": str(case_id)},
            )
            row = result.fetchone()
            if row is None:
                return None

            # Parse new fields with fallbacks for old reports
            ads_summary = None
            if hasattr(row, 'ads_summary') and row.ads_summary:
                ads_data = row.ads_summary
                if isinstance(ads_data, str):
                    ads_data = json.loads(ads_data)
                ads_summary = AdSummary(**ads_data)

            similarity_leads = []
            if hasattr(row, 'similarity_leads') and row.similarity_leads:
                leads_data = row.similarity_leads
                if isinstance(leads_data, str):
                    leads_data = json.loads(leads_data)
                similarity_leads = [SimilarityLead(**l) for l in leads_data]

            return CaseReport(
                id=UUID(row.id) if isinstance(row.id, str) else row.id,
                case_id=UUID(row.case_id) if isinstance(row.case_id, str) else row.case_id,
                generated_at=row.generated_at,
                report_version=row.report_version,
                summary=ReportSummary(**row.summary),
                top_entities=[RankedEntity(**e) for e in row.top_entities],
                top_relationships=[RankedRelationship(**r) for r in row.top_relationships],
                cross_border_flags=[CrossBorderFlag(**f) for f in row.cross_border_flags],
                unknowns=[Unknown(**u) for u in row.unknowns],
                evidence_index=[EvidenceCitation(**e) for e in row.evidence_index],
                ads_summary=ads_summary,
                similarity_leads=similarity_leads,
            )
        except Exception as e:
            logger.warning(f"Failed to get report: {e}")
            return None
        finally:
            if session:
                await session.close()


# Singleton instance
_generator: ReportGenerator | None = None


def get_report_generator() -> ReportGenerator:
    """Get the report generator singleton."""
    global _generator
    if _generator is None:
        _generator = ReportGenerator()
    return _generator
