"""Case report generator.

Produces ranked findings with evidence links, cross-border flags,
and unknown sections.
"""

import logging
import math
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_db_session
from ...graph.queries import GraphQueries
from ..models import (
    Case,
    CaseReport,
    CrossBorderFlag,
    EvidenceCitation,
    RankedEntity,
    RankedRelationship,
    ReportSummary,
    Unknown,
)

logger = logging.getLogger(__name__)


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
        """Get the database session."""
        if self._session is not None:
            return self._session
        return await get_db_session().__anext__()

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

        # Rank entities
        ranked_entities = self._rank_entities(entities)[:self.MAX_ENTITIES]

        # Rank relationships
        ranked_relationships = self._rank_relationships(relationships)[:self.MAX_RELATIONSHIPS]

        # Identify cross-border connections
        cross_border = await self._find_cross_border(session_id)

        # Identify unknowns
        unknowns = await self._find_unknowns(session_id)

        # Get evidence citations
        evidence_index = await self._get_evidence_index(case.id)

        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()

        # Count pending matches
        pending_matches = await self._count_pending_matches(case.id)

        # Create summary
        summary = ReportSummary(
            entry_point=f"{case.entry_point_type.value}: {case.entry_point_value[:50]}",
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
        return CaseReport(
            id=uuid4(),
            case_id=case.id,
            generated_at=datetime.utcnow(),
            summary=ReportSummary(
                entry_point=f"{case.entry_point_type.value}: {case.entry_point_value[:50]}",
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
        """Fetch entities discovered in the research session."""
        query = """
        MATCH (e)-[:DISCOVERED_IN]->(s:ResearchSession {id: $session_id})
        OPTIONAL MATCH (e)-[r]-()
        WITH e, count(DISTINCT r) as rel_count
        RETURN e.id as id, e.name as name, e.entity_type as entity_type,
               e.jurisdiction as jurisdiction, e.depth as depth,
               e.confidence as confidence, rel_count
        """

        try:
            results = await self.graph.execute(query, {"session_id": str(session_id)})
            return results or []
        except Exception as e:
            logger.warning(f"Failed to fetch entities: {e}")
            return []

    async def _fetch_relationships(self, session_id: UUID) -> list[dict[str, Any]]:
        """Fetch relationships discovered in the research session."""
        query = """
        MATCH (source)-[r]->(target)
        WHERE r.session_id = $session_id
        RETURN source.id as source_id, source.name as source_name,
               target.id as target_id, target.name as target_name,
               type(r) as rel_type, r.amount as amount, r.confidence as confidence,
               r.evidence_ids as evidence_ids
        """

        try:
            results = await self.graph.execute(query, {"session_id": str(session_id)})
            return results or []
        except Exception as e:
            logger.warning(f"Failed to fetch relationships: {e}")
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
                entity_id=UUID(entity["id"]) if entity.get("id") else uuid4(),
                name=entity.get("name", "Unknown"),
                entity_type=entity.get("entity_type", "unknown"),
                relevance_score=relevance,
                depth=depth,
                key_relationships=[],  # Would need additional query
                jurisdiction=entity.get("jurisdiction"),
            ))

        # Sort by relevance (highest first)
        ranked.sort(key=lambda x: x.relevance_score, reverse=True)
        return ranked

    def _rank_relationships(
        self, relationships: list[dict[str, Any]]
    ) -> list[RankedRelationship]:
        """Rank relationships by significance score.

        significance = confidence * log(amount + 1) * type_weight
        """
        ranked = []

        for rel in relationships:
            rel_type = rel.get("rel_type", "UNKNOWN")
            confidence = rel.get("confidence", 0.8) or 0.8
            amount = rel.get("amount") or 0

            # Calculate significance
            type_weight = RELATIONSHIP_WEIGHTS.get(rel_type, 1.0)
            amount_factor = math.log(amount + 1) + 1  # +1 to ensure non-zero
            significance = confidence * amount_factor * type_weight

            evidence_ids = rel.get("evidence_ids") or []
            if isinstance(evidence_ids, str):
                evidence_ids = [evidence_ids]

            ranked.append(RankedRelationship(
                source_entity_id=UUID(rel["source_id"]) if rel.get("source_id") else uuid4(),
                source_name=rel.get("source_name", "Unknown"),
                target_entity_id=UUID(rel["target_id"]) if rel.get("target_id") else uuid4(),
                target_name=rel.get("target_name", "Unknown"),
                relationship_type=rel_type,
                significance_score=significance,
                amount=amount if amount > 0 else None,
                evidence_ids=[UUID(eid) for eid in evidence_ids if eid],
            ))

        # Sort by significance (highest first)
        ranked.sort(key=lambda x: x.significance_score, reverse=True)
        return ranked

    async def _find_cross_border(self, session_id: UUID) -> list[CrossBorderFlag]:
        """Find US-CA cross-border connections."""
        query = """
        MATCH (us:Organization {jurisdiction: 'US'})-[r]->(ca:Organization {jurisdiction: 'CA'})
        WHERE r.session_id = $session_id
        RETURN us.id as us_id, us.name as us_name,
               ca.id as ca_id, ca.name as ca_name,
               type(r) as rel_type, r.amount as amount, r.evidence_ids as evidence_ids
        UNION
        MATCH (ca:Organization {jurisdiction: 'CA'})-[r]->(us:Organization {jurisdiction: 'US'})
        WHERE r.session_id = $session_id
        RETURN us.id as us_id, us.name as us_name,
               ca.id as ca_id, ca.name as ca_name,
               type(r) as rel_type, r.amount as amount, r.evidence_ids as evidence_ids
        """

        flags = []
        try:
            results = await self.graph.execute(query, {"session_id": str(session_id)})
            for row in results or []:
                evidence_ids = row.get("evidence_ids") or []
                if isinstance(evidence_ids, str):
                    evidence_ids = [evidence_ids]

                flags.append(CrossBorderFlag(
                    us_entity_id=UUID(row["us_id"]) if row.get("us_id") else uuid4(),
                    us_entity_name=row.get("us_name", "Unknown"),
                    ca_entity_id=UUID(row["ca_id"]) if row.get("ca_id") else uuid4(),
                    ca_entity_name=row.get("ca_name", "Unknown"),
                    relationship_type=row.get("rel_type", "UNKNOWN"),
                    amount=row.get("amount"),
                    evidence_ids=[UUID(eid) for eid in evidence_ids if eid],
                ))
        except Exception as e:
            logger.warning(f"Failed to find cross-border connections: {e}")

        return flags

    async def _find_unknowns(self, session_id: UUID) -> list[Unknown]:
        """Find entities that couldn't be fully traced."""
        query = """
        MATCH (e)-[:DISCOVERED_IN]->(s:ResearchSession {id: $session_id})
        WHERE e.trace_incomplete = true OR e.no_sources_found = true
        RETURN e.name as name, e.trace_reason as reason,
               e.attempted_sources as sources
        """

        unknowns = []
        try:
            results = await self.graph.execute(query, {"session_id": str(session_id)})
            for row in results or []:
                sources = row.get("sources") or []
                if isinstance(sources, str):
                    sources = [sources]

                unknowns.append(Unknown(
                    entity_name=row.get("name", "Unknown"),
                    reason=row.get("reason", "Unknown reason"),
                    attempted_sources=sources,
                ))
        except Exception as e:
            logger.warning(f"Failed to find unknowns: {e}")

        return unknowns

    async def _get_evidence_index(self, case_id: UUID) -> list[EvidenceCitation]:
        """Get all evidence citations for a case."""
        session = await self._get_session()
        result = await session.execute(
            """
            SELECT id, evidence_type, source_url, retrieved_at
            FROM evidence
            WHERE case_id = :case_id
            ORDER BY retrieved_at
            """,
            {"case_id": str(case_id)},
        )
        rows = result.fetchall()

        return [
            EvidenceCitation(
                evidence_id=UUID(row.id) if isinstance(row.id, str) else row.id,
                source_type=row.evidence_type,
                source_url=row.source_url,
                retrieved_at=row.retrieved_at,
            )
            for row in rows
        ]

    async def _count_pending_matches(self, case_id: UUID) -> int:
        """Count pending entity matches for a case."""
        session = await self._get_session()
        result = await session.execute(
            """
            SELECT COUNT(*) FROM entity_matches
            WHERE case_id = :case_id AND status = 'pending'
            """,
            {"case_id": str(case_id)},
        )
        return result.scalar() or 0

    async def _store_report(self, report: CaseReport) -> None:
        """Store a report in the database."""
        session = await self._get_session()

        # Check if report exists (upsert)
        existing = await session.execute(
            "SELECT id FROM case_reports WHERE case_id = :case_id",
            {"case_id": str(report.case_id)},
        )
        if existing.fetchone():
            # Update existing report
            await session.execute(
                """
                UPDATE case_reports SET
                    generated_at = :generated_at,
                    report_version = report_version + 1,
                    summary = :summary,
                    top_entities = :top_entities,
                    top_relationships = :top_relationships,
                    cross_border_flags = :cross_border_flags,
                    unknowns = :unknowns,
                    evidence_index = :evidence_index
                WHERE case_id = :case_id
                """,
                {
                    "case_id": str(report.case_id),
                    "generated_at": report.generated_at,
                    "summary": report.summary.model_dump(),
                    "top_entities": [e.model_dump() for e in report.top_entities],
                    "top_relationships": [r.model_dump() for r in report.top_relationships],
                    "cross_border_flags": [f.model_dump() for f in report.cross_border_flags],
                    "unknowns": [u.model_dump() for u in report.unknowns],
                    "evidence_index": [e.model_dump() for e in report.evidence_index],
                },
            )
        else:
            # Insert new report
            await session.execute(
                """
                INSERT INTO case_reports (
                    id, case_id, generated_at, report_version, summary,
                    top_entities, top_relationships, cross_border_flags,
                    unknowns, evidence_index
                ) VALUES (
                    :id, :case_id, :generated_at, :report_version, :summary,
                    :top_entities, :top_relationships, :cross_border_flags,
                    :unknowns, :evidence_index
                )
                """,
                {
                    "id": str(report.id),
                    "case_id": str(report.case_id),
                    "generated_at": report.generated_at,
                    "report_version": report.report_version,
                    "summary": report.summary.model_dump(),
                    "top_entities": [e.model_dump() for e in report.top_entities],
                    "top_relationships": [r.model_dump() for r in report.top_relationships],
                    "cross_border_flags": [f.model_dump() for f in report.cross_border_flags],
                    "unknowns": [u.model_dump() for u in report.unknowns],
                    "evidence_index": [e.model_dump() for e in report.evidence_index],
                },
            )

        await session.commit()

    async def get_report(self, case_id: UUID) -> CaseReport | None:
        """Get an existing report for a case."""
        session = await self._get_session()
        result = await session.execute(
            "SELECT * FROM case_reports WHERE case_id = :case_id",
            {"case_id": str(case_id)},
        )
        row = result.fetchone()
        if row is None:
            return None

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
        )


# Singleton instance
_generator: ReportGenerator | None = None


def get_report_generator() -> ReportGenerator:
    """Get the report generator singleton."""
    global _generator
    if _generator is None:
        _generator = ReportGenerator()
    return _generator
