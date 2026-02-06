"""Funding Chain Report Generator.

Traces paths from political ads to corporate funders, ranking chains by
confidence × corroboration count and classifying evidence types.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from ...db import get_db_session, get_neo4j_session
from ...logging import get_context_logger

logger = get_context_logger(__name__)


class FundingChainReportGenerator:
    """Generates funding chain reports tracing ad-to-funder paths."""

    def __init__(self):
        """Initialize the generator."""
        self.logger = logger

    async def generate(self, case_id: UUID, format: str = "json") -> dict:
        """Generate a funding chain report for a case.

        Args:
            case_id: UUID of the case
            format: Output format ("json" or "markdown")

        Returns:
            Report data as dict (JSON) or markdown string
        """
        self.logger.info(f"Generating funding chain report for case {case_id}")

        # Get case details
        async with get_db_session() as db:
            from sqlalchemy import text

            case_query = text("""
                SELECT id, name, entry_point_type, entry_point_value, research_session_id
                FROM cases
                WHERE id = :case_id
            """)
            result = await db.execute(case_query, {"case_id": case_id})
            case_row = result.fetchone()

            if not case_row:
                raise ValueError(f"Case {case_id} not found")

            case_data = {
                "id": case_row.id,
                "name": case_row.name,
                "entry_point_type": case_row.entry_point_type,
                "entry_point_value": case_row.entry_point_value,
                "research_session_id": case_row.research_session_id,
            }

        # Query Neo4j for funding chains
        funding_chains = await self._query_funding_chains(case_data)
        
        # Build summary
        summary = await self._build_summary(case_data, funding_chains)
        
        # Identify cross-border flags
        cross_border_flags = await self._find_cross_border_flags(case_data)
        
        # Build evidence index
        evidence_index = await self._build_evidence_index(case_data)

        # Build report structure
        report = {
            "case_id": str(case_id),
            "generated_at": datetime.utcnow().isoformat(),
            "summary": summary,
            "funding_chains": funding_chains,
            "cross_border_flags": cross_border_flags,
            "evidence_index": evidence_index,
        }

        if format == "markdown":
            return self._to_markdown(report)
        
        return report

    async def _query_funding_chains(self, case_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Query Neo4j for paths from political ads to corporate funders.

        Returns chains ranked by confidence × corroboration count.
        """
        session_id = case_data.get("research_session_id")
        if not session_id:
            self.logger.warning(f"Case {case_data['id']} has no research session")
            return []

        chains = []

        try:
            async with get_neo4j_session() as session:
                # Find paths from ads (SPONSORED_BY) through contributors (CONTRIBUTED_TO)
                # to corporate funders (FUNDED_BY, OWNS, etc.)
                query = """
                // Find all paths from ads to corporate funders
                MATCH path = (ad:Ad)-[:SPONSORED_BY]->(advertiser:Organization)
                    -[:CONTRIBUTED_TO|FUNDED_BY*1..5]->(funder:Organization)
                WHERE funder.entity_type IN ['corporation', 'organization']
                  AND (funder.jurisdiction = 'US' OR funder.jurisdiction = 'CA')
                
                // Extract path details
                WITH path, ad, advertiser, funder,
                     [r IN relationships(path) | type(r)] as rel_types,
                     [r IN relationships(path) | r.confidence] as confidences,
                     [r IN relationships(path) | r.source] as sources,
                     [n IN nodes(path)[1..-1] | n] as intermediaries
                
                // Calculate chain metrics
                WITH ad, advertiser, funder, rel_types, confidences, sources, intermediaries,
                     reduce(total = 1.0, c IN confidences | total * COALESCE(c, 0.5)) as overall_confidence,
                     size([s IN sources WHERE s IS NOT NULL]) as corroboration_count
                
                // Build chain links
                WITH ad, advertiser, funder, rel_types, confidences, sources, intermediaries,
                     overall_confidence, corroboration_count,
                     [i IN range(0, size(rel_types)-1) |
                      {
                        from_entity: CASE WHEN i = 0 THEN advertiser.id ELSE nodes(path)[i].id END,
                        to_entity: CASE WHEN i = size(rel_types)-1 THEN funder.id ELSE nodes(path)[i+1].id END,
                        relationship_type: rel_types[i],
                        confidence: confidences[i],
                        evidence_type: CASE 
                          WHEN sources[i] IS NOT NULL AND confidences[i] >= 0.9 THEN 'proven'
                          WHEN sources[i] IS NOT NULL THEN 'corroborated'
                          ELSE 'inferred'
                        END,
                        evidence_sources: CASE WHEN sources[i] IS NOT NULL THEN [sources[i]] ELSE [] END
                      }
                     ] as links
                
                // Rank by confidence × corroboration
                WITH ad, advertiser, funder, links, overall_confidence, corroboration_count,
                     overall_confidence * (1.0 + corroboration_count * 0.2) as rank_score
                
                RETURN 
                  id(path) as chain_id,
                  ad.id as ad_id,
                  ad.name as ad_name,
                  advertiser.id as advertiser_id,
                  advertiser.name as advertiser_name,
                  funder.id as funder_id,
                  funder.name as funder_name,
                  links,
                  overall_confidence,
                  corroboration_count,
                  rank_score
                
                ORDER BY rank_score DESC
                LIMIT 50
                """

                result = await session.run(query)
                records = await result.data()

                for idx, record in enumerate(records):
                    chain = {
                        "chain_id": f"chain-{idx+1}",
                        "overall_confidence": float(record.get("overall_confidence", 0.5)),
                        "corroboration_count": int(record.get("corroboration_count", 0)),
                        "links": record.get("links", []),
                    }
                    chains.append(chain)

        except Exception as e:
            self.logger.error(f"Failed to query funding chains: {e}", exc_info=True)
            return []

        return chains

    async def _build_summary(
        self, case_data: dict[str, Any], chains: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Build report summary statistics."""
        # Count unique entities and relationships
        entity_ids = set()
        relationship_count = 0
        sources_queried = set()
        sources_with_results = set()

        for chain in chains:
            for link in chain.get("links", []):
                entity_ids.add(link.get("from_entity"))
                entity_ids.add(link.get("to_entity"))
                relationship_count += 1
                
                for source in link.get("evidence_sources", []):
                    sources_queried.add(source)
                    sources_with_results.add(source)

        # Determine sources queried vs sources with results
        all_sources = {"elections_canada", "irs990", "cra", "beneficial_ownership", 
                      "google_ads", "meta_ads", "lobbying_bc"}
        sources_without_results = all_sources - sources_with_results

        return {
            "entry_point": case_data.get("entry_point_value", "Unknown"),
            "total_entities": len(entity_ids),
            "total_relationships": relationship_count,
            "sources_queried": sorted(list(sources_queried)),
            "sources_with_results": sorted(list(sources_with_results)),
            "sources_without_results": sorted(list(sources_without_results)),
        }

    async def _find_cross_border_flags(
        self, case_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Find cross-border US-CA connections."""
        session_id = case_data.get("research_session_id")
        if not session_id:
            return []

        flags = []

        try:
            async with get_neo4j_session() as session:
                query = """
                // Find relationships crossing US-CA border
                MATCH (us:Organization)-[r:FUNDED_BY|OWNS|CONTRIBUTED_TO]->(ca:Organization)
                WHERE us.jurisdiction = 'US' AND ca.jurisdiction = 'CA'
                
                RETURN 
                  us.id as us_entity_id,
                  us.name as us_entity_name,
                  ca.id as ca_entity_id,
                  ca.name as ca_entity_name,
                  type(r) as relationship_type,
                  r.amount as amount,
                  r.confidence as confidence
                
                ORDER BY r.confidence DESC
                LIMIT 20
                """

                result = await session.run(query)
                records = await result.data()

                for record in records:
                    flags.append({
                        "us_entity_id": record.get("us_entity_id"),
                        "us_entity_name": record.get("us_entity_name"),
                        "ca_entity_id": record.get("ca_entity_id"),
                        "ca_entity_name": record.get("ca_entity_name"),
                        "relationship_type": record.get("relationship_type"),
                        "amount": float(record.get("amount", 0)) if record.get("amount") else None,
                        "confidence": float(record.get("confidence", 0.5)),
                    })

        except Exception as e:
            self.logger.error(f"Failed to find cross-border flags: {e}", exc_info=True)
            return []

        return flags

    async def _build_evidence_index(
        self, case_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Build evidence index from case evidence."""
        async with get_db_session() as db:
            from sqlalchemy import text

            query = text("""
                SELECT id, evidence_type, source_url, retrieved_at
                FROM evidence
                WHERE case_id = :case_id
                ORDER BY retrieved_at DESC
            """)
            result = await db.execute(query, {"case_id": case_data["id"]})
            rows = result.fetchall()

            evidence = []
            for row in rows:
                evidence.append({
                    "source": row.evidence_type or "unknown",
                    "type": row.evidence_type or "unknown",
                    "url": row.source_url,
                    "retrieved_at": row.retrieved_at.isoformat() if row.retrieved_at else None,
                })

            return evidence

    def _to_markdown(self, report: dict[str, Any]) -> str:
        """Convert report to markdown format."""
        lines = []
        
        # Header
        lines.append("# Funding Chain Report")
        lines.append("")
        lines.append(f"**Case ID:** {report['case_id']}")
        lines.append(f"**Generated:** {report['generated_at']}")
        lines.append("")

        # Summary
        summary = report.get("summary", {})
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- **Entry Point:** {summary.get('entry_point', 'Unknown')}")
        lines.append(f"- **Total Entities:** {summary.get('total_entities', 0)}")
        lines.append(f"- **Total Relationships:** {summary.get('total_relationships', 0)}")
        lines.append("")
        lines.append("### Sources")
        lines.append(f"- **Queried:** {', '.join(summary.get('sources_queried', []))}")
        lines.append(f"- **With Results:** {', '.join(summary.get('sources_with_results', []))}")
        lines.append(f"- **Without Results:** {', '.join(summary.get('sources_without_results', []))}")
        lines.append("")

        # Funding Chains
        chains = report.get("funding_chains", [])
        lines.append("## Funding Chains")
        lines.append("")
        if not chains:
            lines.append("*No funding chains found.*")
        else:
            for chain in chains:
                lines.append(f"### Chain {chain.get('chain_id', 'unknown')}")
                lines.append("")
                lines.append(f"- **Confidence:** {chain.get('overall_confidence', 0):.2%}")
                lines.append(f"- **Corroboration Count:** {chain.get('corroboration_count', 0)}")
                lines.append("")
                lines.append("#### Links")
                lines.append("")
                for link in chain.get("links", []):
                    evidence_type = link.get("evidence_type", "unknown")
                    confidence = link.get("confidence", 0)
                    sources = ", ".join(link.get("evidence_sources", []))
                    lines.append(
                        f"- **{link.get('from_entity')}** → "
                        f"**{link.get('to_entity')}** "
                        f"({link.get('relationship_type', 'unknown')})"
                    )
                    lines.append(
                        f"  - Confidence: {confidence:.2%} | "
                        f"Evidence: {evidence_type} | "
                        f"Sources: {sources or 'none'}"
                    )
                lines.append("")

        # Cross-Border Flags
        flags = report.get("cross_border_flags", [])
        lines.append("## Cross-Border Flags")
        lines.append("")
        if not flags:
            lines.append("*No cross-border connections found.*")
        else:
            for flag in flags:
                lines.append(
                    f"- **{flag.get('us_entity_name')}** (US) → "
                    f"**{flag.get('ca_entity_name')}** (CA) "
                    f"({flag.get('relationship_type', 'unknown')})"
                )
                if flag.get("amount"):
                    lines.append(f"  - Amount: ${flag.get('amount'):,.2f}")
                lines.append(f"  - Confidence: {flag.get('confidence', 0):.2%}")
                lines.append("")

        # Evidence Index
        evidence = report.get("evidence_index", [])
        lines.append("## Evidence Index")
        lines.append("")
        if not evidence:
            lines.append("*No evidence records found.*")
        else:
            for ev in evidence:
                lines.append(f"- **{ev.get('source', 'unknown')}** ({ev.get('type', 'unknown')})")
                if ev.get("url"):
                    lines.append(f"  - URL: {ev.get('url')}")
                if ev.get("retrieved_at"):
                    lines.append(f"  - Retrieved: {ev.get('retrieved_at')}")
                lines.append("")

        return "\n".join(lines)
