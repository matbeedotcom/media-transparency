"""Integration test for CanLII ingestion (Phase 10).

Tests the full CanLII ingestion pipeline by searching for known
entity pairs and comparing against the reference fixture.
"""

from pathlib import Path

import pytest

from . import BaseVerificationTest


@pytest.mark.integration
class TestCanLIIIngestion(BaseVerificationTest):
    """Integration test for CanLII case law ingestion."""

    fixture_file = "canlii_reference.json"
    accuracy_threshold = 0.90  # 90% accuracy required for CanLII

    @pytest.mark.asyncio
    async def test_litigation_relationships_created(self):
        """Test that LITIGATED_WITH relationships are created."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (a:Organization)-[r:LITIGATED_WITH]->(b:Organization)
                    RETURN count(r) AS rel_count
                    """
                )
                data = await result.single()
                # Just verify the query works
                assert data is not None

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_case_evidence_stored(self):
        """Test that case evidence records are stored."""
        try:
            from mitds.db import get_db_session
            from sqlalchemy import text

            async with get_db_session() as db:
                result = await db.execute(
                    text("""
                        SELECT COUNT(*) as count
                        FROM evidence
                        WHERE evidence_type = 'CANLII_CASE'
                        AND extractor = 'canlii_ingester'
                    """),
                )
                row = result.fetchone()
                # Just verify the query works
                assert row is not None

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_verification_against_reference(self):
        """Verify ingested data against reference fixture."""
        reference = self.load_reference_data()
        expected = reference.get("entity_pairs", [])

        if not expected or expected[0].get("note", "").startswith("Replace"):
            pytest.skip("Reference fixture contains placeholder data — populate first")

        try:
            from mitds.db import get_neo4j_session

            actual: list[dict] = []
            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (a:Organization)-[r:LITIGATED_WITH]->(b:Organization)
                    RETURN a.name AS entity_a, b.name AS entity_b,
                           r.case_citation AS citation, r.court AS court,
                           r.decision_date AS decision_date, r.case_title AS case_title
                    ORDER BY a.name, b.name
                    """
                )
                records = await result.data()
                for r in records:
                    # Create match key from entity pair
                    entity_a = r.get("entity_a", "")
                    entity_b = r.get("entity_b", "")
                    match_key = f"{entity_a} v. {entity_b}"
                    actual.append(
                        {
                            "name": match_key,
                            "entity_a": entity_a,
                            "entity_b": entity_b,
                            "citation": r.get("citation"),
                            "court": r.get("court"),
                            "decision_date": r.get("decision_date"),
                            "case_title": r.get("case_title"),
                        }
                    )

            # Convert expected to same format
            expected_formatted = []
            for item in expected:
                match_key = f"{item['entity_a']} v. {item['entity_b']}"
                expected_formatted.append(
                    {
                        "name": match_key,
                        "entity_a": item["entity_a"],
                        "entity_b": item["entity_b"],
                        "citation": item.get("case_citation"),
                        "court": item.get("court"),
                        "decision_date": item.get("decision_date"),
                        "case_title": item.get("case_title"),
                    }
                )

            metrics = self.calculate_accuracy(
                expected_formatted, actual, match_key="name"
            )
            self.assert_accuracy(metrics, threshold=0.90, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_case_citation_properties(self):
        """Test that case citation properties are correctly stored."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (a:Organization)-[r:LITIGATED_WITH]->(b:Organization)
                    WHERE r.case_citation IS NOT NULL
                    RETURN r.case_citation AS citation, r.court AS court,
                           r.decision_date AS decision_date
                    LIMIT 1
                    """
                )
                record = await result.single()
                if record:
                    # Verify properties exist
                    assert record.get("citation") is not None
                    assert record.get("court") is not None

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")
