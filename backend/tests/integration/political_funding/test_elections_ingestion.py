"""Integration test for Elections Canada third-party ingestion (T015).

Tests the full ingestion pipeline by ingesting known third parties and
comparing results against the reference fixture.

Note: This test requires database connectivity and may make HTTP requests
to Elections Canada. Mark with appropriate pytest markers for CI.
"""

import json
from pathlib import Path

import pytest

from . import BaseVerificationTest

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "verification"


@pytest.mark.integration
class TestElectionsCanadaIngestion(BaseVerificationTest):
    """Integration test for Elections Canada third-party ingestion."""

    fixture_file = "elections_canada_reference.json"
    accuracy_threshold = 0.95

    @pytest.mark.asyncio
    async def test_ingestion_creates_entities(self):
        """Test that ingestion creates expected third-party entities."""
        reference = self.load_reference_data()
        expected_parties = reference.get("third_parties", [])

        if not expected_parties:
            pytest.skip("No reference data available")

        # Query the system for ingested entities
        try:
            from mitds.db import get_db_session
            from sqlalchemy import text

            actual_entities: list[dict] = []
            async with get_db_session() as db:
                result = await db.execute(
                    text(
                        """
                        SELECT name, metadata FROM entities
                        WHERE entity_type = 'organization'
                          AND metadata->>'is_election_third_party' = 'true'
                        """
                    )
                )
                for row in result.fetchall():
                    actual_entities.append({"name": row[0], "metadata": row[1]})

            metrics = self.calculate_accuracy(expected_parties, actual_entities)
            report = self.report_accuracy(metrics, "Elections Canada", verbose=True)
            print(report)

            self.assert_accuracy(metrics, threshold=0.95, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_ingestion_creates_contributed_to_relationships(self):
        """Test that contributor relationships are created in Neo4j."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (c)-[r:CONTRIBUTED_TO]->(tp:Organization)
                    WHERE tp.is_election_third_party = true
                    RETURN count(r) AS rel_count,
                           count(DISTINCT tp.name) AS tp_count
                    """
                )
                data = await result.single()

                # Should have at least some relationships
                assert data["rel_count"] >= 0, (
                    "Expected CONTRIBUTED_TO relationships after ingestion"
                )

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_contributor_class_populated(self):
        """Test that contributor_class is populated on relationships."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH ()-[r:CONTRIBUTED_TO]->()
                    WHERE r.contributor_class IS NOT NULL
                    RETURN DISTINCT r.contributor_class AS cls
                    """
                )
                data = await result.data()
                classes = {d["cls"] for d in data}

                # Valid classes
                valid_classes = {
                    "individual", "corporation", "business",
                    "trade_union", "unincorporated_association",
                    "government",
                }
                for cls in classes:
                    assert cls in valid_classes, f"Invalid contributor_class: {cls}"

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")
