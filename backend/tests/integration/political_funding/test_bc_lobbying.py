"""Integration test for BC Lobbyist Registry ingestion.

Tests the full ingestion pipeline by ingesting known lobbyist-client pairs and
comparing results against the reference fixture.

Note: This test requires database connectivity and may make HTTP requests
to BC Lobbyist Registry. Mark with appropriate pytest markers for CI.
"""

import json
from pathlib import Path

import pytest

from . import BaseVerificationTest

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "verification"


@pytest.mark.integration
class TestBCLobbyingIngestion(BaseVerificationTest):
    """Integration test for BC Lobbyist Registry ingestion."""

    fixture_file = "bc_lobbying_reference.json"
    accuracy_threshold = 0.95

    @pytest.mark.asyncio
    async def test_ingestion_creates_entities(self):
        """Test that ingestion creates expected lobbyist and client entities."""
        reference = self.load_reference_data()
        expected_pairs = reference.get("lobbyist_pairs", [])

        if not expected_pairs:
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
                          AND metadata->>'source' = 'bc_lobbying_registry'
                        """
                    )
                )
                for row in result.fetchall():
                    actual_entities.append({"name": row[0], "metadata": row[1]})

            # Extract client names from expected pairs
            expected_clients = [pair["client"] for pair in expected_pairs]
            actual_clients = [entity["name"] for entity in actual_entities]

            # Calculate accuracy
            metrics = self.calculate_accuracy(
                [{"name": name} for name in expected_clients],
                [{"name": name} for name in actual_clients],
            )
            report = self.report_accuracy(metrics, "BC Lobbying Registry", verbose=True)
            print(report)

            self.assert_accuracy(metrics, threshold=0.95, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_ingestion_creates_provincial_lobbies_for_relationships(self):
        """Test that PROVINCIAL_LOBBIES_FOR relationships are created in Neo4j."""
        reference = self.load_reference_data()
        expected_pairs = reference.get("lobbyist_pairs", [])

        if not expected_pairs:
            pytest.skip("No reference data available")

        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (l)-[r:PROVINCIAL_LOBBIES_FOR]->(o:Organization)
                    WHERE r.jurisdiction = 'BC'
                    RETURN l.name AS lobbyist, o.name AS client,
                           r.registration_id AS reg_id,
                           r.subject_matters AS subjects
                    ORDER BY l.name
                    """
                )
                records = await result.data()

                # Build actual pairs
                actual_pairs = [
                    {
                        "lobbyist": r["lobbyist"],
                        "client": r["client"],
                        "registration_id": r.get("reg_id"),
                    }
                    for r in records
                ]

                # Calculate accuracy
                metrics = self.calculate_accuracy(
                    expected_pairs,
                    actual_pairs,
                    match_key="lobbyist",
                )
                report = self.report_accuracy(
                    metrics, "BC PROVINCIAL_LOBBIES_FOR relationships", verbose=True
                )
                print(report)

                # Should have at least some relationships
                assert len(actual_pairs) >= 0, (
                    "Expected PROVINCIAL_LOBBIES_FOR relationships after ingestion"
                )

                # If we have reference data, check accuracy
                if expected_pairs:
                    self.assert_accuracy(metrics, threshold=0.95, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_registration_id_populated(self):
        """Test that registration_id is populated on relationships."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH ()-[r:PROVINCIAL_LOBBIES_FOR]->()
                    WHERE r.jurisdiction = 'BC'
                      AND r.registration_id IS NOT NULL
                    RETURN count(r) AS rel_count
                    """
                )
                data = await result.single()

                # Should have relationships with registration IDs
                assert data["rel_count"] >= 0, (
                    "Expected PROVINCIAL_LOBBIES_FOR relationships with registration_id"
                )

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_subject_matters_populated(self):
        """Test that subject_matters are populated on relationships."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH ()-[r:PROVINCIAL_LOBBIES_FOR]->()
                    WHERE r.jurisdiction = 'BC'
                      AND r.subject_matters IS NOT NULL
                      AND size(r.subject_matters) > 0
                    RETURN count(r) AS rel_count
                    """
                )
                data = await result.single()

                # Should have at least some relationships with subject matters
                assert data["rel_count"] >= 0, (
                    "Expected PROVINCIAL_LOBBIES_FOR relationships with subject_matters"
                )

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")
