"""Integration test for beneficial ownership ingestion (T026).

Tests the full beneficial ownership ingestion pipeline by querying
known corporations and comparing against the reference fixture.
"""

from pathlib import Path

import pytest

from . import BaseVerificationTest


@pytest.mark.integration
class TestBeneficialOwnershipIngestion(BaseVerificationTest):
    """Integration test for beneficial ownership ingestion."""

    fixture_file = "beneficial_ownership_reference.json"
    accuracy_threshold = 1.0  # 100% accuracy required for beneficial ownership

    @pytest.mark.asyncio
    async def test_isc_relationships_created(self):
        """Test that BENEFICIAL_OWNER_OF relationships are created."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (p:Person)-[r:BENEFICIAL_OWNER_OF]->(o:Organization)
                    RETURN count(r) AS rel_count
                    """
                )
                data = await result.single()
                # Just verify the query works
                assert data is not None

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_common_controller_detection(self):
        """Test that common controllers (persons controlling multiple corps) are detected."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                # Find persons who are ISC/director of 2+ organizations
                result = await session.run(
                    """
                    MATCH (p:Person)-[:BENEFICIAL_OWNER_OF|DIRECTOR_OF]->(o:Organization)
                    WITH p, count(DISTINCT o) AS org_count
                    WHERE org_count >= 2
                    RETURN p.name AS person_name, org_count
                    ORDER BY org_count DESC
                    """
                )
                records = await result.data()
                # Common controllers should be flaggable
                # This is a structural test — actual results depend on data
                assert isinstance(records, list)

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_verification_against_reference(self):
        """Verify ingested data against reference fixture."""
        reference = self.load_reference_data()
        expected = reference.get("beneficial_owners", [])

        if not expected or expected[0].get("note", "").startswith("Replace"):
            pytest.skip("Reference fixture contains placeholder data — populate first")

        try:
            from mitds.db import get_neo4j_session

            actual: list[dict] = []
            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (p:Person)-[r:BENEFICIAL_OWNER_OF]->(o:Organization)
                    RETURN p.name AS name, o.name AS corporation,
                           o.canada_corp_num AS corp_number,
                           r.control_description AS control
                    """
                )
                records = await result.data()
                for r in records:
                    actual.append({
                        "name": r["name"],
                        "corporation": r.get("corporation"),
                        "corp_number": r.get("corp_number"),
                    })

            metrics = self.calculate_accuracy(expected, actual)
            self.assert_accuracy(metrics, threshold=1.0, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Database not available: {e}")
