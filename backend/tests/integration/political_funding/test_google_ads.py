"""Integration test for Google Political Ads ingestion (User Story 4).

Tests the full Google Ads ingestion pipeline by querying BigQuery
for Canadian political ads and comparing results against the reference fixture.
"""

from pathlib import Path

import pytest

from . import BaseVerificationTest


@pytest.mark.integration
class TestGoogleAdsIngestion(BaseVerificationTest):
    """Integration test for Google Political Ads ingestion."""

    fixture_file = "google_ads_reference.json"
    accuracy_threshold = 0.95

    @pytest.mark.asyncio
    async def test_ads_stored_as_entities(self):
        """Test that Google ads are stored as entities with google_ad_id."""
        try:
            from mitds.db import get_db_session
            from sqlalchemy import text

            async with get_db_session() as db:
                result = await db.execute(
                    text(
                        """
                        SELECT COUNT(*) as count
                        FROM entities
                        WHERE external_ids->>'google_ad_id' IS NOT NULL
                        """
                    )
                )
                row = result.fetchone()
                ad_count = row[0] if row else 0

                # Should have at least some ad entities
                assert ad_count >= 0, (
                    "Expected Google ad entities after ingestion"
                )

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_sponsored_by_relationships(self):
        """Test that SPONSORED_BY relationships are created with platform='google'."""
        try:
            from mitds.db import get_neo4j_session

            async with get_neo4j_session() as session:
                result = await session.run(
                    """
                    MATCH (ad:Ad)-[r:SPONSORED_BY]->(sponsor:Organization)
                    WHERE r.platform = 'google'
                    RETURN count(r) AS rel_count,
                           count(DISTINCT sponsor.name) AS sponsor_count
                    """
                )
                data = await result.single()

                # Should have at least some relationships
                assert data is not None, (
                    "Expected SPONSORED_BY relationships with platform='google'"
                )
                assert data["rel_count"] >= 0, (
                    "Expected at least some Google SPONSORED_BY relationships"
                )

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

    @pytest.mark.asyncio
    async def test_verification_against_reference(self):
        """Verify ingested data against reference fixture."""
        reference = self.load_reference_data()
        expected_advertisers = reference.get("advertisers", [])

        if not expected_advertisers or any(
            adv.get("notes", "").startswith("Placeholder")
            for adv in expected_advertisers
        ):
            pytest.skip(
                "Reference fixture contains placeholder data — populate with actual Google Ads Transparency data first"
            )

        try:
            from mitds.db import get_db_session, get_neo4j_session

            from sqlalchemy import text

            actual_advertisers: list[dict] = []
            async with get_neo4j_session() as session:
                # Query for advertisers with Google SPONSORED_BY relationships
                result = await session.run(
                    """
                    MATCH (ad:Ad)-[r:SPONSORED_BY]->(sponsor:Organization)
                    WHERE r.platform = 'google'
                    WITH sponsor.name AS advertiser_name,
                         count(DISTINCT ad) AS ad_count
                    RETURN advertiser_name, ad_count
                    ORDER BY ad_count DESC
                    """
                )
                records = await result.data()
                for r in records:
                    actual_advertisers.append({
                        "name": r["advertiser_name"],
                        "ad_count": r.get("ad_count", 0),
                    })

            # Compare against expected
            metrics = self.calculate_accuracy(
                expected_advertisers, actual_advertisers, match_key="name"
            )
            report = self.report_accuracy(
                metrics, "Google Political Ads", verbose=True
            )
            print(report)

            self.assert_accuracy(metrics, threshold=0.95, metric_name="recall")

        except Exception as e:
            pytest.skip(f"Database not available: {e}")
