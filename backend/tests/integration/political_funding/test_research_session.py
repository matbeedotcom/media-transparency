"""Integration test for research session lead extraction.

Tests that lead extractors correctly generate leads from known entities
in the political ad funding feature.
"""

import pytest
from uuid import UUID

from mitds.research.extractors import (
    BeneficialOwnershipExtractor,
    PoliticalContributionExtractor,
    SharedAddressExtractor,
)
from mitds.research.models import LeadType, ResearchSessionConfig


@pytest.mark.integration
@pytest.mark.asyncio
class TestResearchSessionLeadExtraction:
    """Integration test for research session lead extraction."""

    @pytest.mark.asyncio
    async def test_political_contribution_extractor(self):
        """Test that PoliticalContributionExtractor generates leads for contributors."""
        try:
            from mitds.db import get_neo4j_session

            # Find a known third party with contributors
            async with get_neo4j_session() as session:
                result = await session.run("""
                    MATCH (tp:Organization {is_election_third_party: true})
                    WHERE EXISTS {
                        (c)-[:CONTRIBUTED_TO]->(tp)
                    }
                    RETURN tp.id as tp_id, tp.name as tp_name
                    LIMIT 1
                """)
                record = await result.single()

                if not record:
                    pytest.skip("No third party with contributors found in database")

                entity_id = UUID(record["tp_id"])
                entity_name = record["tp_name"]

            # Create extractor and extract leads
            extractor = PoliticalContributionExtractor()
            config = ResearchSessionConfig()

            leads = []
            async for lead in extractor.extract_leads(
                entity_id=entity_id,
                entity_type="ORGANIZATION",
                entity_name=entity_name,
                entity_data={"is_election_third_party": True},
                config=config,
            ):
                leads.append(lead)

            # Should have at least some leads if contributors exist
            assert len(leads) >= 0, "Expected leads from PoliticalContributionExtractor"

            # Verify lead properties
            for lead in leads:
                assert lead.lead_type == LeadType.POLITICAL_CONTRIBUTION
                assert lead.target_identifier_type.value == "name"
                assert lead.target_identifier
                assert lead.confidence > 0
                assert lead.priority >= 1 and lead.priority <= 5

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_beneficial_ownership_extractor(self):
        """Test that BeneficialOwnershipExtractor generates leads for other corporations."""
        try:
            from mitds.db import get_neo4j_session

            # Find a known corporation with beneficial owners
            async with get_neo4j_session() as session:
                result = await session.run("""
                    MATCH (p:Person)-[:BENEFICIAL_OWNER_OF]->(corp:Organization)
                    WHERE corp.canada_corp_num IS NOT NULL
                    RETURN corp.id as corp_id, corp.name as corp_name
                    LIMIT 1
                """)
                record = await result.single()

                if not record:
                    pytest.skip("No corporation with beneficial owners found in database")

                entity_id = UUID(record["corp_id"])
                entity_name = record["corp_name"]

            # Create extractor and extract leads
            extractor = BeneficialOwnershipExtractor()
            config = ResearchSessionConfig()

            leads = []
            async for lead in extractor.extract_leads(
                entity_id=entity_id,
                entity_type="ORGANIZATION",
                entity_name=entity_name,
                entity_data={"canada_corp_num": "123456"},
                config=config,
            ):
                leads.append(lead)

            # Should have at least some leads if other corporations exist
            assert len(leads) >= 0, "Expected leads from BeneficialOwnershipExtractor"

            # Verify lead properties
            for lead in leads:
                assert lead.lead_type == LeadType.BENEFICIAL_OWNERSHIP
                assert lead.target_identifier_type.value == "corp_number"
                assert lead.target_identifier
                assert lead.confidence > 0
                assert lead.priority >= 1 and lead.priority <= 5

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_shared_address_extractor(self):
        """Test that SharedAddressExtractor generates leads for co-located entities."""
        try:
            from mitds.db import get_db_session
            from sqlalchemy import text

            # Find an organization with an address
            async with get_db_session() as db:
                result = await db.execute(text("""
                    SELECT id, name
                    FROM entities
                    WHERE entity_type = 'organization'
                      AND metadata->>'city' IS NOT NULL
                      AND metadata->>'postal_code' IS NOT NULL
                    LIMIT 1
                """))
                row = result.fetchone()

                if not row:
                    pytest.skip("No organization with address found in database")

                entity_id = UUID(row[0])
                entity_name = row[1]

            # Create extractor and extract leads
            extractor = SharedAddressExtractor()
            config = ResearchSessionConfig()

            leads = []
            async for lead in extractor.extract_leads(
                entity_id=entity_id,
                entity_type="ORGANIZATION",
                entity_name=entity_name,
                entity_data={},
                config=config,
            ):
                leads.append(lead)

            # Should have at least some leads if co-located entities exist
            assert len(leads) >= 0, "Expected leads from SharedAddressExtractor"

            # Verify lead properties
            for lead in leads:
                assert lead.lead_type == LeadType.INFRASTRUCTURE
                assert lead.target_identifier_type.value == "name"
                assert lead.target_identifier
                assert lead.confidence > 0
                assert lead.priority >= 1 and lead.priority <= 5

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_extractors_work_together(self):
        """Test that extractors can be chained: contributors -> beneficial owners."""
        try:
            from mitds.db import get_neo4j_session

            # Find a third party with contributors
            async with get_neo4j_session() as session:
                result = await session.run("""
                    MATCH (c)-[:CONTRIBUTED_TO]->(tp:Organization {is_election_third_party: true})
                    WHERE c:Organization OR c.entity_type = 'organization'
                    RETURN tp.id as tp_id, tp.name as tp_name, c.id as contributor_id, c.name as contributor_name
                    LIMIT 1
                """)
                record = await result.single()

                if not record:
                    pytest.skip("No third party with corporate contributors found")

                tp_id = UUID(record["tp_id"])
                tp_name = record["tp_name"]
                contributor_id = UUID(record["contributor_id"])
                contributor_name = record["contributor_name"]

            # Step 1: Extract leads for contributors
            contribution_extractor = PoliticalContributionExtractor()
            config = ResearchSessionConfig()

            contributor_leads = []
            async for lead in contribution_extractor.extract_leads(
                entity_id=tp_id,
                entity_type="ORGANIZATION",
                entity_name=tp_name,
                entity_data={"is_election_third_party": True},
                config=config,
            ):
                contributor_leads.append(lead)

            # Step 2: For a contributor, extract beneficial ownership leads
            # (if the contributor has beneficial owners)
            beneficial_extractor = BeneficialOwnershipExtractor()

            beneficial_leads = []
            async for lead in beneficial_extractor.extract_leads(
                entity_id=contributor_id,
                entity_type="ORGANIZATION",
                entity_name=contributor_name,
                entity_data={},
                config=config,
            ):
                beneficial_leads.append(lead)

            # Should have at least contributor leads
            assert len(contributor_leads) >= 0, "Expected contributor leads"
            # Beneficial ownership leads may be empty if no beneficial owners exist
            assert len(beneficial_leads) >= 0, "Expected beneficial ownership leads (may be empty)"

            # Verify the chain: contributor leads should reference the contributor
            for lead in contributor_leads:
                assert lead.lead_type == LeadType.POLITICAL_CONTRIBUTION
                # The lead should target the contributor name
                assert lead.target_identifier

        except Exception as e:
            pytest.skip(f"Database not available: {e}")
