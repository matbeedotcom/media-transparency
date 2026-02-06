"""Integration tests for Funding Chain Report generation."""

import pytest
from uuid import UUID, uuid4

from mitds.cases.reports.funding_chain import FundingChainReportGenerator


@pytest.mark.integration
@pytest.mark.asyncio
class TestFundingChainReport:
    """Test funding chain report generation."""

    async def test_generate_report_json(self, db_session, neo4j_session):
        """Test generating a JSON report for a case with known connections."""
        # Create a test case with research session
        from sqlalchemy import text

        case_id = uuid4()
        session_id = uuid4()

        # Create case in PostgreSQL
        async with db_session.begin():
            await db_session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value, 
                                      status, research_session_id, created_at)
                    VALUES (:id, :name, :type, :value, :status, :session_id, NOW())
                """),
                {
                    "id": case_id,
                    "name": "Test Funding Chain Case",
                    "type": "meta_ad",
                    "value": "Test Ad",
                    "status": "completed",
                    "session_id": session_id,
                }
            )

        # Create test graph in Neo4j
        try:
            async with neo4j_session:
                # Create ad -> advertiser -> contributor -> funder chain
                await neo4j_session.run(
                    """
                    CREATE (ad:Ad {id: $ad_id, name: 'Test Ad', meta_ad_id: 'test-ad-1'})
                    CREATE (advertiser:Organization {id: $advertiser_id, name: 'Test Advertiser', 
                                                      entity_type: 'organization', jurisdiction: 'CA'})
                    CREATE (contributor:Organization {id: $contributor_id, name: 'Test Contributor',
                                                      entity_type: 'organization', jurisdiction: 'CA'})
                    CREATE (funder:Organization {id: $funder_id, name: 'Test Funder',
                                                 entity_type: 'corporation', jurisdiction: 'CA'})
                    
                    CREATE (ad)-[:SPONSORED_BY {confidence: 0.95, source: 'meta_ads'}]->(advertiser)
                    CREATE (advertiser)-[:CONTRIBUTED_TO {confidence: 0.85, source: 'elections_canada',
                                                          amount: 10000.0}]->(contributor)
                    CREATE (contributor)-[:FUNDED_BY {confidence: 0.90, source: 'irs990',
                                                      amount: 50000.0}]->(funder)
                    """,
                    ad_id=str(uuid4()),
                    advertiser_id=str(uuid4()),
                    contributor_id=str(uuid4()),
                    funder_id=str(uuid4()),
                )

        except Exception as e:
            pytest.skip(f"Neo4j not available: {e}")

        # Generate report
        generator = FundingChainReportGenerator()
        report = await generator.generate(case_id, format="json")

        # Verify report structure
        assert report is not None
        assert "case_id" in report
        assert "generated_at" in report
        assert "summary" in report
        assert "funding_chains" in report
        assert "cross_border_flags" in report
        assert "evidence_index" in report

        # Verify summary
        summary = report["summary"]
        assert "entry_point" in summary
        assert "total_entities" in summary
        assert "total_relationships" in summary
        assert "sources_queried" in summary
        assert "sources_with_results" in summary
        assert "sources_without_results" in summary

        # Verify funding chains structure
        chains = report["funding_chains"]
        assert isinstance(chains, list)
        
        if chains:
            chain = chains[0]
            assert "chain_id" in chain
            assert "overall_confidence" in chain
            assert "corroboration_count" in chain
            assert "links" in chain
            assert isinstance(chain["links"], list)
            
            if chain["links"]:
                link = chain["links"][0]
                assert "from_entity" in link
                assert "to_entity" in link
                assert "relationship_type" in link
                assert "confidence" in link
                assert "evidence_type" in link
                assert "evidence_sources" in link
                assert link["evidence_type"] in ["proven", "corroborated", "inferred"]

    async def test_generate_report_markdown(self, db_session, neo4j_session):
        """Test generating a markdown report."""
        from sqlalchemy import text

        case_id = uuid4()

        # Create case
        async with db_session.begin():
            await db_session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value, 
                                      status, created_at)
                    VALUES (:id, :name, :type, :value, :status, NOW())
                """),
                {
                    "id": case_id,
                    "name": "Test Case",
                    "type": "meta_ad",
                    "value": "Test Ad",
                    "status": "completed",
                }
            )

        # Generate markdown report
        generator = FundingChainReportGenerator()
        markdown = await generator.generate(case_id, format="markdown")

        # Verify markdown structure
        assert isinstance(markdown, str)
        assert "# Funding Chain Report" in markdown
        assert "## Summary" in markdown
        assert "## Funding Chains" in markdown
        assert "## Cross-Border Flags" in markdown
        assert "## Evidence Index" in markdown

    async def test_report_with_no_session(self, db_session):
        """Test report generation for case without research session."""
        from sqlalchemy import text

        case_id = uuid4()

        # Create case without research session
        async with db_session.begin():
            await db_session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value, 
                                      status, created_at)
                    VALUES (:id, :name, :type, :value, :status, NOW())
                """),
                {
                    "id": case_id,
                    "name": "Test Case",
                    "type": "meta_ad",
                    "value": "Test Ad",
                    "status": "completed",
                }
            )

        # Generate report
        generator = FundingChainReportGenerator()
        report = await generator.generate(case_id, format="json")

        # Should return empty chains but valid structure
        assert report is not None
        assert report["funding_chains"] == []
        assert report["summary"]["total_entities"] == 0
        assert report["summary"]["total_relationships"] == 0

    async def test_report_coverage(self, db_session, neo4j_session):
        """Test that report covers all expected sections."""
        from sqlalchemy import text

        case_id = uuid4()

        # Create case with evidence
        async with db_session.begin():
            await db_session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value, 
                                      status, created_at)
                    VALUES (:id, :name, :type, :value, :status, NOW())
                """),
                {
                    "id": case_id,
                    "name": "Test Case",
                    "type": "meta_ad",
                    "value": "Test Ad",
                    "status": "completed",
                }
            )
            
            # Add evidence
            evidence_id = uuid4()
            await db_session.execute(
                text("""
                    INSERT INTO evidence (id, case_id, evidence_type, source_url, retrieved_at)
                    VALUES (:id, :case_id, :type, :url, NOW())
                """),
                {
                    "id": evidence_id,
                    "case_id": case_id,
                    "type": "meta_ads",
                    "url": "https://example.com/ad",
                }
            )

        # Generate report
        generator = FundingChainReportGenerator()
        report = await generator.generate(case_id, format="json")

        # Verify all sections present
        assert "case_id" in report
        assert "generated_at" in report
        assert "summary" in report
        assert "funding_chains" in report
        assert "cross_border_flags" in report
        assert "evidence_index" in report
        
        # Verify evidence index includes our evidence
        evidence = report["evidence_index"]
        assert len(evidence) >= 1
        assert any(ev.get("source") == "meta_ads" for ev in evidence)
