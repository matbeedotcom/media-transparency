"""Real integration tests against actual database connections.

These tests require running infrastructure:
- PostgreSQL (port 5432)
- Neo4j (port 7687)
- Redis (port 6379)
- MinIO (port 9000)

Run with: pytest tests/integration/cases/test_real_integration.py -v -s
"""

import json
import pytest
from datetime import datetime
from uuid import uuid4


class TestRealDatabaseConnections:
    """Test actual database connectivity."""

    @pytest.mark.asyncio
    async def test_postgres_connection(self):
        """Test PostgreSQL connection."""
        from mitds.db import get_engine, get_session_factory
        from sqlalchemy import text

        engine = get_engine()
        session_factory = get_session_factory()

        async with session_factory() as session:
            result = await session.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            assert row[0] == 1
            print("[PASS] PostgreSQL connection successful")

    @pytest.mark.asyncio
    async def test_neo4j_connection(self):
        """Test Neo4j connection."""
        from mitds.db import get_neo4j_driver

        driver = await get_neo4j_driver()
        async with driver.session() as session:
            result = await session.run("RETURN 1 as test")
            record = await result.single()
            assert record["test"] == 1
            print("[PASS] Neo4j connection successful")

    @pytest.mark.asyncio
    async def test_redis_connection(self):
        """Test Redis connection."""
        from mitds.db import get_redis

        redis = await get_redis()
        await redis.set("test_key", "test_value")
        value = await redis.get("test_key")
        # Redis may return string or bytes depending on config
        assert value in (b"test_value", "test_value")
        await redis.delete("test_key")
        print("[PASS] Redis connection successful")


class TestRealCaseCreation:
    """Test case creation with real databases."""

    @pytest.mark.asyncio
    async def test_create_case_in_database(self):
        """Test creating a case in the real database."""
        from mitds.db import get_session_factory
        from mitds.cases.models import CaseStatus, EntryPointType
        from sqlalchemy import text

        case_id = uuid4()
        now = datetime.utcnow()

        session_factory = get_session_factory()
        async with session_factory() as session:
            # Insert a test case
            await session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value, 
                                      config, status, created_at, updated_at)
                    VALUES (:id, :name, :entry_point_type, :entry_point_value,
                            :config, :status, :created_at, :updated_at)
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "id": str(case_id),
                    "name": "Integration Test Case",
                    "entry_point_type": EntryPointType.TEXT.value,
                    "entry_point_value": "Test text content for integration testing",
                    "config": json.dumps({"max_depth": 2}),
                    "status": CaseStatus.INITIALIZING.value,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            await session.commit()

            # Verify the case was created
            result = await session.execute(
                text("SELECT name, status FROM cases WHERE id = :id"),
                {"id": str(case_id)}
            )
            row = result.fetchone()

            assert row is not None, "Case was not created"
            assert row[0] == "Integration Test Case"
            assert row[1] == CaseStatus.INITIALIZING.value

            print(f"[PASS] Case created with ID: {case_id}")

            # Clean up
            await session.execute(
                text("DELETE FROM cases WHERE id = :id"),
                {"id": str(case_id)}
            )
            await session.commit()
            print("[PASS] Case cleaned up")


class TestRealEntityExtraction:
    """Test entity extraction with real services."""

    def test_deterministic_extraction(self):
        """Test deterministic entity extraction."""
        from mitds.cases.extraction.deterministic import DeterministicExtractor

        extractor = DeterministicExtractor()

        # Sample text with various entities
        text = """
        Koch Industries Inc. (EIN: 48-0951655) donated $5 million to 
        Americans for Prosperity Foundation (BN: 123456789RR0001).
        Contact info at koch.com or visit americansforprosperity.org.
        """

        entities = extractor.extract(text)

        # Verify extractions
        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        bn_entities = [e for e in entities if e.identifier_type == "bn"]
        org_entities = [e for e in entities if e.entity_type == "organization"]
        domain_entities = [e for e in entities if e.identifier_type == "domain"]

        print(f"\nExtracted entities:")
        for e in entities:
            print(f"  - {e.entity_type}: {e.value} ({e.identifier_type}, conf: {e.confidence})")

        assert len(ein_entities) >= 1, "Should extract EIN"
        assert ein_entities[0].value == "48-0951655"
        print("[PASS] EIN extracted correctly")

        assert len(bn_entities) >= 1, "Should extract BN"
        assert "123456789RR0001" in bn_entities[0].value.upper()
        print("[PASS] BN extracted correctly")

        assert len(org_entities) >= 1, "Should extract organizations"
        print(f"[PASS] Extracted {len(org_entities)} organization(s)")

        assert len(domain_entities) >= 1, "Should extract domains"
        print(f"[PASS] Extracted {len(domain_entities)} domain(s)")


class TestRealTextAdapter:
    """Test text adapter with real storage."""

    @pytest.mark.asyncio
    async def test_text_validation_and_extraction(self):
        """Test text adapter validation and lead extraction."""
        from mitds.cases.adapters.text import TextAdapter

        adapter = TextAdapter(enable_llm=False)

        # Test text content
        test_text = """
        PRESS RELEASE - FOR IMMEDIATE RELEASE
        
        Koch Industries Inc. announces partnership with Americans for Prosperity Foundation
        to fund policy research initiatives across North America.
        
        Contact: media@koch.com
        Tax ID: 48-0951655
        """

        # Validate
        validation = await adapter.validate(test_text)
        assert validation.is_valid is True
        assert validation.metadata["source_type"] == "press_release"
        print(f"[PASS] Text validated, source type: {validation.metadata['source_type']}")

        # Check extraction would work
        from mitds.cases.extraction.deterministic import DeterministicExtractor
        extractor = DeterministicExtractor()
        entities = extractor.extract(test_text)

        orgs = [e for e in entities if e.entity_type == "organization"]
        eins = [e for e in entities if e.identifier_type == "ein"]

        print(f"[PASS] Would extract {len(orgs)} organizations and {len(eins)} EINs")
        assert len(orgs) >= 1
        assert len(eins) >= 1


class TestRealSponsorResolution:
    """Test sponsor resolution with real Neo4j."""

    @pytest.mark.asyncio
    async def test_sponsor_resolver_initialization(self):
        """Test sponsor resolver can initialize with real Neo4j."""
        from mitds.cases.resolution.sponsor import SponsorResolver
        from mitds.db import get_neo4j_driver

        # Get a real Neo4j session
        driver = await get_neo4j_driver()
        async with driver.session() as session:
            resolver = SponsorResolver(neo4j_session=session)

            # Test threshold methods work
            assert resolver.should_auto_merge(0.95) is True
            assert resolver.should_auto_merge(0.85) is False
            assert resolver.should_queue_for_review(0.85) is True
            assert resolver.should_discard(0.5) is True

            print("[PASS] SponsorResolver initialized with real Neo4j session")
            print(f"  - Auto-merge threshold: {resolver.auto_merge_threshold}")
            print(f"  - Review threshold: {resolver.review_threshold}")


class TestRealEndToEndFlow:
    """End-to-end test with real infrastructure."""

    @pytest.mark.asyncio
    async def test_complete_text_case_flow(self):
        """Test complete flow: validate -> extract -> store."""
        from mitds.db import get_session_factory, get_neo4j_driver
        from mitds.cases.adapters.text import TextAdapter
        from mitds.cases.extraction.deterministic import DeterministicExtractor
        from mitds.cases.models import CaseStatus, EntryPointType
        from sqlalchemy import text

        print("\n=== Starting End-to-End Integration Test ===\n")

        # 1. Validate text input
        adapter = TextAdapter(enable_llm=False)
        test_text = """
        Americans for Prosperity Foundation (EIN: 45-3492655) today announced
        a $2 million grant to the Fraser Institute for policy research.
        The initiative is supported by Koch Industries Inc.
        
        For more information, visit americansforprosperity.org
        """

        validation = await adapter.validate(test_text)
        assert validation.is_valid
        print(f"1. [PASS] Text validated ({validation.metadata['char_count']} chars)")

        # 2. Extract entities
        extractor = DeterministicExtractor()
        entities = extractor.extract(test_text)

        eins = [e for e in entities if e.identifier_type == "ein"]
        orgs = [e for e in entities if e.entity_type == "organization"]
        domains = [e for e in entities if e.identifier_type == "domain"]

        print(f"2. [PASS] Extracted {len(entities)} total entities:")
        print(f"   - {len(eins)} EIN(s): {[e.value for e in eins]}")
        print(f"   - {len(orgs)} organization(s): {[e.value[:40] for e in orgs]}")
        print(f"   - {len(domains)} domain(s): {[e.value for e in domains]}")

        # 3. Create case in PostgreSQL
        case_id = uuid4()
        now = datetime.utcnow()

        session_factory = get_session_factory()
        async with session_factory() as session:
            await session.execute(
                text("""
                    INSERT INTO cases (id, name, entry_point_type, entry_point_value,
                                      config, status, created_at, updated_at)
                    VALUES (:id, :name, :entry_point_type, :entry_point_value,
                            :config, :status, :created_at, :updated_at)
                """),
                {
                    "id": str(case_id),
                    "name": "E2E Integration Test",
                    "entry_point_type": EntryPointType.TEXT.value,
                    "entry_point_value": test_text[:200],
                    "config": json.dumps({"max_depth": 2, "enable_llm": False}),
                    "status": CaseStatus.PROCESSING.value,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            await session.commit()
            print(f"3. [PASS] Case created in PostgreSQL (ID: {case_id})")

            # 4. Store extracted leads
            for i, entity in enumerate(entities[:5]):  # Store first 5
                await session.execute(
                    text("""
                        INSERT INTO extracted_leads (id, case_id, evidence_id, entity_type,
                                                    extracted_value, identifier_type, confidence,
                                                    extraction_method, created_at)
                        VALUES (:id, :case_id, :evidence_id, :entity_type,
                                :extracted_value, :identifier_type, :confidence,
                                :extraction_method, :created_at)
                    """),
                    {
                        "id": str(uuid4()),
                        "case_id": str(case_id),
                        "evidence_id": None,
                        "entity_type": entity.entity_type,
                        "extracted_value": entity.value[:255],
                        "identifier_type": entity.identifier_type,
                        "confidence": entity.confidence,
                        "extraction_method": "deterministic",
                        "created_at": now,
                    }
                )
            await session.commit()
            print(f"4. [PASS] Stored {min(5, len(entities))} leads in PostgreSQL")

            # 5. Verify data in database
            result = await session.execute(
                text("SELECT COUNT(*) FROM extracted_leads WHERE case_id = :case_id"),
                {"case_id": str(case_id)}
            )
            lead_count = result.scalar()
            assert lead_count >= 1
            print(f"5. [PASS] Verified {lead_count} leads in database")

            # 6. Test Neo4j connectivity
            driver = await get_neo4j_driver()
            async with driver.session() as neo4j_session:
                result = await neo4j_session.run(
                    "RETURN $case_id as case_id, $count as lead_count",
                    {"case_id": str(case_id), "count": lead_count}
                )
                record = await result.single()
                assert record["case_id"] == str(case_id)
                print("6. [PASS] Neo4j query executed successfully")

            # Cleanup
            await session.execute(
                text("DELETE FROM extracted_leads WHERE case_id = :case_id"),
                {"case_id": str(case_id)}
            )
            await session.execute(
                text("DELETE FROM cases WHERE id = :id"),
                {"id": str(case_id)}
            )
            await session.commit()
            print("7. [PASS] Test data cleaned up")

        print("\n=== End-to-End Integration Test Complete ===\n")
