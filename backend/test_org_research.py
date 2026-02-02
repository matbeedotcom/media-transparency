"""Direct test of organization research functionality."""

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

async def test_organization_research():
    print("\n" + "="*60)
    print("ORGANIZATION RESEARCH INTEGRATION TEST")
    print("="*60)
    
    # 1. Test database connections
    print("\n1. Testing Database Connections...")
    
    from mitds.db import get_session_factory, get_neo4j_driver, get_redis
    
    # PostgreSQL
    session_factory = get_session_factory()
    async with session_factory() as session:
        from sqlalchemy import text
        result = await session.execute(text("SELECT 1"))
        print("   [OK] PostgreSQL connected")
    
    # Neo4j
    driver = await get_neo4j_driver()
    async with driver.session() as neo4j_session:
        result = await neo4j_session.run("RETURN 1 as test")
        await result.single()
        print("   [OK] Neo4j connected")
    
    # Redis
    redis = await get_redis()
    await redis.set("test", "1")
    await redis.delete("test")
    print("   [OK] Redis connected")
    
    # 2. Test corporation adapter
    print("\n2. Testing Corporation Adapter...")
    from mitds.cases.adapters.corporation import CorporationAdapter
    
    adapter = CorporationAdapter()
    corp_name = "Koch Industries"
    
    validation = await adapter.validate(corp_name)
    print(f"   Validation: is_valid={validation.is_valid}")
    print(f"   Query type: {validation.metadata.get('query_type')}")
    print(f"   Query value: {validation.metadata.get('query_value')}")
    
    # 3. Test entity extraction from sample text
    print("\n3. Testing Entity Extraction...")
    from mitds.cases.extraction.deterministic import DeterministicExtractor
    
    sample_text = """
    Koch Industries Inc. (EIN: 48-0951655) is a multinational corporation 
    headquartered in Wichita, Kansas. The company owns Georgia-Pacific LLC 
    and Invista. Through the Charles Koch Foundation (BN: 123456789RR0001),
    Koch has funded policy research organizations including the Fraser Institute
    and Americans for Prosperity Foundation. Contact: info@kochind.com
    """
    
    extractor = DeterministicExtractor()
    entities = extractor.extract(sample_text)
    
    print(f"   Found {len(entities)} entities:")
    for e in entities:
        print(f"   - {e.entity_type}: {e.value[:50]}... (conf: {e.confidence})")
    
    # 4. Test case creation in database
    print("\n4. Testing Case Creation...")
    
    case_id = uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    async with session_factory() as session:
        from sqlalchemy import text
        
        # Create case
        await session.execute(
            text("""
                INSERT INTO cases (id, name, entry_point_type, entry_point_value,
                                  config, status, created_at, updated_at)
                VALUES (:id, :name, :entry_point_type, :entry_point_value,
                        :config, :status, :created_at, :updated_at)
            """),
            {
                "id": str(case_id),
                "name": "Koch Industries Research",
                "entry_point_type": "corporation",
                "entry_point_value": corp_name,
                "config": json.dumps({"max_depth": 2}),
                "status": "processing",
                "created_at": now,
                "updated_at": now,
            }
        )
        await session.commit()
        print(f"   [OK] Created case: {case_id}")
        
        # Store extracted leads
        leads_stored = 0
        for entity in entities[:10]:
            await session.execute(
                text("""
                    INSERT INTO extracted_leads (id, case_id, entity_type,
                                                extracted_value, identifier_type, 
                                                confidence, extraction_method, created_at)
                    VALUES (:id, :case_id, :entity_type, :extracted_value, 
                            :identifier_type, :confidence, :extraction_method, :created_at)
                """),
                {
                    "id": str(uuid4()),
                    "case_id": str(case_id),
                    "entity_type": entity.entity_type,
                    "extracted_value": entity.value[:255],
                    "identifier_type": entity.identifier_type,
                    "confidence": entity.confidence,
                    "extraction_method": "deterministic",
                    "created_at": now,
                }
            )
            leads_stored += 1
        await session.commit()
        print(f"   [OK] Stored {leads_stored} extracted leads")
        
        # Verify
        result = await session.execute(
            text("SELECT COUNT(*) FROM extracted_leads WHERE case_id = :case_id"),
            {"case_id": str(case_id)}
        )
        count = result.scalar()
        print(f"   [OK] Verified {count} leads in database")
    
    # 5. Test sponsor resolution setup
    print("\n5. Testing Sponsor Resolution...")
    from mitds.cases.resolution.sponsor import SponsorResolver
    
    async with driver.session() as neo4j_session:
        resolver = SponsorResolver(neo4j_session=neo4j_session)
        
        # Test identifier matching
        ein_entity = next((e for e in entities if e.identifier_type == "ein"), None)
        if ein_entity:
            print(f"   Looking up EIN: {ein_entity.value}")
            try:
                match = await resolver.resolve(ein_entity)
                if match:
                    print(f"   [OK] Found match: confidence={match.confidence}")
                else:
                    print(f"   [INFO] No existing match in graph (expected for new entity)")
            except Exception as e:
                print(f"   [INFO] Resolver test: {e}")
        
        print(f"   Auto-merge threshold: {resolver.auto_merge_threshold}")
        print(f"   Review threshold: {resolver.review_threshold}")
    
    # 6. Test data sources (ISED lookup)
    print("\n6. Testing Data Source Lookup (ISED API)...")
    try:
        from mitds.ingestion.canadian.ised import ISEDIngester
        
        ingester = ISEDIngester()
        # Just test initialization
        print(f"   [OK] ISED Ingester initialized")
        print(f"   API configured: {ingester.api_key is not None}")
    except ImportError as e:
        print(f"   [SKIP] ISED module not available: {e}")
    except Exception as e:
        print(f"   [WARN] ISED setup issue: {e}")
    
    # 7. Clean up test data
    print("\n7. Cleaning Up Test Data...")
    async with session_factory() as session:
        from sqlalchemy import text
        
        await session.execute(
            text("DELETE FROM extracted_leads WHERE case_id = :case_id"),
            {"case_id": str(case_id)}
        )
        await session.execute(
            text("DELETE FROM cases WHERE id = :id"),
            {"id": str(case_id)}
        )
        await session.commit()
        print("   [OK] Test data cleaned up")
    
    print("\n" + "="*60)
    print("ORGANIZATION RESEARCH TEST COMPLETE")
    print("="*60)
    
    # Summary
    print("\nSummary:")
    print("  - Database connections: WORKING")
    print("  - Entity extraction: WORKING")
    print("  - Case management: WORKING")
    print("  - Sponsor resolution: CONFIGURED")
    print(f"  - Entities found in sample: {len(entities)}")
    print("    * EINs: " + str(len([e for e in entities if e.identifier_type == "ein"])))
    print("    * BNs: " + str(len([e for e in entities if e.identifier_type == "bn"])))
    print("    * Orgs: " + str(len([e for e in entities if e.entity_type == "organization"])))
    print("    * Domains: " + str(len([e for e in entities if e.identifier_type == "domain"])))

if __name__ == "__main__":
    asyncio.run(test_organization_research())
