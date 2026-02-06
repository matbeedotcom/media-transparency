"""Test Canadian corporation research functionality."""

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4


async def test_canadian_corp():
    print("\n" + "=" * 60)
    print("CANADIAN CORPORATION RESEARCH TEST: Postmedia Network")
    print("=" * 60)

    # Sample text about Postmedia (Canadian media company)
    sample_text = """
    Postmedia Network Inc. (TSX: PNC.A, PNC.B) is Canada's largest newspaper 
    publisher, headquartered in Toronto, Ontario. The company owns major 
    newspapers including the National Post, Vancouver Sun, Calgary Herald, 
    Edmonton Journal, and Ottawa Citizen.
    
    Postmedia was formed in 2010 when it acquired the newspaper assets from 
    Canwest Global Communications Corp. The company has faced scrutiny over 
    its ownership structure, with American hedge fund Chatham Asset Management 
    holding significant influence through secured debt.
    
    Corporate Information:
    - Business Number: 809735683RC0001
    - Corporation Number: 7652981
    - Registered Office: 365 Bloor Street East, Toronto, ON
    - Website: postmedia.com
    - Email: info@postmedia.com
    
    Recent acquisitions include SaltWire Network properties and various 
    community newspapers. The company has partnerships with Sun Media Corporation
    and Quebecor Media Inc. for content sharing.
    """

    # 1. Extract entities
    print("\n1. Extracting Entities...")
    from mitds.cases.extraction.deterministic import DeterministicExtractor

    extractor = DeterministicExtractor()
    entities = extractor.extract(sample_text)

    print(f"   Found {len(entities)} entities:")

    # Group by type
    bns = [e for e in entities if e.identifier_type == "bn"]
    orgs = [e for e in entities if e.entity_type == "organization"]
    domains = [e for e in entities if e.identifier_type == "domain"]
    corp_nums = [e for e in entities if e.identifier_type == "corp_number"]

    print(f"\n   Business Numbers ({len(bns)}):")
    for e in bns:
        print(f"     - {e.value} (conf: {e.confidence})")

    print(f"\n   Organizations ({len(orgs)}):")
    for e in orgs[:8]:  # First 8
        val = e.value[:50] + "..." if len(e.value) > 50 else e.value
        print(f"     - {val} (conf: {e.confidence})")

    print(f"\n   Domains ({len(domains)}):")
    for e in domains:
        print(f"     - {e.value} (conf: {e.confidence})")

    # 2. Test case creation
    print("\n2. Creating Research Case...")
    from mitds.db import get_session_factory
    from sqlalchemy import text

    session_factory = get_session_factory()
    case_id = uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

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
                "name": "Postmedia Network Research",
                "entry_point_type": "corporation",
                "entry_point_value": "Postmedia Network Inc.",
                "config": json.dumps({"max_depth": 2, "jurisdiction": "CA"}),
                "status": "processing",
                "created_at": now,
                "updated_at": now,
            },
        )
        await session.commit()
        print(f"   [OK] Case created: {case_id}")

        # Store leads
        leads_stored = 0
        for entity in entities:
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
                },
            )
            leads_stored += 1
        await session.commit()
        print(f"   [OK] Stored {leads_stored} leads")

    # 3. Query Neo4j for related entities
    print("\n3. Checking Neo4j for Related Entities...")
    from mitds.db import get_neo4j_driver

    driver = await get_neo4j_driver()
    async with driver.session() as neo4j_session:
        # Check if any media companies exist
        result = await neo4j_session.run(
            """
            MATCH (o:Organization)
            WHERE o.jurisdiction = 'CA' 
            AND (o.name CONTAINS 'Media' OR o.name CONTAINS 'News' OR o.name CONTAINS 'Post')
            RETURN o.name as name, o.id as id
            LIMIT 10
        """
        )
        records = [r async for r in result]

        if records:
            print(f"   Found {len(records)} related media organizations:")
            for r in records:
                print(f"     - {r['name']}")
        else:
            print("   [INFO] No existing media orgs in graph (expected for fresh DB)")

        # Check for any organizations with BN
        if bns:
            bn_value = bns[0].value.replace("RC", "").replace("0001", "")[:9]
            result = await neo4j_session.run(
                """
                MATCH (o:Organization)
                WHERE o.bn CONTAINS $bn_prefix
                RETURN o.name as name, o.bn as bn
                LIMIT 5
            """,
                {"bn_prefix": bn_value},
            )
            bn_records = [r async for r in result]

            if bn_records:
                print(f"   Found organization with matching BN prefix:")
                for r in bn_records:
                    print(f"     - {r['name']} (BN: {r['bn']})")

    # 4. Summary
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(
        f"""
Corporation: Postmedia Network Inc.
Case ID: {case_id}

Entities Extracted:
  - Business Numbers: {len(bns)}
  - Organizations: {len(orgs)}
  - Domains: {len(domains)}
  - Total: {len(entities)}

Key Identifiers Found:"""
    )
    for bn in bns:
        print(f"  * BN: {bn.value}")
    for d in domains:
        print(f"  * Domain: {d.value}")

    # 5. Cleanup
    print("\n5. Cleaning Up...")
    async with session_factory() as session:
        await session.execute(
            text("DELETE FROM extracted_leads WHERE case_id = :case_id"),
            {"case_id": str(case_id)},
        )
        await session.execute(
            text("DELETE FROM cases WHERE id = :id"),
            {"id": str(case_id)},
        )
        await session.commit()
        print("   [OK] Test data cleaned up")


if __name__ == "__main__":
    asyncio.run(test_canadian_corp())
