#!/usr/bin/env python3
"""
Load sample data for MITDS development and testing.

Creates a realistic sample dataset with:
- Organizations (foundations, nonprofits)
- Persons (directors, officers)
- Outlets (media organizations)
- Funding relationships
- Evidence records
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any

# Namespace for deterministic UUIDs (allows reproducible sample data)
MITDS_NAMESPACE = uuid.UUID("d1234567-0000-0000-0000-000000000000")


def make_uuid(name: str) -> str:
    """Generate deterministic UUID from a name."""
    return str(uuid.uuid5(MITDS_NAMESPACE, name))


# Sample data representing a realistic funding network
SAMPLE_ORGANIZATIONS = [
    {
        "id": make_uuid("org-foundation-alpha"),
        "name": "Alpha Family Foundation",
        "org_type": "FOUNDATION",
        "ein": "12-3456789",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "founded_year": 1995,
        "description": "Private family foundation focused on education and media",
    },
    {
        "id": make_uuid("org-foundation-beta"),
        "name": "Beta Charitable Trust",
        "org_type": "FOUNDATION",
        "ein": "98-7654321",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "founded_year": 2001,
        "description": "Charitable trust supporting journalism and public policy",
    },
    {
        "id": make_uuid("org-foundation-gamma"),
        "name": "Gamma Institute for Public Policy",
        "org_type": "NONPROFIT",
        "ein": "45-6789012",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "founded_year": 2010,
        "description": "Think tank focused on economic policy research",
    },
    {
        "id": make_uuid("org-nonprofit-delta"),
        "name": "Delta Media Education Fund",
        "org_type": "NONPROFIT",
        "ein": "34-5678901",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "founded_year": 2015,
        "description": "Media literacy and journalism training organization",
    },
    {
        "id": make_uuid("org-canada-epsilon"),
        "name": "Epsilon Foundation Canada",
        "org_type": "FOUNDATION",
        "bn": "123456789RR0001",
        "jurisdiction": "CA",
        "status": "ACTIVE",
        "founded_year": 2005,
        "description": "Canadian charitable foundation supporting media initiatives",
    },
]

SAMPLE_OUTLETS = [
    {
        "id": make_uuid("outlet-news-daily"),
        "name": "The Daily Observer",
        "media_type": "DIGITAL_NATIVE",
        "domain": "dailyobserver.example.com",
        "jurisdiction": "US",
        "founded_year": 2012,
        "description": "Digital-first news outlet covering national politics",
    },
    {
        "id": make_uuid("outlet-policy-review"),
        "name": "Policy Review Weekly",
        "media_type": "DIGITAL_NATIVE",
        "domain": "policyreview.example.com",
        "jurisdiction": "US",
        "founded_year": 2016,
        "description": "Policy-focused publication with commentary",
    },
    {
        "id": make_uuid("outlet-civic-times"),
        "name": "Civic Times",
        "media_type": "DIGITAL_NATIVE",
        "domain": "civictimes.example.com",
        "jurisdiction": "US",
        "founded_year": 2018,
        "description": "Local news and civic engagement coverage",
    },
    {
        "id": make_uuid("outlet-north-report"),
        "name": "Northern Report",
        "media_type": "DIGITAL_NATIVE",
        "domain": "northernreport.example.ca",
        "jurisdiction": "CA",
        "founded_year": 2014,
        "description": "Canadian news outlet with cross-border coverage",
    },
]

SAMPLE_PERSONS = [
    {
        "id": make_uuid("person-john-smith"),
        "name": "John Smith",
        "aliases": ["J. Smith", "John R. Smith"],
        "description": "Foundation executive and board member",
    },
    {
        "id": make_uuid("person-jane-doe"),
        "name": "Jane Doe",
        "aliases": ["J. Doe"],
        "description": "Media executive and nonprofit director",
    },
    {
        "id": make_uuid("person-robert-johnson"),
        "name": "Robert Johnson",
        "aliases": ["Bob Johnson", "R. Johnson"],
        "description": "Policy researcher and foundation trustee",
    },
]

# Funding relationships (funder -> recipient with amounts)
SAMPLE_FUNDING = [
    # Alpha Foundation funds multiple outlets
    {
        "funder_id": make_uuid("org-foundation-alpha"),
        "recipient_id": make_uuid("outlet-news-daily"),
        "amount": 500000,
        "fiscal_year": 2023,
        "purpose": "General operating support",
    },
    {
        "funder_id": make_uuid("org-foundation-alpha"),
        "recipient_id": make_uuid("outlet-policy-review"),
        "amount": 250000,
        "fiscal_year": 2023,
        "purpose": "Investigative journalism program",
    },
    {
        "funder_id": make_uuid("org-foundation-alpha"),
        "recipient_id": make_uuid("org-nonprofit-delta"),
        "amount": 150000,
        "fiscal_year": 2023,
        "purpose": "Media training programs",
    },
    # Beta Trust funds same outlets (shared funder pattern)
    {
        "funder_id": make_uuid("org-foundation-beta"),
        "recipient_id": make_uuid("outlet-news-daily"),
        "amount": 350000,
        "fiscal_year": 2023,
        "purpose": "Digital expansion grant",
    },
    {
        "funder_id": make_uuid("org-foundation-beta"),
        "recipient_id": make_uuid("outlet-policy-review"),
        "amount": 200000,
        "fiscal_year": 2023,
        "purpose": "Policy coverage initiative",
    },
    {
        "funder_id": make_uuid("org-foundation-beta"),
        "recipient_id": make_uuid("outlet-civic-times"),
        "amount": 175000,
        "fiscal_year": 2023,
        "purpose": "Local journalism fund",
    },
    # Gamma Institute funds outlets
    {
        "funder_id": make_uuid("org-foundation-gamma"),
        "recipient_id": make_uuid("outlet-policy-review"),
        "amount": 100000,
        "fiscal_year": 2023,
        "purpose": "Economic policy coverage",
    },
    # Canadian foundation funds North American outlets
    {
        "funder_id": make_uuid("org-canada-epsilon"),
        "recipient_id": make_uuid("outlet-north-report"),
        "amount": 300000,
        "fiscal_year": 2023,
        "purpose": "Cross-border journalism",
    },
    {
        "funder_id": make_uuid("org-canada-epsilon"),
        "recipient_id": make_uuid("outlet-news-daily"),
        "amount": 100000,
        "fiscal_year": 2023,
        "purpose": "US-Canada reporting partnership",
    },
    # Delta nonprofit receives and gives (pass-through)
    {
        "funder_id": make_uuid("org-nonprofit-delta"),
        "recipient_id": make_uuid("outlet-civic-times"),
        "amount": 75000,
        "fiscal_year": 2023,
        "purpose": "Journalist fellowship program",
    },
]

# Board/director relationships
SAMPLE_ROLES = [
    {
        "person_id": make_uuid("person-john-smith"),
        "org_id": make_uuid("org-foundation-alpha"),
        "role": "DIRECTOR",
        "title": "Board Chair",
        "start_date": "2010-01-15",
    },
    {
        "person_id": make_uuid("person-john-smith"),
        "org_id": make_uuid("org-foundation-beta"),
        "role": "DIRECTOR",
        "title": "Trustee",
        "start_date": "2015-06-01",
    },
    {
        "person_id": make_uuid("person-jane-doe"),
        "org_id": make_uuid("outlet-news-daily"),
        "role": "DIRECTOR",
        "title": "Board Member",
        "start_date": "2018-03-01",
    },
    {
        "person_id": make_uuid("person-jane-doe"),
        "org_id": make_uuid("org-nonprofit-delta"),
        "role": "DIRECTOR",
        "title": "Advisory Board",
        "start_date": "2020-01-01",
    },
    {
        "person_id": make_uuid("person-robert-johnson"),
        "org_id": make_uuid("org-foundation-gamma"),
        "role": "EMPLOYED_BY",
        "title": "Senior Fellow",
        "start_date": "2012-09-01",
    },
    {
        "person_id": make_uuid("person-robert-johnson"),
        "org_id": make_uuid("outlet-policy-review"),
        "role": "DIRECTOR",
        "title": "Editorial Advisor",
        "start_date": "2019-01-01",
    },
]


async def load_to_neo4j(driver: Any) -> dict[str, int]:
    """Load sample data into Neo4j."""
    counts = {"organizations": 0, "outlets": 0, "persons": 0, "funding": 0, "roles": 0}

    async with driver.session() as session:
        # Clear existing sample data (delete all entities with labels we're creating)
        await session.run(
            "MATCH (n) WHERE n:Organization OR n:Outlet OR n:Person DETACH DELETE n"
        )

        # Create organizations
        for org in SAMPLE_ORGANIZATIONS:
            await session.run(
                """
                CREATE (o:Organization:Entity {
                    id: $id,
                    name: $name,
                    org_type: $org_type,
                    ein: $ein,
                    bn: $bn,
                    jurisdiction: $jurisdiction,
                    status: $status,
                    founded_year: $founded_year,
                    description: $description,
                    entity_type: 'ORGANIZATION',
                    confidence: 1.0,
                    created_at: datetime()
                })
                """,
                id=org["id"],
                name=org["name"],
                org_type=org["org_type"],
                ein=org.get("ein"),
                bn=org.get("bn"),
                jurisdiction=org["jurisdiction"],
                status=org["status"],
                founded_year=org["founded_year"],
                description=org["description"],
            )
            counts["organizations"] += 1

        # Create outlets
        for outlet in SAMPLE_OUTLETS:
            await session.run(
                """
                CREATE (o:Outlet:Entity {
                    id: $id,
                    name: $name,
                    media_type: $media_type,
                    domain: $domain,
                    jurisdiction: $jurisdiction,
                    founded_year: $founded_year,
                    description: $description,
                    entity_type: 'OUTLET',
                    confidence: 1.0,
                    created_at: datetime()
                })
                """,
                id=outlet["id"],
                name=outlet["name"],
                media_type=outlet["media_type"],
                domain=outlet["domain"],
                jurisdiction=outlet["jurisdiction"],
                founded_year=outlet["founded_year"],
                description=outlet["description"],
            )
            counts["outlets"] += 1

        # Create persons
        for person in SAMPLE_PERSONS:
            await session.run(
                """
                CREATE (p:Person:Entity {
                    id: $id,
                    name: $name,
                    aliases: $aliases,
                    description: $description,
                    entity_type: 'PERSON',
                    confidence: 1.0,
                    created_at: datetime()
                })
                """,
                id=person["id"],
                name=person["name"],
                aliases=person.get("aliases", []),
                description=person["description"],
            )
            counts["persons"] += 1

        # Create funding relationships
        for funding in SAMPLE_FUNDING:
            await session.run(
                """
                MATCH (funder:Entity {id: $funder_id})
                MATCH (recipient:Entity {id: $recipient_id})
                CREATE (recipient)-[r:FUNDED_BY {
                    id: $rel_id,
                    amount: $amount,
                    fiscal_year: $fiscal_year,
                    purpose: $purpose,
                    confidence: 1.0,
                    created_at: datetime()
                }]->(funder)
                """,
                funder_id=funding["funder_id"],
                recipient_id=funding["recipient_id"],
                rel_id=f"funding-{uuid.uuid4().hex[:8]}",
                amount=funding["amount"],
                fiscal_year=funding["fiscal_year"],
                purpose=funding["purpose"],
            )
            counts["funding"] += 1

        # Create role relationships
        for role in SAMPLE_ROLES:
            rel_type = role["role"]
            await session.run(
                f"""
                MATCH (person:Person {{id: $person_id}})
                MATCH (org:Entity {{id: $org_id}})
                CREATE (person)-[r:{rel_type} {{
                    id: $rel_id,
                    title: $title,
                    start_date: date($start_date),
                    confidence: 1.0,
                    created_at: datetime()
                }}]->(org)
                """,
                person_id=role["person_id"],
                org_id=role["org_id"],
                rel_id=f"role-{uuid.uuid4().hex[:8]}",
                title=role["title"],
                start_date=role["start_date"],
            )
            counts["roles"] += 1

    return counts


async def load_to_postgres(conn: Any) -> dict[str, int]:
    """Load sample evidence and event records to PostgreSQL."""
    counts = {"evidence": 0, "events": 0}

    # Create evidence records for funding relationships
    for i, funding in enumerate(SAMPLE_FUNDING):
        evidence_id = str(uuid.uuid4())
        await conn.execute(
            """
            INSERT INTO evidence (id, evidence_type, source_url, archive_url, retrieved_at, raw_content)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO NOTHING
            """,
            evidence_id,
            "IRS_990",
            f"https://s3.amazonaws.com/irs-form-990/sample-{i:03d}.xml",
            f"https://web.archive.org/web/20240101/irs-form-990/sample-{i:03d}.xml",
            datetime.utcnow() - timedelta(days=i),
            json.dumps(funding),
        )
        counts["evidence"] += 1

    # Create ingestion run record
    run_id = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO ingestion_runs (id, source, status, started_at, completed_at, records_processed, records_created)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO NOTHING
        """,
        run_id,
        "sample_data",
        "completed",
        datetime.utcnow() - timedelta(minutes=5),
        datetime.utcnow(),
        len(SAMPLE_FUNDING) + len(SAMPLE_ORGANIZATIONS) + len(SAMPLE_OUTLETS),
        len(SAMPLE_FUNDING) + len(SAMPLE_ORGANIZATIONS) + len(SAMPLE_OUTLETS),
    )

    return counts


async def main() -> int:
    """Load all sample data."""
    print("\n" + "=" * 60)
    print("MITDS Sample Data Loader")
    print("=" * 60 + "\n")

    # Check and load to Neo4j
    print("Loading data to Neo4j...")
    try:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "neo4j_dev_password"),
        )
        neo4j_counts = await load_to_neo4j(driver)
        await driver.close()
        print(f"  - Organizations: {neo4j_counts['organizations']}")
        print(f"  - Outlets: {neo4j_counts['outlets']}")
        print(f"  - Persons: {neo4j_counts['persons']}")
        print(f"  - Funding relationships: {neo4j_counts['funding']}")
        print(f"  - Role relationships: {neo4j_counts['roles']}")
        print("  \033[92m[OK] Neo4j data loaded\033[0m\n")
    except Exception as e:
        print(f"  \033[91m[FAIL] Neo4j error: {e}\033[0m\n")
        return 1

    # Load to PostgreSQL
    print("Loading data to PostgreSQL...")
    try:
        import asyncpg

        conn = await asyncpg.connect(
            host="localhost",
            port=5432,
            user="mitds",
            password="mitds_dev_password",
            database="mitds",
        )
        pg_counts = await load_to_postgres(conn)
        await conn.close()
        print(f"  - Evidence records: {pg_counts['evidence']}")
        print("  \033[92m[OK] PostgreSQL data loaded\033[0m\n")
    except Exception as e:
        print(f"  \033[91m[FAIL] PostgreSQL error: {e}\033[0m\n")
        return 1

    print("=" * 60)
    print("\033[92mSample data loaded successfully!\033[0m")
    print("=" * 60)
    print("\nYou can now:")
    print("  - Query funding clusters via API: GET /api/v1/relationships/funding-clusters")
    print("  - Search entities: GET /api/v1/entities?q=Alpha")
    print("  - Browse Neo4j at http://localhost:7474")
    print("  - View MinIO at http://localhost:9001")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
