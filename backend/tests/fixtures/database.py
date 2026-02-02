"""Database fixtures for integration tests.

Provides fixtures for setting up and tearing down test databases.
"""

import asyncio
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio

from .entities import SAMPLE_ORGANIZATIONS, SAMPLE_PERSONS, SAMPLE_OUTLETS
from .relationships import (
    SAMPLE_FUNDING_RELATIONSHIPS,
    SAMPLE_EMPLOYMENT_RELATIONSHIPS,
    SAMPLE_DIRECTOR_RELATIONSHIPS,
)
from .evidence import SAMPLE_EVIDENCE


# =========================
# Async Event Loop Fixture
# =========================


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =========================
# Database Session Fixtures
# =========================


@pytest_asyncio.fixture
async def db_session():
    """Create a database session for testing.

    Sets up a test database connection and ensures cleanup after tests.
    """
    from mitds.config import get_settings
    from mitds.db import get_db_session

    # Use test database URL if available
    settings = get_settings()

    async with get_db_session() as session:
        yield session
        # Rollback any uncommitted changes
        await session.rollback()


@pytest_asyncio.fixture
async def neo4j_session():
    """Create a Neo4j session for testing.

    Sets up a test graph database connection and ensures cleanup after tests.
    """
    from mitds.db import get_neo4j_session

    async with get_neo4j_session() as session:
        yield session


# =========================
# Data Seeding Fixtures
# =========================


@pytest_asyncio.fixture
async def seeded_postgres(db_session):
    """Seed PostgreSQL with test data.

    Creates test entities and evidence in the database.
    Returns the session for further use.
    """
    from sqlalchemy import text

    # Insert test entities
    for entity in SAMPLE_ORGANIZATIONS + SAMPLE_PERSONS + SAMPLE_OUTLETS:
        await db_session.execute(
            text("""
                INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                VALUES (:id, :name, :entity_type, :external_ids, :metadata, :created_at)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": entity["id"],
                "name": entity["name"],
                "entity_type": entity["entity_type"],
                "external_ids": "{}",
                "metadata": "{}",
                "created_at": entity["created_at"],
            },
        )

    # Insert test evidence
    for evidence in SAMPLE_EVIDENCE:
        await db_session.execute(
            text("""
                INSERT INTO evidence (
                    id, evidence_type, source_url, retrieved_at, extractor,
                    extractor_version, raw_data_ref, extraction_confidence, content_hash
                )
                VALUES (
                    :id, :evidence_type, :source_url, :retrieved_at, :extractor,
                    :version, :raw_ref, :confidence, :hash
                )
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": evidence["id"],
                "evidence_type": evidence["evidence_type"],
                "source_url": evidence["source_url"],
                "retrieved_at": evidence["retrieved_at"],
                "extractor": evidence["extractor"],
                "version": evidence["extractor_version"],
                "raw_ref": evidence["raw_data_ref"],
                "confidence": evidence["extraction_confidence"],
                "hash": evidence["content_hash"],
            },
        )

    await db_session.commit()
    yield db_session


@pytest_asyncio.fixture
async def seeded_neo4j(neo4j_session):
    """Seed Neo4j with test data.

    Creates test entities and relationships in the graph database.
    Returns the session for further use.
    """
    # Create entity nodes
    for entity in SAMPLE_ORGANIZATIONS:
        await neo4j_session.run(
            """
            MERGE (n:Organization {id: $id})
            SET n.name = $name,
                n.entity_type = $entity_type,
                n.org_type = $org_type,
                n.jurisdiction = $jurisdiction,
                n.confidence = $confidence
            """,
            {
                "id": str(entity["id"]),
                "name": entity["name"],
                "entity_type": entity["entity_type"],
                "org_type": entity.get("org_type"),
                "jurisdiction": entity.get("jurisdiction"),
                "confidence": entity.get("confidence", 1.0),
            },
        )

    for entity in SAMPLE_PERSONS:
        await neo4j_session.run(
            """
            MERGE (n:Person {id: $id})
            SET n.name = $name,
                n.entity_type = $entity_type,
                n.confidence = $confidence
            """,
            {
                "id": str(entity["id"]),
                "name": entity["name"],
                "entity_type": entity["entity_type"],
                "confidence": entity.get("confidence", 1.0),
            },
        )

    for entity in SAMPLE_OUTLETS:
        await neo4j_session.run(
            """
            MERGE (n:Outlet {id: $id})
            SET n.name = $name,
                n.entity_type = $entity_type,
                n.confidence = $confidence
            """,
            {
                "id": str(entity["id"]),
                "name": entity["name"],
                "entity_type": entity["entity_type"],
                "confidence": entity.get("confidence", 1.0),
            },
        )

    # Create relationships
    for rel in SAMPLE_FUNDING_RELATIONSHIPS:
        await neo4j_session.run(
            """
            MATCH (source {id: $source_id}), (target {id: $target_id})
            MERGE (source)-[r:FUNDED_BY {id: $rel_id}]->(target)
            SET r.amount = $amount,
                r.fiscal_year = $fiscal_year,
                r.confidence = $confidence
            """,
            {
                "source_id": str(rel["source_id"]),
                "target_id": str(rel["target_id"]),
                "rel_id": str(rel["id"]),
                "amount": rel["properties"].get("amount"),
                "fiscal_year": rel["properties"].get("fiscal_year"),
                "confidence": rel.get("confidence", 1.0),
            },
        )

    for rel in SAMPLE_EMPLOYMENT_RELATIONSHIPS:
        await neo4j_session.run(
            """
            MATCH (source {id: $source_id}), (target {id: $target_id})
            MERGE (source)-[r:EMPLOYED_BY {id: $rel_id}]->(target)
            SET r.title = $title,
                r.confidence = $confidence
            """,
            {
                "source_id": str(rel["source_id"]),
                "target_id": str(rel["target_id"]),
                "rel_id": str(rel["id"]),
                "title": rel["properties"].get("title"),
                "confidence": rel.get("confidence", 1.0),
            },
        )

    for rel in SAMPLE_DIRECTOR_RELATIONSHIPS:
        await neo4j_session.run(
            """
            MATCH (source {id: $source_id}), (target {id: $target_id})
            MERGE (source)-[r:DIRECTOR_OF {id: $rel_id}]->(target)
            SET r.role = $role,
                r.confidence = $confidence
            """,
            {
                "source_id": str(rel["source_id"]),
                "target_id": str(rel["target_id"]),
                "rel_id": str(rel["id"]),
                "role": rel["properties"].get("role"),
                "confidence": rel.get("confidence", 1.0),
            },
        )

    yield neo4j_session


@pytest_asyncio.fixture
async def seeded_databases(seeded_postgres, seeded_neo4j):
    """Seed both databases with test data.

    Returns tuple of (postgres_session, neo4j_session).
    """
    yield seeded_postgres, seeded_neo4j


# =========================
# Cleanup Fixtures
# =========================


@pytest_asyncio.fixture
async def clean_postgres(db_session):
    """Ensure clean PostgreSQL state before and after test.

    Truncates all tables before yielding.
    """
    from sqlalchemy import text

    tables = ["entities", "evidence", "audit_log", "ingestion_runs", "quality_metrics"]

    for table in tables:
        try:
            await db_session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
        except Exception:
            pass  # Table may not exist

    await db_session.commit()
    yield db_session


@pytest_asyncio.fixture
async def clean_neo4j(neo4j_session):
    """Ensure clean Neo4j state before and after test.

    Deletes all nodes and relationships.
    """
    await neo4j_session.run("MATCH (n) DETACH DELETE n")
    yield neo4j_session


@pytest_asyncio.fixture
async def clean_databases(clean_postgres, clean_neo4j):
    """Ensure clean state in both databases.

    Returns tuple of (postgres_session, neo4j_session).
    """
    yield clean_postgres, clean_neo4j
