"""Database connection managers for MITDS.

Provides async connections to PostgreSQL, Neo4j, and Redis.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as redis
from neo4j import AsyncGraphDatabase, AsyncDriver
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

# =========================
# SQLAlchemy Setup
# =========================

# Naming convention for constraints (improves migration compatibility)
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""

    metadata = MetaData(naming_convention=convention)


# Engine and session factory (lazy initialization)
_engine = None
_session_factory = None


def get_engine():
    """Get or create the async SQLAlchemy engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            echo=settings.api_debug,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory():
    """Get or create the async session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session.

    Usage:
        async with get_db_session() as session:
            result = await session.execute(query)
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions."""
    async with get_db_session() as session:
        yield session


# =========================
# Neo4j Setup
# =========================

_neo4j_driver: AsyncDriver | None = None


async def get_neo4j_driver() -> AsyncDriver:
    """Get or create the Neo4j async driver."""
    global _neo4j_driver
    if _neo4j_driver is None:
        settings = get_settings()
        _neo4j_driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
    return _neo4j_driver


@asynccontextmanager
async def get_neo4j_session():
    """Get a Neo4j async session.

    Usage:
        async with get_neo4j_session() as session:
            result = await session.run("MATCH (n) RETURN n LIMIT 1")
    """
    driver = await get_neo4j_driver()
    session = driver.session()
    try:
        yield session
    finally:
        await session.close()


async def close_neo4j():
    """Close the Neo4j driver connection."""
    global _neo4j_driver
    if _neo4j_driver is not None:
        await _neo4j_driver.close()
        _neo4j_driver = None


# =========================
# Redis Setup
# =========================

_redis_client: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    """Get or create the Redis async client."""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_client


async def close_redis():
    """Close the Redis connection."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None


# =========================
# Cleanup
# =========================


async def close_all_connections():
    """Close all database connections (for shutdown)."""
    global _engine

    # Close Neo4j
    await close_neo4j()

    # Close Redis
    await close_redis()

    # Close SQLAlchemy
    if _engine is not None:
        await _engine.dispose()
        _engine = None
