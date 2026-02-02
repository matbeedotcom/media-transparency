"""Redis caching for frequently accessed entities.

Provides caching layer to reduce database load for common queries.
"""

import asyncio
import json
from datetime import timedelta
from functools import wraps
from typing import Any, Callable, TypeVar
from uuid import UUID

from .config import get_settings
from .logging import get_context_logger

logger = get_context_logger(__name__)

T = TypeVar("T")


# Cache TTL configurations (in seconds)
CACHE_TTL = {
    "entity": 300,  # 5 minutes for individual entities
    "entity_list": 60,  # 1 minute for entity lists
    "relationship": 300,  # 5 minutes for relationships
    "search_result": 120,  # 2 minutes for search results
    "detection_score": 600,  # 10 minutes for detection scores
    "stats": 60,  # 1 minute for statistics
    "default": 300,  # 5 minutes default
}

# Cache key prefixes
KEY_PREFIX = "mitds:"


class CacheBackend:
    """Abstract cache backend interface."""

    async def get(self, key: str) -> Any | None:
        """Get value from cache."""
        raise NotImplementedError

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in cache."""
        raise NotImplementedError

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        raise NotImplementedError

    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern."""
        raise NotImplementedError

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        raise NotImplementedError

    async def close(self) -> None:
        """Close the cache connection."""
        pass


class InMemoryCache(CacheBackend):
    """Simple in-memory cache for development/testing."""

    def __init__(self):
        self._cache: dict[str, tuple[Any, float | None]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        import time

        async with self._lock:
            if key not in self._cache:
                return None

            value, expires_at = self._cache[key]
            if expires_at and time.time() > expires_at:
                del self._cache[key]
                return None

            return value

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        import time

        async with self._lock:
            expires_at = time.time() + ttl if ttl else None
            self._cache[key] = (value, expires_at)
            return True

    async def delete(self, key: str) -> bool:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def delete_pattern(self, pattern: str) -> int:
        import fnmatch

        async with self._lock:
            keys_to_delete = [
                k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)
            ]
            for key in keys_to_delete:
                del self._cache[key]
            return len(keys_to_delete)

    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None


class RedisCache(CacheBackend):
    """Redis-based cache for production."""

    def __init__(self, redis_client):
        self._redis = redis_client

    async def get(self, key: str) -> Any | None:
        try:
            data = await self._redis.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.warning(f"Redis cache get error: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        try:
            data = json.dumps(value, default=str)
            if ttl:
                await self._redis.setex(key, ttl, data)
            else:
                await self._redis.set(key, data)
            return True
        except Exception as e:
            logger.warning(f"Redis cache set error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        try:
            result = await self._redis.delete(key)
            return result > 0
        except Exception as e:
            logger.warning(f"Redis cache delete error: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        try:
            cursor = 0
            deleted = 0
            while True:
                cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
                if keys:
                    deleted += await self._redis.delete(*keys)
                if cursor == 0:
                    break
            return deleted
        except Exception as e:
            logger.warning(f"Redis cache delete pattern error: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        try:
            return await self._redis.exists(key) > 0
        except Exception as e:
            logger.warning(f"Redis cache exists error: {e}")
            return False

    async def close(self) -> None:
        try:
            await self._redis.close()
        except Exception:
            pass


# Global cache instance
_cache: CacheBackend | None = None


async def get_cache() -> CacheBackend:
    """Get or create cache instance.

    Uses Redis in production, falls back to in-memory for development.
    """
    global _cache

    if _cache is not None:
        return _cache

    settings = get_settings()

    if settings.is_production or settings.redis_url:
        try:
            import redis.asyncio as redis

            client = redis.from_url(settings.redis_url)
            await client.ping()
            _cache = RedisCache(client)
            logger.info("Using Redis cache backend")
        except Exception as e:
            logger.warning(f"Redis unavailable for caching: {e}")
            _cache = InMemoryCache()
    else:
        _cache = InMemoryCache()
        logger.info("Using in-memory cache backend")

    return _cache


async def close_cache() -> None:
    """Close the cache connection."""
    global _cache
    if _cache:
        await _cache.close()
        _cache = None


# =========================
# Cache Key Builders
# =========================


def entity_key(entity_id: str | UUID) -> str:
    """Build cache key for an entity."""
    return f"{KEY_PREFIX}entity:{entity_id}"


def entity_list_key(
    entity_type: str | None = None,
    jurisdiction: str | None = None,
    page: int = 0,
) -> str:
    """Build cache key for entity list."""
    parts = [KEY_PREFIX, "entities"]
    if entity_type:
        parts.append(f"type:{entity_type}")
    if jurisdiction:
        parts.append(f"jurisdiction:{jurisdiction}")
    parts.append(f"page:{page}")
    return ":".join(parts)


def search_key(query: str, filters: dict[str, Any] | None = None) -> str:
    """Build cache key for search results."""
    import hashlib

    filter_str = json.dumps(filters or {}, sort_keys=True)
    hash_input = f"{query}:{filter_str}"
    query_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"{KEY_PREFIX}search:{query_hash}"


def relationship_key(entity_id: str | UUID, rel_type: str | None = None) -> str:
    """Build cache key for entity relationships."""
    if rel_type:
        return f"{KEY_PREFIX}rel:{entity_id}:{rel_type}"
    return f"{KEY_PREFIX}rel:{entity_id}"


def detection_score_key(entity_id: str | UUID) -> str:
    """Build cache key for detection scores."""
    return f"{KEY_PREFIX}detection:{entity_id}"


def stats_key(stat_type: str) -> str:
    """Build cache key for statistics."""
    return f"{KEY_PREFIX}stats:{stat_type}"


# =========================
# Caching Operations
# =========================


async def cache_entity(entity_id: str | UUID, data: dict[str, Any]) -> bool:
    """Cache an entity.

    Args:
        entity_id: Entity UUID
        data: Entity data to cache

    Returns:
        True if cached successfully
    """
    cache = await get_cache()
    key = entity_key(entity_id)
    return await cache.set(key, data, CACHE_TTL["entity"])


async def get_cached_entity(entity_id: str | UUID) -> dict[str, Any] | None:
    """Get a cached entity.

    Args:
        entity_id: Entity UUID

    Returns:
        Cached entity data or None
    """
    cache = await get_cache()
    key = entity_key(entity_id)
    return await cache.get(key)


async def invalidate_entity(entity_id: str | UUID) -> bool:
    """Invalidate entity cache.

    Args:
        entity_id: Entity UUID

    Returns:
        True if invalidated
    """
    cache = await get_cache()

    # Delete entity and related caches
    await cache.delete(entity_key(entity_id))
    await cache.delete_pattern(f"{KEY_PREFIX}rel:{entity_id}*")
    await cache.delete(detection_score_key(entity_id))

    # Invalidate list caches (they may contain this entity)
    await cache.delete_pattern(f"{KEY_PREFIX}entities:*")
    await cache.delete_pattern(f"{KEY_PREFIX}search:*")

    return True


async def cache_search_results(
    query: str,
    filters: dict[str, Any] | None,
    results: list[dict[str, Any]],
) -> bool:
    """Cache search results.

    Args:
        query: Search query
        filters: Search filters
        results: Search results

    Returns:
        True if cached successfully
    """
    cache = await get_cache()
    key = search_key(query, filters)
    return await cache.set(key, results, CACHE_TTL["search_result"])


async def get_cached_search_results(
    query: str,
    filters: dict[str, Any] | None,
) -> list[dict[str, Any]] | None:
    """Get cached search results.

    Args:
        query: Search query
        filters: Search filters

    Returns:
        Cached results or None
    """
    cache = await get_cache()
    key = search_key(query, filters)
    return await cache.get(key)


async def cache_detection_score(
    entity_id: str | UUID,
    scores: dict[str, Any],
) -> bool:
    """Cache detection scores for an entity.

    Args:
        entity_id: Entity UUID
        scores: Detection scores

    Returns:
        True if cached successfully
    """
    cache = await get_cache()
    key = detection_score_key(entity_id)
    return await cache.set(key, scores, CACHE_TTL["detection_score"])


async def get_cached_detection_score(
    entity_id: str | UUID,
) -> dict[str, Any] | None:
    """Get cached detection scores.

    Args:
        entity_id: Entity UUID

    Returns:
        Cached scores or None
    """
    cache = await get_cache()
    key = detection_score_key(entity_id)
    return await cache.get(key)


# =========================
# Cache Decorator
# =========================


def cached(
    key_builder: Callable[..., str],
    ttl_key: str = "default",
) -> Callable:
    """Decorator to cache function results.

    Args:
        key_builder: Function to build cache key from args
        ttl_key: Key to look up TTL in CACHE_TTL

    Returns:
        Decorated function
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            cache = await get_cache()
            key = key_builder(*args, **kwargs)
            ttl = CACHE_TTL.get(ttl_key, CACHE_TTL["default"])

            # Try to get from cache
            cached_value = await cache.get(key)
            if cached_value is not None:
                logger.debug(f"Cache hit: {key}")
                return cached_value

            # Call function and cache result
            logger.debug(f"Cache miss: {key}")
            result = await func(*args, **kwargs)

            if result is not None:
                await cache.set(key, result, ttl)

            return result

        return wrapper

    return decorator


# =========================
# Cache Warming
# =========================


async def warm_entity_cache(entity_ids: list[str | UUID]) -> int:
    """Pre-warm cache for a list of entities.

    Args:
        entity_ids: List of entity IDs to cache

    Returns:
        Number of entities cached
    """
    from .db import get_db_session
    from sqlalchemy import text

    cached = 0

    async with get_db_session() as db:
        for entity_id in entity_ids:
            result = await db.execute(
                text("SELECT * FROM entities WHERE id = :id"),
                {"id": entity_id if isinstance(entity_id, UUID) else UUID(entity_id)},
            )
            row = result.fetchone()

            if row:
                entity_data = {
                    "id": str(row.id),
                    "name": row.name,
                    "entity_type": row.entity_type,
                    "external_ids": row.external_ids,
                    "metadata": row.metadata,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
                if await cache_entity(entity_id, entity_data):
                    cached += 1

    logger.info(f"Warmed cache for {cached} entities")
    return cached


async def clear_all_cache() -> int:
    """Clear all MITDS cache entries.

    Returns:
        Number of entries cleared
    """
    cache = await get_cache()
    return await cache.delete_pattern(f"{KEY_PREFIX}*")
