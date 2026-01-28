"""Abstract base class for data ingesters.

All data source connectors must implement this interface
to ensure consistent behavior and retry logic.

## Architecture Overview

The ingestion system uses a dual-database architecture:
- **PostgreSQL**: Primary storage for entities, relationships, and ingestion metadata
- **Neo4j**: Graph database for relationship traversal and network analysis

## Implementing a New Ingester

1. Inherit from `BaseIngester[YourRecordModel]`
2. Implement required methods:
   - `fetch_records()`: Async generator yielding records from the source
   - `process_record()`: Process a single record (store in PostgreSQL + Neo4j)
   - `get_last_sync_time()`: Get last sync timestamp from PostgreSQL
   - `save_sync_time()`: Save sync timestamp (usually no-op, handled by base)

3. Follow database patterns:
   - Use `get_db_session()` context manager for PostgreSQL
   - Use `get_neo4j_session()` context manager for Neo4j
   - Wrap Neo4j operations in try/except to allow graceful degradation
   - Do NOT call `db.commit()` - the context manager handles it automatically

4. Return proper results from `process_record()`:
   - `{"created": True, "entity_id": "..."}` for new entities
   - `{"updated": True, "entity_id": "..."}` for updated entities
   - `{"duplicate": True, "entity_id": "..."}` for duplicates

Example:
    ```python
    class MyIngester(BaseIngester[MyRecordModel]):
        def __init__(self):
            super().__init__("my_source")

        async def fetch_records(self, config):
            # Yield records from your data source
            for item in data_source:
                yield MyRecordModel(**item)

        async def process_record(self, record):
            # Store in PostgreSQL
            async with get_db_session() as db:
                # ... insert/update operations
                # NO need to call db.commit() - context manager does it

            # Store in Neo4j (wrapped in try/except)
            try:
                async with get_neo4j_session() as session:
                    await session.run("MERGE ...")
            except Exception as e:
                self.logger.warning(f"Neo4j sync failed: {e}")

            return {"created": True, "entity_id": str(entity_id)}
    ```
"""

import json
import logging
import sys
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel
from tqdm import tqdm

from ..logging import get_context_logger, log_ingestion_start, log_ingestion_complete, log_ingestion_error


@contextmanager
def suppress_db_logging():
    """Temporarily suppress SQLAlchemy and other verbose logging for clean tqdm output.

    This suppresses:
    - SQLAlchemy engine echo output
    - SQLAlchemy loggers
    - Neo4j, httpx, and other verbose loggers
    """
    loggers_to_suppress = [
        "sqlalchemy.engine.Engine",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "sqlalchemy.orm",
        "sqlalchemy",
        "neo4j",
        "httpx",
        "httpcore",
        "aiosqlite",
        "asyncpg",
    ]

    original_state = {}
    for logger_name in loggers_to_suppress:
        logger = logging.getLogger(logger_name)
        original_state[logger_name] = {
            "level": logger.level,
            "disabled": logger.disabled,
            "handlers": [(h, h.level) for h in logger.handlers],
        }
        # Set to CRITICAL to suppress everything except critical errors
        logger.setLevel(logging.CRITICAL)
        logger.disabled = True
        # Also set all handlers to CRITICAL
        for handler in logger.handlers:
            handler.setLevel(logging.CRITICAL)

    # Also suppress SQLAlchemy engine echo
    previous_echo_suppressed = None
    try:
        from ..db import set_echo_suppressed
        previous_echo_suppressed = set_echo_suppressed(True)
    except ImportError:
        pass

    try:
        yield
    finally:
        # Restore logger state
        for logger_name, state in original_state.items():
            logger = logging.getLogger(logger_name)
            logger.setLevel(state["level"])
            logger.disabled = state["disabled"]
            for handler, level in state["handlers"]:
                handler.setLevel(level)

        # Restore engine echo suppression
        if previous_echo_suppressed is not None:
            try:
                from ..db import set_echo_suppressed
                set_echo_suppressed(previous_echo_suppressed)
            except ImportError:
                pass


def create_progress_bar(
    desc: str,
    total: int | None = None,
    unit: str = "rec",
    leave: bool = True,
) -> tqdm:
    """Create a tqdm progress bar with consistent styling.

    Args:
        desc: Description shown on the left
        total: Total count (None for unknown)
        unit: Unit label (e.g., "rec", "KB", "files")
        leave: Whether to leave the bar visible after completion

    Returns:
        tqdm progress bar instance

    Usage:
        pbar = create_progress_bar("Downloading entities", total=1000, unit="KB")
        for chunk in download_stream():
            pbar.update(len(chunk))
        pbar.close()
    """
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        dynamic_ncols=True,
        file=sys.stderr,
        leave=leave,
    )


async def download_with_progress(
    url: str,
    desc: str = "Downloading",
    httpx_client=None,
) -> bytes:
    """Download a file with progress bar.

    Args:
        url: URL to download
        desc: Progress bar description
        httpx_client: Optional httpx.AsyncClient (creates one if not provided)

    Returns:
        Downloaded content as bytes

    Usage:
        content = await download_with_progress(
            "https://example.com/data.json.gz",
            desc="Downloading entities"
        )
    """
    import httpx

    close_client = False
    if httpx_client is None:
        httpx_client = httpx.AsyncClient(timeout=httpx.Timeout(300.0))
        close_client = True

    try:
        async with httpx_client.stream("GET", url) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))

            with suppress_db_logging():
                pbar = tqdm(
                    total=total if total else None,
                    desc=desc,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    dynamic_ncols=True,
                    file=sys.stderr,
                )

                chunks = []
                try:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        chunks.append(chunk)
                        pbar.update(len(chunk))
                finally:
                    pbar.close()

            return b"".join(chunks)
    finally:
        if close_client:
            await httpx_client.aclose()

# Type variable for ingested record type
T = TypeVar("T", bound=BaseModel)


class IngestionConfig(BaseModel):
    """Configuration for an ingestion run."""

    incremental: bool = True
    limit: int | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    target_entities: list[str] | None = None
    extra_params: dict[str, Any] = {}


class IngestionResult(BaseModel):
    """Result of an ingestion run."""

    run_id: UUID
    source: str
    status: str  # "completed", "failed", "partial"
    started_at: datetime
    completed_at: datetime | None = None
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    duplicates_found: int = 0
    errors: list[dict[str, Any]] = []
    log_output: str = ""

    @property
    def duration_seconds(self) -> float | None:
        """Calculate run duration in seconds."""
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class BaseIngester(ABC, Generic[T]):
    """Abstract base class for data source ingesters.

    Provides common functionality:
    - Retry logic with exponential backoff
    - Progress tracking
    - Error handling and logging
    - Incremental vs full refresh support
    """

    def __init__(self, source_name: str):
        """Initialize the ingester.

        Args:
            source_name: Name of the data source (e.g., 'irs990', 'cra')
        """
        self.source_name = source_name
        self.run_id: UUID | None = None
        self._logger = None

    @property
    def logger(self):
        """Get a logger with run context."""
        if self._logger is None or self.run_id is None:
            self._logger = get_context_logger(
                f"mitds.ingestion.{self.source_name}",
                source=self.source_name,
            )
        return self._logger

    @abstractmethod
    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[T]:
        """Fetch records from the data source.

        Args:
            config: Ingestion configuration

        Yields:
            Parsed records of type T
        """
        ...

    @abstractmethod
    async def process_record(self, record: T) -> dict[str, Any]:
        """Process a single record.

        Args:
            record: The record to process

        Returns:
            Processing result with status and details
        """
        ...

    @abstractmethod
    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync.

        Returns:
            Last sync timestamp or None if never synced
        """
        ...

    @abstractmethod
    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the timestamp of a successful sync.

        Args:
            timestamp: Sync completion timestamp
        """
        ...

    async def run(
        self, config: IngestionConfig | None = None, run_id: UUID | None = None
    ) -> IngestionResult:
        """Run the ingestion process.

        Args:
            config: Optional ingestion configuration
            run_id: Optional run ID (from API layer). If not provided, a new UUID is generated.

        Returns:
            Ingestion result with statistics
        """
        from .run_log import start_capture, finish_capture, RunLogHandler

        if config is None:
            config = IngestionConfig()

        self.run_id = run_id or uuid4()
        run_id_str = str(self.run_id)

        # Set up per-run log capture
        start_capture(run_id_str)
        handler = RunLogHandler(run_id_str)
        handler.setFormatter(logging.Formatter("%(message)s"))
        source_logger = logging.getLogger(f"mitds.ingestion.{self.source_name}")
        source_logger.addHandler(handler)

        result = IngestionResult(
            run_id=self.run_id,
            source=self.source_name,
            status="running",
            started_at=datetime.utcnow(),
        )

        try:
            log_ingestion_start(self.source_name, run_id_str)
            self.logger.info(
                "Starting ingestion",
                extra={"config": config.model_dump(), "run_id": run_id_str},
            )

            try:
                # If incremental, get last sync time
                if config.incremental:
                    last_sync = await self.get_last_sync_time()
                    if last_sync:
                        config.date_from = last_sync
                        self.logger.info(
                            f"Incremental sync from {last_sync.isoformat()}"
                        )

                # Use tqdm for progress tracking with suppressed verbose logging
                total = config.limit if config.limit else None
                desc = f"Ingesting {self.source_name}"

                with suppress_db_logging():
                    pbar = tqdm(
                        total=total,
                        desc=desc,
                        unit="rec",
                        dynamic_ncols=True,
                        file=sys.stderr,
                    )

                    try:
                        async for record in self.fetch_records(config):
                            record_name = getattr(record, "name", None) or getattr(record, "corporation_name", None) or getattr(record, "id", "unknown")
                            try:
                                process_result = await self.process_record(record)
                                result.records_processed += 1

                                if process_result.get("created"):
                                    result.records_created += 1
                                elif process_result.get("updated"):
                                    result.records_updated += 1
                                elif process_result.get("duplicate"):
                                    result.duplicates_found += 1

                                # Update progress bar with current stats
                                pbar.set_postfix(
                                    created=result.records_created,
                                    updated=result.records_updated,
                                    dup=result.duplicates_found,
                                    err=len(result.errors),
                                    refresh=False,
                                )
                                pbar.update(1)

                                # Check limit
                                if config.limit and result.records_processed >= config.limit:
                                    pbar.set_description(f"{desc} (limit reached)")
                                    break

                            except Exception as e:
                                result.records_processed += 1
                                error_info = {
                                    "record_id": getattr(record, "id", None),
                                    "error": str(e),
                                    "error_type": type(e).__name__,
                                }
                                result.errors.append(error_info)

                                # Update progress bar even on error
                                pbar.set_postfix(
                                    created=result.records_created,
                                    updated=result.records_updated,
                                    dup=result.duplicates_found,
                                    err=len(result.errors),
                                    refresh=False,
                                )
                                pbar.update(1)

                                # Continue processing other records
                                continue
                    finally:
                        pbar.close()

                # Print summary after progress bar
                print(
                    f"\n{self.source_name} ingestion complete:\n"
                    f"  Processed: {result.records_processed}\n"
                    f"  Created:   {result.records_created}\n"
                    f"  Updated:   {result.records_updated}\n"
                    f"  Duplicates:{result.duplicates_found}\n"
                    f"  Errors:    {len(result.errors)}",
                    file=sys.stderr,
                )

                # Log to capture (for API/logs)
                self.logger.info(
                    f"Ingestion complete: {result.records_processed} processed, "
                    f"{result.records_created} created, {result.records_updated} updated, "
                    f"{result.duplicates_found} duplicates, {len(result.errors)} errors"
                )

                # Mark as complete
                result.status = "completed" if not result.errors else "partial"
                result.completed_at = datetime.utcnow()

                # Save sync time
                if result.status in ("completed", "partial"):
                    await self.save_sync_time(result.started_at)

                log_ingestion_complete(
                    self.source_name,
                    run_id_str,
                    result.records_processed,
                    result.duration_seconds or 0,
                )

            except Exception as e:
                result.status = "failed"
                result.completed_at = datetime.utcnow()
                result.errors.append({
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "fatal": True,
                })
                log_ingestion_error(self.source_name, run_id_str, str(e))
                self.logger.exception("Ingestion failed")

        finally:
            # Detach handler and flush captured logs
            source_logger.removeHandler(handler)
            result.log_output = finish_capture(run_id_str)

        return result


class RetryConfig(BaseModel):
    """Configuration for retry behavior."""

    max_retries: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0


async def with_retry(
    func,
    config: RetryConfig | None = None,
    logger=None,
):
    """Execute a function with exponential backoff retry.

    Args:
        func: Async function to execute
        config: Retry configuration
        logger: Optional logger for retry messages

    Returns:
        Function result

    Raises:
        Exception: If all retries are exhausted
    """
    import asyncio

    if config is None:
        config = RetryConfig()

    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await func()
        except Exception as e:
            last_exception = e

            if attempt < config.max_retries:
                delay = min(
                    config.base_delay * (config.exponential_base ** attempt),
                    config.max_delay,
                )

                if logger:
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s"
                    )

                await asyncio.sleep(delay)
            else:
                if logger:
                    logger.error(
                        f"All {config.max_retries + 1} attempts failed"
                    )

    raise last_exception


# =========================
# Database Helper Utilities
# =========================


class Neo4jHelper:
    """Helper class for common Neo4j operations in ingesters.

    Provides standardized patterns for creating nodes and relationships
    with proper error handling and idempotent operations.

    Usage:
        neo4j = Neo4jHelper(logger)
        async with get_neo4j_session() as session:
            await neo4j.merge_organization(session, org_data)
            await neo4j.merge_person(session, person_data)
            await neo4j.create_relationship(session, "OWNS", source_id, target_id, props)
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    async def merge_organization(
        self,
        session,
        *,
        id: str,
        name: str,
        org_type: str | None = None,
        external_ids: dict[str, str] | None = None,
        properties: dict[str, Any] | None = None,
        merge_key: str = "name",
    ) -> bool:
        """Merge an Organization node in Neo4j.

        Args:
            session: Neo4j async session
            id: MITDS entity UUID
            name: Organization name
            org_type: Organization type (nonprofit, corporation, etc.)
            external_ids: External identifiers (bn, ein, cik, etc.)
            properties: Additional properties to set
            merge_key: Property to use for MERGE (default: name)

        Returns:
            True if successful, False if failed
        """
        now = datetime.utcnow().isoformat()

        props = {
            "id": id,
            "name": name,
            "entity_type": "ORGANIZATION",
            "updated_at": now,
        }

        if org_type:
            props["org_type"] = org_type

        if external_ids:
            for key, value in external_ids.items():
                if value:
                    props[key] = value

        if properties:
            props.update(properties)

        try:
            if merge_key == "name":
                await session.run(
                    """
                    MERGE (o:Organization {name: $name})
                    ON CREATE SET o += $props
                    ON MATCH SET o.updated_at = $now,
                                 o.id = COALESCE(o.id, $props.id)
                    """,
                    name=name,
                    props=props,
                    now=now,
                )
            else:
                # Use a specific external ID as merge key
                merge_value = external_ids.get(merge_key) if external_ids else None
                if not merge_value:
                    self.logger.warning(f"No value for merge_key '{merge_key}', using name instead")
                    return await self.merge_organization(
                        session, id=id, name=name, org_type=org_type,
                        external_ids=external_ids, properties=properties, merge_key="name"
                    )

                await session.run(
                    f"""
                    MERGE (o:Organization {{{merge_key}: $merge_value}})
                    ON CREATE SET o += $props
                    ON MATCH SET o.name = COALESCE(o.name, $props.name),
                                 o.updated_at = $now,
                                 o.id = COALESCE(o.id, $props.id)
                    """,
                    merge_value=merge_value,
                    props=props,
                    now=now,
                )

            return True
        except Exception as e:
            self.logger.warning(f"Neo4j merge_organization failed for {name}: {e}")
            return False

    async def merge_person(
        self,
        session,
        *,
        id: str,
        name: str,
        external_ids: dict[str, str] | None = None,
        properties: dict[str, Any] | None = None,
        merge_key: str = "name",
    ) -> bool:
        """Merge a Person node in Neo4j.

        Args:
            session: Neo4j async session
            id: MITDS entity UUID
            name: Person name
            external_ids: External identifiers
            properties: Additional properties to set
            merge_key: Property to use for MERGE (default: name)

        Returns:
            True if successful, False if failed
        """
        now = datetime.utcnow().isoformat()

        props = {
            "id": id,
            "name": name,
            "entity_type": "PERSON",
            "updated_at": now,
        }

        if external_ids:
            for key, value in external_ids.items():
                if value:
                    props[key] = value

        if properties:
            props.update(properties)

        try:
            if merge_key == "name":
                await session.run(
                    """
                    MERGE (p:Person {name: $name})
                    ON CREATE SET p += $props
                    ON MATCH SET p.updated_at = $now,
                                 p.id = COALESCE(p.id, $props.id)
                    """,
                    name=name,
                    props=props,
                    now=now,
                )
            else:
                merge_value = external_ids.get(merge_key) if external_ids else None
                if not merge_value:
                    return await self.merge_person(
                        session, id=id, name=name, external_ids=external_ids,
                        properties=properties, merge_key="name"
                    )

                await session.run(
                    f"""
                    MERGE (p:Person {{{merge_key}: $merge_value}})
                    ON CREATE SET p += $props
                    ON MATCH SET p.name = COALESCE(p.name, $props.name),
                                 p.updated_at = $now
                    """,
                    merge_value=merge_value,
                    props=props,
                    now=now,
                )

            return True
        except Exception as e:
            self.logger.warning(f"Neo4j merge_person failed for {name}: {e}")
            return False

    async def create_relationship(
        self,
        session,
        rel_type: str,
        source_label: str,
        source_key: str,
        source_value: str,
        target_label: str,
        target_key: str,
        target_value: str,
        properties: dict[str, Any] | None = None,
        merge_on: list[str] | None = None,
    ) -> bool:
        """Create a relationship between two nodes.

        Args:
            session: Neo4j async session
            rel_type: Relationship type (e.g., "OWNS", "FUNDED_BY")
            source_label: Source node label (e.g., "Organization")
            source_key: Source node match property (e.g., "name")
            source_value: Source node match value
            target_label: Target node label
            target_key: Target node match property
            target_value: Target node match value
            properties: Relationship properties
            merge_on: Additional properties to include in MERGE key

        Returns:
            True if successful, False if failed
        """
        now = datetime.utcnow().isoformat()

        props = {
            "updated_at": now,
        }
        if properties:
            props.update(properties)

        try:
            # Build MERGE clause for relationship
            if merge_on:
                merge_props = ", ".join(f"{k}: ${k}" for k in merge_on)
                query = f"""
                    MATCH (s:{source_label} {{{source_key}: $source_value}})
                    MATCH (t:{target_label} {{{target_key}: $target_value}})
                    MERGE (s)-[r:{rel_type} {{{merge_props}}}]->(t)
                    SET r += $props
                """
                params = {
                    "source_value": source_value,
                    "target_value": target_value,
                    "props": props,
                }
                for k in merge_on:
                    params[k] = properties.get(k) if properties else None
            else:
                query = f"""
                    MATCH (s:{source_label} {{{source_key}: $source_value}})
                    MATCH (t:{target_label} {{{target_key}: $target_value}})
                    MERGE (s)-[r:{rel_type}]->(t)
                    SET r += $props
                """
                params = {
                    "source_value": source_value,
                    "target_value": target_value,
                    "props": props,
                }

            await session.run(query, **params)
            return True
        except Exception as e:
            self.logger.warning(
                f"Neo4j relationship {source_value} -[{rel_type}]-> {target_value} failed: {e}"
            )
            return False

    async def sync_entity(
        self,
        session,
        *,
        entity_type: str,
        id: str,
        name: str,
        external_ids: dict[str, str] | None = None,
        properties: dict[str, Any] | None = None,
        merge_key: str = "name",
    ) -> bool:
        """Sync an entity to Neo4j based on type.

        Args:
            session: Neo4j async session
            entity_type: "ORGANIZATION" or "PERSON"
            id: MITDS entity UUID
            name: Entity name
            external_ids: External identifiers
            properties: Additional properties
            merge_key: Property to use for MERGE

        Returns:
            True if successful, False if failed
        """
        if entity_type == "PERSON":
            return await self.merge_person(
                session,
                id=id,
                name=name,
                external_ids=external_ids,
                properties=properties,
                merge_key=merge_key,
            )
        else:
            return await self.merge_organization(
                session,
                id=id,
                name=name,
                external_ids=external_ids,
                properties=properties,
                merge_key=merge_key,
            )


class PostgresHelper:
    """Helper class for common PostgreSQL operations in ingesters.

    Provides standardized patterns for entity and relationship operations.

    IMPORTANT: Do NOT call db.commit() when using these helpers -
    the get_db_session() context manager handles commits automatically.

    Usage:
        pg = PostgresHelper(logger)
        async with get_db_session() as db:
            entity_id = await pg.upsert_entity(db, entity_data)
            await pg.upsert_relationship(db, rel_data)
            # NO commit needed - context manager handles it
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    async def find_entity_by_external_id(
        self,
        db,
        external_id_key: str,
        external_id_value: str,
    ) -> UUID | None:
        """Find an entity by external ID.

        Args:
            db: SQLAlchemy async session
            external_id_key: External ID key (e.g., "littlesis_id", "bn")
            external_id_value: External ID value

        Returns:
            Entity UUID if found, None otherwise
        """
        from sqlalchemy import text

        result = await db.execute(
            text(f"""
                SELECT id FROM entities
                WHERE external_ids->>'{external_id_key}' = :value
            """),
            {"value": external_id_value},
        )
        row = result.fetchone()
        return row.id if row else None

    async def find_entity_by_name(
        self,
        db,
        name: str,
        entity_type: str | None = None,
    ) -> UUID | None:
        """Find an entity by name (case-insensitive).

        Args:
            db: SQLAlchemy async session
            name: Entity name
            entity_type: Optional entity type filter

        Returns:
            Entity UUID if found, None otherwise
        """
        from sqlalchemy import text

        if entity_type:
            result = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                    AND entity_type = :entity_type
                """),
                {"name": name, "entity_type": entity_type},
            )
        else:
            result = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                """),
                {"name": name},
            )
        row = result.fetchone()
        return row.id if row else None

    async def upsert_entity(
        self,
        db,
        *,
        name: str,
        entity_type: str,
        external_ids: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        find_by: str = "name",  # "name" or external ID key
    ) -> tuple[UUID, bool]:
        """Insert or update an entity.

        Args:
            db: SQLAlchemy async session
            name: Entity name
            entity_type: Entity type ("organization", "person")
            external_ids: External identifiers
            metadata: Additional metadata
            find_by: How to find existing entity ("name" or external ID key)

        Returns:
            Tuple of (entity_id, is_new)
        """
        from sqlalchemy import text

        # Find existing entity
        if find_by == "name":
            existing_id = await self.find_entity_by_name(db, name, entity_type)
        else:
            ext_value = external_ids.get(find_by) if external_ids else None
            if ext_value:
                existing_id = await self.find_entity_by_external_id(db, find_by, ext_value)
            else:
                existing_id = await self.find_entity_by_name(db, name, entity_type)

        now = datetime.utcnow()

        if existing_id:
            # Update existing
            await db.execute(
                text("""
                    UPDATE entities SET
                        metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                        external_ids = COALESCE(external_ids, '{}'::jsonb) || CAST(:external_ids AS jsonb),
                        updated_at = :updated_at
                    WHERE id = :id
                """),
                {
                    "id": existing_id,
                    "metadata": json.dumps(metadata or {}),
                    "external_ids": json.dumps(external_ids or {}),
                    "updated_at": now,
                },
            )
            return existing_id, False
        else:
            # Insert new
            new_id = uuid4()
            await db.execute(
                text("""
                    INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at, updated_at)
                    VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb),
                            CAST(:metadata AS jsonb), :created_at, :updated_at)
                """),
                {
                    "id": new_id,
                    "name": name,
                    "entity_type": entity_type,
                    "external_ids": json.dumps(external_ids or {}),
                    "metadata": json.dumps(metadata or {}),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return new_id, True

    async def upsert_relationship(
        self,
        db,
        *,
        rel_type: str,
        from_entity_id: UUID,
        to_entity_id: UUID,
        properties: dict[str, Any] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        is_current: bool = True,
        confidence: float = 1.0,
        find_by_property: str | None = None,  # Property key to find existing
    ) -> tuple[UUID, bool]:
        """Insert or update a relationship.

        Args:
            db: SQLAlchemy async session
            rel_type: Relationship type
            from_entity_id: Source entity UUID
            to_entity_id: Target entity UUID
            properties: Relationship properties
            start_date: Relationship start date
            end_date: Relationship end date
            is_current: Whether relationship is current
            confidence: Confidence score
            find_by_property: Property key to identify existing relationship

        Returns:
            Tuple of (relationship_id, is_new)
        """
        from sqlalchemy import text

        now = datetime.utcnow()

        # Check for existing relationship
        existing_id = None
        if find_by_property and properties and find_by_property in properties:
            result = await db.execute(
                text(f"""
                    SELECT id FROM relationships
                    WHERE properties->>'{find_by_property}' = :prop_value
                """),
                {"prop_value": str(properties[find_by_property])},
            )
            row = result.fetchone()
            existing_id = row.id if row else None

        if existing_id:
            # Update existing
            await db.execute(
                text("""
                    UPDATE relationships SET
                        properties = COALESCE(properties, '{}'::jsonb) || CAST(:properties AS jsonb),
                        start_date = COALESCE(:start_date, start_date),
                        end_date = COALESCE(:end_date, end_date),
                        is_current = :is_current,
                        confidence = :confidence,
                        updated_at = :updated_at
                    WHERE id = :id
                """),
                {
                    "id": existing_id,
                    "properties": json.dumps(properties or {}),
                    "start_date": start_date,
                    "end_date": end_date,
                    "is_current": is_current,
                    "confidence": confidence,
                    "updated_at": now,
                },
            )
            return existing_id, False
        else:
            # Insert new
            new_id = uuid4()
            await db.execute(
                text("""
                    INSERT INTO relationships (
                        id, relationship_type, from_entity_id, to_entity_id,
                        properties, start_date, end_date, is_current, confidence,
                        created_at, updated_at
                    ) VALUES (
                        :id, :rel_type, :from_id, :to_id,
                        CAST(:properties AS jsonb), :start_date, :end_date,
                        :is_current, :confidence, :created_at, :updated_at
                    )
                """),
                {
                    "id": new_id,
                    "rel_type": rel_type,
                    "from_id": from_entity_id,
                    "to_id": to_entity_id,
                    "properties": json.dumps(properties or {}),
                    "start_date": start_date,
                    "end_date": end_date,
                    "is_current": is_current,
                    "confidence": confidence,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return new_id, True
