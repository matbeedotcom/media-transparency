"""Template Ingester - Example implementation for new data sources.

This template demonstrates the standard patterns for implementing a new ingester.
Copy this file and modify it for your specific data source.

## Quick Start

1. Copy this file: `cp _template.py my_source.py`
2. Rename the classes and update the source_name
3. Define your Pydantic model for records
4. Implement fetch_records() to yield records from your source
5. Implement process_record() to store in PostgreSQL and Neo4j
6. Add to __init__.py exports
7. Register in CLI if needed

## Key Patterns

### Database Operations
- Use `get_db_session()` context manager for PostgreSQL
- Use `get_neo4j_session()` context manager for Neo4j
- NEVER call `db.commit()` - the context manager handles it
- ALWAYS wrap Neo4j operations in try/except for graceful degradation

### Return Values from process_record()
- `{"created": True, "entity_id": "..."}` - new entity created
- `{"updated": True, "entity_id": "..."}` - existing entity updated
- `{"duplicate": True, "entity_id": "..."}` - duplicate, no changes

### HTTP Client Management
- Use a lazy-initialized property for HTTP clients
- Implement a close() method for cleanup
- Use connection pooling (httpx.AsyncClient, not requests)

### Progress Tracking
- The base class uses tqdm progress bars automatically during run()
- Don't implement your own record counting - the base class tracks it
- Use config.limit to respect record limits
- Verbose database logging is suppressed during processing for clean output
- For custom download stages, use `download_with_progress()` or `create_progress_bar()`

## Testing Your Ingester

```bash
# Run with limit to test
python -m mitds.cli ingest my_source --limit 10

# Run with target entities to filter
python -m mitds.cli ingest my_source --target "Entity Name"
```
"""

import json
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import (
    BaseIngester,
    IngestionConfig,
    IngestionResult,
    Neo4jHelper,
    PostgresHelper,
    # Progress utilities (for custom download stages)
    create_progress_bar,
    download_with_progress,
    suppress_db_logging,
)

logger = get_context_logger(__name__)


# =========================
# Data Models
# =========================


class TemplateRecord(BaseModel):
    """Record model for the template ingester.

    Define fields that match your data source. All fields should have
    sensible defaults or be marked as required.

    The `name` field is commonly used by the base class for logging,
    so include it if your records have a primary name/identifier.
    """

    id: str = Field(..., description="Unique identifier from the source")
    name: str = Field(..., description="Primary name of the entity")
    entity_type: str = Field(default="organization", description="organization or person")

    # Optional fields with defaults
    description: str | None = None
    external_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Dates
    created_date: datetime | None = None
    updated_date: datetime | None = None


# =========================
# Ingester Implementation
# =========================


class TemplateIngester(BaseIngester[TemplateRecord]):
    """Template ingester demonstrating standard patterns.

    Replace 'template' with your source name and implement the
    fetch_records() and process_record() methods.
    """

    def __init__(self):
        # Source name should be lowercase, underscored (e.g., "elections_canada")
        super().__init__("template_source")

        # Lazy-initialized clients
        self._http_client: httpx.AsyncClient | None = None

        # Helper instances for database operations
        self._neo4j = Neo4jHelper(logger)
        self._postgres = PostgresHelper(logger)

    # =========================
    # HTTP Client Management
    # =========================

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialized HTTP client with connection pooling."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
                headers={
                    "User-Agent": "MITDS/1.0 (Media Influence Transparency)",
                    "Accept": "application/json",
                },
            )
        return self._http_client

    async def close(self):
        """Clean up resources. Call this when done with the ingester."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    # =========================
    # Required: fetch_records()
    # =========================

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[TemplateRecord]:
        """Fetch records from the data source.

        This is an async generator that yields records one at a time.
        The base class handles:
        - Limit enforcement (config.limit)
        - Progress logging
        - Error handling

        Args:
            config: Ingestion configuration with options like:
                - incremental: bool (True for updates only)
                - limit: int | None (max records)
                - date_from/date_to: datetime (date range)
                - target_entities: list[str] (filter by name)
                - extra_params: dict (custom parameters)

        Yields:
            TemplateRecord instances
        """
        self.logger.info("Starting fetch from template source")

        # Example: Use incremental sync if available
        if config.incremental:
            last_sync = await self.get_last_sync_time()
            if last_sync:
                self.logger.info(f"Incremental sync from {last_sync.isoformat()}")
                # Modify your query to only fetch records updated since last_sync

        # Example: Fetch from API
        # response = await self.http_client.get("https://api.example.com/data")
        # data = response.json()

        # Example: Simulated data for template
        sample_data = [
            {
                "id": "sample-1",
                "name": "Sample Organization 1",
                "entity_type": "organization",
                "description": "A sample organization for testing",
            },
            {
                "id": "sample-2",
                "name": "Sample Person 1",
                "entity_type": "person",
                "description": "A sample person for testing",
            },
        ]

        for item in sample_data:
            # Apply target entity filter if specified
            if config.target_entities:
                name_lower = item["name"].lower()
                if not any(t.lower() in name_lower for t in config.target_entities):
                    continue

            record = TemplateRecord(**item)
            yield record

            # Note: Don't implement limit checking here
            # The base class run() method handles it automatically

    # =========================
    # Required: process_record()
    # =========================

    async def process_record(self, record: TemplateRecord) -> dict[str, Any]:
        """Process a single record - store in PostgreSQL and Neo4j.

        This method should:
        1. Store/update the entity in PostgreSQL
        2. Store/update the entity in Neo4j (with error handling)
        3. Return the result status

        Args:
            record: The record to process

        Returns:
            Dict with keys:
            - created: bool (True if new entity)
            - updated: bool (True if updated existing)
            - duplicate: bool (True if no changes needed)
            - entity_id: str (the entity UUID)
        """
        result = {"created": False, "updated": False, "duplicate": False, "entity_id": None}

        # =========================
        # Step 1: PostgreSQL Operations
        # =========================
        # Use context manager - it handles commit/rollback automatically

        async with get_db_session() as db:
            # Option A: Use the PostgresHelper for common patterns
            entity_id, is_new = await self._postgres.upsert_entity(
                db,
                name=record.name,
                entity_type=record.entity_type,
                external_ids={
                    "template_id": record.id,
                    "template_url": record.external_url,
                },
                metadata={
                    "source": "template_source",
                    "description": record.description,
                    **record.metadata,
                },
                find_by="template_id",  # Use external ID to find existing
            )

            # Option B: Manual SQL for complex operations
            # check_result = await db.execute(
            #     text("SELECT id FROM entities WHERE external_ids->>'template_id' = :tid"),
            #     {"tid": record.id},
            # )
            # existing = check_result.fetchone()
            # ... handle insert/update ...

            # IMPORTANT: Do NOT call db.commit() here!
            # The context manager handles it automatically.

            result["entity_id"] = str(entity_id)
            if is_new:
                result["created"] = True
            else:
                result["updated"] = True

        # =========================
        # Step 2: Neo4j Operations
        # =========================
        # ALWAYS wrap in try/except - Neo4j failures shouldn't fail the whole record

        try:
            async with get_neo4j_session() as session:
                # Option A: Use Neo4jHelper for common patterns
                if record.entity_type == "person":
                    await self._neo4j.merge_person(
                        session,
                        id=result["entity_id"],
                        name=record.name,
                        external_ids={"template_id": record.id},
                        properties={
                            "description": record.description,
                            "source": "template_source",
                        },
                    )
                else:
                    await self._neo4j.merge_organization(
                        session,
                        id=result["entity_id"],
                        name=record.name,
                        external_ids={"template_id": record.id},
                        properties={
                            "description": record.description,
                            "source": "template_source",
                        },
                    )

                # Option B: Raw Cypher for complex operations
                # await session.run(
                #     """
                #     MERGE (o:Organization {template_id: $template_id})
                #     ON CREATE SET o += $props
                #     ON MATCH SET o.updated_at = $now
                #     """,
                #     template_id=record.id,
                #     props={...},
                #     now=datetime.utcnow().isoformat(),
                # )

                # Create relationships if applicable
                # await self._neo4j.create_relationship(
                #     session,
                #     rel_type="OWNS",
                #     source_label="Organization",
                #     source_key="name",
                #     source_value="Parent Corp",
                #     target_label="Organization",
                #     target_key="name",
                #     target_value=record.name,
                #     properties={"source": "template_source"},
                # )

                self.logger.debug(f"Neo4j: synced {record.name}")

        except Exception as e:
            # Log but don't fail - Neo4j is supplementary to PostgreSQL
            self.logger.warning(f"Neo4j sync failed for {record.name}: {e}")

        return result

    # =========================
    # Required: Sync Time Methods
    # =========================

    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync.

        This is used for incremental syncing - only fetch records
        that have changed since this timestamp.
        """
        async with get_db_session() as db:
            result = await db.execute(
                text("""
                    SELECT MAX(completed_at) as last_sync
                    FROM ingestion_runs
                    WHERE source = :source AND status IN ('completed', 'partial')
                """),
                {"source": self.source_name},
            )
            row = result.fetchone()
            return row.last_sync if row else None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the sync timestamp.

        The base class run() method calls this automatically,
        so usually this can be a no-op.
        """
        # Usually no-op - the base class handles this via ingestion_runs table
        pass


# =========================
# Convenience Function
# =========================


async def run_template_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    **extra_params,
) -> dict[str, Any]:
    """Run the template ingestion.

    This is the main entry point called by CLI and API.

    Args:
        limit: Maximum records to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of entity names to filter
        **extra_params: Additional parameters passed to config

    Returns:
        Ingestion result dictionary
    """
    ingester = TemplateIngester()

    try:
        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
            extra_params=extra_params,
        )
        result = await ingester.run(config)
        return result.model_dump()
    finally:
        # Always clean up resources
        await ingester.close()
