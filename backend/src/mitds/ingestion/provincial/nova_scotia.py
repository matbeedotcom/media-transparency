"""Nova Scotia Co-operatives Registry ingester.

Downloads and processes the list of co-operatives registered at the
Registry of Joint Stock Companies from the Nova Scotia Open Data Portal.

Data includes:
- Registry identification number
- Co-operative name
- Year of incorporation
- Mailing address
- Non-profit or for-profit classification
- Co-op type (housing, investment, retail, services, agriculture, etc.)

Source: https://data.novascotia.ca/Business-and-Industry/Nova-Scotia-Co-operatives/k29k-n2db

Usage:
    from mitds.ingestion.provincial import run_nova_scotia_coops_ingestion

    result = await run_nova_scotia_coops_ingestion(
        incremental=True,
        limit=100,
    )
"""

import csv
import hashlib
import sys
from collections.abc import AsyncIterator
from datetime import date, datetime
from io import StringIO
from typing import Any
from uuid import UUID, uuid4

import httpx
from sqlalchemy import text

from ..base import (
    BaseIngester,
    IngestionConfig,
    IngestionResult,
    Neo4jHelper,
    PostgresHelper,
)
from .models import (
    Address,
    ProvincialCorporationRecord,
    ProvincialCorpStatus,
    ProvincialCorpType,
)


class NovaScotiaCoopsIngester(BaseIngester[ProvincialCorporationRecord]):
    """Ingester for Nova Scotia co-operatives registry.

    Downloads CSV data from the Nova Scotia Open Data Portal containing
    all registered co-operatives in the province.
    """

    # Nova Scotia Open Data API endpoint for co-operatives
    DATA_URL = "https://data.novascotia.ca/api/views/k29k-n2db/rows.csv?accessType=DOWNLOAD"

    def __init__(self):
        """Initialize the Nova Scotia co-operatives ingester."""
        super().__init__("ns-coops")
        self._neo4j = Neo4jHelper(self.logger)
        self._postgres = PostgresHelper(self.logger)
        self._existing_hashes: set[str] = set()

    async def download_data(self) -> str:
        """Download co-operatives CSV from Nova Scotia Open Data.

        Returns:
            CSV content as string
        """
        self.logger.info(f"Downloading NS co-ops data from {self.DATA_URL}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(self.DATA_URL)
            response.raise_for_status()

            # The API returns CSV directly
            content = response.text
            self.logger.info(f"Downloaded {len(content):,} bytes")
            return content

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[ProvincialCorporationRecord]:
        """Fetch and parse co-operative records from Nova Scotia Open Data.

        Args:
            config: Ingestion configuration

        Yields:
            ProvincialCorporationRecord for each co-operative
        """
        csv_content = await self.download_data()

        # Parse CSV
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)

        print(f"Parsing CSV file... found {len(rows):,} records", file=sys.stderr)

        # Load existing hashes for incremental sync
        if config.incremental:
            await self._load_existing_hashes()

        processed = 0
        for row in rows:
            record = self._parse_row(row)
            if record:
                # Check for incremental skip
                if config.incremental:
                    record_hash = record.compute_record_hash()
                    if record_hash in self._existing_hashes:
                        continue

                yield record
                processed += 1

                if config.limit and processed >= config.limit:
                    break

    def _parse_row(self, row: dict) -> ProvincialCorporationRecord | None:
        """Parse a single row from the Nova Scotia co-ops CSV.

        Actual columns from NS Open Data:
        - Registry ID
        - Co-op Name
        - Incorporation Year (YYYY-MM-DD format)
        - Address
        - Town
        - Province/State
        - Postal Code
        - Non-Profit(N)/For-Profit(P)
        - Type

        Args:
            row: Dictionary of column name -> value

        Returns:
            ProvincialCorporationRecord if valid, None to skip
        """
        # Get name - required field
        name = row.get("Co-op Name", "").strip()
        if not name:
            return None

        # Get registry ID - required field
        registry_id = row.get("Registry ID", "").strip()
        if not registry_id:
            return None

        # Parse incorporation date (YYYY-MM-DD format)
        incorporation_date = None
        date_str = row.get("Incorporation Year", "").strip()
        if date_str:
            try:
                # Try full date format first (YYYY-MM-DD)
                incorporation_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                try:
                    # Try year only
                    year = int(date_str[:4])
                    incorporation_date = date(year, 1, 1)
                except (ValueError, TypeError):
                    pass

        # Determine corp type based on Non-Profit(N)/For-Profit(P) field
        profit_status = row.get("Non-Profit(N)/For-Profit(P)", "").strip().upper()
        coop_type = row.get("Type", "").strip()

        if profit_status == "N":
            corp_type = ProvincialCorpType.COOPERATIVE  # Non-profit cooperative
            corp_type_raw = f"Non-profit Cooperative - {coop_type}" if coop_type else "Non-profit Cooperative"
        elif profit_status == "P":
            corp_type = ProvincialCorpType.COOPERATIVE  # For-profit cooperative
            corp_type_raw = f"For-profit Cooperative - {coop_type}" if coop_type else "For-profit Cooperative"
        else:
            corp_type = ProvincialCorpType.COOPERATIVE
            corp_type_raw = f"Cooperative - {coop_type}" if coop_type else "Cooperative"

        # Build address
        address = None
        city = row.get("Town", "").strip()
        province = row.get("Province/State", "").strip() or "NS"
        postal = row.get("Postal Code", "").strip()
        street = row.get("Address", "").strip()

        if city or postal or street:
            address = Address(
                street_address=street or None,
                city=city or None,
                province=province,
                postal_code=postal or None,
            )

        return ProvincialCorporationRecord(
            name=name,
            name_french=None,
            registration_number=registry_id,
            business_number=None,  # Not provided in NS data
            corp_type_raw=corp_type_raw,
            status_raw="Registered",  # Assume active if in registry
            incorporation_date=incorporation_date,
            jurisdiction="NS",
            registered_address=address,
            source_url=self.DATA_URL,
        )

    async def _load_existing_hashes(self) -> None:
        """Load existing record hashes for incremental sync."""
        from ...db import get_db_session

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    text("""
                        SELECT metadata->>'record_hash' as hash
                        FROM entities
                        WHERE provincial_registry_id LIKE 'NS-%'
                        AND metadata->>'record_hash' IS NOT NULL
                    """)
                )
                rows = result.fetchall()
                self._existing_hashes = {row.hash for row in rows if row.hash}
                self.logger.info(f"Loaded {len(self._existing_hashes)} existing record hashes")
        except Exception as e:
            self.logger.warning(f"Could not load existing hashes: {e}")
            self._existing_hashes = set()

    async def process_record(
        self, record: ProvincialCorporationRecord
    ) -> dict[str, Any]:
        """Process a single co-operative record.

        Args:
            record: The co-operative record to process

        Returns:
            Dictionary with processing result
        """
        from ...db import get_db_session, get_neo4j_session
        import json

        async with get_db_session() as db:
            # Check if entity already exists
            result = await db.execute(
                text("SELECT id FROM entities WHERE provincial_registry_id = :registry_id"),
                {"registry_id": record.provincial_registry_id},
            )
            existing = result.fetchone()

            now = datetime.utcnow()
            record_hash = record.compute_record_hash()

            metadata = {
                "provincial_corp_type": record.corp_type_parsed.value,
                "provincial_corp_type_raw": record.corp_type_raw,
                "provincial_status": record.status_parsed.value,
                "provincial_status_raw": record.status_raw,
                "incorporation_date": record.incorporation_date.isoformat() if record.incorporation_date else None,
                "jurisdiction": record.jurisdiction,
                "registered_address": {
                    "street": record.registered_address.street_address if record.registered_address else None,
                    "city": record.registered_address.city if record.registered_address else None,
                    "province": record.registered_address.province if record.registered_address else None,
                    "postal_code": record.registered_address.postal_code if record.registered_address else None,
                } if record.registered_address else None,
                "source_url": record.source_url,
                "record_hash": record_hash,
                "last_synced": now.isoformat(),
            }

            if existing:
                # Update existing entity
                await db.execute(
                    text("""
                        UPDATE entities SET
                            metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                            updated_at = :updated_at
                        WHERE id = :id
                    """),
                    {"id": existing.id, "metadata": json.dumps(metadata), "updated_at": now},
                )
                entity_id = existing.id
                result_type = "duplicate"
            else:
                # Create new entity
                entity_id = uuid4()
                external_ids = {
                    "provincial_registry": "NS",
                    "ns_coop_registry_id": record.registration_number,
                }

                await db.execute(
                    text("""
                        INSERT INTO entities (
                            id, name, entity_type, provincial_registry_id,
                            external_ids, metadata, created_at, updated_at
                        ) VALUES (
                            :id, :name, :entity_type, :registry_id,
                            CAST(:external_ids AS jsonb), CAST(:metadata AS jsonb),
                            :created_at, :updated_at
                        )
                    """),
                    {
                        "id": entity_id,
                        "name": record.name,
                        "entity_type": "organization",
                        "registry_id": record.provincial_registry_id,
                        "external_ids": json.dumps(external_ids),
                        "metadata": json.dumps(metadata),
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                result_type = "created"

        # Sync to Neo4j
        try:
            async with get_neo4j_session() as session:
                await self._neo4j.merge_organization(
                    session,
                    id=str(entity_id),
                    name=record.name,
                    org_type="cooperative",
                    external_ids={
                        "provincial_registry_id": record.provincial_registry_id,
                    },
                    properties={
                        "jurisdiction": "CA-NS",
                        "provincial_corp_type": record.corp_type_parsed.value,
                        "provincial_status": record.status_parsed.value,
                        "incorporation_date": record.incorporation_date.isoformat() if record.incorporation_date else None,
                    },
                )
        except Exception as e:
            self.logger.warning(f"Neo4j sync failed for {record.name}: {e}")

        return {result_type: True, "entity_id": str(entity_id)}

    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync."""
        from ...db import get_db_session

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    text("""
                        SELECT completed_at FROM ingestion_runs
                        WHERE source = :source
                        AND status IN ('completed', 'partial')
                        ORDER BY completed_at DESC
                        LIMIT 1
                    """),
                    {"source": self.source_name},
                )
                row = result.fetchone()
                return row.completed_at if row else None
        except Exception:
            return None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the timestamp of a successful sync."""
        pass  # Handled by base class


async def run_nova_scotia_coops_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run Nova Scotia co-operatives ingestion.

    Args:
        incremental: If True, only process changed records
        limit: Maximum number of records to process
        run_id: Optional run ID for tracking

    Returns:
        Ingestion result dictionary with statistics

    Example:
        result = await run_nova_scotia_coops_ingestion(limit=100)
        print(f"Processed {result['records_processed']} co-ops")
    """
    ingester = NovaScotiaCoopsIngester()

    config = IngestionConfig(
        incremental=incremental,
        limit=limit,
    )

    result = await ingester.run(config, run_id=run_id)

    return {
        "run_id": str(result.run_id),
        "source": result.source,
        "status": result.status,
        "started_at": result.started_at.isoformat(),
        "completed_at": result.completed_at.isoformat() if result.completed_at else None,
        "records_processed": result.records_processed,
        "records_created": result.records_created,
        "records_updated": result.records_updated,
        "duplicates_found": result.duplicates_found,
        "errors": result.errors,
    }
