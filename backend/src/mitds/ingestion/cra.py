"""CRA Registered Charities ingester.

Ingests data from the CRA Charities Listings dataset on Canada Open Data Portal.
Key data points:
- Business Number (BN) - stable identifier
- Legal name, operating name
- Directors and trustees
- Gifts to qualified donees (funding relationships)

Data source: https://open.canada.ca/data/en/dataset/registered-charities
Format: CSV files, updated monthly
"""

import asyncio
import csv
import io
import re
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import UUID, uuid4
from zipfile import ZipFile

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..models import (
    Address,
    Organization,
    OrgStatus,
    OrgType,
)
from ..storage import StorageClient, compute_content_hash, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, with_retry, RetryConfig

logger = get_context_logger(__name__)

# CRA Open Data URLs (2023 dataset from Open Government Portal)
CRA_CHARITIES_URL = "https://open.canada.ca/data/en/dataset/05b3abd0-e70f-4b3b-a9c5-acc436bd15b6"
CRA_IDENTIFICATION_URL = "https://open.canada.ca/data/dataset/05b3abd0-e70f-4b3b-a9c5-acc436bd15b6/resource/31a52caf-fa79-4ab3-bded-1ccc7b61c17f/download/ident_2023_update.csv"
CRA_FINANCIALS_URL = "https://open.canada.ca/data/dataset/05b3abd0-e70f-4b3b-a9c5-acc436bd15b6/resource/0b9b4b01-5cb6-4981-b007-ae88f48cc799/download/financial_d_and_schedule_6_2023_updated.csv"
CRA_QUALIFIED_DONEES_URL = "https://open.canada.ca/data/dataset/05b3abd0-e70f-4b3b-a9c5-acc436bd15b6/resource/c603fe1f-cc4c-480e-b1cd-7fd949c42487/download/qualified_donees_2023_updated.csv"
CRA_DIRECTORS_URL = "https://open.canada.ca/data/dataset/05b3abd0-e70f-4b3b-a9c5-acc436bd15b6/resource/798a4a5f-f1ac-41a1-82d7-ef777f905bfe/download/directors_2023.csv"


class CRACharity(BaseModel):
    """Parsed CRA charity record."""

    # Identification
    bn: str = Field(..., description="Business Number")
    legal_name: str = Field(..., description="Legal name")
    operating_name: str | None = Field(default=None, description="Operating name")

    # Status
    status: str | None = Field(default=None, description="Registration status")
    effective_date: date | None = Field(default=None, description="Status effective date")
    designation: str | None = Field(default=None, description="Charity designation")

    # Location
    address: Address | None = None
    province: str | None = None

    # Category
    category: str | None = Field(default=None, description="Category code")
    category_desc: str | None = Field(default=None, description="Category description")

    # Fiscal period
    fiscal_period_end: date | None = None

    # Financial data (from T3010)
    total_revenue: float | None = None
    total_expenditures: float | None = None
    gifts_to_qualified_donees: float | None = None

    # Gifts to qualified donees (Schedule F)
    qualified_donee_gifts: list[dict[str, Any]] = Field(default_factory=list)


class CRAIngester(BaseIngester[CRACharity]):
    """Ingester for CRA Registered Charities data.

    Downloads bulk CSV files and extracts:
    - Organization details
    - Gifts to qualified donees (funding relationships)
    """

    def __init__(self):
        super().__init__("cra")
        self._http_client: httpx.AsyncClient | None = None
        self._storage: StorageClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=300.0),
                follow_redirects=True,
            )
        return self._http_client

    @property
    def storage(self) -> StorageClient:
        """Get storage client."""
        if self._storage is None:
            self._storage = get_storage()
        return self._storage

    async def close(self):
        """Close the HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[CRACharity]:
        """Fetch CRA charity records from bulk CSV files.

        Downloads identification, financials, and qualified donees data.
        """
        self.logger.info("Downloading CRA identification data...")
        identification = await self._download_and_extract(
            CRA_IDENTIFICATION_URL, "identification"
        )

        self.logger.info("Downloading CRA financials data...")
        financials = await self._download_and_extract(
            CRA_FINANCIALS_URL, "financials"
        )

        self.logger.info("Downloading CRA qualified donees data...")
        qualified_donees = await self._download_and_extract(
            CRA_QUALIFIED_DONEES_URL, "qualified_donees"
        )

        # Index financials by BN
        financials_by_bn: dict[str, dict] = {}
        if financials:
            for row in financials:
                bn = row.get("BN", row.get("bn", ""))
                if bn:
                    financials_by_bn[bn] = row

        # Index qualified donees by BN
        donees_by_bn: dict[str, list[dict]] = {}
        if qualified_donees:
            for row in qualified_donees:
                bn = row.get("BN", row.get("bn", ""))
                if bn:
                    if bn not in donees_by_bn:
                        donees_by_bn[bn] = []
                    donees_by_bn[bn].append(row)

        # Filter by target entities (BNs) if specified
        if config.target_entities:
            target_bns = set(config.target_entities)
            # Also add normalized versions
            target_bns_normalized = set()
            for bn in target_bns:
                target_bns_normalized.add(bn)
                normalized = self._normalize_bn(bn)
                if normalized:
                    target_bns_normalized.add(normalized)

            identification = [
                row for row in identification
                if (row.get("BN", row.get("bn", "")).strip() in target_bns_normalized
                    or self._normalize_bn(row.get("BN", row.get("bn", "")).strip()) in target_bns_normalized)
            ]
            self.logger.info(
                f"Filtered to {len(identification)} charities for "
                f"{len(config.target_entities)} target BNs"
            )

        # Process identification records
        for row in identification:
            try:
                charity = self._parse_charity(row, financials_by_bn, donees_by_bn)
                if charity:
                    yield charity
            except Exception as e:
                self.logger.warning(f"Failed to parse charity: {e}")
                continue

    async def _download_and_extract(
        self, url: str, data_type: str
    ) -> list[dict[str, str]]:
        """Download and parse CSV data (supports both direct CSV and ZIP files)."""
        async def _do_download():
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.content

        try:
            content = await with_retry(_do_download, logger=self.logger)
        except Exception as e:
            self.logger.error(f"Failed to download {data_type}: {e}")
            return []

        # Determine file type from URL
        is_csv = url.lower().endswith(".csv")
        extension = "csv" if is_csv else "zip"

        # Store raw file
        storage_key = generate_storage_key(
            "cra",
            f"{data_type}_{datetime.now().strftime('%Y%m%d')}",
            extension=extension,
        )
        await asyncio.to_thread(
            self.storage.upload_file,
            content,
            storage_key,
            content_type="text/csv" if is_csv else "application/zip",
            metadata={"data_type": data_type},
        )

        # Parse CSV (from ZIP or direct)
        try:
            if is_csv:
                return await asyncio.to_thread(
                    self._parse_csv_content, content, data_type
                )
            else:
                return await asyncio.to_thread(
                    self._extract_csv_from_zip, content, data_type
                )
        except Exception as e:
            self.logger.error(f"Failed to parse {data_type}: {e}")
            return []

    def _parse_csv_content(
        self, content: bytes, data_type: str
    ) -> list[dict[str, str]]:
        """Parse CSV content directly (sync, runs in thread)."""
        decoded = content.decode("utf-8-sig")  # Handle BOM
        reader = csv.DictReader(io.StringIO(decoded))
        rows = list(reader)
        self.logger.info(f"Parsed {len(rows)} rows from {data_type}")
        return rows

    def _extract_csv_from_zip(
        self, zip_content: bytes, data_type: str
    ) -> list[dict[str, str]]:
        """Extract and parse CSV data from a ZIP file (sync, runs in thread)."""
        with ZipFile(io.BytesIO(zip_content)) as zf:
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_files:
                self.logger.warning(f"No CSV files found in {data_type} ZIP")
                return []

            csv_content = zf.read(csv_files[0])

            # Parse CSV
            decoded = csv_content.decode("utf-8-sig")  # Handle BOM
            reader = csv.DictReader(io.StringIO(decoded))
            rows = list(reader)
            self.logger.info(f"Parsed {len(rows)} rows from {data_type}")
            return rows

    def _parse_charity(
        self,
        row: dict[str, str],
        financials_by_bn: dict[str, dict],
        donees_by_bn: dict[str, list[dict]],
    ) -> CRACharity | None:
        """Parse a charity from identification row."""
        # Get BN (Business Number)
        bn = row.get("BN", row.get("bn", "")).strip()
        if not bn:
            return None

        # Validate BN format
        if not self._validate_bn(bn):
            # Try to fix common issues
            bn = self._normalize_bn(bn)
            if not bn:
                return None

        # Get legal name
        legal_name = (
            row.get("Legal Name", row.get("legal_name", ""))
            or row.get("LegalNameEng", "")
            or row.get("LEGAL_NAME", "")
        ).strip()

        if not legal_name:
            return None

        # Get operating name
        operating_name = (
            row.get("Operating Name", row.get("operating_name", ""))
            or row.get("OperatingNameEng", "")
        )
        if operating_name:
            operating_name = operating_name.strip()

        # Get status
        status = row.get("Status", row.get("status", ""))
        effective_date_str = row.get("Effective Date", row.get("effective_date", ""))
        effective_date = None
        if effective_date_str:
            try:
                effective_date = datetime.strptime(
                    effective_date_str.strip(), "%Y-%m-%d"
                ).date()
            except ValueError:
                pass

        # Get designation
        designation = row.get("Designation", row.get("designation", ""))

        # Get address
        address = Address(
            street=row.get("Address", row.get("address", "")),
            city=row.get("City", row.get("city", "")),
            state=row.get("Province", row.get("province", "")),
            postal_code=row.get("Postal Code", row.get("postal_code", "")),
            country="CA",
        )

        province = row.get("Province", row.get("province", ""))

        # Get category
        category = row.get("Category", row.get("category", ""))
        category_desc = row.get("Category Description", row.get("category_desc", ""))

        # Get financial data
        fin_data = financials_by_bn.get(bn, {})
        total_revenue = self._parse_float(
            fin_data.get("Total Revenue", fin_data.get("total_revenue", ""))
        )
        total_expenditures = self._parse_float(
            fin_data.get("Total Expenditures", fin_data.get("total_expenditures", ""))
        )
        gifts_to_qd = self._parse_float(
            fin_data.get(
                "Gifts to qualified donees",
                fin_data.get("gifts_to_qualified_donees", ""),
            )
        )

        fiscal_end_str = fin_data.get(
            "Fiscal Period End", fin_data.get("fiscal_period_end", "")
        )
        fiscal_period_end = None
        if fiscal_end_str:
            try:
                fiscal_period_end = datetime.strptime(
                    fiscal_end_str.strip(), "%Y-%m-%d"
                ).date()
            except ValueError:
                pass

        # Get qualified donee gifts
        donee_records = donees_by_bn.get(bn, [])
        qualified_donee_gifts = []

        for donee_row in donee_records:
            donee_name = (
                donee_row.get("Associated charity - Legal name", "")
                or donee_row.get("Donee Name", "")
                or donee_row.get("donee_name", "")
            ).strip()

            donee_bn = (
                donee_row.get("Associated charity - BN/Registration #", "")
                or donee_row.get("Donee BN", "")
                or donee_row.get("donee_bn", "")
            ).strip()

            amount = self._parse_float(
                donee_row.get("Total amount of gifts", "")
                or donee_row.get("Amount", "")
                or donee_row.get("amount", "")
            )

            if donee_name:
                qualified_donee_gifts.append({
                    "donee_name": donee_name,
                    "donee_bn": self._normalize_bn(donee_bn) if donee_bn else None,
                    "amount": amount,
                })

        return CRACharity(
            bn=bn,
            legal_name=legal_name,
            operating_name=operating_name,
            status=status,
            effective_date=effective_date,
            designation=designation,
            address=address,
            province=province,
            category=category,
            category_desc=category_desc,
            fiscal_period_end=fiscal_period_end,
            total_revenue=total_revenue,
            total_expenditures=total_expenditures,
            gifts_to_qualified_donees=gifts_to_qd,
            qualified_donee_gifts=qualified_donee_gifts,
        )

    def _validate_bn(self, bn: str) -> bool:
        """Validate BN format: 9 digits + RR + 4 digits."""
        pattern = r"^\d{9}RR\d{4}$"
        return bool(re.match(pattern, bn))

    def _normalize_bn(self, bn: str) -> str | None:
        """Normalize BN to standard format."""
        if not bn:
            return None

        # Remove spaces and dashes
        clean = re.sub(r"[\s-]", "", bn.upper())

        # Check if valid after cleaning
        if self._validate_bn(clean):
            return clean

        # Try to extract valid BN from string
        match = re.search(r"(\d{9}RR\d{4})", clean)
        if match:
            return match.group(1)

        return None

    def _parse_float(self, value: str) -> float | None:
        """Parse a float value from string."""
        if not value:
            return None

        try:
            # Remove commas and currency symbols
            clean = re.sub(r"[,$]", "", value.strip())
            return float(clean) if clean else None
        except ValueError:
            return None

    async def process_record(self, record: CRACharity) -> dict[str, Any]:
        """Process a parsed CRA charity record.

        Creates/updates:
        - Organization entity
        - FUNDED_BY relationships for gifts to qualified donees
        """
        result = {"created": False, "updated": False, "duplicate": False}

        # Create or update organization in Neo4j
        async with get_neo4j_session() as session:
            # Check if organization exists
            query_check = """
            MATCH (o:Organization {bn: $bn})
            RETURN o.id as id, o.updated_at as updated_at
            """
            check_result = await session.run(query_check, bn=record.bn)
            existing = await check_result.single()

            org_id = uuid4() if not existing else UUID(existing["id"])

            if existing:
                result["updated"] = True
            else:
                result["created"] = True

            # Map status
            status = "active"
            if record.status:
                status_lower = record.status.lower()
                if "revoked" in status_lower or "annulled" in status_lower:
                    status = "revoked"
                elif "inactive" in status_lower:
                    status = "inactive"

            # Create/update organization node
            org_props = {
                "id": str(org_id),
                "name": record.legal_name,
                "bn": record.bn,
                "entity_type": "ORGANIZATION",
                "org_type": "nonprofit",
                "status": status,
                "jurisdiction": f"CA-{record.province}" if record.province else "CA",
                "confidence": 1.0,
                "updated_at": datetime.utcnow().isoformat(),
            }

            if record.operating_name:
                org_props["operating_name"] = record.operating_name

            if record.designation:
                org_props["designation"] = record.designation

            if record.category:
                org_props["category"] = record.category

            if record.address:
                org_props["address_street"] = record.address.street
                org_props["address_city"] = record.address.city
                org_props["address_state"] = record.address.state
                org_props["address_postal"] = record.address.postal_code
                org_props["address_country"] = record.address.country

            if record.total_revenue is not None:
                org_props["total_revenue"] = record.total_revenue

            if not existing:
                org_props["created_at"] = datetime.utcnow().isoformat()

            query_upsert = """
            MERGE (o:Organization {bn: $bn})
            SET o += $props
            RETURN o.id as id
            """
            await session.run(query_upsert, bn=record.bn, props=org_props)

            # Process gifts to qualified donees
            for gift in record.qualified_donee_gifts:
                if not gift.get("donee_name"):
                    continue

                donee_bn = gift.get("donee_bn")

                # Create or find recipient organization
                if donee_bn:
                    recipient_query = """
                    MERGE (r:Organization {bn: $bn})
                    ON CREATE SET
                        r.id = $id,
                        r.name = $name,
                        r.entity_type = 'ORGANIZATION',
                        r.org_type = 'nonprofit',
                        r.jurisdiction = 'CA',
                        r.confidence = 0.8,
                        r.created_at = $now
                    SET r.updated_at = $now
                    RETURN r.id as id
                    """
                    await session.run(
                        recipient_query,
                        bn=donee_bn,
                        id=str(uuid4()),
                        name=gift["donee_name"],
                        now=datetime.utcnow().isoformat(),
                    )
                else:
                    recipient_query = """
                    MERGE (r:Organization {name: $name, jurisdiction: 'CA'})
                    ON CREATE SET
                        r.id = $id,
                        r.entity_type = 'ORGANIZATION',
                        r.org_type = 'unknown',
                        r.confidence = 0.5,
                        r.created_at = $now
                    SET r.updated_at = $now
                    RETURN r.id as id
                    """
                    await session.run(
                        recipient_query,
                        id=str(uuid4()),
                        name=gift["donee_name"],
                        now=datetime.utcnow().isoformat(),
                    )

                # Create FUNDED_BY relationship (recipient <- funder)
                if donee_bn:
                    funded_query = """
                    MATCH (recipient:Organization {bn: $recipient_bn})
                    MATCH (funder:Organization {bn: $funder_bn})
                    MERGE (recipient)-[r:FUNDED_BY]->(funder)
                    SET r.amount = $amount,
                        r.amount_currency = 'CAD',
                        r.fiscal_year = $fiscal_year,
                        r.confidence = 1.0,
                        r.updated_at = $now
                    """
                else:
                    funded_query = """
                    MATCH (recipient:Organization {name: $recipient_name, jurisdiction: 'CA'})
                    MATCH (funder:Organization {bn: $funder_bn})
                    MERGE (recipient)-[r:FUNDED_BY]->(funder)
                    SET r.amount = $amount,
                        r.amount_currency = 'CAD',
                        r.fiscal_year = $fiscal_year,
                        r.confidence = 0.8,
                        r.updated_at = $now
                    """

                fiscal_year = None
                if record.fiscal_period_end:
                    fiscal_year = record.fiscal_period_end.year

                await session.run(
                    funded_query,
                    recipient_bn=donee_bn,
                    recipient_name=gift["donee_name"],
                    funder_bn=record.bn,
                    amount=gift.get("amount"),
                    fiscal_year=fiscal_year,
                    now=datetime.utcnow().isoformat(),
                )

        return result

    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync."""
        async with get_db_session() as session:
            from sqlalchemy import text

            query = text("""
                SELECT MAX(completed_at) as last_sync
                FROM ingestion_runs
                WHERE source = :source AND status IN ('completed', 'partial')
            """)
            result = await session.execute(query, {"source": self.source_name})
            row = result.first()
            if row and row.last_sync:
                return row.last_sync
        return None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the timestamp of a successful sync."""
        # Sync time is saved implicitly via ingestion_runs table
        pass


# Celery task for scheduled ingestion
def get_cra_celery_task():
    """Get the Celery task for CRA ingestion.

    Returns the task function to be registered with Celery.
    """
    from ..worker import celery_app

    @celery_app.task(name="mitds.ingestion.cra.ingest")
    def ingest_cra_task(incremental: bool = True):
        """Celery task for CRA charities ingestion.

        Args:
            incremental: Whether to do incremental sync
        """
        import asyncio

        async def run_ingestion():
            ingester = CRAIngester()
            try:
                config = IngestionConfig(incremental=incremental)
                result = await ingester.run(config)
                return result.model_dump()
            finally:
                await ingester.close()

        return asyncio.run(run_ingestion())

    return ingest_cra_task


async def run_cra_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run CRA ingestion directly (not via Celery).

    Args:
        incremental: Whether to do incremental sync
        limit: Maximum number of records to process
        target_entities: Optional list of BNs to ingest specifically
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = CRAIngester()
    try:
        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
        )
        result = await ingester.run(config, run_id=run_id)
        return result.model_dump()
    finally:
        await ingester.close()
