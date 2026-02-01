"""Targeted ingester for provinces without bulk data access.

This module provides ingesters for provinces that don't offer bulk data downloads.
These ingesters support two modes:
1. Web scraping - search provincial registry websites directly (fragile but automated)
2. CSV upload - accept pre-gathered data in standard format (reliable but manual)

Provinces without bulk data:
- Saskatchewan (SK) - ISC Online Services
- Manitoba (MB) - Companies Office
- New Brunswick (NB) - Service New Brunswick
- Prince Edward Island (PEI/PE) - Corporate Registry
- Newfoundland and Labrador (NL) - Registry of Companies
- Northwest Territories (NT) - MACA Registry
- Yukon (YT) - Corporate Affairs
- Nunavut (NU) - Legal Registries

Usage:
    # Web scraping mode
    from mitds.ingestion.provincial import run_targeted_ingestion

    result = await run_targeted_ingestion(
        province="SK",
        target_entities=["Postmedia Network", "Corus Entertainment"],
    )

    # CSV upload mode
    result = await run_targeted_ingestion(
        province="SK",
        from_csv="/path/to/known_entities.csv",
    )
"""

import csv
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
from rapidfuzz import fuzz
from sqlalchemy import text

from ..base import (
    BaseIngester,
    IngestionConfig,
    Neo4jHelper,
    PostgresHelper,
)
from .models import (
    Address,
    EntityMatchResult,
    ProvincialCorporationRecord,
    ProvincialCorpStatus,
    ProvincialCorpType,
)


# CSV format for manual upload
# Expected columns: name,registration_number,business_number,corp_type,status,incorporation_date,street,city,province,postal_code
CSV_EXPECTED_COLUMNS = [
    "name",
    "registration_number",
    "corp_type",
    "status",
]


class BaseTargetedIngester(BaseIngester[ProvincialCorporationRecord], ABC):
    """Abstract base class for targeted ingesters (web scraping + CSV upload).

    Supports two ingestion modes:
    1. Target mode: Search for specific entities by name via web scraping
    2. CSV mode: Import pre-gathered data from a CSV file

    Subclasses must implement:
    - province: Province code
    - search_entity(name): Search registry website for entity
    - get_registry_url(): Base URL for the registry
    """

    FUZZY_MATCH_THRESHOLD = 0.85
    AUTO_LINK_THRESHOLD = 0.95

    def __init__(self):
        """Initialize the targeted ingester."""
        super().__init__(f"{self.province.lower()}-targeted")
        self._neo4j = Neo4jHelper(self.logger)
        self._postgres = PostgresHelper(self.logger)
        self._http_client: httpx.AsyncClient | None = None

    @property
    @abstractmethod
    def province(self) -> str:
        """Return the province code (e.g., 'SK', 'MB', 'NB')."""
        ...

    @abstractmethod
    def get_registry_url(self) -> str:
        """Return the base URL for the provincial registry."""
        ...

    @abstractmethod
    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity by name in the provincial registry.

        Args:
            name: Entity name to search for

        Returns:
            ProvincialCorporationRecord if found, None otherwise
        """
        ...

    def get_search_url(self, name: str) -> str:
        """Build search URL for the provincial registry.

        Override in subclasses to customize search URL construction.

        Args:
            name: Entity name to search for

        Returns:
            Full URL for searching the entity
        """
        import urllib.parse
        base_url = self.get_registry_url()
        return f"{base_url}?search={urllib.parse.quote(name)}"

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for web requests."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7",
                },
                follow_redirects=True,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def parse_csv_file(self, csv_path: str | Path) -> list[ProvincialCorporationRecord]:
        """Parse a CSV file containing corporation data.

        Expected columns:
        - name (required)
        - registration_number (required)
        - business_number (optional)
        - corp_type (required)
        - status (required)
        - incorporation_date (optional, YYYY-MM-DD)
        - street (optional)
        - city (optional)
        - province (optional, defaults to self.province)
        - postal_code (optional)

        Args:
            csv_path: Path to CSV file

        Returns:
            List of ProvincialCorporationRecord objects
        """
        records = []
        path = Path(csv_path)

        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate required columns
            if reader.fieldnames:
                missing = set(CSV_EXPECTED_COLUMNS) - set(reader.fieldnames)
                if missing:
                    raise ValueError(
                        f"Missing required columns: {missing}. "
                        f"Expected: {CSV_EXPECTED_COLUMNS}"
                    )

            for row in reader:
                record = self._parse_csv_row(row)
                if record:
                    records.append(record)

        return records

    def _parse_csv_row(self, row: dict) -> ProvincialCorporationRecord | None:
        """Parse a single row from the uploaded CSV.

        Args:
            row: Dictionary of column name -> value

        Returns:
            ProvincialCorporationRecord if valid, None to skip
        """
        name = row.get("name", "").strip()
        registration_number = row.get("registration_number", "").strip()

        if not name or not registration_number:
            return None

        # Parse optional fields
        business_number = row.get("business_number", "").strip() or None
        corp_type = row.get("corp_type", "unknown").strip()
        status = row.get("status", "unknown").strip()

        # Parse incorporation date
        incorporation_date = None
        date_str = row.get("incorporation_date", "").strip()
        if date_str:
            try:
                incorporation_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                pass

        # Build address
        address = None
        city = row.get("city", "").strip()
        postal = row.get("postal_code", "").strip()
        if city or postal:
            address = Address(
                street_address=row.get("street", "").strip() or None,
                city=city or None,
                province=row.get("province", "").strip() or self.province,
                postal_code=postal or None,
            )

        return ProvincialCorporationRecord(
            name=name,
            name_french=None,
            registration_number=registration_number,
            business_number=business_number,
            corp_type_raw=corp_type,
            status_raw=status,
            incorporation_date=incorporation_date,
            jurisdiction=self.province,
            registered_address=address,
            source_url=f"csv-upload:{Path(row.get('_source_file', 'unknown')).name}",
        )

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[ProvincialCorporationRecord]:
        """Fetch records from target entities or CSV file.

        Uses either:
        - Web scraping if target_entities is provided
        - CSV parsing if csv_file is provided via config extension
        """
        # Check for CSV file in config (stored in target_entities[0] with csv: prefix)
        csv_file = None
        target_entities = config.target_entities or []

        # Check for CSV file marker
        if target_entities and len(target_entities) == 1:
            first = target_entities[0]
            if first.startswith("csv:"):
                csv_file = first[4:]  # Remove "csv:" prefix
                target_entities = []

        if csv_file:
            # CSV upload mode
            print(f"Loading from CSV file: {csv_file}", file=sys.stderr)
            records = self.parse_csv_file(csv_file)
            print(f"Found {len(records):,} records", file=sys.stderr)

            processed = 0
            for record in records:
                yield record
                processed += 1
                if config.limit and processed >= config.limit:
                    break

        elif target_entities:
            # Web scraping mode
            print(f"Searching for {len(target_entities)} target entities...", file=sys.stderr)

            processed = 0
            for entity_name in target_entities:
                try:
                    print(f"  Searching: {entity_name}...", file=sys.stderr, end=" ")
                    record = await self.search_entity(entity_name)

                    if record:
                        print("found", file=sys.stderr)
                        yield record
                        processed += 1
                    else:
                        print("not found", file=sys.stderr)

                    if config.limit and processed >= config.limit:
                        break

                except Exception as e:
                    print(f"error: {e}", file=sys.stderr)
                    self.logger.warning(f"Failed to search for {entity_name}: {e}")
                    continue

            await self.close()

        else:
            print("No target entities or CSV file provided", file=sys.stderr)
            return

    async def process_record(
        self, record: ProvincialCorporationRecord
    ) -> dict[str, Any]:
        """Process a single corporation record."""
        from ...db import get_db_session, get_neo4j_session
        import json

        match_result = await self.match_existing_entity(record)

        async with get_db_session() as db:
            if match_result.is_match and match_result.is_auto_linkable:
                entity_id = match_result.matched_entity_id
                await self._update_entity(db, entity_id, record)
                result_type = "updated"
            elif match_result.requires_review:
                entity_id, is_new = await self._create_entity(db, record)
                result_type = "created"
            else:
                entity_id, is_new = await self._create_entity(db, record)
                result_type = "created" if is_new else "duplicate"

        # Sync to Neo4j
        try:
            async with get_neo4j_session() as session:
                await self._neo4j.merge_organization(
                    session,
                    id=str(entity_id),
                    name=record.name,
                    org_type=record.corp_type_parsed.value,
                    external_ids={
                        "provincial_registry_id": record.provincial_registry_id,
                        "business_number": record.business_number,
                    },
                    properties={
                        "jurisdiction": f"CA-{record.jurisdiction}",
                        "provincial_corp_type": record.corp_type_parsed.value,
                        "provincial_status": record.status_parsed.value,
                        "incorporation_date": record.incorporation_date.isoformat() if record.incorporation_date else None,
                    },
                )
        except Exception as e:
            self.logger.warning(f"Neo4j sync failed for {record.name}: {e}")

        return {
            result_type: True,
            "entity_id": str(entity_id),
        }

    async def _update_entity(
        self, db, entity_id: UUID, record: ProvincialCorporationRecord
    ) -> None:
        """Update existing entity with provincial data."""
        import json
        now = datetime.utcnow()
        metadata = self._build_metadata(record, now)

        await db.execute(
            text("""
                UPDATE entities SET
                    provincial_registry_id = :registry_id,
                    metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                    updated_at = :updated_at
                WHERE id = :id
            """),
            {
                "id": entity_id,
                "registry_id": record.provincial_registry_id,
                "metadata": json.dumps(metadata),
                "updated_at": now,
            },
        )

    async def _create_entity(
        self, db, record: ProvincialCorporationRecord
    ) -> tuple[UUID, bool]:
        """Create or update entity from provincial record."""
        import json

        result = await db.execute(
            text("SELECT id FROM entities WHERE provincial_registry_id = :registry_id"),
            {"registry_id": record.provincial_registry_id},
        )
        existing = result.fetchone()

        now = datetime.utcnow()
        metadata = self._build_metadata(record, now)

        if existing:
            await db.execute(
                text("""
                    UPDATE entities SET
                        metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                        updated_at = :updated_at
                    WHERE id = :id
                """),
                {"id": existing.id, "metadata": json.dumps(metadata), "updated_at": now},
            )
            return existing.id, False
        else:
            new_id = uuid4()
            external_ids = {
                "provincial_registry": record.jurisdiction,
                f"{record.jurisdiction.lower()}_corp_number": record.registration_number,
            }
            if record.business_number:
                external_ids["business_number"] = record.business_number

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
                    "id": new_id,
                    "name": record.name,
                    "entity_type": "organization",
                    "registry_id": record.provincial_registry_id,
                    "external_ids": json.dumps(external_ids),
                    "metadata": json.dumps(metadata),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return new_id, True

    def _build_metadata(
        self, record: ProvincialCorporationRecord, now: datetime
    ) -> dict:
        """Build metadata dict for entity storage."""
        addr = record.registered_address
        return {
            "provincial_corp_type": record.corp_type_parsed.value,
            "provincial_corp_type_raw": record.corp_type_raw,
            "provincial_status": record.status_parsed.value,
            "provincial_status_raw": record.status_raw,
            "incorporation_date": record.incorporation_date.isoformat() if record.incorporation_date else None,
            "jurisdiction": record.jurisdiction,
            "registered_address": {
                "street": addr.street_address if addr else None,
                "city": addr.city if addr else None,
                "province": addr.province if addr else None,
                "postal_code": addr.postal_code if addr else None,
            } if addr else None,
            "source_url": record.source_url,
            "record_hash": record.compute_record_hash(),
            "last_synced": now.isoformat(),
            "ingestion_method": "targeted",
        }

    async def match_existing_entity(
        self, record: ProvincialCorporationRecord
    ) -> EntityMatchResult:
        """Match a provincial record against existing entities."""
        from ...db import get_db_session

        async with get_db_session() as db:
            # Try business number match first
            if record.business_number:
                result = await db.execute(
                    text("""
                        SELECT id, name FROM entities
                        WHERE external_ids->>'business_number' = :bn
                        LIMIT 1
                    """),
                    {"bn": record.business_number},
                )
                bn_match = result.fetchone()
                if bn_match:
                    return EntityMatchResult(
                        provincial_record_name=record.name,
                        matched_entity_id=bn_match.id,
                        matched_entity_name=bn_match.name,
                        match_score=1.0,
                        match_method="business_number",
                        requires_review=False,
                    )

            # Try exact name match
            result = await db.execute(
                text("""
                    SELECT id, name FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                    AND entity_type = 'organization'
                    AND provincial_registry_id IS NULL
                    LIMIT 1
                """),
                {"name": record.name},
            )
            exact_match = result.fetchone()

            if exact_match:
                return EntityMatchResult(
                    provincial_record_name=record.name,
                    matched_entity_id=exact_match.id,
                    matched_entity_name=exact_match.name,
                    match_score=1.0,
                    match_method="exact",
                    requires_review=False,
                )

            # Try fuzzy match
            result = await db.execute(
                text("""
                    SELECT id, name FROM entities
                    WHERE entity_type = 'organization'
                    AND provincial_registry_id IS NULL
                    AND similarity(LOWER(name), LOWER(:name)) > 0.3
                    ORDER BY similarity(LOWER(name), LOWER(:name)) DESC
                    LIMIT 5
                """),
                {"name": record.name},
            )
            candidates = result.fetchall()

            best_match = None
            best_score = 0.0

            for candidate in candidates:
                score = fuzz.token_sort_ratio(
                    record.name.lower(), candidate.name.lower()
                ) / 100.0

                if score > best_score:
                    best_score = score
                    best_match = candidate

            if best_match and best_score >= self.FUZZY_MATCH_THRESHOLD:
                return EntityMatchResult(
                    provincial_record_name=record.name,
                    matched_entity_id=best_match.id,
                    matched_entity_name=best_match.name,
                    match_score=best_score,
                    match_method="fuzzy",
                    requires_review=best_score < self.AUTO_LINK_THRESHOLD,
                )

        return EntityMatchResult(
            provincial_record_name=record.name,
            match_score=0.0,
            match_method="none",
        )

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
        pass


# =============================================================================
# Province-Specific Scrapers
# =============================================================================


class SaskatchewanTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Saskatchewan corporation registry.

    Uses ISC Online Services for entity searches.
    Note: This is web scraping and may break if the website changes.
    """

    @property
    def province(self) -> str:
        return "SK"

    def get_registry_url(self) -> str:
        return "https://corporateregistry.isc.ca/CorpRegistry/CorpSearch/"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the Saskatchewan registry.

        Note: This is a simplified implementation. The actual ISC website
        may require JavaScript rendering or session handling.
        """
        try:
            client = await self._get_http_client()

            # Saskatchewan ISC uses a form-based search
            # This is a simplified version - actual implementation may need
            # session handling, CSRF tokens, or Selenium for JS rendering
            search_url = self.get_registry_url()

            response = await client.get(search_url)
            if response.status_code != 200:
                return None

            # For now, return None as full scraping requires more complex handling
            # In production, this would parse the HTML response or use Selenium
            self.logger.warning(
                f"Saskatchewan web scraping requires complex session handling. "
                f"Consider using CSV upload mode for: {name}"
            )
            return None

        except Exception as e:
            self.logger.warning(f"SK search error for {name}: {e}")
            return None

    def map_corp_type(self, raw: str) -> ProvincialCorpType:
        """Map Saskatchewan corporation type to standard classification."""
        raw_lower = raw.lower()

        if "non-profit" in raw_lower or "nonprofit" in raw_lower:
            return ProvincialCorpType.NONPROFIT
        if "cooperative" in raw_lower or "co-operative" in raw_lower:
            return ProvincialCorpType.COOPERATIVE
        if any(kw in raw_lower for kw in ["business", "corporation", "inc", "ltd"]):
            return ProvincialCorpType.FOR_PROFIT

        return ProvincialCorpType.UNKNOWN

    def map_status(self, raw: str) -> ProvincialCorpStatus:
        """Map Saskatchewan status to standard classification."""
        raw_lower = raw.lower()

        if "active" in raw_lower or "good standing" in raw_lower:
            return ProvincialCorpStatus.ACTIVE
        if "dissolved" in raw_lower:
            return ProvincialCorpStatus.DISSOLVED
        if "struck" in raw_lower or "cancelled" in raw_lower:
            return ProvincialCorpStatus.STRUCK

        return ProvincialCorpStatus.UNKNOWN


class ManitobaTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Manitoba corporation registry.

    Uses Companies Office search.
    Note: This is web scraping and may break if the website changes.
    """

    @property
    def province(self) -> str:
        return "MB"

    def get_registry_url(self) -> str:
        return "https://companiesoffice.gov.mb.ca/search/"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the Manitoba registry."""
        try:
            # Manitoba also uses complex form-based search
            self.logger.warning(
                f"Manitoba web scraping requires complex session handling. "
                f"Consider using CSV upload mode for: {name}"
            )
            return None
        except Exception as e:
            self.logger.warning(f"MB search error for {name}: {e}")
            return None


class NewBrunswickTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for New Brunswick corporation registry.

    Uses Service New Brunswick Corporate Registry search.
    """

    @property
    def province(self) -> str:
        return "NB"

    def get_registry_url(self) -> str:
        return "https://www.pxw1.snb.ca/snb7001/e/2000/2500e.asp"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the New Brunswick registry."""
        try:
            self.logger.warning(
                f"New Brunswick web scraping requires complex session handling. "
                f"Consider using CSV upload mode for: {name}"
            )
            return None
        except Exception as e:
            self.logger.warning(f"NB search error for {name}: {e}")
            return None


class PEITargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Prince Edward Island corporation registry."""

    @property
    def province(self) -> str:
        return "PE"

    def get_registry_url(self) -> str:
        return "https://www.princeedwardisland.ca/en/feature/corporate-registry"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the PEI registry."""
        try:
            self.logger.warning(
                f"PEI web scraping requires complex session handling. "
                f"Consider using CSV upload mode for: {name}"
            )
            return None
        except Exception as e:
            self.logger.warning(f"PE search error for {name}: {e}")
            return None


class NewfoundlandTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Newfoundland and Labrador corporation registry."""

    @property
    def province(self) -> str:
        return "NL"

    def get_registry_url(self) -> str:
        return "https://cado.eservices.gov.nl.ca/CADOInternet/Main.aspx"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the NL registry."""
        try:
            self.logger.warning(
                f"Newfoundland web scraping requires complex session handling. "
                f"Consider using CSV upload mode for: {name}"
            )
            return None
        except Exception as e:
            self.logger.warning(f"NL search error for {name}: {e}")
            return None


# =============================================================================
# Territory Ingesters
# =============================================================================


class OntarioTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Ontario corporation registry.

    Note: Ontario does NOT provide bulk corporation data publicly.
    Only intermediary/partner lists are available on data.ontario.ca.
    This ingester supports CSV upload for manually gathered data.
    """

    @property
    def province(self) -> str:
        return "ON"

    def get_registry_url(self) -> str:
        return "https://www.ontario.ca/page/ontario-business-registry"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the Ontario Business Registry.

        Note: Ontario's registry doesn't support direct URL searches.
        Use CSV upload mode for Ontario corporation data.
        """
        self.logger.warning(
            f"Ontario web scraping not available. "
            f"Ontario Business Registry requires authenticated access. "
            f"Use CSV upload mode for: {name}"
        )
        return None

    def map_corp_type(self, raw: str) -> ProvincialCorpType:
        """Map Ontario corporation type to standard classification."""
        raw_lower = raw.lower()

        if "not-for-profit" in raw_lower or "not for profit" in raw_lower:
            return ProvincialCorpType.NOT_FOR_PROFIT
        if "non-profit" in raw_lower or "nonprofit" in raw_lower:
            return ProvincialCorpType.NONPROFIT
        if "co-operative" in raw_lower or "cooperative" in raw_lower:
            return ProvincialCorpType.COOPERATIVE
        if "professional" in raw_lower:
            return ProvincialCorpType.PROFESSIONAL
        if "extra-provincial" in raw_lower or "extraprovincial" in raw_lower:
            return ProvincialCorpType.EXTRAPROVINCIAL
        if any(kw in raw_lower for kw in ["business", "corporation", "inc", "ltd"]):
            return ProvincialCorpType.FOR_PROFIT

        return ProvincialCorpType.UNKNOWN

    def map_status(self, raw: str) -> ProvincialCorpStatus:
        """Map Ontario status to standard classification."""
        raw_lower = raw.lower()

        if "active" in raw_lower:
            return ProvincialCorpStatus.ACTIVE
        if "inactive" in raw_lower:
            return ProvincialCorpStatus.INACTIVE
        if "dissolved" in raw_lower:
            return ProvincialCorpStatus.DISSOLVED
        if "cancelled" in raw_lower:
            return ProvincialCorpStatus.STRUCK
        if "amalgamated" in raw_lower:
            return ProvincialCorpStatus.AMALGAMATED

        return ProvincialCorpStatus.UNKNOWN


class NWTTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Northwest Territories corporation registry."""

    @property
    def province(self) -> str:
        return "NT"

    def get_registry_url(self) -> str:
        return "https://www.maca.gov.nt.ca/"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the NT registry."""
        self.logger.warning(
            f"NT web scraping not available. Use CSV upload mode for: {name}"
        )
        return None


class YukonTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Yukon corporation registry."""

    @property
    def province(self) -> str:
        return "YT"

    def get_registry_url(self) -> str:
        return "https://corporateonline.gov.yk.ca/"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the YT registry."""
        self.logger.warning(
            f"Yukon web scraping not available. Use CSV upload mode for: {name}"
        )
        return None


class NunavutTargetedIngester(BaseTargetedIngester):
    """Targeted ingester for Nunavut corporation registry."""

    @property
    def province(self) -> str:
        return "NU"

    def get_registry_url(self) -> str:
        return "https://www.nunavutlegalregistries.ca/"

    async def search_entity(self, name: str) -> ProvincialCorporationRecord | None:
        """Search for an entity in the NU registry."""
        self.logger.warning(
            f"Nunavut web scraping not available. Use CSV upload mode for: {name}"
        )
        return None


# =============================================================================
# Registry and Entry Points
# =============================================================================


# Province code to ingester class mapping
TARGETED_INGESTERS: dict[str, type[BaseTargetedIngester]] = {
    "ON": OntarioTargetedIngester,
    "SK": SaskatchewanTargetedIngester,
    "MB": ManitobaTargetedIngester,
    "NB": NewBrunswickTargetedIngester,
    "PE": PEITargetedIngester,
    "PEI": PEITargetedIngester,  # Alias
    "NL": NewfoundlandTargetedIngester,
    "NT": NWTTargetedIngester,
    "YT": YukonTargetedIngester,
    "NU": NunavutTargetedIngester,
}


def get_targeted_ingester(province: str) -> BaseTargetedIngester:
    """Get the targeted ingester for a province.

    Args:
        province: Province code (SK, MB, NB, PE/PEI, NL, NT, YT, NU)

    Returns:
        Appropriate targeted ingester instance

    Raises:
        ValueError: If province code is not recognized
    """
    province_upper = province.upper()

    if province_upper not in TARGETED_INGESTERS:
        available = ", ".join(sorted(set(TARGETED_INGESTERS.keys())))
        raise ValueError(
            f"Unknown province code: {province}. "
            f"Available: {available}"
        )

    return TARGETED_INGESTERS[province_upper]()


async def run_targeted_ingestion(
    province: str,
    target_entities: list[str] | None = None,
    from_csv: str | None = None,
    limit: int | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run targeted ingestion for a province without bulk data.

    Supports two modes:
    1. Target mode: Provide target_entities list to search for specific orgs
    2. CSV mode: Provide from_csv path to import pre-gathered data

    Args:
        province: Province code (SK, MB, NB, PE, NL, NT, YT, NU)
        target_entities: List of entity names to search for (web scraping)
        from_csv: Path to CSV file with corporation data (manual upload)
        limit: Maximum records to process
        run_id: Optional run ID for tracking

    Returns:
        Ingestion result dictionary with statistics

    Raises:
        ValueError: If neither target_entities nor from_csv is provided

    Example:
        # Web scraping mode
        result = await run_targeted_ingestion(
            province="SK",
            target_entities=["Postmedia Network", "Corus Entertainment"],
        )

        # CSV upload mode
        result = await run_targeted_ingestion(
            province="SK",
            from_csv="/path/to/known_entities.csv",
        )
    """
    if not target_entities and not from_csv:
        raise ValueError(
            "Either target_entities or from_csv must be provided"
        )

    ingester = get_targeted_ingester(province)

    # Build target list - CSV files are marked with prefix
    targets = target_entities.copy() if target_entities else []
    if from_csv:
        targets = [f"csv:{from_csv}"]

    config = IngestionConfig(
        incremental=False,  # Targeted ingestion is always full
        limit=limit,
        target_entities=targets,
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
        "province": province.upper(),
        "mode": "csv" if from_csv else "targeted",
    }


def generate_csv_template(output_path: str | Path | None = None) -> str:
    """Generate a CSV template for manual data upload.

    Args:
        output_path: Optional path to write the template file

    Returns:
        CSV template as string

    Example:
        # Get template as string
        template = generate_csv_template()

        # Write to file
        generate_csv_template("./my_corporations.csv")
    """
    headers = [
        "name",
        "registration_number",
        "business_number",
        "corp_type",
        "status",
        "incorporation_date",
        "street",
        "city",
        "province",
        "postal_code",
    ]

    example_rows = [
        [
            "Example Corporation Inc.",
            "SK123456789",
            "123456789RC0001",
            "for_profit",
            "active",
            "2020-01-15",
            "123 Main St",
            "Regina",
            "SK",
            "S4P 1A1",
        ],
        [
            "Example Non-Profit Society",
            "SK987654321",
            "",
            "nonprofit",
            "active",
            "2018-06-01",
            "456 Oak Ave",
            "Saskatoon",
            "SK",
            "S7K 2B3",
        ],
    ]

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in example_rows:
        writer.writerow(row)

    template = output.getvalue()

    if output_path:
        path = Path(output_path)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(template)

    return template
