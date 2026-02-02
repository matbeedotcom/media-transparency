"""Canada Corporations (ISED) ingester.

Ingests federal corporation data from Innovation, Science and Economic Development Canada.
Key data points:
- Corporation name, number, status
- Business number (BN)
- Directors and officers
- Registered office address
- Incorporation date and jurisdiction

Data sources:
- ISED Federal Corporation API: https://api.ised-isde.canada.ca/en/docs?api=corporations
- Open Government Portal: https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2

Coverage: Federal Canadian corporations (CBCA, NFP Act, Coop Act)
Free API, no key required.
"""

import asyncio
import csv
import io
import json
import os
import time
import zipfile
from datetime import datetime, date
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

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
    Person,
)
from ..models.evidence import Evidence, EvidenceType
from ..storage import compute_content_hash, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, IngestionResult, with_retry

logger = get_context_logger(__name__)

# Disk cache for bulk data downloads (avoids re-downloading 200+ MB ZIP)
_BULK_CACHE_DIR = Path(
    os.environ.get(
        "MITDS_CACHE_DIR",
        str(Path(__file__).resolve().parents[4] / ".cache" / "mitds"),
    )
) / "ingestion"
_BULK_CACHE_TTL_HOURS = 24

# ISED API endpoints
ISED_API_BASE = "https://ised-isde.canada.ca/cc/lgcy"
ISED_API_V2_BASE = "https://apigateway-passerelledapi.ised-isde.canada.ca/corporations/api/v2"

# Open Government Portal bulk data
OPEN_DATA_CORPORATIONS_URL = "https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2"
# Direct download from ISED (XML format in ZIP)
BULK_DATA_URL = "https://ised-isde.canada.ca/cc/lgcy/download/OPEN_DATA_SPLIT.zip"

# Corporation status mapping
STATUS_MAP = {
    "Active": OrgStatus.ACTIVE,
    "Dissolved": OrgStatus.INACTIVE,
    "Inactive": OrgStatus.INACTIVE,
    "Amalgamated": OrgStatus.INACTIVE,
    "Continued out": OrgStatus.INACTIVE,
    "Revoked": OrgStatus.REVOKED,
}

# Corporation type mapping
TYPE_MAP = {
    "CBCA": OrgType.CORPORATION,  # Canada Business Corporations Act
    "CCA": OrgType.NONPROFIT,  # Canada Corporations Act (Part II - NFP)
    "NFP": OrgType.NONPROFIT,  # Canada Not-for-profit Corporations Act
    "COOP": OrgType.CORPORATION,  # Canada Cooperatives Act (map to corporation)
    "BOTA": OrgType.UNKNOWN,  # Boards of Trade Act
}


class CanadaCorporation(BaseModel):
    """Parsed Canadian federal corporation record."""

    corporation_number: str = Field(..., description="Federal corporation number")
    corporation_name: str = Field(..., description="Legal name")
    business_number: str | None = Field(None, description="9-digit CRA business number")

    # Status and type
    status: str = "Active"
    corporation_type: str | None = None  # CBCA, CCA, NFP, COOP, BOTA

    # Dates
    incorporation_date: date | None = None
    amalgamation_date: date | None = None
    dissolution_date: date | None = None

    # Registered office
    registered_office: Address | None = None

    # Directors
    directors: list[dict[str, Any]] = Field(default_factory=list)

    # Activity
    activity_code: str | None = None
    activity_description: str | None = None


class CanadaDirector(BaseModel):
    """Director of a Canadian corporation."""

    name: str
    address: Address | None = None
    appointment_date: date | None = None
    cessation_date: date | None = None
    is_resident_canadian: bool | None = None


class CanadaCorporationsIngester(BaseIngester[CanadaCorporation]):
    """Ingester for Canadian federal corporation data.

    Uses the Open Government Portal bulk data export which includes:
    - All federal corporations (CBCA, NFP, COOP, BOTA)
    - Directors and officers
    - Historical data

    Free, no API key required.
    """

    def __init__(self):
        """Initialize the Canada Corporations ingester."""
        super().__init__(source_name="canada_corps")
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=30.0),
                headers={
                    "User-Agent": "MITDS Research contact@mitds.org",
                    "Accept": "*/*",
                },
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def get_last_sync_time(self) -> datetime | None:
        """Get timestamp of last successful sync."""
        async with get_db_session() as db:
            from sqlalchemy import text
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
        """Save sync timestamp (handled by base class run method)."""
        pass

    async def fetch_directors(self, corporation_number: str) -> list[CanadaDirector]:
        """Fetch directors for a corporation from the ISED v2 API.

        Requires ISED_API_KEY to be configured. Returns empty list if
        API key is not set or the request fails.

        Args:
            corporation_number: 7-digit federal corporation number

        Returns:
            List of parsed director records
        """
        settings = get_settings()
        if not settings.ised_api_key:
            self.logger.debug("ISED API key not configured, skipping director fetch")
            return []

        self.logger.info(f"  Fetching directors from ISED API for corp #{corporation_number}")
        url = f"{ISED_API_V2_BASE}/corporations/{corporation_number}/directors"
        try:
            response = await self.http_client.get(
                url,
                headers={
                    "user-key": settings.ised_api_key,
                    "Accept-Language": "en",
                    "Accept": "application/json",
                },
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            self.logger.debug(f"  ISED API response: {data}")

            # Handle HAL+JSON format: directors are in _embedded.directors
            directors = []
            director_list = (
                data.get("_embedded", {}).get("directors", [])
                or data.get("directors", [])
                or (data if isinstance(data, list) else [])
            )
            for d in director_list:
                # Build name from parts
                parts = [
                    d.get("firstName", ""),
                    d.get("middleName", ""),
                    d.get("lastName", ""),
                ]
                name = " ".join(p for p in parts if p).strip()
                if not name:
                    continue

                # Parse address if present
                addr = None
                if d.get("serviceAddress"):
                    sa = d["serviceAddress"]
                    addr = Address(
                        street=sa.get("line1", ""),
                        city=sa.get("city", ""),
                        state=sa.get("subdivisionCode", ""),
                        postal_code=sa.get("postalCode", ""),
                        country="CA",
                    )

                directors.append(CanadaDirector(
                    name=name,
                    address=addr,
                ))

            return directors

        except httpx.HTTPStatusError as e:
            self.logger.warning(
                f"ISED API returned {e.response.status_code} for corp #{corporation_number}"
            )
            return []
        except Exception as e:
            self.logger.warning(f"Failed to fetch directors for corp #{corporation_number}: {e}")
            return []

    async def fetch_bulk_data(self) -> bytes:
        """Download bulk data ZIP from Open Government Portal.

        Uses disk caching to avoid re-downloading the ~200 MB ZIP file
        on every ingestion run. Cached file expires after 24 hours.

        Returns:
            ZIP file contents as bytes
        """
        cache_file = _BULK_CACHE_DIR / "canada_corps_bulk.zip"
        meta_file = _BULK_CACHE_DIR / "canada_corps_bulk.meta"

        # Check disk cache
        if cache_file.exists() and meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                age_hours = (time.time() - meta.get("fetched_at", 0)) / 3600
                if age_hours < _BULK_CACHE_TTL_HOURS:
                    self.logger.info(
                        f"Using cached bulk data ({age_hours:.1f}h old, "
                        f"{cache_file.stat().st_size / 1_000_000:.1f} MB)"
                    )
                    return await asyncio.to_thread(cache_file.read_bytes)
                self.logger.info(f"Bulk data cache expired ({age_hours:.1f}h old), re-downloading")
            except Exception as e:
                self.logger.warning(f"Failed to read bulk data cache: {e}")

        # Download fresh copy
        self.logger.info("Downloading Canada Corporations bulk data...")

        async def _fetch():
            response = await self.http_client.get(BULK_DATA_URL)
            response.raise_for_status()
            return response.content

        content = await with_retry(_fetch, logger=self.logger)

        # Save to disk cache
        try:
            _BULK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(cache_file.write_bytes, content)
            meta_file.write_text(
                json.dumps({"fetched_at": time.time(), "size_bytes": len(content)}),
                encoding="utf-8",
            )
            self.logger.info(
                f"Cached bulk data to disk ({len(content) / 1_000_000:.1f} MB)"
            )
        except Exception as e:
            self.logger.warning(f"Failed to cache bulk data to disk: {e}")

        return content

    def parse_bulk_data(self, zip_content: bytes) -> list[dict[str, Any]]:
        """Parse the bulk data ZIP file.

        The ZIP contains XML files with corporation data in ISED format.

        Args:
            zip_content: ZIP file contents

        Returns:
            List of corporation records
        """
        import xml.etree.ElementTree as ET

        records = []

        # Namespace for ISED CorpCan XML
        ns = {"cc": "http://www.ic.gc.ca/corpcan"}

        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # Look for OPEN_DATA_*.xml files (skip codes.xml and schema files)
            xml_files = [f for f in zf.namelist() if f.startswith("OPEN_DATA_") and f.endswith(".xml")]
            self.logger.info(f"Found {len(xml_files)} data XML files in archive")

            for filename in xml_files:
                self.logger.debug(f"Processing {filename}...")

                try:
                    with zf.open(filename) as f:
                        content = f.read()

                        # Parse XML
                        root = ET.fromstring(content)

                        # Find corporations container - handle both namespaced and non-namespaced
                        corporations = root.find("corporations")
                        if corporations is None:
                            corporations = root.find("cc:corporations", ns)
                        if corporations is None:
                            # Try direct iteration for simpler structure
                            corporations = root

                        # Find all corporation elements
                        for corp_elem in corporations.findall("corporation"):
                            record = self._extract_corporation_data(corp_elem, ns)
                            if record and record.get("corporation_number"):
                                records.append(record)

                except ET.ParseError as e:
                    self.logger.warning(f"XML parse error in {filename}: {e}")
                    continue
                except Exception as e:
                    self.logger.warning(f"Error processing {filename}: {e}")
                    continue

        self.logger.info(f"Parsed {len(records)} corporation records from XML")
        return records

    def _extract_corporation_data(self, corp_elem: Any, ns: dict) -> dict[str, Any]:
        """Extract corporation data from an XML element.

        Args:
            corp_elem: Corporation XML element
            ns: XML namespace dict

        Returns:
            Dict with corporation data
        """
        record = {}

        # Corporation ID
        corp_id = corp_elem.get("corporationId")
        if corp_id:
            record["corporation_number"] = corp_id

        # Current name (find name element with current="true")
        names_elem = corp_elem.find("names")
        if names_elem is not None:
            for name_elem in names_elem.findall("name"):
                if name_elem.get("current") == "true":
                    record["corporation_name"] = name_elem.text
                    break
            # Fallback to first name if no current
            if "corporation_name" not in record:
                first_name = names_elem.find("name")
                if first_name is not None and first_name.text:
                    record["corporation_name"] = first_name.text

        # Business number
        bn_elem = corp_elem.find("businessNumbers/businessNumber")
        if bn_elem is not None and bn_elem.text:
            record["business_number"] = bn_elem.text

        # Status (find current status)
        statuses_elem = corp_elem.find("statuses")
        if statuses_elem is not None:
            for status_elem in statuses_elem.findall("status"):
                if status_elem.get("current") == "true":
                    status_code = status_elem.get("code")
                    # Map status codes: 1=Active, 2=Dissolved, etc.
                    status_map = {"1": "Active", "2": "Dissolved", "3": "Revoked", "4": "Amalgamated"}
                    record["status"] = status_map.get(status_code, "Unknown")
                    break

        # Act (corporation type - find current act)
        acts_elem = corp_elem.find("acts")
        if acts_elem is not None:
            for act_elem in acts_elem.findall("act"):
                if act_elem.get("current") == "true":
                    act_code = act_elem.get("code")
                    # Map act codes: 6=CBCA, 7=CCA Part II (NFP), 8=BOTA, etc.
                    act_map = {"6": "CBCA", "7": "NFP", "8": "BOTA", "9": "COOP", "10": "CNFPA"}
                    record["corporation_type"] = act_map.get(act_code, "Unknown")
                    break

        # Address (find current registered office address, code 2)
        addresses_elem = corp_elem.find("addresses")
        if addresses_elem is not None:
            for addr_elem in addresses_elem.findall("address"):
                if addr_elem.get("current") == "true" and addr_elem.get("code") == "2":
                    street = addr_elem.findtext("addressLine", "")
                    city = addr_elem.findtext("city", "")
                    province_elem = addr_elem.find("province")
                    province = province_elem.get("code") if province_elem is not None else ""
                    postal_code = addr_elem.findtext("postalCode", "")

                    record["street"] = street
                    record["city"] = city
                    record["province"] = province
                    record["postal_code"] = postal_code
                    break

        return record

    def parse_corporation(self, row: dict[str, Any]) -> CanadaCorporation | None:
        """Parse a corporation record from XML/CSV row.

        Args:
            row: Row data as dict (from XML element or CSV)

        Returns:
            Parsed corporation or None if invalid
        """
        # Get corporation number (required) - try various field name formats
        corp_num = (
            row.get("Corporation Number") or
            row.get("corporation_number") or
            row.get("corporationNumber") or
            row.get("corpNum") or
            row.get("corp_num") or
            row.get("CorporationNumber")
        )
        if not corp_num:
            return None

        # Get name (required)
        name = (
            row.get("Corporation Name") or
            row.get("corporation_name") or
            row.get("corporationName") or
            row.get("name") or
            row.get("Name") or
            row.get("CorporationName")
        )
        if not name:
            return None

        # Parse dates
        inc_date = None
        date_str = (
            row.get("Incorporation Date") or
            row.get("incorporation_date") or
            row.get("incorporationDate") or
            row.get("IncorporationDate")
        )
        if date_str:
            for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y%m%d"]:
                try:
                    inc_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    continue

        # Parse address
        address = None
        if any(row.get(f) for f in ["Street", "City", "Province", "street", "city", "province"]):
            address = Address(
                street=row.get("Street") or row.get("street"),
                city=row.get("City") or row.get("city"),
                state=row.get("Province") or row.get("province"),
                postal_code=row.get("Postal Code") or row.get("postal_code"),
                country="CA",
            )

        return CanadaCorporation(
            corporation_number=str(corp_num).strip(),
            corporation_name=name.strip(),
            business_number=row.get("Business Number") or row.get("business_number") or row.get("bn"),
            status=row.get("Status") or row.get("status") or "Active",
            corporation_type=row.get("Act") or row.get("act") or row.get("corporation_type"),
            incorporation_date=inc_date,
            registered_office=address,
            activity_code=row.get("Activity Code") or row.get("activity_code"),
            activity_description=row.get("Activity Description") or row.get("activity_description"),
        )

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[CanadaCorporation]:
        """Fetch corporation records from Canada Open Government data.

        Args:
            config: Ingestion configuration

        Yields:
            Parsed corporation records
        """
        try:
            # Download bulk data
            zip_content = await self.fetch_bulk_data()
            self.logger.info(f"Downloaded {len(zip_content)} bytes")

            # Parse bulk data
            raw_records = await asyncio.to_thread(self.parse_bulk_data, zip_content)
            self.logger.info(f"Parsed {len(raw_records)} raw records")

            # Filter by target entities (corporation numbers) if specified
            if config.target_entities:
                target_nums = set(config.target_entities)
                raw_records = [
                    r for r in raw_records
                    if str(r.get("corporation_number", "")).strip() in target_nums
                ]
                self.logger.info(
                    f"Filtered to {len(raw_records)} corporations for "
                    f"{len(config.target_entities)} target corporation numbers"
                )

            # Apply limit
            if config.limit:
                raw_records = raw_records[:config.limit]

            # Parse and yield corporations
            for row in raw_records:
                corp = self.parse_corporation(row)
                if corp:
                    yield corp
                else:
                    self.logger.warning(
                        f"Failed to parse corporation record: "
                        f"corp_num={row.get('corporation_number', 'missing')}, "
                        f"name={row.get('corporation_name', 'missing')}"
                    )

        except zipfile.BadZipFile:
            self.logger.error("Invalid ZIP file received from Open Government Portal")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching Canada Corporations data: {e}")
            raise

    async def process_record(self, record: CanadaCorporation) -> dict[str, Any]:
        """Process a corporation record into the database.

        Args:
            record: Parsed corporation record

        Returns:
            Processing result with entity IDs
        """
        result = {"created": False, "updated": False, "entity_id": None}
        self.logger.info(
            f"Processing: {record.corporation_name} "
            f"(corp #{record.corporation_number}, type={record.corporation_type}, status={record.status})"
        )

        async with get_db_session() as db:
            from sqlalchemy import text

            # Check if entity exists by corporation number
            existing = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE external_ids->>'canada_corp_num' = :corp_num
                """),
                {"corp_num": record.corporation_number},
            )
            row = existing.fetchone()

            # Determine org type
            org_type = TYPE_MAP.get(record.corporation_type, OrgType.CORPORATION)

            # Determine status
            org_status = STATUS_MAP.get(record.status, OrgStatus.ACTIVE)

            entity_data = {
                "name": record.corporation_name,
                "entity_type": "organization",
                "org_type": org_type.value,
                "jurisdiction": "CA",
                "status": org_status.value,
                "external_ids": {
                    "canada_corp_num": record.corporation_number,
                    "business_number": record.business_number,
                },
                "metadata": {
                    "corporation_type": record.corporation_type,
                    "incorporation_date": record.incorporation_date.isoformat() if record.incorporation_date else None,
                    "activity_code": record.activity_code,
                    "activity_description": record.activity_description,
                },
            }

            if row:
                # Update existing
                await db.execute(
                    text("""
                        UPDATE entities
                        SET name = :name, metadata = CAST(:metadata AS jsonb), updated_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": row.id, "name": record.corporation_name, "metadata": json.dumps(entity_data["metadata"])},
                )
                result["updated"] = True
                result["entity_id"] = str(row.id)
                self.logger.info(f"  PostgreSQL: updated existing entity {row.id}")
            else:
                # Create new
                new_id = uuid4()
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb), CAST(:metadata AS jsonb), NOW())
                    """),
                    {
                        "id": new_id,
                        "name": record.corporation_name,
                        "entity_type": "organization",
                        "external_ids": json.dumps(entity_data["external_ids"]),
                        "metadata": json.dumps(entity_data["metadata"]),
                    },
                )
                result["created"] = True
                result["entity_id"] = str(new_id)
                self.logger.info(f"  PostgreSQL: created new entity {new_id}")

            await db.commit()

            # Create evidence record
            evidence_id = uuid4()
            await db.execute(
                text("""
                    INSERT INTO evidence (id, evidence_type, source_url, retrieved_at, extractor, extractor_version, raw_data_ref, extraction_confidence, content_hash)
                    VALUES (:id, :evidence_type, :source_url, NOW(), :extractor, :version, :raw_ref, :confidence, :hash)
                """),
                {
                    "id": evidence_id,
                    "evidence_type": EvidenceType.CANADA_CORP_RECORD.value,
                    "source_url": f"https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html?corpId={record.corporation_number}",
                    "extractor": "canada_corps_ingester",
                    "version": "1.0.0",
                    "raw_ref": f"canada_corps/{record.corporation_number}",
                    "confidence": 0.95,
                    "hash": compute_content_hash(record.model_dump_json().encode("utf-8")),
                },
            )
            await db.commit()
            self.logger.info(f"  Evidence: recorded (type={EvidenceType.CANADA_CORP_RECORD.value})")

        # --- Neo4j Organization node upsert (with CRA cross-linking) ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                org_props = {
                    "id": result["entity_id"],
                    "name": record.corporation_name,
                    "entity_type": "ORGANIZATION",
                    "org_type": org_type.value,
                    "status": org_status.value,
                    "jurisdiction": "CA",
                    "canada_corp_num": record.corporation_number,
                    "confidence": 0.95,
                    "updated_at": now,
                }

                if record.business_number:
                    org_props["bn"] = record.business_number
                if record.corporation_type:
                    org_props["corporation_type"] = record.corporation_type
                if record.registered_office:
                    if record.registered_office.street:
                        org_props["address_street"] = record.registered_office.street
                    if record.registered_office.city:
                        org_props["address_city"] = record.registered_office.city
                    if record.registered_office.state:
                        org_props["address_state"] = record.registered_office.state
                    if record.registered_office.postal_code:
                        org_props["address_postal"] = record.registered_office.postal_code
                    if record.registered_office.country:
                        org_props["address_country"] = record.registered_office.country

                # --- CRA cross-link: check if CRA already has an org with matching BN ---
                cra_linked = False
                if record.business_number:
                    cra_check = await session.run(
                        """
                        MATCH (o:Organization)
                        WHERE o.bn STARTS WITH $bn_prefix AND o.bn <> $bn_prefix
                        RETURN o.id as id, o.bn as bn
                        LIMIT 1
                        """,
                        bn_prefix=record.business_number,
                    )
                    cra_node = await cra_check.single()

                    if cra_node:
                        # CRA node exists — merge into it
                        org_props.pop("created_at", None)
                        await session.run(
                            """
                            MATCH (o:Organization {bn: $bn})
                            SET o.canada_corp_num = $corp_num,
                                o += $props
                            """,
                            bn=cra_node["bn"],
                            corp_num=record.corporation_number,
                            props=org_props,
                        )
                        cra_linked = True
                        self.logger.info(
                            f"  Neo4j: cross-linked with CRA node (BN={cra_node['bn']})"
                        )

                if not cra_linked:
                    # Normal MERGE on canada_corp_num
                    check_result = await session.run(
                        "MATCH (o:Organization {canada_corp_num: $corp_num}) RETURN o.id as id",
                        corp_num=record.corporation_number,
                    )
                    existing_node = await check_result.single()

                    if not existing_node:
                        org_props["created_at"] = now

                    await session.run(
                        """
                        MERGE (o:Organization {canada_corp_num: $corp_num})
                        SET o += $props
                        RETURN o.id as id
                        """,
                        corp_num=record.corporation_number,
                        props=org_props,
                    )
                    neo4j_action = "updated" if existing_node else "created"
                    self.logger.info(
                        f"  Neo4j: {neo4j_action} Organization node (corp #{record.corporation_number})"
                    )

                # --- Fetch and create Director nodes + DIRECTOR_OF relationships ---
                directors = record.directors
                if not directors:
                    fetched = await self.fetch_directors(record.corporation_number)
                    directors = [d.model_dump() for d in fetched]

                if directors:
                    self.logger.info(f"  Directors: processing {len(directors)} directors")
                    dir_created = 0
                    for director in directors:
                        d_name = director.get("name", "") if isinstance(director, dict) else director.name
                        if not d_name:
                            continue

                        person_id = str(uuid4())
                        person_props = {
                            "id": person_id,
                            "name": d_name,
                            "entity_type": "PERSON",
                            "confidence": 0.9,
                            "updated_at": now,
                        }

                        # MERGE Person node
                        await session.run(
                            """
                            MERGE (p:Person {name: $name})
                            ON CREATE SET p += $create_props
                            SET p.updated_at = $now
                            RETURN p.id as id
                            """,
                            name=d_name,
                            create_props=person_props,
                            now=now,
                        )

                        # Build DIRECTOR_OF relationship properties
                        rel_props = {
                            "confidence": 0.95,
                            "source": "canada_corps",
                            "updated_at": now,
                        }
                        appt = director.get("appointment_date") if isinstance(director, dict) else director.appointment_date
                        cess = director.get("cessation_date") if isinstance(director, dict) else director.cessation_date
                        if appt:
                            rel_props["valid_from"] = str(appt)
                        if cess:
                            rel_props["valid_to"] = str(cess)

                        # Create DIRECTOR_OF relationship
                        await session.run(
                            """
                            MATCH (p:Person {name: $person_name})
                            MATCH (o:Organization {canada_corp_num: $corp_num})
                            MERGE (p)-[r:DIRECTOR_OF]->(o)
                            SET r += $props
                            """,
                            person_name=d_name,
                            corp_num=record.corporation_number,
                            props=rel_props,
                        )
                        dir_created += 1

                    self.logger.info(f"  Neo4j: created {dir_created} DIRECTOR_OF relationships")
                else:
                    self.logger.info("  Directors: none found (ISED API key not configured or no data)")

        except Exception as e:
            self.logger.warning(f"  Neo4j: FAILED for {record.corporation_name} — {e}")

        return result


async def run_canada_corps_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run Canada Corporations ingestion.

    Args:
        limit: Maximum number of corporations to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of corporation numbers to ingest specifically
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = CanadaCorporationsIngester()

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
