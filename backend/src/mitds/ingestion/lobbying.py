"""Canada Lobbying Registry ingester.

Ingests data from the Office of the Commissioner of Lobbying of Canada.
Data source: https://lobbycanada.gc.ca/en/open-data/

Key data points:
- Lobbyist registrations (consultant and in-house)
- Clients being represented
- Subject matters of lobbying
- Government institutions contacted
- Monthly communication reports with DPOHs
"""

import asyncio
import csv
import io
import json
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import uuid4
from zipfile import ZipFile

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..storage import StorageClient, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, with_retry

logger = get_context_logger(__name__)

# Lobbying Registry Open Data URLs
LOBBYING_REGISTRATIONS_URL = "https://lobbycanada.gc.ca/media/zwcjycef/registrations_enregistrements_ocl_cal.zip"
LOBBYING_COMMUNICATIONS_URL = "https://lobbycanada.gc.ca/media/mqbbmaqk/communications_ocl_cal.zip"


class LobbyingClient(BaseModel):
    """Client being represented by a lobbyist."""

    name: str
    description: str | None = None
    business_number: str | None = None


class LobbyingRegistration(BaseModel):
    """A lobbying registration record."""

    # Registration info
    registration_id: str = Field(..., description="Unique registration ID")
    registration_type: str | None = Field(default=None, description="consultant or in-house")
    status: str | None = Field(default=None, description="Active, Terminated, etc.")

    # Dates
    effective_date: date | None = None
    end_date: date | None = None
    posted_date: date | None = None

    # Lobbyist info
    lobbyist_name: str | None = None
    lobbyist_type: str | None = None  # consultant, in-house-organization, in-house-corporation

    # Organization/Employer
    registrant_name: str | None = None
    registrant_type: str | None = None

    # Client (for consultant lobbyists)
    client_name: str | None = None
    client_description: str | None = None

    # Subject matter
    subject_matters: list[str] = Field(default_factory=list)

    # Government institutions
    institutions: list[str] = Field(default_factory=list)

    # Beneficiaries (who benefits from the lobbying)
    beneficiaries: list[str] = Field(default_factory=list)


class LobbyingCommunication(BaseModel):
    """A monthly communication report."""

    communication_id: str | None = None
    registration_id: str | None = None

    # Communication details
    communication_date: date | None = None
    dpoh_name: str | None = None  # Designated Public Office Holder
    dpoh_title: str | None = None
    dpoh_institution: str | None = None

    # Subject
    subject_matter: str | None = None

    # Lobbyist
    lobbyist_name: str | None = None


class LobbyingIngester(BaseIngester[LobbyingRegistration]):
    """Ingester for Canada Lobbying Registry data."""

    def __init__(self):
        super().__init__("lobbying")
        self._http_client: httpx.AsyncClient | None = None
        self._storage: StorageClient | None = None
        self._communications: list[dict] = []

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=300.0),
                follow_redirects=True,
            )
        return self._http_client

    @property
    def storage(self) -> StorageClient:
        if self._storage is None:
            self._storage = get_storage()
        return self._storage

    async def close(self):
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_last_sync_time(self) -> datetime | None:
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
        pass  # Handled by base class

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[LobbyingRegistration]:
        """Fetch lobbying registration records."""

        # Download registrations
        self.logger.info("Downloading lobbying registrations...")
        registrations_data = await self._download_and_extract(
            LOBBYING_REGISTRATIONS_URL, "registrations"
        )

        # Download communications for enrichment
        self.logger.info("Downloading lobbying communications...")
        self._communications = await self._download_and_extract(
            LOBBYING_COMMUNICATIONS_URL, "communications"
        )
        self.logger.info(f"Loaded {len(self._communications)} communication records")

        # Find the main registrations file
        main_file = None
        subject_matters = {}
        institutions = {}
        beneficiaries = {}

        for filename, rows in registrations_data.items():
            lower_name = filename.lower()
            if 'subjectmatter' in lower_name and 'detail' not in lower_name:
                # Index subject matters by registration number
                # Columns: REG_NUM_ENR, EN_SM_CATEGORY_MATIERE_AN
                for row in rows:
                    reg_id = row.get('REG_NUM_ENR', row.get('Registration_num', ''))
                    subject = row.get('EN_SM_CATEGORY_MATIERE_AN', row.get('Subject_Matter', ''))
                    if reg_id and subject and subject != 'null':
                        if reg_id not in subject_matters:
                            subject_matters[reg_id] = []
                        if subject not in subject_matters[reg_id]:
                            subject_matters[reg_id].append(subject)
            elif 'governmentinst' in lower_name or 'govtinst' in lower_name:
                # Columns: REG_NUM_ENR, EN_GOVTINST_NM_AN
                for row in rows:
                    reg_id = row.get('REG_NUM_ENR', row.get('Registration_num', ''))
                    inst = row.get('EN_GOVTINST_NM_AN', row.get('Institution', ''))
                    if reg_id and inst and inst != 'null':
                        if reg_id not in institutions:
                            institutions[reg_id] = []
                        if inst not in institutions[reg_id]:
                            institutions[reg_id].append(inst)
            elif 'beneficiar' in lower_name:
                # Columns: REG_NUM_ENR, EN_BENEFICIARY_NM_AN
                for row in rows:
                    reg_id = row.get('REG_NUM_ENR', row.get('Registration_num', ''))
                    ben = row.get('EN_BENEFICIARY_NM_AN', row.get('Beneficiary_name', ''))
                    if reg_id and ben and ben != 'null':
                        if reg_id not in beneficiaries:
                            beneficiaries[reg_id] = []
                        if ben not in beneficiaries[reg_id]:
                            beneficiaries[reg_id].append(ben)
            elif 'primary' in lower_name:
                main_file = rows

        if not main_file:
            # Use the largest file as main
            main_file = max(registrations_data.values(), key=len) if registrations_data else []

        self.logger.info(f"Processing {len(main_file)} registration records")

        # Filter by target entities if specified (uses substring matching)
        if config.target_entities:
            target_patterns = [n.lower() for n in config.target_entities]
            filtered = []
            for row in main_file:
                # Use actual column names from the data
                client = row.get('EN_CLIENT_ORG_CORP_NM_AN', row.get('Client', '')).lower()
                firm = row.get('EN_FIRM_NM_FIRME_AN', row.get('Registrant_name', '')).lower()
                for pattern in target_patterns:
                    if pattern in client or pattern in firm:
                        filtered.append(row)
                        break
            main_file = filtered
            self.logger.info(f"Filtered to {len(main_file)} records for target entities")

        for row in main_file:
            try:
                registration = self._parse_registration(
                    row, subject_matters, institutions, beneficiaries
                )
                if registration:
                    yield registration
            except Exception as e:
                self.logger.warning(f"Failed to parse registration: {e}")
                continue

    async def _download_and_extract(
        self, url: str, data_type: str
    ) -> dict[str, list[dict]]:
        """Download and extract CSV files from a ZIP archive."""

        async def _do_download():
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.content

        try:
            content = await with_retry(_do_download, logger=self.logger)
        except Exception as e:
            self.logger.error(f"Failed to download {data_type}: {e}")
            return {}

        # Store raw file
        storage_key = generate_storage_key(
            "lobbying",
            f"{data_type}_{datetime.now().strftime('%Y%m%d')}",
            extension="zip",
        )
        await asyncio.to_thread(
            self.storage.upload_file,
            content,
            storage_key,
            content_type="application/zip",
            metadata={"data_type": data_type},
        )

        # Extract CSVs from ZIP
        try:
            return await asyncio.to_thread(
                self._extract_csvs_from_zip, content, data_type
            )
        except Exception as e:
            self.logger.error(f"Failed to extract {data_type}: {e}")
            return {}

    def _extract_csvs_from_zip(
        self, zip_content: bytes, data_type: str
    ) -> dict[str, list[dict]]:
        """Extract all CSV files from ZIP (sync, runs in thread)."""
        result = {}

        with ZipFile(io.BytesIO(zip_content)) as zf:
            csv_files = [n for n in zf.namelist() if n.endswith('.csv')]
            self.logger.info(f"Found {len(csv_files)} CSV files in {data_type} ZIP")

            for csv_file in csv_files:
                try:
                    csv_content = zf.read(csv_file)
                    # Try multiple encodings - Canadian govt data often has French chars
                    decoded = None
                    for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            decoded = csv_content.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue

                    if decoded is None:
                        # Last resort: decode with errors='replace'
                        decoded = csv_content.decode('utf-8', errors='replace')

                    reader = csv.DictReader(io.StringIO(decoded))
                    rows = list(reader)
                    result[csv_file] = rows
                    self.logger.info(f"  {csv_file}: {len(rows)} rows")
                except Exception as e:
                    self.logger.warning(f"Failed to parse {csv_file}: {e}")

        return result

    def _parse_registration(
        self,
        row: dict[str, str],
        subject_matters: dict[str, list[str]],
        institutions: dict[str, list[str]],
        beneficiaries: dict[str, list[str]],
    ) -> LobbyingRegistration | None:
        """Parse a registration from CSV row.

        Column names from actual data (Registration_PrimaryExport.csv):
        - REG_ID_ENR: Registration ID (internal)
        - REG_NUM_ENR: Registration number (public-facing)
        - REG_TYPE_ENR: 1=consultant, 3=in-house
        - EN_FIRM_NM_FIRME_AN: Firm name (for consultant lobbyists)
        - RGSTRNT_LAST_NM_DCLRNT, RGSTRNT_1ST_NM_PRENOM_DCLRNT: Registrant name
        - EN_CLIENT_ORG_CORP_NM_AN: Client organization name
        - EFFECTIVE_DATE_VIGUEUR, END_DATE_FIN: Date range
        """

        # Get registration ID
        reg_id = (
            row.get('REG_NUM_ENR', '')
            or row.get('REG_ID_ENR', '')
            or row.get('Registration_num', '')
        ).strip()

        if not reg_id:
            return None

        # Parse dates
        def parse_date(val: str) -> date | None:
            if not val or val == 'null':
                return None
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    return datetime.strptime(val.strip(), fmt).date()
                except ValueError:
                    continue
            return None

        effective_date = parse_date(
            row.get('EFFECTIVE_DATE_VIGUEUR', row.get('Effective_date', ''))
        )
        end_date = parse_date(
            row.get('END_DATE_FIN', row.get('End_date', ''))
        )
        posted_date = parse_date(
            row.get('POSTED_DATE_PUBLICATION', row.get('Posted_date', ''))
        )

        # Lobbyist/Registrant info (the person doing the lobbying)
        first_name = row.get('RGSTRNT_1ST_NM_PRENOM_DCLRNT', row.get('First_name', ''))
        last_name = row.get('RGSTRNT_LAST_NM_DCLRNT', row.get('Last_name', ''))
        lobbyist_name = f"{first_name} {last_name}".strip()
        if lobbyist_name == '' or lobbyist_name == 'null null':
            lobbyist_name = None

        # Firm (for consultant lobbyists)
        firm_name = row.get('EN_FIRM_NM_FIRME_AN', '').strip()
        if firm_name == 'null':
            firm_name = None

        # Client (the organization being represented)
        client_name = (
            row.get('EN_CLIENT_ORG_CORP_NM_AN', '')
            or row.get('Client', '')
        ).strip()
        if client_name == 'null':
            client_name = None

        # Registration type: 1=consultant, 3=in-house
        reg_type_code = row.get('REG_TYPE_ENR', row.get('Type', ''))
        reg_type = None
        if reg_type_code == '1':
            reg_type = 'consultant'
        elif reg_type_code == '3':
            reg_type = 'in-house'
        elif reg_type_code:
            reg_type = reg_type_code

        return LobbyingRegistration(
            registration_id=reg_id,
            registration_type=reg_type,
            status=None,  # Not in primary export
            effective_date=effective_date,
            end_date=end_date,
            posted_date=posted_date,
            lobbyist_name=lobbyist_name,
            registrant_name=firm_name,  # Firm is the registrant for consultants
            client_name=client_name,
            client_description=None,
            subject_matters=subject_matters.get(reg_id, []),
            institutions=institutions.get(reg_id, []),
            beneficiaries=beneficiaries.get(reg_id, []),
        )

    async def process_record(self, record: LobbyingRegistration) -> dict[str, Any]:
        """Process a lobbying registration record."""
        result = {"created": False, "updated": False, "entity_id": None}

        self.logger.info(
            f"Processing: {record.client_name or record.registrant_name} "
            f"(reg #{record.registration_id})"
        )

        # Determine the primary entity (client for consultants, registrant for in-house)
        primary_name = record.client_name or record.registrant_name
        if not primary_name:
            self.logger.warning(f"  No primary name for registration {record.registration_id}")
            return result

        # --- PostgreSQL: Create/Update entity ---
        async with get_db_session() as db:
            # Check if entity exists
            check_result = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                    AND entity_type = 'organization'
                """),
                {"name": primary_name},
            )
            existing = check_result.fetchone()

            entity_data = {
                "name": primary_name,
                "entity_type": "organization",
                "external_ids": {"lobbying_registration": record.registration_id},
                "metadata": {
                    "source": "lobbying_registry",
                    "registration_type": record.registration_type,
                    "status": record.status,
                    "subject_matters": record.subject_matters,
                    "institutions_lobbied": record.institutions,
                },
            }

            if existing:
                await db.execute(
                    text("""
                        UPDATE entities
                        SET metadata = CAST(:metadata AS jsonb),
                            external_ids = external_ids || CAST(:external_ids AS jsonb),
                            updated_at = NOW()
                        WHERE id = :id
                    """),
                    {
                        "id": existing.id,
                        "metadata": json.dumps(entity_data["metadata"]),
                        "external_ids": json.dumps(entity_data["external_ids"]),
                    },
                )
                result["updated"] = True
                result["entity_id"] = str(existing.id)
                self.logger.info(f"  PostgreSQL: updated entity {existing.id}")
            else:
                new_id = uuid4()
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb),
                                CAST(:metadata AS jsonb), NOW())
                    """),
                    {
                        "id": new_id,
                        "name": primary_name,
                        "entity_type": "organization",
                        "external_ids": json.dumps(entity_data["external_ids"]),
                        "metadata": json.dumps(entity_data["metadata"]),
                    },
                )
                result["created"] = True
                result["entity_id"] = str(new_id)
                self.logger.info(f"  PostgreSQL: created entity {new_id}")

            await db.commit()

        # --- Neo4j: Create nodes and relationships ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Create/update Organization node for the lobbying client/registrant
                org_props = {
                    "id": result["entity_id"],
                    "name": primary_name,
                    "entity_type": "ORGANIZATION",
                    "lobbying_registration": record.registration_id,
                    "lobbying_status": record.status,
                    "updated_at": now,
                }

                await session.run(
                    """
                    MERGE (o:Organization {name: $name})
                    ON CREATE SET o += $props
                    ON MATCH SET o.lobbying_registration = $props.lobbying_registration,
                                 o.lobbying_status = $props.lobbying_status,
                                 o.updated_at = $props.updated_at
                    """,
                    name=primary_name,
                    props=org_props,
                )

                # Create Person node for the lobbyist
                if record.lobbyist_name:
                    person_props = {
                        "id": str(uuid4()),
                        "name": record.lobbyist_name,
                        "entity_type": "PERSON",
                        "lobbyist_type": record.registration_type,
                        "updated_at": now,
                    }

                    await session.run(
                        """
                        MERGE (p:Person {name: $name})
                        ON CREATE SET p += $props
                        SET p.lobbyist_type = $props.lobbyist_type,
                            p.updated_at = $props.updated_at
                        """,
                        name=record.lobbyist_name,
                        props=person_props,
                    )

                    # Create LOBBIES_FOR relationship
                    rel_props = {
                        "registration_id": record.registration_id,
                        "registration_type": record.registration_type,
                        "status": record.status,
                        "source": "lobbying_registry",
                        "confidence": 1.0,
                        "updated_at": now,
                    }
                    if record.effective_date:
                        rel_props["valid_from"] = record.effective_date.isoformat()
                    if record.end_date:
                        rel_props["valid_to"] = record.end_date.isoformat()

                    await session.run(
                        """
                        MATCH (p:Person {name: $lobbyist_name})
                        MATCH (o:Organization {name: $client_name})
                        MERGE (p)-[r:LOBBIES_FOR]->(o)
                        SET r += $props
                        """,
                        lobbyist_name=record.lobbyist_name,
                        client_name=primary_name,
                        props=rel_props,
                    )

                    self.logger.info(
                        f"  Neo4j: {record.lobbyist_name} -[LOBBIES_FOR]-> {primary_name}"
                    )

                # Create nodes for government institutions lobbied
                for institution in record.institutions[:10]:  # Limit to avoid too many
                    await session.run(
                        """
                        MERGE (i:Organization {name: $name})
                        ON CREATE SET i.entity_type = 'GOVERNMENT',
                                      i.is_government = true,
                                      i.updated_at = $now
                        """,
                        name=institution,
                        now=now,
                    )

                    # Create LOBBIED relationship
                    await session.run(
                        """
                        MATCH (client:Organization {name: $client_name})
                        MATCH (govt:Organization {name: $govt_name})
                        MERGE (client)-[r:LOBBIED]->(govt)
                        SET r.registration_id = $reg_id,
                            r.subject_matters = $subjects,
                            r.source = 'lobbying_registry',
                            r.updated_at = $now
                        """,
                        client_name=primary_name,
                        govt_name=institution,
                        reg_id=record.registration_id,
                        subjects=record.subject_matters[:5],
                        now=now,
                    )

                if record.institutions:
                    self.logger.info(
                        f"  Neo4j: created LOBBIED relationships to {len(record.institutions)} institutions"
                    )

                self.logger.info(f"  Neo4j: completed for {primary_name}")

        except Exception as e:
            self.logger.warning(f"  Neo4j: FAILED - {e}")

        return result


async def run_lobbying_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run Lobbying Registry ingestion.

    Args:
        limit: Maximum number of registrations to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of client/registrant names to filter
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = LobbyingIngester()

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
