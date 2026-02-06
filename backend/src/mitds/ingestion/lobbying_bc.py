"""BC Lobbyist Registry ingester.

Ingests data from the Office of the Registrar of Lobbyists for British Columbia.
Data source: https://lobbyistsregistrar.bc.ca/app/secure/orl/lrs/do/mssDtstRprt?file=ORL_Registration_Data.zip

Key data points:
- Lobbyist registrations (consultant and in-house)
- Clients being represented
- Subject matters of lobbying
- Government institutions contacted
"""

import asyncio
import csv
import io
import json
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import uuid4, UUID

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..storage import StorageClient, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, with_retry, Neo4jHelper, PostgresHelper

logger = get_context_logger(__name__)

# BC Lobbyist Registry Open Data URL
BC_LOBBYING_REGISTRATIONS_URL = (
    "https://lobbyistsregistrar.bc.ca/app/secure/orl/lrs/do/mssDtstRprt?file=ORL_Registration_Data.zip"
)


class BCLobbyistRegistration(BaseModel):
    """A BC lobbying registration record."""

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


class BCLobbyingIngester(BaseIngester[BCLobbyistRegistration]):
    """Ingester for BC Lobbyist Registry data."""

    def __init__(self):
        super().__init__("bc_lobbying")
        self._http_client: httpx.AsyncClient | None = None
        self._storage: StorageClient | None = None
        self._neo4j = Neo4jHelper(self.logger)
        self._postgres = PostgresHelper(self.logger)

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=300.0),
                follow_redirects=True,
                headers={
                    "User-Agent": "MITDS/1.0 (Media Influence Transparency; research)",
                    "Accept": "application/zip, application/octet-stream, */*",
                },
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
    ) -> AsyncIterator[BCLobbyistRegistration]:
        """Fetch BC lobbying registration records."""

        # Download registrations
        self.logger.info("Downloading BC lobbying registrations...")
        registrations_data = await self._download_and_extract(
            BC_LOBBYING_REGISTRATIONS_URL, "bc_registrations"
        )

        # Find the main registrations file and related files
        main_file = None
        subject_matters = {}
        institutions = {}

        for filename, rows in registrations_data.items():
            lower_name = filename.lower()
            if "subject" in lower_name or "matter" in lower_name:
                # Index subject matters by registration ID
                # Column names may vary - try common patterns
                for row in rows:
                    reg_id = (
                        row.get("Registration_ID", "")
                        or row.get("RegistrationID", "")
                        or row.get("REG_ID", "")
                        or row.get("Registration_Number", "")
                    ).strip()
                    subject = (
                        row.get("Subject_Matter", "")
                        or row.get("SubjectMatter", "")
                        or row.get("Subject", "")
                        or row.get("Matter", "")
                    ).strip()
                    if reg_id and subject and subject.lower() != "null":
                        if reg_id not in subject_matters:
                            subject_matters[reg_id] = []
                        if subject not in subject_matters[reg_id]:
                            subject_matters[reg_id].append(subject)
            elif "institution" in lower_name or "government" in lower_name:
                # Index institutions by registration ID
                for row in rows:
                    reg_id = (
                        row.get("Registration_ID", "")
                        or row.get("RegistrationID", "")
                        or row.get("REG_ID", "")
                        or row.get("Registration_Number", "")
                    ).strip()
                    inst = (
                        row.get("Institution_Name", "")
                        or row.get("InstitutionName", "")
                        or row.get("Institution", "")
                        or row.get("Government_Institution", "")
                    ).strip()
                    if reg_id and inst and inst.lower() != "null":
                        if reg_id not in institutions:
                            institutions[reg_id] = []
                        if inst not in institutions[reg_id]:
                            institutions[reg_id].append(inst)
            elif "registration" in lower_name and "detail" not in lower_name:
                # Main registration file
                main_file = rows

        if not main_file:
            # Use the largest file as main
            main_file = max(registrations_data.values(), key=len) if registrations_data else []

        self.logger.info(f"Processing {len(main_file)} registration records")

        # Filter by target entities if specified
        if config.target_entities:
            target_patterns = [n.lower() for n in config.target_entities]
            filtered = []
            for row in main_file:
                client = (
                    row.get("Client_Name", "")
                    or row.get("ClientName", "")
                    or row.get("Client", "")
                    or ""
                ).lower()
                registrant = (
                    row.get("Registrant_Name", "")
                    or row.get("RegistrantName", "")
                    or row.get("Registrant", "")
                    or ""
                ).lower()
                lobbyist = (
                    row.get("Lobbyist_Name", "")
                    or row.get("LobbyistName", "")
                    or row.get("Lobbyist", "")
                    or ""
                ).lower()
                for pattern in target_patterns:
                    if pattern in client or pattern in registrant or pattern in lobbyist:
                        filtered.append(row)
                        break
            main_file = filtered
            self.logger.info(f"Filtered to {len(main_file)} records for target entities")

        for row in main_file:
            try:
                registration = self._parse_registration(row, subject_matters, institutions)
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
            "bc_lobbying",
            f"{data_type}_{datetime.now().strftime('%Y%m%d')}",
            extension="zip",
        )
        await asyncio.to_thread(
            self.storage.upload_file,
            content,
            storage_key,
            content_type="application/zip",
            metadata={"data_type": data_type, "jurisdiction": "BC"},
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
        from zipfile import ZipFile

        result = {}

        with ZipFile(io.BytesIO(zip_content)) as zf:
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            self.logger.info(f"Found {len(csv_files)} CSV files in {data_type} ZIP")

            for csv_file in csv_files:
                try:
                    csv_content = zf.read(csv_file)
                    # Try multiple encodings
                    decoded = None
                    for encoding in ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]:
                        try:
                            decoded = csv_content.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue

                    if decoded is None:
                        decoded = csv_content.decode("utf-8", errors="replace")

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
    ) -> BCLobbyistRegistration | None:
        """Parse a registration from CSV row.

        BC ORL data structure (columns may vary):
        - Registration_ID or RegistrationID: Registration ID
        - Registration_Type or RegistrationType: Type of registration
        - Status: Registration status
        - Effective_Date or EffectiveDate: Start date
        - End_Date or EndDate: End date
        - Posted_Date or PostedDate: Posted date
        - Lobbyist_Name or LobbyistName: Name of lobbyist
        - Registrant_Name or RegistrantName: Name of registrant organization
        - Client_Name or ClientName: Name of client (for consultants)
        - Subject_Matter or SubjectMatter: Subject matter (may be in separate file)
        """

        # Get registration ID
        reg_id = (
            row.get("Registration_ID", "")
            or row.get("RegistrationID", "")
            or row.get("REG_ID", "")
            or row.get("Registration_Number", "")
            or row.get("RegistrationNumber", "")
        ).strip()

        if not reg_id:
            return None

        # Parse dates
        def parse_date(val: str) -> date | None:
            if not val or val.lower() == "null":
                return None
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(val.strip(), fmt).date()
                except ValueError:
                    continue
            return None

        effective_date = parse_date(
            row.get("Effective_Date", "")
            or row.get("EffectiveDate", "")
            or row.get("Start_Date", "")
            or row.get("StartDate", "")
        )
        end_date = parse_date(
            row.get("End_Date", "") or row.get("EndDate", "") or row.get("Termination_Date", "")
        )
        posted_date = parse_date(
            row.get("Posted_Date", "")
            or row.get("PostedDate", "")
            or row.get("Publication_Date", "")
        )

        # Lobbyist info
        lobbyist_name = (
            row.get("Lobbyist_Name", "")
            or row.get("LobbyistName", "")
            or row.get("Lobbyist", "")
            or ""
        ).strip()
        if lobbyist_name.lower() == "null":
            lobbyist_name = None

        # Registrant (organization doing the lobbying for in-house)
        registrant_name = (
            row.get("Registrant_Name", "")
            or row.get("RegistrantName", "")
            or row.get("Registrant", "")
            or row.get("Organization_Name", "")
            or ""
        ).strip()
        if registrant_name.lower() == "null":
            registrant_name = None

        # Client (for consultant lobbyists)
        client_name = (
            row.get("Client_Name", "")
            or row.get("ClientName", "")
            or row.get("Client", "")
            or ""
        ).strip()
        if client_name.lower() == "null":
            client_name = None

        # Registration type
        reg_type_raw = (
            row.get("Registration_Type", "")
            or row.get("RegistrationType", "")
            or row.get("Type", "")
            or ""
        ).strip().lower()
        reg_type = None
        if "consultant" in reg_type_raw:
            reg_type = "consultant"
        elif "in-house" in reg_type_raw or "inhouse" in reg_type_raw:
            reg_type = "in-house"
        elif reg_type_raw:
            reg_type = reg_type_raw

        # Status
        status = (
            row.get("Status", "")
            or row.get("Registration_Status", "")
            or row.get("RegistrationStatus", "")
            or ""
        ).strip()
        if status.lower() == "null":
            status = None

        return BCLobbyistRegistration(
            registration_id=reg_id,
            registration_type=reg_type,
            status=status,
            effective_date=effective_date,
            end_date=end_date,
            posted_date=posted_date,
            lobbyist_name=lobbyist_name,
            registrant_name=registrant_name,
            client_name=client_name,
            subject_matters=subject_matters.get(reg_id, []),
            institutions=institutions.get(reg_id, []),
        )

    async def process_record(self, record: BCLobbyistRegistration) -> dict[str, Any]:
        """Process a BC lobbying registration record."""
        result = {"created": False, "updated": False, "duplicate": False, "entity_id": None}

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
            entity_id, is_new = await self._postgres.upsert_entity(
                db,
                name=primary_name,
                entity_type="organization",
                external_ids={"bc_lobbying_registration": record.registration_id},
                metadata={
                    "source": "bc_lobbying_registry",
                    "jurisdiction": "BC",
                    "registration_type": record.registration_type,
                    "status": record.status,
                    "subject_matters": record.subject_matters,
                    "institutions_lobbied": record.institutions,
                },
                find_by="name",
            )

            result["entity_id"] = str(entity_id)
            if is_new:
                result["created"] = True
                self.logger.info(f"  PostgreSQL: created entity {entity_id}")
            else:
                result["updated"] = True
                self.logger.info(f"  PostgreSQL: updated entity {entity_id}")

        # --- Neo4j: Create nodes and relationships ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Create/update Organization node for the lobbying client/registrant
                await self._neo4j.merge_organization(
                    session,
                    id=str(entity_id),
                    name=primary_name,
                    org_type=None,
                    external_ids={"bc_lobbying_registration": record.registration_id},
                    properties={
                        "bc_lobbying_status": record.status,
                        "bc_lobbying_type": record.registration_type,
                    },
                    merge_key="name",
                )

                # Create Person node for the lobbyist (if present)
                lobbyist_entity_id = None
                if record.lobbyist_name:
                    lobbyist_entity_id = uuid4()
                    await self._neo4j.merge_person(
                        session,
                        id=str(lobbyist_entity_id),
                        name=record.lobbyist_name,
                        external_ids={},
                        properties={
                            "lobbyist_type": record.registration_type,
                            "jurisdiction": "BC",
                        },
                        merge_key="name",
                    )

                    # Create PROVINCIAL_LOBBIES_FOR relationship
                    rel_props = {
                        "jurisdiction": "BC",
                        "registration_id": record.registration_id,
                        "registration_type": record.registration_type,
                        "status": record.status,
                        "subject_matters": record.subject_matters[:10],  # Limit array size
                        "source": "bc_lobbying_registry",
                        "updated_at": now,
                    }
                    if record.effective_date:
                        rel_props["valid_from"] = record.effective_date.isoformat()
                    if record.end_date:
                        rel_props["valid_to"] = record.end_date.isoformat()

                    await self._neo4j.create_relationship(
                        session,
                        rel_type="PROVINCIAL_LOBBIES_FOR",
                        source_label="Person",
                        source_key="name",
                        source_value=record.lobbyist_name,
                        target_label="Organization",
                        target_key="name",
                        target_value=primary_name,
                        properties=rel_props,
                        merge_on=["registration_id", "jurisdiction"],
                    )

                    self.logger.info(
                        f"  Neo4j: {record.lobbyist_name} -[PROVINCIAL_LOBBIES_FOR]-> {primary_name}"
                    )

                # If registrant is an organization (in-house), create relationship
                if record.registrant_name and record.registrant_name != primary_name:
                    await self._neo4j.merge_organization(
                        session,
                        id=str(uuid4()),
                        name=record.registrant_name,
                        org_type=None,
                        external_ids={},
                        properties={},
                        merge_key="name",
                    )

                    # Create relationship from registrant org to client org
                    if record.client_name:
                        rel_props = {
                            "jurisdiction": "BC",
                            "registration_id": record.registration_id,
                            "source": "bc_lobbying_registry",
                            "updated_at": now,
                        }
                        await self._neo4j.create_relationship(
                            session,
                            rel_type="PROVINCIAL_LOBBIES_FOR",
                            source_label="Organization",
                            source_key="name",
                            source_value=record.registrant_name,
                            target_label="Organization",
                            target_key="name",
                            target_value=primary_name,
                            properties=rel_props,
                            merge_on=["registration_id", "jurisdiction"],
                        )

                # Create nodes for government institutions lobbied
                for institution in record.institutions[:10]:  # Limit to avoid too many
                    await self._neo4j.merge_organization(
                        session,
                        id=str(uuid4()),
                        name=institution,
                        org_type="government",
                        external_ids={},
                        properties={"is_government": True, "jurisdiction": "BC"},
                        merge_key="name",
                    )

                    # Create LOBBIED relationship
                    await self._neo4j.create_relationship(
                        session,
                        rel_type="LOBBIED",
                        source_label="Organization",
                        source_key="name",
                        source_value=primary_name,
                        target_label="Organization",
                        target_key="name",
                        target_value=institution,
                        properties={
                            "jurisdiction": "BC",
                            "registration_id": record.registration_id,
                            "subject_matters": record.subject_matters[:5],
                            "source": "bc_lobbying_registry",
                            "updated_at": now,
                        },
                        merge_on=["registration_id"],
                    )

                if record.institutions:
                    self.logger.info(
                        f"  Neo4j: created LOBBIED relationships to {len(record.institutions)} institutions"
                    )

                self.logger.info(f"  Neo4j: completed for {primary_name}")

        except Exception as e:
            self.logger.warning(f"  Neo4j: FAILED - {e}")

        return result


async def run_bc_lobbying_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run BC Lobbying Registry ingestion.

    Args:
        limit: Maximum number of registrations to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of client/registrant names to filter
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = BCLobbyingIngester()

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
