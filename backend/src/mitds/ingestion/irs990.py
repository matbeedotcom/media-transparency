"""IRS 990 nonprofit filings ingester.

Ingests data from the IRS bulk data files on AWS S3.
Key data points:
- Organization name, EIN, address
- Officers/directors with compensation (Part VII)
- Grants made to other organizations (Schedule I)
- Related organizations (Schedule R)

Data source: s3://irs-form-990/
Coverage: 2011-present, ~500K filings/year
"""

import asyncio
import re
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import UUID, uuid4
from xml.etree import ElementTree as ET

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
from ..storage import StorageClient, compute_content_hash, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, with_retry, RetryConfig

logger = get_context_logger(__name__)

# IRS 990 S3 bucket base URL (public, no auth required)
IRS_990_BASE_URL = "https://s3.amazonaws.com/irs-form-990"
IRS_990_INDEX_URL = f"{IRS_990_BASE_URL}/index_{{year}}.json"

# XML namespaces used in 990 filings
NS = {
    "irs": "http://www.irs.gov/efile",
}


class IRS990Filing(BaseModel):
    """Parsed IRS 990 filing record."""

    # Filing metadata
    object_id: str = Field(..., description="IRS object ID")
    ein: str = Field(..., description="Employer Identification Number")
    tax_period: str = Field(..., description="Tax period (YYYYMM)")
    form_type: str = Field(..., description="Form type (990, 990EZ, 990PF)")
    url: str = Field(..., description="URL to XML file")

    # Organization info
    name: str
    address: Address | None = None
    tax_year: int
    formation_year: int | None = None
    state: str | None = None

    # Officers (from Part VII)
    officers: list[dict[str, Any]] = Field(default_factory=list)

    # Grants made (from Schedule I)
    grants_made: list[dict[str, Any]] = Field(default_factory=list)

    # Related organizations (from Schedule R)
    related_orgs: list[dict[str, Any]] = Field(default_factory=list)


class IRS990IndexEntry(BaseModel):
    """Entry from the IRS 990 index file."""

    object_id: str = Field(alias="OBJECT_ID")
    ein: str = Field(alias="EIN")
    tax_period: str = Field(alias="TAX_PERIOD")
    taxpayer_name: str = Field(alias="TAXPAYER_NAME")
    return_type: str = Field(alias="RETURN_TYPE")
    url: str = Field(alias="URL")


class IRS990Ingester(BaseIngester[IRS990Filing]):
    """Ingester for IRS 990 nonprofit filings from AWS S3.

    Downloads bulk XML files and extracts:
    - Organization details
    - Officer/director information with compensation
    - Grant relationships (Schedule I)
    """

    def __init__(self):
        super().__init__("irs990")
        self._http_client: httpx.AsyncClient | None = None
        self._storage: StorageClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
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
    ) -> AsyncIterator[IRS990Filing]:
        """Fetch IRS 990 filings from AWS S3.

        Downloads the index for specified years and yields parsed filings.
        """
        settings = get_settings()

        # Determine years to process
        current_year = datetime.now().year
        start_year = config.extra_params.get("start_year", current_year - 1)
        end_year = config.extra_params.get("end_year", current_year)

        for year in range(start_year, end_year + 1):
            self.logger.info(f"Processing year {year}")

            # Fetch index for this year
            index_entries = await self._fetch_index(year)
            self.logger.info(f"Found {len(index_entries)} filings for {year}")

            # Apply date filter if incremental
            if config.date_from:
                # Filter by tax period (YYYYMM)
                min_period = config.date_from.strftime("%Y%m")
                index_entries = [
                    e for e in index_entries if e.tax_period >= min_period
                ]
                self.logger.info(
                    f"After date filter: {len(index_entries)} filings"
                )

            # Filter by target entities (EINs) if specified
            if config.target_entities:
                # Normalize EINs by stripping dashes for comparison
                target_eins = {
                    ein.replace("-", "") for ein in config.target_entities
                }
                index_entries = [
                    e for e in index_entries
                    if e.ein.replace("-", "") in target_eins
                ]
                self.logger.info(
                    f"Filtered to {len(index_entries)} filings for "
                    f"{len(target_eins)} target EINs"
                )

            # Process each filing
            for entry in index_entries:
                try:
                    filing = await self._download_and_parse(entry)
                    if filing:
                        yield filing
                except Exception as e:
                    self.logger.warning(
                        f"Failed to process {entry.object_id}: {e}"
                    )
                    continue

    async def _fetch_index(self, year: int) -> list[IRS990IndexEntry]:
        """Fetch the index file for a given year."""
        url = IRS_990_INDEX_URL.format(year=year)

        async def _do_fetch():
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.json()

        data = await with_retry(_do_fetch, logger=self.logger)

        entries = []
        for item in data.get("Filings" + str(year), data.get("Filings", [])):
            try:
                entry = IRS990IndexEntry.model_validate(item)
                # Only process 990, 990EZ, 990PF forms
                if entry.return_type in ("990", "990EZ", "990PF"):
                    entries.append(entry)
            except Exception as e:
                self.logger.debug(f"Skipping invalid index entry: {e}")

        return entries

    async def _download_and_parse(
        self, entry: IRS990IndexEntry
    ) -> IRS990Filing | None:
        """Download and parse a single filing."""
        # Download XML
        async def _do_download():
            response = await self.http_client.get(entry.url)
            response.raise_for_status()
            return response.content

        xml_content = await with_retry(_do_download, logger=self.logger)

        # Store raw XML
        storage_key = generate_storage_key(
            "irs990",
            entry.object_id,
            extension="xml",
        )
        self.storage.upload_file(
            xml_content,
            storage_key,
            content_type="application/xml",
            metadata={
                "ein": entry.ein,
                "tax_period": entry.tax_period,
                "object_id": entry.object_id,
            },
        )

        # Parse XML
        return self._parse_990_xml(xml_content, entry)

    def _parse_990_xml(
        self, xml_content: bytes, entry: IRS990IndexEntry
    ) -> IRS990Filing | None:
        """Parse IRS 990 XML content."""
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            self.logger.warning(f"XML parse error for {entry.object_id}: {e}")
            return None

        # Find the return data element
        return_data = root.find(".//irs:ReturnData", NS)
        if return_data is None:
            # Try without namespace
            return_data = root.find(".//ReturnData")
        if return_data is None:
            return_data = root

        # Extract form data (990, 990EZ, or 990PF)
        form_990 = (
            return_data.find(".//irs:IRS990", NS)
            or return_data.find(".//IRS990")
            or return_data.find(".//irs:IRS990EZ", NS)
            or return_data.find(".//IRS990EZ")
            or return_data.find(".//irs:IRS990PF", NS)
            or return_data.find(".//IRS990PF")
        )

        if form_990 is None:
            self.logger.warning(f"No form data found in {entry.object_id}")
            return None

        # Extract organization name
        org_name = self._get_text(
            form_990,
            [
                ".//irs:BusinessName/irs:BusinessNameLine1Txt",
                ".//irs:BusinessName/irs:BusinessNameLine1",
                ".//BusinessName/BusinessNameLine1Txt",
                ".//BusinessName/BusinessNameLine1",
                ".//irs:OrganizationName/irs:BusinessNameLine1Txt",
                ".//OrganizationName/BusinessNameLine1Txt",
            ],
        ) or entry.taxpayer_name

        # Extract address
        address = self._extract_address(form_990)

        # Extract tax year
        tax_year = int(entry.tax_period[:4])

        # Extract formation year
        formation_year = self._get_int(
            form_990,
            [
                ".//irs:FormationYr",
                ".//FormationYr",
                ".//irs:YearFormation",
                ".//YearFormation",
            ],
        )

        # Extract state
        state = self._get_text(
            form_990,
            [
                ".//irs:StateOfLegalDomicileCD",
                ".//StateOfLegalDomicileCD",
            ],
        )

        # Extract officers (Part VII)
        officers = self._extract_officers(form_990)

        # Extract grants made (Schedule I)
        grants_made = self._extract_grants_made(return_data)

        # Extract related organizations (Schedule R)
        related_orgs = self._extract_related_orgs(return_data)

        return IRS990Filing(
            object_id=entry.object_id,
            ein=entry.ein,
            tax_period=entry.tax_period,
            form_type=entry.return_type,
            url=entry.url,
            name=org_name,
            address=address,
            tax_year=tax_year,
            formation_year=formation_year,
            state=state,
            officers=officers,
            grants_made=grants_made,
            related_orgs=related_orgs,
        )

    def _get_text(
        self, elem: ET.Element, paths: list[str]
    ) -> str | None:
        """Get text from first matching path."""
        for path in paths:
            found = elem.find(path, NS)
            if found is None:
                # Try without namespace
                path_no_ns = path.replace("irs:", "")
                found = elem.find(path_no_ns)
            if found is not None and found.text:
                return found.text.strip()
        return None

    def _get_int(self, elem: ET.Element, paths: list[str]) -> int | None:
        """Get integer from first matching path."""
        text = self._get_text(elem, paths)
        if text:
            try:
                return int(text)
            except ValueError:
                pass
        return None

    def _extract_address(self, form_990: ET.Element) -> Address | None:
        """Extract organization address."""
        # Try US address first
        us_addr = (
            form_990.find(".//irs:USAddress", NS)
            or form_990.find(".//USAddress")
            or form_990.find(".//irs:AddressUS", NS)
            or form_990.find(".//AddressUS")
        )

        if us_addr is not None:
            return Address(
                street=self._get_text(
                    us_addr,
                    [
                        ".//irs:AddressLine1Txt",
                        ".//AddressLine1Txt",
                        ".//irs:AddressLine1",
                        ".//AddressLine1",
                    ],
                ),
                city=self._get_text(
                    us_addr,
                    [".//irs:CityNm", ".//CityNm", ".//irs:City", ".//City"],
                ),
                state=self._get_text(
                    us_addr,
                    [
                        ".//irs:StateAbbreviationCd",
                        ".//StateAbbreviationCd",
                        ".//irs:State",
                        ".//State",
                    ],
                ),
                postal_code=self._get_text(
                    us_addr,
                    [".//irs:ZIPCd", ".//ZIPCd", ".//irs:ZIPCode", ".//ZIPCode"],
                ),
                country="US",
            )

        return None

    def _extract_officers(self, form_990: ET.Element) -> list[dict[str, Any]]:
        """Extract officers/directors from Part VII."""
        officers = []

        # Part VII officer entries
        officer_elements = (
            form_990.findall(".//irs:Form990PartVIISectionAGrp", NS)
            or form_990.findall(".//Form990PartVIISectionAGrp")
            or form_990.findall(".//irs:OfficerDirectorTrusteeKeyEmpl", NS)
            or form_990.findall(".//OfficerDirectorTrusteeKeyEmpl")
        )

        for officer_elem in officer_elements:
            name = self._get_text(
                officer_elem,
                [
                    ".//irs:PersonNm",
                    ".//PersonNm",
                    ".//irs:NamePerson",
                    ".//NamePerson",
                ],
            )
            if not name:
                continue

            title = self._get_text(
                officer_elem,
                [".//irs:TitleTxt", ".//TitleTxt", ".//irs:Title", ".//Title"],
            )

            # Compensation
            compensation = self._get_int(
                officer_elem,
                [
                    ".//irs:ReportableCompFromOrgAmt",
                    ".//ReportableCompFromOrgAmt",
                    ".//irs:Compensation",
                    ".//Compensation",
                ],
            )

            hours = self._get_text(
                officer_elem,
                [
                    ".//irs:AverageHoursPerWeekRt",
                    ".//AverageHoursPerWeekRt",
                    ".//irs:AvgHoursPerWkDevotedToPosition",
                    ".//AvgHoursPerWkDevotedToPosition",
                ],
            )
            hours_per_week = None
            if hours:
                try:
                    hours_per_week = float(hours)
                except ValueError:
                    pass

            officers.append({
                "name": name,
                "title": title,
                "compensation": compensation,
                "hours_per_week": hours_per_week,
            })

        return officers

    def _extract_grants_made(
        self, return_data: ET.Element
    ) -> list[dict[str, Any]]:
        """Extract grants made from Schedule I."""
        grants = []

        # Find Schedule I
        schedule_i = (
            return_data.find(".//irs:IRS990ScheduleI", NS)
            or return_data.find(".//IRS990ScheduleI")
        )

        if schedule_i is None:
            return grants

        # Grant recipient entries
        grant_elements = (
            schedule_i.findall(".//irs:RecipientTable", NS)
            or schedule_i.findall(".//RecipientTable")
            or schedule_i.findall(".//irs:GrantsToOrgsGrp", NS)
            or schedule_i.findall(".//GrantsToOrgsGrp")
        )

        for grant_elem in grant_elements:
            # Recipient name (organization or individual)
            recipient_name = self._get_text(
                grant_elem,
                [
                    ".//irs:RecipientBusinessName/irs:BusinessNameLine1Txt",
                    ".//RecipientBusinessName/BusinessNameLine1Txt",
                    ".//irs:RecipientBusinessName/irs:BusinessNameLine1",
                    ".//RecipientBusinessName/BusinessNameLine1",
                    ".//irs:RecipientNameBusiness/irs:BusinessNameLine1Txt",
                    ".//RecipientNameBusiness/BusinessNameLine1Txt",
                ],
            )

            if not recipient_name:
                continue

            # Recipient EIN (if organization)
            recipient_ein = self._get_text(
                grant_elem,
                [
                    ".//irs:RecipientEIN",
                    ".//RecipientEIN",
                    ".//irs:EINOfRecipient",
                    ".//EINOfRecipient",
                ],
            )

            # Grant amount
            amount = self._get_int(
                grant_elem,
                [
                    ".//irs:CashGrantAmt",
                    ".//CashGrantAmt",
                    ".//irs:AmountOfCashGrant",
                    ".//AmountOfCashGrant",
                ],
            )

            # Purpose
            purpose = self._get_text(
                grant_elem,
                [
                    ".//irs:PurposeOfGrantTxt",
                    ".//PurposeOfGrantTxt",
                    ".//irs:PurposeOfGrant",
                    ".//PurposeOfGrant",
                ],
            )

            grants.append({
                "recipient_name": recipient_name,
                "recipient_ein": recipient_ein,
                "amount": amount,
                "purpose": purpose,
            })

        return grants

    def _extract_related_orgs(
        self, return_data: ET.Element
    ) -> list[dict[str, Any]]:
        """Extract related organizations from Schedule R."""
        related = []

        # Find Schedule R
        schedule_r = (
            return_data.find(".//irs:IRS990ScheduleR", NS)
            or return_data.find(".//IRS990ScheduleR")
        )

        if schedule_r is None:
            return related

        # Related org entries
        related_elements = (
            schedule_r.findall(".//irs:IdDisregardedEntitiesGrp", NS)
            or schedule_r.findall(".//IdDisregardedEntitiesGrp")
            or schedule_r.findall(".//irs:IdRelatedTaxExemptOrgGrp", NS)
            or schedule_r.findall(".//IdRelatedTaxExemptOrgGrp")
            or schedule_r.findall(".//irs:IdRelatedOrgTxblPartnershipGrp", NS)
            or schedule_r.findall(".//IdRelatedOrgTxblPartnershipGrp")
        )

        for elem in related_elements:
            name = self._get_text(
                elem,
                [
                    ".//irs:DisregardedEntityName/irs:BusinessNameLine1Txt",
                    ".//DisregardedEntityName/BusinessNameLine1Txt",
                    ".//irs:RelatedOrganizationName/irs:BusinessNameLine1Txt",
                    ".//RelatedOrganizationName/BusinessNameLine1Txt",
                ],
            )

            if not name:
                continue

            ein = self._get_text(
                elem,
                [".//irs:EIN", ".//EIN"],
            )

            relationship = self._get_text(
                elem,
                [
                    ".//irs:DirectControllingEntityName/irs:BusinessNameLine1Txt",
                    ".//DirectControllingEntityName/BusinessNameLine1Txt",
                ],
            )

            related.append({
                "name": name,
                "ein": ein,
                "relationship": relationship,
            })

        return related

    async def process_record(self, record: IRS990Filing) -> dict[str, Any]:
        """Process a parsed IRS 990 filing.

        Creates/updates:
        - Organization entity
        - Person entities for officers
        - FUNDED_BY relationships for grants
        - DIRECTOR_OF/EMPLOYED_BY relationships for officers
        """
        result = {"created": False, "updated": False, "duplicate": False}

        # Format EIN
        ein = record.ein
        if len(ein) == 9:
            ein = f"{ein[:2]}-{ein[2:]}"

        # Create or update organization in Neo4j
        async with get_neo4j_session() as session:
            # Check if organization exists
            query_check = """
            MATCH (o:Organization {ein: $ein})
            RETURN o.id as id, o.updated_at as updated_at
            """
            check_result = await session.run(query_check, ein=ein)
            existing = await check_result.single()

            org_id = uuid4() if not existing else UUID(existing["id"])

            if existing:
                result["updated"] = True
            else:
                result["created"] = True

            # Create/update organization node
            org_props = {
                "id": str(org_id),
                "name": record.name,
                "ein": ein,
                "entity_type": "ORGANIZATION",
                "org_type": "nonprofit",
                "status": "active",
                "jurisdiction": record.state or "US",
                "confidence": 1.0,
                "updated_at": datetime.utcnow().isoformat(),
            }

            if record.formation_year:
                org_props["formation_year"] = record.formation_year

            if record.address:
                org_props["address_street"] = record.address.street
                org_props["address_city"] = record.address.city
                org_props["address_state"] = record.address.state
                org_props["address_postal"] = record.address.postal_code

            if not existing:
                org_props["created_at"] = datetime.utcnow().isoformat()

            query_upsert = """
            MERGE (o:Organization {ein: $ein})
            SET o += $props
            RETURN o.id as id
            """
            await session.run(query_upsert, ein=ein, props=org_props)

            # Process officers
            for officer in record.officers:
                if not officer.get("name"):
                    continue

                person_id = str(uuid4())

                # Create person node
                person_query = """
                MERGE (p:Person {name: $name, irs_990_name: $name})
                ON CREATE SET
                    p.id = $person_id,
                    p.entity_type = 'PERSON',
                    p.confidence = 1.0,
                    p.created_at = $now
                SET p.updated_at = $now
                RETURN p.id as id
                """
                person_result = await session.run(
                    person_query,
                    name=officer["name"],
                    person_id=person_id,
                    now=datetime.utcnow().isoformat(),
                )
                person_record = await person_result.single()
                person_id = person_record["id"]

                # Create DIRECTOR_OF or EMPLOYED_BY relationship
                rel_type = "DIRECTOR_OF"
                if officer.get("title"):
                    title_lower = officer["title"].lower()
                    if any(
                        t in title_lower
                        for t in ["director", "trustee", "board"]
                    ):
                        rel_type = "DIRECTOR_OF"
                    else:
                        rel_type = "EMPLOYED_BY"

                rel_query = f"""
                MATCH (p:Person {{id: $person_id}})
                MATCH (o:Organization {{ein: $ein}})
                MERGE (p)-[r:{rel_type}]->(o)
                SET r.title = $title,
                    r.compensation = $compensation,
                    r.hours_per_week = $hours,
                    r.tax_year = $tax_year,
                    r.confidence = 1.0,
                    r.updated_at = $now
                """
                await session.run(
                    rel_query,
                    person_id=person_id,
                    ein=ein,
                    title=officer.get("title"),
                    compensation=officer.get("compensation"),
                    hours=officer.get("hours_per_week"),
                    tax_year=record.tax_year,
                    now=datetime.utcnow().isoformat(),
                )

            # Process grants made
            for grant in record.grants_made:
                if not grant.get("recipient_name"):
                    continue

                recipient_ein = grant.get("recipient_ein")
                if recipient_ein and len(recipient_ein) == 9:
                    recipient_ein = f"{recipient_ein[:2]}-{recipient_ein[2:]}"

                # Create or find recipient organization
                if recipient_ein:
                    recipient_query = """
                    MERGE (r:Organization {ein: $ein})
                    ON CREATE SET
                        r.id = $id,
                        r.name = $name,
                        r.entity_type = 'ORGANIZATION',
                        r.org_type = 'nonprofit',
                        r.confidence = 0.8,
                        r.created_at = $now
                    SET r.updated_at = $now
                    RETURN r.id as id
                    """
                else:
                    recipient_query = """
                    MERGE (r:Organization {name: $name})
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
                    ein=recipient_ein,
                    id=str(uuid4()),
                    name=grant["recipient_name"],
                    now=datetime.utcnow().isoformat(),
                )

                # Create FUNDED_BY relationship (recipient <- funder)
                if recipient_ein:
                    funded_query = """
                    MATCH (recipient:Organization {ein: $recipient_ein})
                    MATCH (funder:Organization {ein: $funder_ein})
                    MERGE (recipient)-[r:FUNDED_BY]->(funder)
                    SET r.amount = $amount,
                        r.amount_currency = 'USD',
                        r.fiscal_year = $fiscal_year,
                        r.grant_purpose = $purpose,
                        r.confidence = 1.0,
                        r.updated_at = $now
                    """
                else:
                    funded_query = """
                    MATCH (recipient:Organization {name: $recipient_name})
                    MATCH (funder:Organization {ein: $funder_ein})
                    MERGE (recipient)-[r:FUNDED_BY]->(funder)
                    SET r.amount = $amount,
                        r.amount_currency = 'USD',
                        r.fiscal_year = $fiscal_year,
                        r.grant_purpose = $purpose,
                        r.confidence = 0.8,
                        r.updated_at = $now
                    """

                await session.run(
                    funded_query,
                    recipient_ein=recipient_ein,
                    recipient_name=grant["recipient_name"],
                    funder_ein=ein,
                    amount=grant.get("amount"),
                    fiscal_year=record.tax_year,
                    purpose=grant.get("purpose"),
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
def get_irs990_celery_task():
    """Get the Celery task for IRS 990 ingestion.

    Returns the task function to be registered with Celery.
    """
    from ..worker import celery_app

    @celery_app.task(name="mitds.ingestion.irs990.ingest")
    def ingest_irs990_task(
        start_year: int | None = None,
        end_year: int | None = None,
        incremental: bool = True,
    ):
        """Celery task for IRS 990 ingestion.

        Args:
            start_year: Start year (default: previous year)
            end_year: End year (default: current year)
            incremental: Whether to do incremental sync
        """
        import asyncio

        async def run_ingestion():
            ingester = IRS990Ingester()
            try:
                config = IngestionConfig(
                    incremental=incremental,
                    extra_params={
                        "start_year": start_year,
                        "end_year": end_year,
                    },
                )
                result = await ingester.run(config)
                return result.model_dump()
            finally:
                await ingester.close()

        return asyncio.run(run_ingestion())

    return ingest_irs990_task


async def run_irs990_ingestion(
    start_year: int | None = None,
    end_year: int | None = None,
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
) -> dict[str, Any]:
    """Run IRS 990 ingestion directly (not via Celery).

    Args:
        start_year: Start year (default: previous year)
        end_year: End year (default: current year)
        incremental: Whether to do incremental sync
        limit: Maximum number of records to process
        target_entities: Optional list of EINs to ingest specifically

    Returns:
        Ingestion result dictionary
    """
    current_year = datetime.now().year

    ingester = IRS990Ingester()
    try:
        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
            extra_params={
                "start_year": start_year or current_year - 1,
                "end_year": end_year or current_year,
            },
        )
        result = await ingester.run(config)
        return result.model_dump()
    finally:
        await ingester.close()
