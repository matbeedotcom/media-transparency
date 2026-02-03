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
from concurrent.futures import ProcessPoolExecutor
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


# ===========================================
# Module-level worker function for multiprocessing
# ===========================================

def _parse_batch_multiprocess(
    batch: list[tuple[bytes, dict]],
    num_workers: int,
) -> list[dict | None]:
    """Parse a batch of XMLs using multiprocessing.

    This is a module-level function that creates its own ProcessPoolExecutor
    to ensure proper isolation on Windows.

    Args:
        batch: List of (xml_content, entry_dict) tuples
        num_workers: Number of worker processes

    Returns:
        List of parsed filing dicts (or None for failed parses)
    """
    if not batch:
        return []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Use map with appropriate chunksize for efficiency
        chunksize = max(1, len(batch) // (num_workers * 2))
        results = list(executor.map(_parse_990_xml_worker, batch, chunksize=chunksize))

    return results


def _parse_990_xml_worker(args: tuple[bytes, dict]) -> dict | None:
    """Parse a single IRS 990 XML in a worker process.

    This is a module-level function to allow pickling for multiprocessing.

    Args:
        args: Tuple of (xml_content bytes, entry dict with metadata)

    Returns:
        Dict representation of IRS990Filing, or None if parsing fails
    """
    xml_content, entry_dict = args

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return None

    # Find the return data element
    return_data = root.find(".//irs:ReturnData", NS)
    if return_data is None:
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
        return None

    def get_text(elem, paths):
        for path in paths:
            found = elem.find(path, NS)
            if found is None:
                path_no_ns = path.replace("irs:", "")
                found = elem.find(path_no_ns)
            if found is not None and found.text:
                return found.text.strip()
        return None

    def get_int(elem, paths):
        text = get_text(elem, paths)
        if text:
            try:
                return int(text)
            except ValueError:
                pass
        return None

    # Extract organization name
    org_name = get_text(
        form_990,
        [
            ".//irs:BusinessName/irs:BusinessNameLine1Txt",
            ".//irs:BusinessName/irs:BusinessNameLine1",
            ".//BusinessName/BusinessNameLine1Txt",
            ".//BusinessName/BusinessNameLine1",
            ".//irs:OrganizationName/irs:BusinessNameLine1Txt",
            ".//OrganizationName/BusinessNameLine1Txt",
        ],
    ) or entry_dict.get("taxpayer_name", "")

    # Extract address
    address = None
    us_addr = (
        form_990.find(".//irs:USAddress", NS)
        or form_990.find(".//USAddress")
        or form_990.find(".//irs:AddressUS", NS)
        or form_990.find(".//AddressUS")
    )
    if us_addr is not None:
        address = {
            "street": get_text(us_addr, [".//irs:AddressLine1Txt", ".//AddressLine1Txt", ".//irs:AddressLine1", ".//AddressLine1"]),
            "city": get_text(us_addr, [".//irs:CityNm", ".//CityNm", ".//irs:City", ".//City"]),
            "state": get_text(us_addr, [".//irs:StateAbbreviationCd", ".//StateAbbreviationCd", ".//irs:State", ".//State"]),
            "postal_code": get_text(us_addr, [".//irs:ZIPCd", ".//ZIPCd", ".//irs:ZIPCode", ".//ZIPCode"]),
            "country": "US",
        }

    # Extract tax year
    tax_period = entry_dict.get("tax_period", "")
    tax_year = int(tax_period[:4]) if len(tax_period) >= 4 else datetime.now().year

    # Extract formation year
    formation_year = get_int(
        form_990,
        [".//irs:FormationYr", ".//FormationYr", ".//irs:YearFormation", ".//YearFormation"],
    )

    # Extract state
    state = get_text(form_990, [".//irs:StateOfLegalDomicileCD", ".//StateOfLegalDomicileCD"])

    # Extract officers (Part VII) - simplified for worker
    officers = []
    officer_elements = (
        form_990.findall(".//irs:Form990PartVIISectionAGrp", NS)
        or form_990.findall(".//Form990PartVIISectionAGrp")
        or form_990.findall(".//irs:OfficerDirectorTrusteeKeyEmpl", NS)
        or form_990.findall(".//OfficerDirectorTrusteeKeyEmpl")
    )
    for officer_elem in officer_elements:
        name = get_text(officer_elem, [".//irs:PersonNm", ".//PersonNm", ".//irs:NamePerson", ".//NamePerson"])
        if not name:
            continue
        title = get_text(officer_elem, [".//irs:TitleTxt", ".//TitleTxt", ".//irs:Title", ".//Title"])
        compensation = get_int(officer_elem, [".//irs:ReportableCompFromOrgAmt", ".//ReportableCompFromOrgAmt", ".//irs:Compensation", ".//Compensation"])
        hours = get_text(officer_elem, [".//irs:AverageHoursPerWeekRt", ".//AverageHoursPerWeekRt", ".//irs:AvgHoursPerWkDevotedToPosition", ".//AvgHoursPerWkDevotedToPosition"])
        hours_per_week = None
        if hours:
            try:
                hours_per_week = float(hours)
            except ValueError:
                pass
        officers.append({"name": name, "title": title, "compensation": compensation, "hours_per_week": hours_per_week})

    # Extract grants (Schedule I) - simplified for worker
    grants_made = []
    schedule_i = return_data.find(".//irs:IRS990ScheduleI", NS) or return_data.find(".//IRS990ScheduleI")
    if schedule_i is not None:
        grant_elements = (
            schedule_i.findall(".//irs:RecipientTable", NS)
            or schedule_i.findall(".//RecipientTable")
            or schedule_i.findall(".//irs:GrantsToOrgsGrp", NS)
            or schedule_i.findall(".//GrantsToOrgsGrp")
        )
        for grant_elem in grant_elements:
            recipient_name = get_text(grant_elem, [
                ".//irs:RecipientBusinessName/irs:BusinessNameLine1Txt",
                ".//RecipientBusinessName/BusinessNameLine1Txt",
                ".//irs:RecipientBusinessName/irs:BusinessNameLine1",
                ".//RecipientBusinessName/BusinessNameLine1",
                ".//irs:RecipientNameBusiness/irs:BusinessNameLine1Txt",
                ".//RecipientNameBusiness/BusinessNameLine1Txt",
            ])
            if not recipient_name:
                continue
            recipient_ein = get_text(grant_elem, [".//irs:RecipientEIN", ".//RecipientEIN", ".//irs:EINOfRecipient", ".//EINOfRecipient"])
            amount = get_int(grant_elem, [".//irs:CashGrantAmt", ".//CashGrantAmt", ".//irs:AmountOfCashGrant", ".//AmountOfCashGrant"])
            purpose = get_text(grant_elem, [".//irs:PurposeOfGrantTxt", ".//PurposeOfGrantTxt", ".//irs:PurposeOfGrant", ".//PurposeOfGrant"])

            # Extract recipient address (simplified)
            grant_addr = grant_elem.find(".//irs:USAddress", NS) or grant_elem.find(".//USAddress")
            recipient_country = "US"
            recipient_city = None
            recipient_state = None
            recipient_postal = None

            if grant_addr is None:
                grant_addr = grant_elem.find(".//irs:ForeignAddress", NS) or grant_elem.find(".//ForeignAddress")
                if grant_addr is not None:
                    recipient_country = get_text(grant_addr, [".//irs:CountryCd", ".//CountryCd", ".//irs:Country", ".//Country"]) or "XX"

            if grant_addr is not None:
                recipient_city = get_text(grant_addr, [".//irs:CityNm", ".//CityNm", ".//irs:City", ".//City"])
                recipient_state = get_text(grant_addr, [".//irs:StateAbbreviationCd", ".//StateAbbreviationCd", ".//irs:ProvinceOrStateNm", ".//ProvinceOrStateNm"])
                recipient_postal = get_text(grant_addr, [".//irs:ZIPCd", ".//ZIPCd", ".//irs:ForeignPostalCd", ".//ForeignPostalCd"])

            grants_made.append({
                "recipient_name": recipient_name,
                "recipient_ein": recipient_ein,
                "amount": amount,
                "purpose": purpose,
                "recipient_city": recipient_city,
                "recipient_state": recipient_state,
                "recipient_postal": recipient_postal,
                "recipient_country": recipient_country,
            })

    # Return as dict (for pickling)
    return {
        "object_id": entry_dict["object_id"],
        "ein": entry_dict["ein"],
        "tax_period": entry_dict["tax_period"],
        "form_type": entry_dict["return_type"],
        "url": entry_dict["url"],
        "name": org_name,
        "address": address,
        "tax_year": tax_year,
        "formation_year": formation_year,
        "state": state,
        "officers": officers,
        "grants_made": grants_made,
        "related_orgs": [],  # Skip for performance
    }


# IRS 990 data URLs (moved from deprecated S3 to IRS direct downloads)
# See: https://www.irs.gov/charities-non-profits/form-990-series-downloads
IRS_990_BASE_URL = "https://apps.irs.gov/pub/epostcard/990/xml"
IRS_990_INDEX_URL = f"{IRS_990_BASE_URL}/{{year}}/index_{{year}}.csv"
# Monthly ZIP files follow pattern: {year}/{year}_TEOS_XML_{month}A.zip

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
        """Fetch IRS 990 filings from IRS bulk downloads.

        Downloads monthly ZIP files and extracts XML filings.
        Uses parallel XML parsing for better performance.
        """
        import io
        import os
        from concurrent.futures import ThreadPoolExecutor
        from zipfile import ZipFile

        # Determine years to process
        current_year = datetime.now().year
        start_year = config.extra_params.get("start_year", current_year - 1)
        end_year = config.extra_params.get("end_year", current_year)
        skip_count = config.extra_params.get("skip", 0)

        # Number of parallel workers for XML parsing (CPU-bound)
        num_workers = config.extra_params.get("num_workers", min(os.cpu_count() or 4, 8))
        parse_batch_size = config.extra_params.get("parse_batch_size", num_workers * 4)

        for year in range(start_year, end_year + 1):
            self.logger.info(f"Processing year {year}")
            print(f"Processing year {year}")  # Direct output for visibility

            # Build index lookup for metadata
            index_entries = await self._fetch_index(year)
            self.logger.info(f"Found {len(index_entries)} entries in index")
            print(f"Found {len(index_entries)} entries in index")

            # Create lookup by object_id
            index_lookup = {e.object_id: e for e in index_entries}

            # Filter by target entities (EINs) if specified
            target_eins = None
            if config.target_entities:
                target_eins = {
                    ein.replace("-", "") for ein in config.target_entities
                }
                self.logger.info(f"Filtering to {len(target_eins)} target EINs")

            # Download and process monthly ZIP files
            records_yielded = 0
            records_seen = 0  # Total records encountered (for skip tracking)

            for month in range(1, 13):
                if config.limit and records_yielded >= config.limit:
                    break

                zip_url = f"{IRS_990_BASE_URL}/{year}/{year}_TEOS_XML_{month:02d}A.zip"
                self.logger.info(f"Downloading {zip_url}")
                print(f"Downloading month {month:02d}...")

                try:
                    zip_content = await self._download_zip(zip_url)
                    if not zip_content:
                        continue

                    # Extract and process XMLs from ZIP
                    with ZipFile(io.BytesIO(zip_content)) as zf:
                        xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
                        self.logger.info(f"  Found {len(xml_files)} XML files in ZIP")
                        print(f"  Found {len(xml_files)} XML files (parsing with {num_workers} workers)")

                        # If we're skipping and haven't reached the skip point yet,
                        # check if this entire ZIP can be skipped
                        if skip_count > 0 and records_seen + len(xml_files) <= skip_count:
                            records_seen += len(xml_files)
                            self.logger.info(f"  Skipping entire ZIP (records {records_seen - len(xml_files) + 1}-{records_seen})")
                            print(f"  Skipping entire ZIP ({records_seen}/{skip_count} records seen)")
                            continue

                        # Collect XMLs to parse in parallel batches
                        parse_batch: list[tuple[bytes, IRS990IndexEntry]] = []

                        for xml_name in xml_files:
                            if config.limit and records_yielded >= config.limit:
                                break

                            records_seen += 1

                            # Skip if we haven't reached the skip point yet
                            if records_seen <= skip_count:
                                if records_seen % 1000 == 0:
                                    print(f"  Skipping... ({records_seen}/{skip_count})")
                                continue

                            try:
                                xml_content = zf.read(xml_name)

                                # Extract object_id from filename (e.g., "202340189349301104_public.xml")
                                object_id = xml_name.replace("_public.xml", "").replace(".xml", "")

                                # Get metadata from index
                                entry = index_lookup.get(object_id)
                                if not entry:
                                    # Create minimal entry from filename
                                    entry = IRS990IndexEntry(
                                        OBJECT_ID=object_id,
                                        EIN="",
                                        TAX_PERIOD=str(year) + "12",
                                        TAXPAYER_NAME="",
                                        RETURN_TYPE="990",
                                        URL="",
                                    )

                                # Apply EIN filter if specified
                                if target_eins and entry.ein.replace("-", "") not in target_eins:
                                    continue

                                # Add to parse batch
                                parse_batch.append((xml_content, entry))

                                # When batch is full, parse in parallel
                                if len(parse_batch) >= parse_batch_size:
                                    async for filing in self._parse_xml_batch_parallel(
                                        parse_batch, num_workers
                                    ):
                                        records_yielded += 1
                                        yield filing
                                    parse_batch = []

                            except Exception as e:
                                self.logger.warning(f"Failed to read {xml_name}: {e}")
                                continue

                        # Parse remaining batch
                        if parse_batch:
                            async for filing in self._parse_xml_batch_parallel(
                                parse_batch, num_workers
                            ):
                                if config.limit and records_yielded >= config.limit:
                                    break
                                records_yielded += 1
                                yield filing

                except Exception as e:
                    self.logger.warning(f"Failed to download/process {zip_url}: {e}")
                    print(f"  Error: {e}")
                    continue

            self.logger.info(f"Completed year {year}: {records_yielded} records yielded, {records_seen} total seen")
            print(f"Completed year {year}: {records_yielded} records yielded")

    async def _parse_xml_batch_parallel(
        self,
        batch: list[tuple[bytes, IRS990IndexEntry]],
        num_workers: int,
    ) -> AsyncIterator[IRS990Filing]:
        """Parse a batch of XMLs in parallel using multiprocessing.

        Uses ProcessPoolExecutor.map() for efficient parallel parsing on Windows.

        Args:
            batch: List of (xml_content, entry) tuples to parse
            num_workers: Number of parallel workers

        Yields:
            Successfully parsed IRS990Filing objects
        """
        # Convert entries to dicts for pickling (Pydantic models may have issues)
        batch_for_workers = [
            (xml_content, {
                "object_id": entry.object_id,
                "ein": entry.ein,
                "tax_period": entry.tax_period,
                "taxpayer_name": entry.taxpayer_name,
                "return_type": entry.return_type,
                "url": entry.url,
            })
            for xml_content, entry in batch
        ]

        # Run parallel parsing in a thread to not block the event loop
        results = await asyncio.to_thread(
            _parse_batch_multiprocess,
            batch_for_workers,
            num_workers,
        )

        for result in results:
            if result is not None:
                # Convert dict back to IRS990Filing
                yield IRS990Filing(**result)

    async def _fetch_index(self, year: int) -> list[IRS990IndexEntry]:
        """Fetch the index file for a given year.

        The IRS provides index files in CSV format at:
        https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv
        """
        import csv
        import io

        url = IRS_990_INDEX_URL.format(year=year)
        self.logger.info(f"Fetching index from: {url}")

        async def _do_fetch():
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.text

        try:
            csv_content = await with_retry(_do_fetch, logger=self.logger)
        except Exception as e:
            self.logger.warning(f"Failed to fetch index for {year}: {e}")
            return []

        entries = []
        reader = csv.DictReader(io.StringIO(csv_content))

        for row in reader:
            try:
                # Map CSV columns to our model
                # CSV columns: RETURN_ID, FILING_TYPE, EIN, TAX_PERIOD, SUB_DATE, TAXPAYER_NAME, RETURN_TYPE, DLN, OBJECT_ID
                object_id = row.get("OBJECT_ID", row.get("RETURN_ID", ""))
                ein = row.get("EIN", "")
                tax_period = row.get("TAX_PERIOD", "")
                taxpayer_name = row.get("TAXPAYER_NAME", "")
                return_type = row.get("RETURN_TYPE", "")

                if not object_id or not ein:
                    continue

                # Build URL to the XML file (if not provided in CSV)
                xml_url = row.get("URL", "")
                if not xml_url:
                    # Construct URL based on object_id pattern
                    xml_url = f"{IRS_990_BASE_URL}/{year}/{object_id}_public.xml"

                entry = IRS990IndexEntry(
                    OBJECT_ID=object_id,
                    EIN=ein,
                    TAX_PERIOD=tax_period,
                    TAXPAYER_NAME=taxpayer_name,
                    RETURN_TYPE=return_type,
                    URL=xml_url,
                )

                # Only process 990, 990EZ, 990PF forms
                if entry.return_type in ("990", "990EZ", "990PF"):
                    entries.append(entry)
            except Exception as e:
                self.logger.debug(f"Skipping invalid index entry: {e}")

        self.logger.info(f"Parsed {len(entries)} entries from index")
        return entries

    async def _download_zip(self, url: str) -> bytes | None:
        """Download a monthly ZIP file."""

        async def _do_download():
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.content

        try:
            content = await with_retry(
                _do_download,
                config=RetryConfig(max_retries=2, base_delay=5.0),
                logger=self.logger,
            )
            self.logger.info(f"  Downloaded {len(content) // 1024 // 1024}MB")
            return content
        except Exception as e:
            self.logger.warning(f"  Failed to download ZIP: {e}")
            return None

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
        await asyncio.to_thread(
            self.storage.upload_file,
            xml_content,
            storage_key,
            content_type="application/xml",
            metadata={
                "ein": entry.ein,
                "tax_period": entry.tax_period,
                "object_id": entry.object_id,
            },
        )

        # Parse XML (offload CPU-bound work to thread pool)
        return await asyncio.to_thread(self._parse_990_xml, xml_content, entry)

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

    def _extract_recipient_address(self, grant_elem: ET.Element) -> Address | None:
        """Extract recipient address from Schedule I grant element.

        Handles both US addresses and foreign addresses (including Canadian).
        """
        # Try US address first
        us_addr = (
            grant_elem.find(".//irs:USAddress", NS)
            or grant_elem.find(".//USAddress")
            or grant_elem.find(".//irs:RecipientUSAddress", NS)
            or grant_elem.find(".//RecipientUSAddress")
            or grant_elem.find(".//irs:AddressUS", NS)
            or grant_elem.find(".//AddressUS")
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

        # Try foreign address (for Canadian and other international recipients)
        foreign_addr = (
            grant_elem.find(".//irs:ForeignAddress", NS)
            or grant_elem.find(".//ForeignAddress")
            or grant_elem.find(".//irs:RecipientForeignAddress", NS)
            or grant_elem.find(".//RecipientForeignAddress")
            or grant_elem.find(".//irs:AddressForeign", NS)
            or grant_elem.find(".//AddressForeign")
        )

        if foreign_addr is not None:
            # Extract country code
            country = self._get_text(
                foreign_addr,
                [
                    ".//irs:CountryCd",
                    ".//CountryCd",
                    ".//irs:Country",
                    ".//Country",
                ],
            )

            return Address(
                street=self._get_text(
                    foreign_addr,
                    [
                        ".//irs:AddressLine1Txt",
                        ".//AddressLine1Txt",
                        ".//irs:AddressLine1",
                        ".//AddressLine1",
                    ],
                ),
                city=self._get_text(
                    foreign_addr,
                    [
                        ".//irs:CityNm",
                        ".//CityNm",
                        ".//irs:City",
                        ".//City",
                    ],
                ),
                state=self._get_text(
                    foreign_addr,
                    [
                        ".//irs:ProvinceOrStateNm",
                        ".//ProvinceOrStateNm",
                        ".//irs:ProvinceOrState",
                        ".//ProvinceOrState",
                        ".//irs:StateProvinceOrCountry",
                        ".//StateProvinceOrCountry",
                    ],
                ),
                postal_code=self._get_text(
                    foreign_addr,
                    [
                        ".//irs:ForeignPostalCd",
                        ".//ForeignPostalCd",
                        ".//irs:PostalCode",
                        ".//PostalCode",
                    ],
                ),
                country=country or "XX",  # XX for unknown foreign
            )

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

            # Extract recipient address (US or foreign)
            recipient_address = self._extract_recipient_address(grant_elem)

            grant_data = {
                "recipient_name": recipient_name,
                "recipient_ein": recipient_ein,
                "amount": amount,
                "purpose": purpose,
            }

            # Add address fields if available
            if recipient_address:
                grant_data["recipient_address"] = recipient_address
                grant_data["recipient_city"] = recipient_address.city
                grant_data["recipient_state"] = recipient_address.state
                grant_data["recipient_postal"] = recipient_address.postal_code
                grant_data["recipient_country"] = recipient_address.country

            grants.append(grant_data)

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

    # ===========================================
    # Batch Processing Methods (Performance Optimized)
    # ===========================================

    def collect_record_data(self, record: IRS990Filing) -> dict[str, Any]:
        """Collect data from a record without writing to Neo4j.

        Extracts all entities and relationships into a structure suitable
        for batch processing with UNWIND queries.

        Args:
            record: Parsed IRS 990 filing

        Returns:
            Dictionary with:
            - org: Organization properties dict
            - officers: List of (person_props, rel_props, rel_type) tuples
            - grants: List of (recipient_props, rel_props) tuples
        """
        now = datetime.utcnow().isoformat()

        # Format EIN
        ein = record.ein
        if len(ein) == 9:
            ein = f"{ein[:2]}-{ein[2:]}"

        # Build organization properties
        org_props = {
            "id": str(uuid4()),
            "name": record.name,
            "ein": ein,
            "entity_type": "ORGANIZATION",
            "org_type": "nonprofit",
            "status": "active",
            "jurisdiction": record.state or "US",
            "confidence": 1.0,
            "updated_at": now,
            "created_at": now,
        }

        if record.formation_year:
            org_props["formation_year"] = record.formation_year

        if record.address:
            org_props["address_street"] = record.address.street
            org_props["address_city"] = record.address.city
            org_props["address_state"] = record.address.state
            org_props["address_postal"] = record.address.postal_code
            org_props["address_country"] = record.address.country

        # Collect officers
        officers_data = []
        for officer in record.officers:
            if not officer.get("name"):
                continue

            # Determine relationship type
            rel_type = "DIRECTOR_OF"
            if officer.get("title"):
                title_lower = officer["title"].lower()
                if not any(t in title_lower for t in ["director", "trustee", "board"]):
                    rel_type = "EMPLOYED_BY"

            person_props = {
                "id": str(uuid4()),
                "name": officer["name"],
                "entity_type": "PERSON",
                "confidence": 1.0,
                "created_at": now,
                "updated_at": now,
            }

            rel_props = {
                "title": officer.get("title"),
                "compensation": officer.get("compensation"),
                "hours_per_week": officer.get("hours_per_week"),
                "tax_year": record.tax_year,
                "confidence": 1.0,
                "updated_at": now,
            }

            officers_data.append({
                "person_props": person_props,
                "rel_props": rel_props,
                "rel_type": rel_type,
                "person_name": officer["name"],
                "org_ein": ein,
            })

        # Collect grants
        grants_data = []
        for grant in record.grants_made:
            if not grant.get("recipient_name"):
                continue

            recipient_ein = grant.get("recipient_ein")
            if recipient_ein and len(recipient_ein) == 9:
                recipient_ein = f"{recipient_ein[:2]}-{recipient_ein[2:]}"

            # Extract address info
            recipient_address = grant.get("recipient_address")
            recipient_city = grant.get("recipient_city")
            recipient_state = grant.get("recipient_state")
            recipient_postal = grant.get("recipient_postal")
            recipient_country = grant.get("recipient_country")
            recipient_street = recipient_address.street if recipient_address else None

            # Determine jurisdiction
            jurisdiction = "US"
            if recipient_country and recipient_country != "US":
                jurisdiction = recipient_country
                if recipient_state:
                    jurisdiction = f"{recipient_country}-{recipient_state}"

            # Determine merge strategy
            if recipient_ein:
                merge_strategy = "ein"
            elif recipient_country and recipient_country != "US":
                merge_strategy = "foreign"
            else:
                merge_strategy = "name"

            recipient_props = {
                "id": str(uuid4()),
                "name": grant["recipient_name"],
                "ein": recipient_ein,
                "entity_type": "ORGANIZATION",
                "org_type": "nonprofit" if recipient_ein else "unknown",
                "confidence": 0.8 if recipient_ein else 0.5,
                "jurisdiction": jurisdiction,
                "address_street": recipient_street,
                "address_city": recipient_city,
                "address_state": recipient_state,
                "address_postal": recipient_postal,
                "address_country": recipient_country,
                "created_at": now,
                "updated_at": now,
            }

            rel_props = {
                "amount": grant.get("amount"),
                "amount_currency": "USD",
                "fiscal_year": record.tax_year,
                "grant_purpose": grant.get("purpose"),
                "confidence": 1.0 if recipient_ein else 0.8,
                "updated_at": now,
            }

            grants_data.append({
                "recipient_props": recipient_props,
                "rel_props": rel_props,
                "merge_strategy": merge_strategy,
                "recipient_name": grant["recipient_name"],
                "recipient_ein": recipient_ein,
                "recipient_country": recipient_country,
                "funder_ein": ein,
            })

        return {
            "org": org_props,
            "officers": officers_data,
            "grants": grants_data,
        }

    async def _batch_upsert_organizations(
        self, session, orgs: list[dict[str, Any]]
    ) -> None:
        """Batch upsert filing organizations using UNWIND.

        Args:
            session: Neo4j async session
            orgs: List of organization property dicts with 'ein' key
        """
        if not orgs:
            return

        query = """
        UNWIND $orgs AS org
        MERGE (o:Organization {ein: org.ein})
        ON CREATE SET
            o.id = org.id,
            o.created_at = org.created_at
        SET o.name = org.name,
            o.entity_type = org.entity_type,
            o.org_type = org.org_type,
            o.status = org.status,
            o.jurisdiction = org.jurisdiction,
            o.confidence = org.confidence,
            o.updated_at = org.updated_at,
            o.formation_year = COALESCE(org.formation_year, o.formation_year),
            o.address_street = COALESCE(org.address_street, o.address_street),
            o.address_city = COALESCE(org.address_city, o.address_city),
            o.address_state = COALESCE(org.address_state, o.address_state),
            o.address_postal = COALESCE(org.address_postal, o.address_postal),
            o.address_country = COALESCE(org.address_country, o.address_country)
        """
        await session.run(query, orgs=orgs)

    async def _batch_upsert_persons(
        self, session, persons: list[dict[str, Any]]
    ) -> None:
        """Batch upsert persons using UNWIND.

        Args:
            session: Neo4j async session
            persons: List of person property dicts with 'name' key
        """
        if not persons:
            return

        # Deduplicate by name (same person may appear multiple times across filings)
        seen_names = set()
        unique_persons = []
        for p in persons:
            if p["name"] not in seen_names:
                seen_names.add(p["name"])
                unique_persons.append(p)

        query = """
        UNWIND $persons AS person
        MERGE (p:Person {name: person.name, irs_990_name: person.name})
        ON CREATE SET
            p.id = person.id,
            p.entity_type = person.entity_type,
            p.confidence = person.confidence,
            p.created_at = person.created_at
        SET p.updated_at = person.updated_at
        """
        await session.run(query, persons=unique_persons)

    async def _batch_create_officer_relationships(
        self, session, rels: list[dict[str, Any]]
    ) -> None:
        """Batch create officer relationships (DIRECTOR_OF/EMPLOYED_BY).

        Args:
            session: Neo4j async session
            rels: List of relationship dicts with person_name, org_ein, rel_type, rel_props
        """
        if not rels:
            return

        # Split by relationship type since we can't parameterize relationship types in Cypher
        director_rels = [r for r in rels if r["rel_type"] == "DIRECTOR_OF"]
        employee_rels = [r for r in rels if r["rel_type"] == "EMPLOYED_BY"]

        if director_rels:
            query = """
            UNWIND $rels AS rel
            MATCH (p:Person {name: rel.person_name, irs_990_name: rel.person_name})
            MATCH (o:Organization {ein: rel.org_ein})
            MERGE (p)-[r:DIRECTOR_OF]->(o)
            SET r.title = rel.rel_props.title,
                r.compensation = rel.rel_props.compensation,
                r.hours_per_week = rel.rel_props.hours_per_week,
                r.tax_year = rel.rel_props.tax_year,
                r.confidence = rel.rel_props.confidence,
                r.updated_at = rel.rel_props.updated_at
            """
            await session.run(query, rels=director_rels)

        if employee_rels:
            query = """
            UNWIND $rels AS rel
            MATCH (p:Person {name: rel.person_name, irs_990_name: rel.person_name})
            MATCH (o:Organization {ein: rel.org_ein})
            MERGE (p)-[r:EMPLOYED_BY]->(o)
            SET r.title = rel.rel_props.title,
                r.compensation = rel.rel_props.compensation,
                r.hours_per_week = rel.rel_props.hours_per_week,
                r.tax_year = rel.rel_props.tax_year,
                r.confidence = rel.rel_props.confidence,
                r.updated_at = rel.rel_props.updated_at
            """
            await session.run(query, rels=employee_rels)

    async def _batch_upsert_grant_recipients(
        self, session, recipients: list[dict[str, Any]]
    ) -> None:
        """Batch upsert grant recipient organizations.

        Handles three merge strategies:
        - ein: Match by EIN (most reliable)
        - foreign: Match by name + country (for foreign recipients without EIN)
        - name: Match by name only (least reliable, US recipients without EIN)

        Args:
            session: Neo4j async session
            recipients: List of grant dicts with recipient_props and merge_strategy
        """
        if not recipients:
            return

        # Split by merge strategy
        with_ein = [r for r in recipients if r["merge_strategy"] == "ein"]
        foreign_no_ein = [r for r in recipients if r["merge_strategy"] == "foreign"]
        us_no_ein = [r for r in recipients if r["merge_strategy"] == "name"]

        if with_ein:
            query = """
            UNWIND $recipients AS r
            MERGE (o:Organization {ein: r.recipient_ein})
            ON CREATE SET
                o.id = r.recipient_props.id,
                o.name = r.recipient_props.name,
                o.entity_type = r.recipient_props.entity_type,
                o.org_type = r.recipient_props.org_type,
                o.confidence = r.recipient_props.confidence,
                o.jurisdiction = r.recipient_props.jurisdiction,
                o.created_at = r.recipient_props.created_at
            SET o.updated_at = r.recipient_props.updated_at,
                o.address_street = COALESCE(r.recipient_props.address_street, o.address_street),
                o.address_city = COALESCE(r.recipient_props.address_city, o.address_city),
                o.address_state = COALESCE(r.recipient_props.address_state, o.address_state),
                o.address_postal = COALESCE(r.recipient_props.address_postal, o.address_postal),
                o.address_country = COALESCE(r.recipient_props.address_country, o.address_country)
            """
            await session.run(query, recipients=with_ein)

        if foreign_no_ein:
            query = """
            UNWIND $recipients AS r
            MERGE (o:Organization {name: r.recipient_name, address_country: r.recipient_country})
            ON CREATE SET
                o.id = r.recipient_props.id,
                o.entity_type = r.recipient_props.entity_type,
                o.org_type = r.recipient_props.org_type,
                o.confidence = r.recipient_props.confidence,
                o.jurisdiction = r.recipient_props.jurisdiction,
                o.created_at = r.recipient_props.created_at
            SET o.updated_at = r.recipient_props.updated_at,
                o.address_street = COALESCE(r.recipient_props.address_street, o.address_street),
                o.address_city = COALESCE(r.recipient_props.address_city, o.address_city),
                o.address_state = COALESCE(r.recipient_props.address_state, o.address_state),
                o.address_postal = COALESCE(r.recipient_props.address_postal, o.address_postal)
            """
            await session.run(query, recipients=foreign_no_ein)

        if us_no_ein:
            query = """
            UNWIND $recipients AS r
            MERGE (o:Organization {name: r.recipient_name})
            ON CREATE SET
                o.id = r.recipient_props.id,
                o.entity_type = r.recipient_props.entity_type,
                o.org_type = r.recipient_props.org_type,
                o.confidence = r.recipient_props.confidence,
                o.jurisdiction = r.recipient_props.jurisdiction,
                o.created_at = r.recipient_props.created_at
            SET o.updated_at = r.recipient_props.updated_at,
                o.address_street = COALESCE(r.recipient_props.address_street, o.address_street),
                o.address_city = COALESCE(r.recipient_props.address_city, o.address_city),
                o.address_state = COALESCE(r.recipient_props.address_state, o.address_state),
                o.address_postal = COALESCE(r.recipient_props.address_postal, o.address_postal),
                o.address_country = COALESCE(r.recipient_props.address_country, o.address_country)
            """
            await session.run(query, recipients=us_no_ein)

    async def _batch_create_funded_by_relationships(
        self, session, rels: list[dict[str, Any]]
    ) -> None:
        """Batch create FUNDED_BY relationships.

        Handles two matching strategies:
        - With recipient EIN: Match recipient by EIN
        - Without recipient EIN: Match recipient by name

        Args:
            session: Neo4j async session
            rels: List of grant dicts with rel_props and matching info
        """
        if not rels:
            return

        # Split by matching strategy
        with_ein = [r for r in rels if r.get("recipient_ein")]
        without_ein = [r for r in rels if not r.get("recipient_ein")]

        if with_ein:
            query = """
            UNWIND $rels AS rel
            MATCH (recipient:Organization {ein: rel.recipient_ein})
            MATCH (funder:Organization {ein: rel.funder_ein})
            MERGE (recipient)-[r:FUNDED_BY]->(funder)
            SET r.amount = rel.rel_props.amount,
                r.amount_currency = rel.rel_props.amount_currency,
                r.fiscal_year = rel.rel_props.fiscal_year,
                r.grant_purpose = rel.rel_props.grant_purpose,
                r.confidence = rel.rel_props.confidence,
                r.updated_at = rel.rel_props.updated_at
            """
            await session.run(query, rels=with_ein)

        if without_ein:
            query = """
            UNWIND $rels AS rel
            MATCH (recipient:Organization {name: rel.recipient_name})
            MATCH (funder:Organization {ein: rel.funder_ein})
            MERGE (recipient)-[r:FUNDED_BY]->(funder)
            SET r.amount = rel.rel_props.amount,
                r.amount_currency = rel.rel_props.amount_currency,
                r.fiscal_year = rel.rel_props.fiscal_year,
                r.grant_purpose = rel.rel_props.grant_purpose,
                r.confidence = rel.rel_props.confidence,
                r.updated_at = rel.rel_props.updated_at
            """
            await session.run(query, rels=without_ein)

    async def _flush_batch(
        self,
        session,
        batch_data: dict[str, list],
        timing: bool = False,
    ) -> dict[str, Any]:
        """Write a batch of collected data to Neo4j.

        Executes 5-8 UNWIND queries instead of N*60+ individual queries.

        Args:
            session: Neo4j async session
            batch_data: Accumulated batch data with orgs, persons, officer_rels,
                       recipients, grant_rels lists
            timing: If True, include timing breakdown in stats

        Returns:
            Stats dict with counts of operations performed (and timing if enabled)
        """
        import time

        stats = {
            "orgs": len(batch_data["orgs"]),
            "persons": len(batch_data["persons"]),
            "officer_rels": len(batch_data["officer_rels"]),
            "recipients": len(batch_data["recipients"]),
            "grant_rels": len(batch_data["grant_rels"]),
        }

        timings = {} if timing else None

        # Execute batch operations in dependency order
        # 1. Create filing organizations first (they're referenced by relationships)
        t0 = time.perf_counter()
        await self._batch_upsert_organizations(session, batch_data["orgs"])
        if timing:
            timings["orgs_ms"] = (time.perf_counter() - t0) * 1000

        # 2. Create persons (they're referenced by officer relationships)
        t0 = time.perf_counter()
        await self._batch_upsert_persons(session, batch_data["persons"])
        if timing:
            timings["persons_ms"] = (time.perf_counter() - t0) * 1000

        # 3. Create officer relationships (now both nodes exist)
        t0 = time.perf_counter()
        await self._batch_create_officer_relationships(session, batch_data["officer_rels"])
        if timing:
            timings["officer_rels_ms"] = (time.perf_counter() - t0) * 1000

        # 4. Create grant recipient organizations
        t0 = time.perf_counter()
        await self._batch_upsert_grant_recipients(session, batch_data["recipients"])
        if timing:
            timings["recipients_ms"] = (time.perf_counter() - t0) * 1000

        # 5. Create FUNDED_BY relationships (now both funder and recipient exist)
        t0 = time.perf_counter()
        await self._batch_create_funded_by_relationships(session, batch_data["grant_rels"])
        if timing:
            timings["grant_rels_ms"] = (time.perf_counter() - t0) * 1000

        if timing:
            stats["timing"] = timings
            stats["total_neo4j_ms"] = sum(timings.values())

        return stats

    async def run_batched(
        self,
        config: IngestionConfig | None = None,
        run_id: UUID | None = None,
        batch_size: int = 100,
    ) -> "IngestionResult":
        """Run ingestion with batch processing for improved performance.

        Instead of executing 12-62 Neo4j queries per record, this method:
        1. Collects data from multiple records
        2. Writes in batches using UNWIND queries (5-8 queries per batch)
        3. Reuses a single Neo4j session for the entire batch

        This can improve throughput from ~1.6s/record to ~50-100ms/record.

        Args:
            config: Ingestion configuration
            run_id: Optional run ID from API layer
            batch_size: Number of records to accumulate before flushing (default: 100)

        Returns:
            Ingestion result with statistics
        """
        import logging
        import sys
        from tqdm import tqdm
        from .base import IngestionResult, suppress_db_logging
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

        # Initialize batch accumulators
        batch_data = {
            "orgs": [],
            "persons": [],
            "officer_rels": [],
            "recipients": [],
            "grant_rels": [],
        }
        batch_record_count = 0

        # Timing accumulators
        import time
        timing_stats = {
            "parse_ms": 0.0,
            "collect_ms": 0.0,
            "neo4j_ms": 0.0,
            "batches_flushed": 0,
        }
        last_timing_report = time.perf_counter()

        try:
            self.logger.info(
                f"Starting batched ingestion (batch_size={batch_size})",
                extra={"config": config.model_dump(), "run_id": run_id_str},
            )

            # If incremental, get last sync time
            if config.incremental:
                last_sync = await self.get_last_sync_time()
                if last_sync:
                    config.date_from = last_sync
                    self.logger.info(f"Incremental sync from {last_sync.isoformat()}")

            total = config.limit if config.limit else None
            desc = f"Ingesting {self.source_name} (batched)"

            with suppress_db_logging():
                pbar = tqdm(
                    total=total,
                    desc=desc,
                    unit="rec",
                    dynamic_ncols=True,
                    file=sys.stderr,
                )

                try:
                    # Use a single Neo4j session for all batches
                    async with get_neo4j_session() as session:
                        async for record in self.fetch_records(config):
                            try:
                                # Collect data from record (no DB writes)
                                t0 = time.perf_counter()
                                data = self.collect_record_data(record)
                                timing_stats["collect_ms"] += (time.perf_counter() - t0) * 1000

                                # Accumulate into batch
                                batch_data["orgs"].append(data["org"])
                                for officer in data["officers"]:
                                    batch_data["persons"].append(officer["person_props"])
                                    batch_data["officer_rels"].append(officer)
                                for grant in data["grants"]:
                                    batch_data["recipients"].append(grant)
                                    batch_data["grant_rels"].append(grant)

                                batch_record_count += 1
                                result.records_processed += 1

                                # Flush batch when full
                                if batch_record_count >= batch_size:
                                    t0 = time.perf_counter()
                                    flush_stats = await self._flush_batch(session, batch_data, timing=True)
                                    neo4j_time = (time.perf_counter() - t0) * 1000
                                    timing_stats["neo4j_ms"] += neo4j_time
                                    timing_stats["batches_flushed"] += 1

                                    result.records_created += batch_record_count
                                    batch_record_count = 0
                                    batch_data = {
                                        "orgs": [],
                                        "persons": [],
                                        "officer_rels": [],
                                        "recipients": [],
                                        "grant_rels": [],
                                    }

                                    # Print timing report every 5 batches
                                    if timing_stats["batches_flushed"] % 5 == 0:
                                        now = time.perf_counter()
                                        elapsed = now - last_timing_report
                                        last_timing_report = now

                                        print(
                                            f"\n  [Timing] Last 5 batches ({batch_size * 5} records):\n"
                                            f"    Collect data: {timing_stats['collect_ms']:.0f}ms\n"
                                            f"    Neo4j writes: {timing_stats['neo4j_ms']:.0f}ms\n"
                                            f"    Last batch breakdown: orgs={flush_stats['timing']['orgs_ms']:.0f}ms, "
                                            f"persons={flush_stats['timing']['persons_ms']:.0f}ms, "
                                            f"officer_rels={flush_stats['timing']['officer_rels_ms']:.0f}ms, "
                                            f"recipients={flush_stats['timing']['recipients_ms']:.0f}ms, "
                                            f"grant_rels={flush_stats['timing']['grant_rels_ms']:.0f}ms\n"
                                            f"    Wall time: {elapsed:.1f}s ({batch_size * 5 / elapsed:.1f} rec/s)",
                                            file=sys.stderr,
                                        )
                                        # Reset timing accumulators
                                        timing_stats["collect_ms"] = 0.0
                                        timing_stats["neo4j_ms"] = 0.0

                                # Update progress bar
                                pbar.set_postfix(
                                    created=result.records_created,
                                    batched=batch_record_count,
                                    err=len(result.errors),
                                    refresh=False,
                                )
                                pbar.update(1)

                                # Check limit
                                if config.limit and result.records_processed >= config.limit:
                                    pbar.set_description(f"{desc} (limit reached)")
                                    break

                            except Exception as e:
                                result.errors.append({
                                    "record_id": getattr(record, "object_id", None),
                                    "error": str(e),
                                    "error_type": type(e).__name__,
                                })
                                pbar.set_postfix(
                                    created=result.records_created,
                                    batched=batch_record_count,
                                    err=len(result.errors),
                                    refresh=False,
                                )
                                pbar.update(1)
                                continue

                        # Flush remaining batch
                        if batch_record_count > 0:
                            await self._flush_batch(session, batch_data)
                            result.records_created += batch_record_count

                finally:
                    pbar.close()

            # Print summary
            print(
                f"\n{self.source_name} batched ingestion complete:\n"
                f"  Processed: {result.records_processed}\n"
                f"  Created:   {result.records_created}\n"
                f"  Errors:    {len(result.errors)}",
                file=sys.stderr,
            )

            self.logger.info(
                f"Batched ingestion complete: {result.records_processed} processed, "
                f"{result.records_created} created, {len(result.errors)} errors"
            )

            result.status = "completed" if not result.errors else "partial"
            result.completed_at = datetime.utcnow()

            if result.status in ("completed", "partial"):
                await self.save_sync_time(result.started_at)

        except Exception as e:
            result.status = "failed"
            result.completed_at = datetime.utcnow()
            result.errors.append({
                "error": str(e),
                "error_type": type(e).__name__,
                "fatal": True,
            })
            self.logger.exception("Batched ingestion failed")

        finally:
            source_logger.removeHandler(handler)
            result.log_output = finish_capture(run_id_str)

        return result

    # ===========================================
    # Original Per-Record Processing (Fallback)
    # ===========================================

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
                org_props["address_country"] = record.address.country

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

                # Extract address info from grant
                recipient_city = grant.get("recipient_city")
                recipient_state = grant.get("recipient_state")
                recipient_postal = grant.get("recipient_postal")
                recipient_country = grant.get("recipient_country")
                recipient_address = grant.get("recipient_address")
                recipient_street = recipient_address.street if recipient_address else None

                # Determine jurisdiction based on country
                jurisdiction = "US"
                if recipient_country and recipient_country != "US":
                    jurisdiction = recipient_country
                    if recipient_state:
                        jurisdiction = f"{recipient_country}-{recipient_state}"

                # Create or find recipient organization with address
                if recipient_ein:
                    recipient_query = """
                    MERGE (r:Organization {ein: $ein})
                    ON CREATE SET
                        r.id = $id,
                        r.name = $name,
                        r.entity_type = 'ORGANIZATION',
                        r.org_type = 'nonprofit',
                        r.confidence = 0.8,
                        r.jurisdiction = $jurisdiction,
                        r.created_at = $now
                    SET r.updated_at = $now,
                        r.address_street = COALESCE($street, r.address_street),
                        r.address_city = COALESCE($city, r.address_city),
                        r.address_state = COALESCE($state, r.address_state),
                        r.address_postal = COALESCE($postal, r.address_postal),
                        r.address_country = COALESCE($country, r.address_country)
                    RETURN r.id as id
                    """
                else:
                    # For recipients without EIN, use name + country for matching
                    # This is especially important for foreign recipients
                    if recipient_country and recipient_country != "US":
                        recipient_query = """
                        MERGE (r:Organization {name: $name, address_country: $country})
                        ON CREATE SET
                            r.id = $id,
                            r.entity_type = 'ORGANIZATION',
                            r.org_type = 'unknown',
                            r.confidence = 0.5,
                            r.jurisdiction = $jurisdiction,
                            r.created_at = $now
                        SET r.updated_at = $now,
                            r.address_street = COALESCE($street, r.address_street),
                            r.address_city = COALESCE($city, r.address_city),
                            r.address_state = COALESCE($state, r.address_state),
                            r.address_postal = COALESCE($postal, r.address_postal)
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
                            r.jurisdiction = $jurisdiction,
                            r.created_at = $now
                        SET r.updated_at = $now,
                            r.address_street = COALESCE($street, r.address_street),
                            r.address_city = COALESCE($city, r.address_city),
                            r.address_state = COALESCE($state, r.address_state),
                            r.address_postal = COALESCE($postal, r.address_postal),
                            r.address_country = COALESCE($country, r.address_country)
                        RETURN r.id as id
                        """

                await session.run(
                    recipient_query,
                    ein=recipient_ein,
                    id=str(uuid4()),
                    name=grant["recipient_name"],
                    jurisdiction=jurisdiction,
                    street=recipient_street,
                    city=recipient_city,
                    state=recipient_state,
                    postal=recipient_postal,
                    country=recipient_country,
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
    run_id: UUID | None = None,
    batched: bool = True,
    batch_size: int = 100,
    skip: int = 0,
    workers: int | None = None,
) -> dict[str, Any]:
    """Run IRS 990 ingestion directly (not via Celery).

    Args:
        start_year: Start year (default: previous year)
        end_year: End year (default: current year)
        incremental: Whether to do incremental sync
        limit: Maximum number of records to process
        target_entities: Optional list of EINs to ingest specifically
        run_id: Optional run ID from API layer
        batched: Use batched processing for better performance (default: True)
        batch_size: Number of records per batch when batched (default: 100)
        skip: Number of records to skip (for resuming from a specific point)
        workers: Number of parallel XML parsing workers (default: auto)

    Returns:
        Ingestion result dictionary
    """
    import os
    current_year = datetime.now().year

    # Determine number of workers
    num_workers = workers or min(os.cpu_count() or 4, 8)
    parse_batch_size = num_workers * 4  # Parse batch size based on workers

    ingester = IRS990Ingester()
    try:
        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
            extra_params={
                "start_year": start_year or current_year - 1,
                "end_year": end_year or current_year,
                "skip": skip,
                "num_workers": num_workers,
                "parse_batch_size": parse_batch_size,
            },
        )

        if batched:
            # Use optimized batch processing (5-8 queries per batch instead of 60+ per record)
            result = await ingester.run_batched(config, run_id=run_id, batch_size=batch_size)
        else:
            # Use original sequential processing (for debugging/fallback)
            result = await ingester.run(config, run_id=run_id)

        return result.model_dump()
    finally:
        await ingester.close()
