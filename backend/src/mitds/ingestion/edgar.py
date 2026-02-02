"""SEC EDGAR company filings ingester.

Ingests data from the SEC EDGAR API (data.sec.gov).
Key data points:
- Company information (CIK, name, SIC code, addresses)
- Filing history (10-K, 10-Q, 8-K, etc.)
- Officers and directors (from DEF 14A proxy statements)
- Beneficial ownership (Schedule 13D/G, Form 4)

Data source: https://data.sec.gov/
Coverage: US public companies, investment funds, ~10K+ filers
Free API, no key required (User-Agent header required)

API Documentation: https://www.sec.gov/edgar/sec-api-documentation
"""

import asyncio
import json
import re
from datetime import datetime, date
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
from .base import BaseIngester, IngestionConfig, IngestionResult, SingleIngestionResult, with_retry

logger = get_context_logger(__name__)

# SEC EDGAR API base URL
EDGAR_BASE_URL = "https://data.sec.gov"
EDGAR_SUBMISSIONS_URL = f"{EDGAR_BASE_URL}/submissions"
# Company tickers are served from www.sec.gov, not data.sec.gov
EDGAR_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# Required User-Agent header for SEC API
# Format: Sample Company Name AdminContact@<sample company domain>.com
USER_AGENT = "MITDS Research contact@mitds.org"

# Canadian jurisdiction codes used in SEC EDGAR state_of_incorporation field
# Maps SEC codes to province/territory names for detection of Canadian companies
# NOTE: "CA" is NOT included because SEC EDGAR uses "CA" for California (US state)
# Canadian companies use A0-A9 (provinces) or B0-B2 (territories) codes
CANADIAN_JURISDICTIONS: dict[str, str] = {
    # Provinces
    "A0": "Alberta",
    "A1": "British Columbia",
    "A2": "Manitoba",
    "A3": "New Brunswick",
    "A4": "Newfoundland and Labrador",
    "A5": "Nova Scotia",
    "A6": "Ontario",
    "A7": "Prince Edward Island",
    "A8": "Quebec",
    "A9": "Saskatchewan",
    # Territories
    "B0": "Northwest Territories",
    "B1": "Nunavut",
    "B2": "Yukon",
    # Generic Canada designation (rare, used when province not specified)
    "CANADA": "Canada (unspecified province)",
}


def is_canadian_jurisdiction(state_of_inc: str | None) -> bool:
    """Check if a state of incorporation code indicates a Canadian entity.

    Args:
        state_of_inc: The state/province of incorporation code from SEC EDGAR

    Returns:
        True if the code indicates a Canadian jurisdiction
    """
    if not state_of_inc:
        return False
    return state_of_inc.upper() in CANADIAN_JURISDICTIONS


class EDGARCompany(BaseModel):
    """Parsed SEC EDGAR company record."""

    cik: str = Field(..., description="Central Index Key (10-digit)")
    name: str = Field(..., description="Company name")
    sic: str | None = Field(None, description="Standard Industrial Classification code")
    sic_description: str | None = Field(None, description="SIC code description")
    ein: str | None = Field(None, description="Employer Identification Number")

    # State/Country
    state_of_incorporation: str | None = None
    fiscal_year_end: str | None = None

    # Addresses
    business_address: Address | None = None
    mailing_address: Address | None = None

    # Exchange info
    exchanges: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)

    # Filing metadata
    filings_count: int = 0
    latest_filing_date: date | None = None

    # Canadian jurisdiction detection
    is_canadian: bool = Field(default=False, description="True if company is incorporated in Canada")

    # Raw submissions data for downstream 13D/13G parsing (transient, not serialized)
    raw_submissions: dict[str, Any] | None = Field(
        default=None, exclude=True, description="Raw API response for filing extraction"
    )


class EDGARFiling(BaseModel):
    """SEC EDGAR filing record."""

    accession_number: str = Field(..., description="Unique filing identifier")
    form_type: str = Field(..., description="Filing form type (10-K, 10-Q, etc.)")
    filing_date: date
    report_date: date | None = None

    # Document URLs
    primary_document: str | None = None
    primary_document_description: str | None = None

    # Size info
    size: int = 0

    # Items reported (for 8-K)
    items: list[str] = Field(default_factory=list)


class EDGARInsider(BaseModel):
    """Insider/officer from proxy or ownership filings."""

    name: str
    title: str | None = None
    is_director: bool = False
    is_officer: bool = False
    is_ten_percent_owner: bool = False

    # Ownership info
    shares_owned: int | None = None
    ownership_type: str | None = None  # direct, indirect


class EDGAROwnershipFiling(BaseModel):
    """Parsed SC 13D/13G beneficial ownership filing."""

    filer_cik: str = Field(..., description="CIK of the beneficial owner (filer)")
    filer_name: str = Field(..., description="Name of the beneficial owner")
    subject_cik: str = Field(..., description="CIK of the company whose shares are owned")
    subject_name: str = Field(..., description="Name of the subject company")

    # Canadian jurisdiction detection for subject company
    subject_is_canadian: bool = Field(default=False, description="True if subject company is Canadian")
    subject_jurisdiction: str | None = Field(default=None, description="Jurisdiction code of subject company")

    # Filing metadata
    accession_number: str
    form_type: str  # SC 13D, SC 13G, SC 13D/A, SC 13G/A
    filing_date: date

    # Ownership details (if parseable from filing)
    ownership_percentage: float | None = None
    shares_owned: int | None = None
    share_class: str | None = None


class EDGARForm4Filing(BaseModel):
    """Parsed SEC Form 4 insider transaction filing."""

    owner_cik: str = Field(..., description="CIK of the reporting owner (insider)")
    owner_name: str = Field(..., description="Name of the reporting owner")
    issuer_cik: str = Field(..., description="CIK of the issuing company")
    issuer_name: str = Field(..., description="Name of the issuing company")

    is_director: bool = False
    is_officer: bool = False
    is_ten_percent_owner: bool = False
    officer_title: str | None = None

    accession_number: str
    filing_date: date


class SECEDGARIngester(BaseIngester[EDGARCompany]):
    """Ingester for SEC EDGAR company and filing data.

    Uses the free SEC EDGAR API which provides:
    - Company submissions (filing history)
    - Company facts (XBRL financial data)
    - Company tickers mapping

    No API key required, but User-Agent header is mandatory.
    """

    def __init__(self):
        """Initialize the SEC EDGAR ingester."""
        super().__init__(source_name="sec_edgar")
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client with required headers."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
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
        pass  # Managed by IngestionResult

    async def ingest_single(
        self,
        identifier: str,
        identifier_type: str,
    ) -> SingleIngestionResult | None:
        """Ingest a single company from SEC EDGAR by CIK or name.

        Args:
            identifier: The CIK, EIN, or company name
            identifier_type: One of "cik", "ein", "name"

        Returns:
            SingleIngestionResult if found and processed, None otherwise
        """
        cik_to_fetch: str | None = None

        if identifier_type == "cik":
            cik_to_fetch = identifier.zfill(10)

        elif identifier_type == "ein":
            # Need to search for EIN - SEC doesn't have a direct EIN lookup
            # We'd need to query existing tickers and cross-reference
            # For now, return None as EIN lookup is not directly supported
            self.logger.info(f"EIN lookup not directly supported by SEC EDGAR: {identifier}")
            return None

        elif identifier_type == "name":
            # Search company tickers for name match
            try:
                tickers_map = await self.fetch_company_tickers()
                identifier_lower = identifier.lower()

                # Look for exact or close name match
                for cik, info in tickers_map.items():
                    if info.get("name", "").lower() == identifier_lower:
                        cik_to_fetch = cik
                        break

                # If no exact match, try partial match
                if not cik_to_fetch:
                    for cik, info in tickers_map.items():
                        if identifier_lower in info.get("name", "").lower():
                            cik_to_fetch = cik
                            break

            except Exception as e:
                self.logger.warning(f"Error searching for company by name: {e}")
                return None

        if not cik_to_fetch:
            self.logger.info(f"Company not found in SEC EDGAR: {identifier}")
            return None

        # Fetch and process the company
        try:
            data = await self.fetch_company_submissions(cik_to_fetch)
            if not data:
                return None

            company = self.parse_company(data)
            company.raw_submissions = data

            # Process the record
            result = await self.process_record(company)

            return SingleIngestionResult(
                entity_id=UUID(result["entity_id"]) if result.get("entity_id") else None,
                entity_name=company.name,
                entity_type="organization",
                is_new=result.get("created", False),
                relationships_created=0,  # Could count from Neo4j ops if needed
                source="sec_edgar",
            )

        except Exception as e:
            self.logger.error(f"Error ingesting company {identifier}: {e}")
            return SingleIngestionResult(
                source="sec_edgar",
                error=str(e),
            )

    async def fetch_company_tickers(self) -> dict[str, dict[str, Any]]:
        """Fetch mapping of tickers to CIKs.

        Returns dict mapping CIK (str) to company info.
        """
        async def _fetch():
            response = await self.http_client.get(EDGAR_COMPANY_TICKERS_URL)
            response.raise_for_status()
            return response.json()

        data = await with_retry(_fetch, logger=self.logger)

        # Convert to CIK-keyed dict
        # Format: {"0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc."}, ...}
        result = {}
        for item in data.values():
            cik = str(item["cik_str"]).zfill(10)
            if cik not in result:
                result[cik] = {
                    "cik": cik,
                    "tickers": [],
                    "name": item["title"],
                }
            result[cik]["tickers"].append(item["ticker"])

        return result

    async def fetch_company_submissions(self, cik: str) -> dict[str, Any] | None:
        """Fetch submission history for a company by CIK.

        Args:
            cik: 10-digit CIK (zero-padded)

        Returns:
            Company submissions data or None if not found
        """
        cik_padded = cik.zfill(10)
        url = f"{EDGAR_SUBMISSIONS_URL}/CIK{cik_padded}.json"

        async def _fetch():
            response = await self.http_client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()

        return await with_retry(_fetch, logger=self.logger)

    def parse_company(self, data: dict[str, Any], tickers: list[str] | None = None) -> EDGARCompany:
        """Parse company data from submissions API response."""
        # Parse addresses
        business_addr = None
        mailing_addr = None

        if addresses := data.get("addresses", {}):
            if bus := addresses.get("business"):
                # Combine street1 and street2 into single street field
                street_parts = [bus.get("street1"), bus.get("street2")]
                street = ", ".join(p for p in street_parts if p)
                state_or_country = bus.get("stateOrCountry", "")
                business_addr = Address(
                    street=street or None,
                    city=bus.get("city"),
                    state=state_or_country if len(state_or_country) == 2 else None,
                    postal_code=bus.get("zipCode"),
                    country="US" if len(state_or_country) == 2 else state_or_country or "US",
                )
            if mail := addresses.get("mailing"):
                # Combine street1 and street2 into single street field
                street_parts = [mail.get("street1"), mail.get("street2")]
                street = ", ".join(p for p in street_parts if p)
                state_or_country = mail.get("stateOrCountry", "")
                mailing_addr = Address(
                    street=street or None,
                    city=mail.get("city"),
                    state=state_or_country if len(state_or_country) == 2 else None,
                    postal_code=mail.get("zipCode"),
                    country="US" if len(state_or_country) == 2 else state_or_country or "US",
                )

        # Count filings and get latest date
        filings = data.get("filings", {}).get("recent", {})
        filing_dates = filings.get("filingDate", [])
        filings_count = len(filing_dates)
        latest_filing = None
        if filing_dates:
            try:
                latest_filing = datetime.strptime(filing_dates[0], "%Y-%m-%d").date()
            except (ValueError, IndexError):
                pass

        # Detect Canadian jurisdiction
        state_of_inc = data.get("stateOfIncorporation")
        company_is_canadian = is_canadian_jurisdiction(state_of_inc)

        return EDGARCompany(
            cik=str(data.get("cik", "")).zfill(10),
            name=data.get("name", "Unknown"),
            sic=data.get("sic"),
            sic_description=data.get("sicDescription"),
            ein=data.get("ein"),
            state_of_incorporation=state_of_inc,
            fiscal_year_end=data.get("fiscalYearEnd"),
            business_address=business_addr,
            mailing_address=mailing_addr,
            exchanges=data.get("exchanges", []),
            tickers=tickers or data.get("tickers", []),
            filings_count=filings_count,
            latest_filing_date=latest_filing,
            is_canadian=company_is_canadian,
        )

    def parse_filings(self, data: dict[str, Any], limit: int | None = None) -> list[EDGARFiling]:
        """Parse recent filings from submissions API response."""
        filings = []
        recent = data.get("filings", {}).get("recent", {})

        accession_numbers = recent.get("accessionNumber", [])
        form_types = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])
        primary_descs = recent.get("primaryDocDescription", [])
        sizes = recent.get("size", [])
        items_list = recent.get("items", [])

        count = min(len(accession_numbers), limit) if limit else len(accession_numbers)

        for i in range(count):
            try:
                filing_date = datetime.strptime(filing_dates[i], "%Y-%m-%d").date()
                report_date = None
                if i < len(report_dates) and report_dates[i]:
                    try:
                        report_date = datetime.strptime(report_dates[i], "%Y-%m-%d").date()
                    except ValueError:
                        pass

                items = []
                if i < len(items_list) and items_list[i]:
                    items = items_list[i].split(",") if isinstance(items_list[i], str) else []

                filings.append(EDGARFiling(
                    accession_number=accession_numbers[i],
                    form_type=form_types[i] if i < len(form_types) else "Unknown",
                    filing_date=filing_date,
                    report_date=report_date,
                    primary_document=primary_docs[i] if i < len(primary_docs) else None,
                    primary_document_description=primary_descs[i] if i < len(primary_descs) else None,
                    size=sizes[i] if i < len(sizes) else 0,
                    items=items,
                ))
            except (ValueError, IndexError) as e:
                self.logger.warning(f"Error parsing filing {i}: {e}")
                continue

        return filings

    # =========================================
    # 13D/13G Ownership Filing Parsing
    # =========================================

    def extract_ownership_filings(
        self, submissions_data: dict[str, Any]
    ) -> list[EDGARFiling]:
        """Extract SC 13D/13G filings from submissions data.

        Args:
            submissions_data: Raw submissions API JSON

        Returns:
            List of EDGARFiling objects for 13D/13G forms only
        """
        all_filings = self.parse_filings(submissions_data)
        ownership_forms = {"SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"}
        return [f for f in all_filings if f.form_type in ownership_forms]

    async def fetch_filing_index(self, cik: str, accession_number: str) -> str | None:
        """Fetch the filing index page to extract subject company info.

        Args:
            cik: Filer's CIK (zero-padded)
            accession_number: Filing accession number (with dashes)

        Returns:
            HTML content of the filing index page, or None
        """
        acc_no_dashes = accession_number.replace("-", "")
        cik_stripped = cik.lstrip("0") or "0"
        url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik_stripped}/{acc_no_dashes}/{accession_number}-index.htm"
        )

        async def _fetch():
            response = await self.http_client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.text

        return await with_retry(_fetch, logger=self.logger)

    async def parse_ownership_from_index(
        self,
        filing: EDGARFiling,
        company_cik: str,
        company_name: str,
        flag_canadian: bool = True,
    ) -> EDGAROwnershipFiling | None:
        """Parse ownership info from a 13D/13G filing index page.

        The filing index page contains two entities:
        - "(Subject)" - the company whose shares are owned
        - "(Filed by)" - the entity that owns the shares

        If the company we're processing is the Subject, the filer OWNS this company.
        If the company we're processing is the Filed by, this company OWNS the subject.

        Args:
            filing: The ownership filing to parse
            company_cik: CIK of the company we're processing
            company_name: Name of the company we're processing
            flag_canadian: Whether to lookup and flag Canadian subject companies

        Returns:
            Parsed ownership filing or None if parsing fails
        """
        index_html = await self.fetch_filing_index(company_cik, filing.accession_number)
        if not index_html:
            return None

        subject = self._extract_company_by_role(index_html, "Subject")
        filer = self._extract_company_by_role(index_html, "Filed by")

        if not subject or not subject.get("cik"):
            self.logger.debug(
                f"Could not extract subject from filing {filing.accession_number}"
            )
            return None

        # Determine filer and subject based on which role our company plays
        subject_cik = subject["cik"]
        subject_name = subject.get("name", "Unknown")
        filer_cik = filer["cik"] if filer and filer.get("cik") else company_cik
        filer_name = filer.get("name", company_name) if filer else company_name

        # Skip self-referencing (filing about own company with no separate filer)
        if subject_cik == filer_cik:
            return None

        # Detect Canadian jurisdiction for subject company
        subject_is_canadian = False
        subject_jurisdiction = None
        if flag_canadian:
            try:
                subject_data = await self.fetch_company_submissions(subject_cik.zfill(10))
                if subject_data:
                    state_of_inc = subject_data.get("stateOfIncorporation")
                    subject_is_canadian = is_canadian_jurisdiction(state_of_inc)
                    subject_jurisdiction = state_of_inc
                    if subject_is_canadian:
                        self.logger.info(
                            f"Canadian company detected: {subject_name} "
                            f"(jurisdiction: {state_of_inc})"
                        )
            except Exception as e:
                self.logger.debug(f"Could not fetch subject company data: {e}")

        return EDGAROwnershipFiling(
            filer_cik=filer_cik.zfill(10),
            filer_name=filer_name,
            subject_cik=subject_cik.zfill(10),
            subject_name=subject_name,
            subject_is_canadian=subject_is_canadian,
            subject_jurisdiction=subject_jurisdiction,
            accession_number=filing.accession_number,
            form_type=filing.form_type,
            filing_date=filing.filing_date,
        )

    def _extract_company_by_role(
        self, html_content: str, role: str
    ) -> dict[str, str] | None:
        """Extract company info for a given role from filing index HTML.

        The modern EDGAR format uses:
          <span class="companyName">COMPANY NAME (Subject)
            <acronym>CIK</acronym>: <a href="...?CIK=0001234567...">0001234567</a>
          </span>

        Args:
            html_content: Full HTML of the filing index page
            role: One of "Subject" or "Filed by"

        Returns:
            Dict with "name" and "cik" keys, or None
        """
        # Pattern uses [^<]+? for the name to prevent crossing HTML tag
        # boundaries (ensures we stay within a single companyName span).
        # CIK is extracted from the href parameter: CIK=0001234567
        pattern = (
            r'<span class="companyName">\s*'
            r"([^<]+?)\s*\(" + re.escape(role) + r"\)"
            r"[\s\S]*?"
            r"CIK=(\d{7,10})"
        )
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            return {
                "name": match.group(1).strip(),
                "cik": match.group(2),
            }

        # Fallback: Older SGML format
        if role == "Subject":
            sgml_match = re.search(
                r"<SUBJECT-COMPANY>.*?<CONFORMED-NAME>([^<\n]+).*?<CIK>(\d+)",
                html_content,
                re.DOTALL,
            )
            if sgml_match:
                return {
                    "name": sgml_match.group(1).strip(),
                    "cik": sgml_match.group(2),
                }
        elif role == "Filed by":
            sgml_match = re.search(
                r"<FILED-BY>.*?<CONFORMED-NAME>([^<\n]+).*?<CIK>(\d+)",
                html_content,
                re.DOTALL,
            )
            if sgml_match:
                return {
                    "name": sgml_match.group(1).strip(),
                    "cik": sgml_match.group(2),
                }

        return None

    # =========================================
    # Form 4 Insider Transaction Parsing
    # =========================================

    def extract_form4_filings(
        self, submissions_data: dict[str, Any], max_filings: int = 5
    ) -> list[EDGARFiling]:
        """Extract Form 4 filings from submissions data.

        Args:
            submissions_data: Raw submissions API JSON
            max_filings: Maximum number of Form 4 filings to return

        Returns:
            List of EDGARFiling objects for Form 4 filings only (most recent first)
        """
        all_filings = self.parse_filings(submissions_data)
        form4_filings = [f for f in all_filings if f.form_type in {"4", "4/A"}]
        return form4_filings[:max_filings]

    async def fetch_form4_xml(self, cik: str, accession_number: str) -> str | None:
        """Fetch the Form 4 XML document from SEC EDGAR archives.

        Args:
            cik: Filer's CIK (zero-padded)
            accession_number: Filing accession number (with dashes)

        Returns:
            XML content of the Form 4, or None
        """
        acc_no_dashes = accession_number.replace("-", "")
        cik_stripped = cik.lstrip("0") or "0"

        # First fetch the filing index to find the XML document
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik_stripped}/{acc_no_dashes}/{accession_number}-index.htm"
        )

        async def _fetch_index():
            response = await self.http_client.get(index_url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.text

        index_html = await with_retry(_fetch_index, logger=self.logger)
        if not index_html:
            return None

        # Find the XML document link (usually named like *.xml)
        xml_match = re.search(
            r'href="([^"]*\.xml)"', index_html, re.IGNORECASE
        )
        if not xml_match:
            return None

        xml_filename = xml_match.group(1)
        # Handle relative or absolute paths
        if xml_filename.startswith("/"):
            xml_url = f"https://www.sec.gov{xml_filename}"
        elif xml_filename.startswith("http"):
            xml_url = xml_filename
        else:
            xml_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik_stripped}/{acc_no_dashes}/{xml_filename}"
            )

        async def _fetch_xml():
            response = await self.http_client.get(xml_url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.text

        return await with_retry(_fetch_xml, logger=self.logger)

    def parse_form4_xml(
        self, xml_content: str, filing: EDGARFiling
    ) -> list[EDGARForm4Filing]:
        """Parse Form 4 XML to extract insider information.

        Form 4 XML structure:
          <ownershipDocument>
            <issuer>
              <issuerCik>...</issuerCik>
              <issuerName>...</issuerName>
            </issuer>
            <reportingOwner>
              <reportingOwnerId>
                <rptOwnerCik>...</rptOwnerCik>
                <rptOwnerName>...</rptOwnerName>
              </reportingOwnerId>
              <reportingOwnerRelationship>
                <isDirector>1</isDirector>
                <isOfficer>1</isOfficer>
                <officerTitle>...</officerTitle>
                <isTenPercentOwner>0</isTenPercentOwner>
              </reportingOwnerRelationship>
            </reportingOwner>
          </ownershipDocument>

        Args:
            xml_content: Raw XML content of Form 4
            filing: The filing metadata

        Returns:
            List of EDGARForm4Filing (one per reporting owner)
        """
        from lxml import etree

        results = []
        try:
            # Use lxml with recovery mode to handle malformed XML
            parser = etree.XMLParser(recover=True, encoding="utf-8")
            # Encode to bytes if string, lxml prefers bytes
            if isinstance(xml_content, str):
                xml_bytes = xml_content.encode("utf-8", errors="replace")
            else:
                xml_bytes = xml_content
            root = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"Failed to parse Form 4 XML: {e}")
            return results

        # Extract issuer info
        issuer = root.find("issuer")
        if issuer is None:
            return results

        issuer_cik = (issuer.findtext("issuerCik") or "").strip()
        issuer_name = (issuer.findtext("issuerName") or "").strip()

        if not issuer_cik:
            return results

        # Extract each reporting owner
        for owner_elem in root.findall("reportingOwner"):
            owner_id = owner_elem.find("reportingOwnerId")
            owner_rel = owner_elem.find("reportingOwnerRelationship")

            if owner_id is None:
                continue

            owner_cik = (owner_id.findtext("rptOwnerCik") or "").strip()
            owner_name = (owner_id.findtext("rptOwnerName") or "").strip()

            if not owner_cik or not owner_name:
                continue

            is_director = False
            is_officer = False
            is_ten_percent = False
            officer_title = None

            if owner_rel is not None:
                is_director = (owner_rel.findtext("isDirector") or "").strip() in ("1", "true")
                is_officer = (owner_rel.findtext("isOfficer") or "").strip() in ("1", "true")
                is_ten_percent = (owner_rel.findtext("isTenPercentOwner") or "").strip() in ("1", "true")
                officer_title = (owner_rel.findtext("officerTitle") or "").strip() or None

            results.append(EDGARForm4Filing(
                owner_cik=owner_cik.zfill(10),
                owner_name=owner_name,
                issuer_cik=issuer_cik.zfill(10),
                issuer_name=issuer_name,
                is_director=is_director,
                is_officer=is_officer,
                is_ten_percent_owner=is_ten_percent,
                officer_title=officer_title,
                accession_number=filing.accession_number,
                filing_date=filing.filing_date,
            ))

        return results

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[EDGARCompany]:
        """Fetch company records from SEC EDGAR.

        Args:
            config: Ingestion configuration

        Yields:
            Parsed company records
        """
        # If targeting specific entities, fetch directly by CIK
        if config.target_entities:
            self.logger.info(
                f"Targeted ingestion for {len(config.target_entities)} CIKs"
            )
            for i, cik in enumerate(config.target_entities):
                try:
                    cik_padded = cik.zfill(10)
                    self.logger.debug(
                        f"Fetching submissions for CIK {cik_padded} "
                        f"({i+1}/{len(config.target_entities)})"
                    )

                    data = await self.fetch_company_submissions(cik_padded)
                    if data:
                        company = self.parse_company(data)
                        company.raw_submissions = data
                        yield company

                    # Rate limiting
                    await asyncio.sleep(0.15)

                except Exception as e:
                    self.logger.warning(f"Error fetching CIK {cik}: {e}")
                    continue
            return

        # Full ingestion: get the ticker mapping for active public companies
        self.logger.info("Fetching company tickers mapping...")
        tickers_map = await self.fetch_company_tickers()

        self.logger.info(f"Found {len(tickers_map)} companies with tickers")

        # Get list of CIKs to process
        ciks = list(tickers_map.keys())

        # Apply limit if specified
        if config.limit:
            ciks = ciks[:config.limit]

        # Fetch submissions for each company
        for i, cik in enumerate(ciks):
            try:
                self.logger.debug(f"Fetching submissions for CIK {cik} ({i+1}/{len(ciks)})")

                data = await self.fetch_company_submissions(cik)
                if data:
                    company = self.parse_company(data, tickers_map.get(cik, {}).get("tickers"))
                    company.raw_submissions = data
                    yield company

                # Rate limiting - SEC asks for max 10 requests/second
                await asyncio.sleep(0.15)

            except Exception as e:
                self.logger.warning(f"Error fetching CIK {cik}: {e}")
                continue

    async def process_record(self, record: EDGARCompany) -> dict[str, Any]:
        """Process a company record into PostgreSQL and Neo4j.

        Creates/updates:
        - Company entity in PostgreSQL
        - Organization node in Neo4j (with sec_cik property)
        - For 13D/13G filings: subject company nodes + OWNS relationships

        Args:
            record: Parsed company record

        Returns:
            Processing result with entity IDs and graph stats
        """
        result = {"created": False, "updated": False, "entity_id": None}

        # --- Step 1: PostgreSQL entity upsert ---
        async with get_db_session() as db:
            from sqlalchemy import text

            existing = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE external_ids->>'sec_cik' = :cik
                """),
                {"cik": record.cik},
            )
            row = existing.fetchone()

            org_type = OrgType.CORPORATION

            entity_data = {
                "name": record.name,
                "entity_type": "organization",
                "org_type": org_type.value,
                "jurisdiction": record.state_of_incorporation or "US",
                "status": OrgStatus.ACTIVE.value,
                "external_ids": {
                    "sec_cik": record.cik,
                    "ein": record.ein,
                    "sic": record.sic,
                },
                "metadata": {
                    "sic_description": record.sic_description,
                    "fiscal_year_end": record.fiscal_year_end,
                    "exchanges": record.exchanges,
                    "tickers": record.tickers,
                    "filings_count": record.filings_count,
                    "latest_filing_date": record.latest_filing_date.isoformat() if record.latest_filing_date else None,
                },
            }

            if row:
                await db.execute(
                    text("""
                        UPDATE entities
                        SET name = :name, metadata = CAST(:metadata AS jsonb), updated_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": row.id, "name": record.name, "metadata": json.dumps(entity_data["metadata"])},
                )
                result["updated"] = True
                result["entity_id"] = str(row.id)
            else:
                new_id = uuid4()
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb), CAST(:metadata AS jsonb), NOW())
                    """),
                    {
                        "id": new_id,
                        "name": record.name,
                        "entity_type": "organization",
                        "external_ids": json.dumps(entity_data["external_ids"]),
                        "metadata": json.dumps(entity_data["metadata"]),
                    },
                )
                result["created"] = True
                result["entity_id"] = str(new_id)

            await db.commit()

            # Evidence record
            evidence_id = uuid4()
            await db.execute(
                text("""
                    INSERT INTO evidence (id, evidence_type, source_url, retrieved_at, extractor, extractor_version, raw_data_ref, extraction_confidence, content_hash)
                    VALUES (:id, :evidence_type, :source_url, NOW(), :extractor, :version, :raw_ref, :confidence, :hash)
                """),
                {
                    "id": evidence_id,
                    "evidence_type": EvidenceType.SEC_EDGAR_FILING.value,
                    "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={record.cik}",
                    "extractor": "sec_edgar_ingester",
                    "version": "1.0.0",
                    "raw_ref": f"sec_edgar/{record.cik}",
                    "confidence": 0.95,
                    "hash": compute_content_hash(record.model_dump_json().encode("utf-8")),
                },
            )
            await db.commit()

        # --- Step 2: Neo4j Organization node upsert ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Determine jurisdiction - use "CA" for Canadian companies
                jurisdiction = record.state_of_incorporation or "US"
                if record.is_canadian:
                    jurisdiction = "CA"

                org_props = {
                    "id": result["entity_id"],
                    "name": record.name,
                    "entity_type": "ORGANIZATION",
                    "org_type": "corporation",
                    "status": "active",
                    "jurisdiction": jurisdiction,
                    "is_canadian": record.is_canadian,
                    "sec_cik": record.cik,
                    "confidence": 0.95,
                    "updated_at": now,
                }

                if record.ein:
                    org_props["ein"] = record.ein
                if record.sic:
                    org_props["sic"] = record.sic
                if record.tickers:
                    org_props["tickers"] = record.tickers
                if record.exchanges:
                    org_props["exchanges"] = record.exchanges

                # Store business address (prefer business over mailing)
                address = record.business_address or record.mailing_address
                if address:
                    if address.street:
                        org_props["address_street"] = address.street
                    if address.city:
                        org_props["address_city"] = address.city
                    if address.state:
                        org_props["address_state"] = address.state
                    if address.postal_code:
                        org_props["address_postal"] = address.postal_code
                    if address.country:
                        org_props["address_country"] = address.country

                check_result = await session.run(
                    "MATCH (o:Organization {sec_cik: $cik}) RETURN o.id as id",
                    cik=record.cik,
                )
                existing_node = await check_result.single()

                if not existing_node:
                    org_props["created_at"] = now

                await session.run(
                    """
                    MERGE (o:Organization {sec_cik: $cik})
                    SET o += $props
                    RETURN o.id as id
                    """,
                    cik=record.cik,
                    props=org_props,
                )
        except Exception as e:
            self.logger.warning(f"Neo4j write failed for {record.name}: {e}")

        # --- Step 3: Process 13D/13G ownership filings ---
        parse_ownership = getattr(self, "_parse_ownership", True)
        if parse_ownership and record.raw_submissions:
            ownership_filings = self.extract_ownership_filings(record.raw_submissions)

            # Deduplicate by subject CIK - only process latest filing per subject
            seen_subjects: set[str] = set()
            unique_filings: list[EDGARFiling] = []
            for filing in ownership_filings:
                # We'll track dedup after parsing, but cap total to avoid too many API calls
                if len(unique_filings) >= 10:
                    break
                unique_filings.append(filing)

            # Track Canadian organizations found
            canadian_orgs_found = 0
            flag_canadian = getattr(self, "_flag_canadian", True)

            for filing in unique_filings:
                try:
                    ownership = await self.parse_ownership_from_index(
                        filing, record.cik, record.name, flag_canadian=flag_canadian
                    )
                    if not ownership:
                        continue

                    # Skip if we already processed this subject company
                    if ownership.subject_cik in seen_subjects:
                        continue
                    seen_subjects.add(ownership.subject_cik)

                    # Track Canadian orgs
                    if ownership.subject_is_canadian:
                        canadian_orgs_found += 1

                    # Rate limiting for additional API calls
                    await asyncio.sleep(0.15)

                    # Create both entity nodes + OWNS relationship in Neo4j
                    # Direction: filer_cik OWNS subject_cik
                    try:
                        async with get_neo4j_session() as session:
                            now = datetime.utcnow().isoformat()

                            # Determine jurisdiction for subject company
                            subject_jurisdiction = ownership.subject_jurisdiction or "US"
                            if ownership.subject_is_canadian:
                                subject_jurisdiction = "CA"

                            # Ensure subject company node exists
                            subject_props = {
                                "id": str(uuid4()),
                                "name": ownership.subject_name,
                                "entity_type": "ORGANIZATION",
                                "org_type": "corporation",
                                "status": "active",
                                "jurisdiction": subject_jurisdiction,
                                "is_canadian": ownership.subject_is_canadian,
                                "sec_cik": ownership.subject_cik,
                                "confidence": 0.8,
                                "updated_at": now,
                            }

                            check = await session.run(
                                "MATCH (o:Organization {sec_cik: $cik}) RETURN o.id as id",
                                cik=ownership.subject_cik,
                            )
                            if not await check.single():
                                subject_props["created_at"] = now

                            await session.run(
                                """
                                MERGE (o:Organization {sec_cik: $cik})
                                ON CREATE SET o += $props
                                ON MATCH SET o.updated_at = $now, o.jurisdiction = $jurisdiction, o.is_canadian = $is_canadian
                                RETURN o.id as id
                                """,
                                cik=ownership.subject_cik,
                                props=subject_props,
                                now=now,
                                jurisdiction=subject_jurisdiction,
                                is_canadian=ownership.subject_is_canadian,
                            )

                            # Ensure filer node exists (may differ from current company)
                            if ownership.filer_cik != record.cik:
                                filer_props = {
                                    "id": str(uuid4()),
                                    "name": ownership.filer_name,
                                    "entity_type": "ORGANIZATION",
                                    "org_type": "corporation",
                                    "status": "active",
                                    "sec_cik": ownership.filer_cik,
                                    "confidence": 0.7,
                                    "updated_at": now,
                                }

                                filer_check = await session.run(
                                    "MATCH (o:Organization {sec_cik: $cik}) RETURN o.id as id",
                                    cik=ownership.filer_cik,
                                )
                                if not await filer_check.single():
                                    filer_props["created_at"] = now

                                await session.run(
                                    """
                                    MERGE (o:Organization {sec_cik: $cik})
                                    ON CREATE SET o += $props
                                    ON MATCH SET o.updated_at = $now
                                    """,
                                    cik=ownership.filer_cik,
                                    props=filer_props,
                                    now=now,
                                )

                            # Create OWNS relationship: filer -> subject
                            owns_props = {
                                "id": str(uuid4()),
                                "source": "sec_edgar",
                                "confidence": 0.85,
                                "filing_accession": ownership.accession_number,
                                "form_type": ownership.form_type,
                                "filing_date": ownership.filing_date.isoformat(),
                                "updated_at": now,
                            }

                            if ownership.ownership_percentage is not None:
                                owns_props["ownership_percentage"] = ownership.ownership_percentage
                            if ownership.shares_owned is not None:
                                owns_props["shares_owned"] = ownership.shares_owned
                            if ownership.share_class:
                                owns_props["share_class"] = ownership.share_class

                            await session.run(
                                """
                                MATCH (owner:Organization {sec_cik: $owner_cik})
                                MATCH (subject:Organization {sec_cik: $subject_cik})
                                MERGE (owner)-[r:OWNS]->(subject)
                                SET r += $props
                                """,
                                owner_cik=ownership.filer_cik,
                                subject_cik=ownership.subject_cik,
                                props=owns_props,
                            )

                            # Log with Canadian indicator
                            canadian_marker = " [CANADIAN]" if ownership.subject_is_canadian else ""
                            self.logger.info(
                                f"OWNS: {ownership.filer_name} -> {ownership.subject_name}{canadian_marker} "
                                f"(filing: {ownership.form_type} {ownership.filing_date})"
                            )

                    except Exception as e:
                        self.logger.warning(
                            f"Neo4j ownership write failed for "
                            f"{record.name} -> {ownership.subject_name}: {e}"
                        )
                        continue

                except Exception as e:
                    self.logger.warning(
                        f"Error processing ownership filing "
                        f"{filing.accession_number}: {e}"
                    )
                    continue

        # --- Step 4: Process Form 4 insider transaction filings ---
        parse_insiders = getattr(self, "_parse_insiders", True)
        if parse_insiders and record.raw_submissions:
            form4_filings = self.extract_form4_filings(record.raw_submissions)

            for filing in form4_filings:
                try:
                    # Rate limiting for additional API calls
                    await asyncio.sleep(0.15)

                    xml_content = await self.fetch_form4_xml(
                        record.cik, filing.accession_number
                    )
                    if not xml_content:
                        continue

                    insiders = await asyncio.to_thread(
                        self.parse_form4_xml, xml_content, filing
                    )

                    for insider in insiders:
                        try:
                            async with get_neo4j_session() as session:
                                now = datetime.utcnow().isoformat()

                                # MERGE Person node keyed on sec_cik
                                person_props = {
                                    "id": str(uuid4()),
                                    "name": insider.owner_name,
                                    "entity_type": "PERSON",
                                    "sec_cik": insider.owner_cik,
                                    "confidence": 0.9,
                                    "updated_at": now,
                                }

                                check = await session.run(
                                    "MATCH (p:Person {sec_cik: $cik}) RETURN p.id as id",
                                    cik=insider.owner_cik,
                                )
                                if not await check.single():
                                    person_props["created_at"] = now

                                await session.run(
                                    """
                                    MERGE (p:Person {sec_cik: $cik})
                                    ON CREATE SET p += $props
                                    ON MATCH SET p.updated_at = $now, p.name = $name
                                    RETURN p.id as id
                                    """,
                                    cik=insider.owner_cik,
                                    props=person_props,
                                    now=now,
                                    name=insider.owner_name,
                                )

                                # Create DIRECTOR_OF relationship if applicable
                                if insider.is_director:
                                    director_props = {
                                        "id": str(uuid4()),
                                        "confidence": 0.9,
                                        "valid_from": insider.filing_date.isoformat(),
                                        "filing_accession": insider.accession_number,
                                        "updated_at": now,
                                    }
                                    await session.run(
                                        """
                                        MATCH (p:Person {sec_cik: $person_cik})
                                        MATCH (o:Organization {sec_cik: $org_cik})
                                        MERGE (p)-[r:DIRECTOR_OF]->(o)
                                        SET r += $props
                                        """,
                                        person_cik=insider.owner_cik,
                                        org_cik=insider.issuer_cik,
                                        props=director_props,
                                    )
                                    self.logger.info(
                                        f"DIRECTOR_OF: {insider.owner_name} -> {insider.issuer_name}"
                                    )

                                # Create EMPLOYED_BY relationship if applicable
                                if insider.is_officer:
                                    employed_props = {
                                        "id": str(uuid4()),
                                        "confidence": 0.9,
                                        "valid_from": insider.filing_date.isoformat(),
                                        "filing_accession": insider.accession_number,
                                        "updated_at": now,
                                    }
                                    if insider.officer_title:
                                        employed_props["officer_title"] = insider.officer_title
                                    await session.run(
                                        """
                                        MATCH (p:Person {sec_cik: $person_cik})
                                        MATCH (o:Organization {sec_cik: $org_cik})
                                        MERGE (p)-[r:EMPLOYED_BY]->(o)
                                        SET r += $props
                                        """,
                                        person_cik=insider.owner_cik,
                                        org_cik=insider.issuer_cik,
                                        props=employed_props,
                                    )
                                    self.logger.info(
                                        f"EMPLOYED_BY: {insider.owner_name} -> {insider.issuer_name}"
                                        f" ({insider.officer_title or 'officer'})"
                                    )

                        except Exception as e:
                            self.logger.warning(
                                f"Neo4j insider write failed for "
                                f"{insider.owner_name} -> {record.name}: {e}"
                            )
                            continue

                except Exception as e:
                    self.logger.warning(
                        f"Error processing Form 4 filing "
                        f"{filing.accession_number}: {e}"
                    )
                    continue

        return result


async def run_sec_edgar_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    parse_ownership: bool = True,
    parse_insiders: bool = True,
    flag_canadian: bool = True,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run SEC EDGAR ingestion.

    Args:
        limit: Maximum number of companies to process
        incremental: Whether to do incremental sync
        parse_ownership: Whether to parse 13D/13G ownership filings
        parse_insiders: Whether to parse Form 4 insider transaction filings
        flag_canadian: Whether to detect and flag Canadian companies
        target_entities: Optional list of CIKs to ingest specifically
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = SECEDGARIngester()
    ingester._parse_ownership = parse_ownership
    ingester._parse_insiders = parse_insiders
    ingester._flag_canadian = flag_canadian

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
