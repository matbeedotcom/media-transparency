"""SEDAR+ Canadian securities filings ingester.

Ingests Early Warning Reports and Alternative Monthly Reports from SEDAR+
(System for Electronic Document Analysis and Retrieval Plus).

Key data points:
- Early Warning Reports (Form 62-103F1): Required when acquiring >10% of a company
- Alternative Monthly Reports: Monthly disclosure for 10%+ owners

Data source: https://www.sedarplus.ca/
Coverage: Canadian public companies and their significant shareholders

Note: SEDAR+ does not have a public API. This ingester supports:
1. Manual CSV export from SEDAR+ web interface
2. Direct document URL access for parsing
3. Future: Integration with third-party APIs (QuoteMedia, Refinitiv)

Rate limiting: 1.0s delay between requests (conservative, no official limit)
"""

import asyncio
import hashlib
import re
from datetime import date, datetime
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..models.evidence import EvidenceType
from ..storage import compute_content_hash
from .base import BaseIngester, IngestionConfig, IngestionResult, SingleIngestionResult, with_retry

logger = get_context_logger(__name__)


# =============================================================================
# Entity Resolution Helpers (T047, T048, T051)
# =============================================================================


def normalize_company_name(name: str) -> str:
    """Normalize a company name for matching (T047).

    Normalizes company names for better matching by:
    1. Converting to uppercase
    2. Removing common suffixes (Inc., Corp., Ltd., etc.)
    3. Removing punctuation
    4. Normalizing whitespace

    Args:
        name: Raw company name

    Returns:
        Normalized company name
    """
    if not name:
        return ""

    # Convert to uppercase
    normalized = name.upper()

    # Remove common corporate suffixes
    suffixes = [
        r"\s+INC\.?$",
        r"\s+INCORPORATED$",
        r"\s+CORP\.?$",
        r"\s+CORPORATION$",
        r"\s+LTD\.?$",
        r"\s+LIMITED$",
        r"\s+LLC\.?$",
        r"\s+L\.?L\.?C\.?$",
        r"\s+LLP\.?$",
        r"\s+L\.?L\.?P\.?$",
        r"\s+CO\.?$",
        r"\s+COMPANY$",
        r"\s+PLC\.?$",
        r"\s+LP\.?$",
        r"\s+L\.?P\.?$",
        r"\s+HOLDINGS?$",
        r"\s+GROUP$",
        r"\s+TRUST$",
        r"\s+FUND$",
        r"\s+ENTERPRISES?$",
    ]

    for suffix in suffixes:
        normalized = re.sub(suffix, "", normalized)

    # Remove punctuation except spaces
    normalized = re.sub(r"[^\w\s]", " ", normalized)

    # Normalize whitespace
    normalized = " ".join(normalized.split())

    return normalized.strip()


async def find_existing_entity_by_name(
    db,
    name: str,
    entity_type: str = "organization",
    threshold: float = 0.85,
) -> tuple[UUID | None, float]:
    """Find an existing entity by fuzzy name matching (T048).

    Uses rapidfuzz for fuzzy string matching to find entities that may
    be the same company but with slightly different names.

    Args:
        db: SQLAlchemy async session
        name: Company name to search for
        entity_type: Entity type to search (default: organization)
        threshold: Minimum similarity score (0.0-1.0) for a match

    Returns:
        Tuple of (entity_id, confidence) or (None, 0.0) if no match
    """
    from sqlalchemy import text

    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning("rapidfuzz not installed, falling back to exact match only")
        # Fallback to exact match
        result = await db.execute(
            text("""
                SELECT id FROM entities
                WHERE LOWER(name) = LOWER(:name)
                AND entity_type = :entity_type
            """),
            {"name": name, "entity_type": entity_type},
        )
        row = result.fetchone()
        return (row.id, 1.0) if row else (None, 0.0)

    normalized_name = normalize_company_name(name)

    # First try exact normalized match (highest confidence)
    result = await db.execute(
        text("""
            SELECT id, name FROM entities
            WHERE entity_type = :entity_type
        """),
        {"entity_type": entity_type},
    )
    rows = result.fetchall()

    best_match_id = None
    best_score = 0.0

    for row in rows:
        normalized_existing = normalize_company_name(row.name)

        # Exact normalized match
        if normalized_name == normalized_existing:
            return (row.id, 1.0)

        # Fuzzy match using token_set_ratio (good for names with different word order)
        score = fuzz.token_set_ratio(normalized_name, normalized_existing) / 100.0

        if score >= threshold and score > best_score:
            best_score = score
            best_match_id = row.id

    if best_match_id:
        return (best_match_id, best_score)

    return (None, 0.0)


def calculate_match_confidence(match_type: str, fuzzy_score: float = 0.0) -> float:
    """Calculate confidence score for entity matches (T051).

    Args:
        match_type: Type of match ("exact", "normalized", "fuzzy", "identifier")
        fuzzy_score: Fuzzy match score if applicable (0.0-1.0)

    Returns:
        Confidence score (0.0-1.0)
    """
    confidence_map = {
        "identifier": 1.0,  # Matching external IDs (CIK, SEDAR profile, BN)
        "exact": 1.0,       # Exact name match
        "normalized": 0.95,  # Normalized name match
        "fuzzy": min(0.85, fuzzy_score),  # Fuzzy match capped at 0.85
    }
    return confidence_map.get(match_type, 0.5)


# =============================================================================
# SEDI (System for Electronic Disclosure by Insiders) Support
# =============================================================================

# SEDI base URLs for scraping insider ownership data
SEDI_BASE_URL = "https://www.sedi.ca/sedi"
SEDI_INSIDER_SEARCH_URL = f"{SEDI_BASE_URL}/SVTSelectSediInsider"
SEDI_INSIDER_PROFILE_URL = f"{SEDI_BASE_URL}/SVTItSVTIt03ViewInsiderProfile"

# SEDI requires browser automation due to bot protection (ShieldSquare/Radware)
SEDI_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0"

# Check for Playwright availability
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# =============================================================================
# Data Models (T026, T027)
# =============================================================================


class SEDARFiling(BaseModel):
    """Parsed SEDAR+ ownership filing record (T026).

    Represents a parsed SEDAR+ Early Warning Report or Alternative Monthly Report.
    """

    # Document identification
    document_id: str = Field(..., description="SEDAR+ document unique identifier")
    document_type: str = Field(
        ..., description="Filing type: early_warning, alternative_monthly"
    )
    filing_date: date

    # Parties
    acquirer_name: str = Field(..., description="Name of acquiring entity")
    acquirer_sedar_profile: str | None = None
    issuer_name: str = Field(..., description="Name of company whose shares are owned")
    issuer_sedar_profile: str | None = None

    # Ownership details
    ownership_percentage: float | None = None
    shares_owned: int | None = None
    share_class: str | None = Field(default="Common", description="Class of securities")
    previous_ownership_percentage: float | None = None

    # Document metadata
    document_url: str | None = None
    content_type: str = "text/html"  # or "application/pdf"

    # Raw data for storage
    raw_content_hash: str | None = None


class SEDAROwnership(BaseModel):
    """Processed ownership relationship from SEDAR+ filings (T027).

    Represents a processed ownership relationship extracted from SEDAR+ filings.
    """

    # Relationship parties
    owner_name: str
    owner_sedar_profile: str | None
    owner_entity_id: UUID | None = None  # Resolved MITDS entity

    subject_name: str
    subject_sedar_profile: str | None
    subject_entity_id: UUID | None = None  # Resolved MITDS entity

    # Ownership details
    ownership_percentage: float | None
    shares_owned: int | None
    share_class: str | None

    # Filing reference
    filing_document_id: str
    filing_date: date
    filing_type: str  # early_warning, alternative_monthly

    # Confidence
    extraction_confidence: float = Field(default=0.8, ge=0.0, le=1.0)


# =============================================================================
# Parser Implementation (T028-T031)
# =============================================================================


class EarlyWarningReportParser:
    """Parser for SEDAR+ Early Warning Reports (Form 62-103F1) (T028).

    Extracts ownership information from HTML and PDF documents.
    """

    # Regex patterns for extracting ownership data (T031)
    OWNERSHIP_PERCENTAGE_PATTERNS = [
        # Pattern: "12.5%" or "12.5 %" or "12.5 percent"
        r"(?:ownership|owns?|holding|stake|position)[\s:]*(?:of\s+)?(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of|ownership|of\s+the|of\s+outstanding)",
        r"(?:percentage|percent)[\s:]*(\d+(?:\.\d+)?)",
    ]

    SHARES_OWNED_PATTERNS = [
        # Pattern: "1,234,567 shares" or "1234567 common shares"
        r"(\d{1,3}(?:,\d{3})*)\s*(?:common\s+)?shares",
        r"(?:owns?|acquired?|holds?|holding)[\s:]*(\d{1,3}(?:,\d{3})*)",
        r"(?:number\s+of\s+shares?)[\s:]*(\d{1,3}(?:,\d{3})*)",
    ]

    DATE_PATTERNS = [
        # Pattern: "January 15, 2026" or "2026-01-15" or "15/01/2026"
        r"(\w+\s+\d{1,2},?\s+\d{4})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
    ]

    ISSUER_NAME_PATTERNS = [
        # Early Warning Report typically has "ISSUER NAME" or "Name of Reporting Issuer"
        r"(?:issuer|reporting\s+issuer|subject\s+company)[\s:]*([A-Z][A-Za-z0-9\s\.,&'()-]+?)(?:\n|<|$)",
        r"(?:securities\s+of)[\s:]*([A-Z][A-Za-z0-9\s\.,&'()-]+?)(?:\n|<|$)",
    ]

    ACQUIRER_NAME_PATTERNS = [
        # Pattern: "Acquirer Name" or "Name of Acquirer" or "Filed by"
        r"(?:acquirer|filer|filed\s+by|reporting\s+person)[\s:]*([A-Z][A-Za-z0-9\s\.,&'()-]+?)(?:\n|<|$)",
    ]

    def __init__(self, logger=None):
        self.logger = logger or get_context_logger(__name__)

    def parse(self, content: bytes, content_type: str) -> SEDAROwnership | None:
        """Parse an Early Warning Report document.

        Args:
            content: Document content as bytes
            content_type: MIME type ("text/html" or "application/pdf")

        Returns:
            Parsed ownership data or None if parsing fails
        """
        if content_type == "text/html" or content_type.startswith("text/"):
            return self._parse_html(content)
        elif content_type == "application/pdf":
            return self._parse_pdf(content)
        else:
            self.logger.warning(f"Unsupported content type: {content_type}")
            return None

    def _parse_html(self, content: bytes) -> SEDAROwnership | None:
        """Parse HTML Early Warning Report (T029).

        Uses lxml for DOM parsing with confidence 0.9.
        """
        try:
            from lxml import html

            text = content.decode("utf-8", errors="replace")
            doc = html.fromstring(text)

            # Extract text content from the document
            text_content = doc.text_content()

            return self._extract_ownership_from_text(
                text_content,
                extraction_confidence=0.9,
            )

        except Exception as e:
            self.logger.warning(f"HTML parsing failed: {e}")
            return None

    def _parse_pdf(self, content: bytes) -> SEDAROwnership | None:
        """Parse PDF Early Warning Report (T030).

        Uses pdfplumber for text extraction with confidence 0.7.
        Falls back to OCR (pytesseract) if text extraction fails.
        """
        try:
            import pdfplumber
            from io import BytesIO

            text_content = ""
            with pdfplumber.open(BytesIO(content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_content += page_text + "\n"

            if not text_content.strip():
                # PDF might be image-based, would need OCR
                self.logger.warning("PDF text extraction returned empty, may need OCR")
                return None

            return self._extract_ownership_from_text(
                text_content,
                extraction_confidence=0.7,
            )

        except ImportError:
            self.logger.error("pdfplumber not installed, cannot parse PDF")
            return None
        except Exception as e:
            self.logger.warning(f"PDF parsing failed: {e}")
            return None

    def _extract_ownership_from_text(
        self,
        text: str,
        extraction_confidence: float = 0.8,
    ) -> SEDAROwnership | None:
        """Extract ownership information from text content.

        Args:
            text: Plain text from document
            extraction_confidence: Base confidence score

        Returns:
            Parsed ownership data or None
        """
        # Extract ownership percentage
        ownership_pct = None
        for pattern in self.OWNERSHIP_PERCENTAGE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    ownership_pct = float(match.group(1))
                    break
                except (ValueError, IndexError):
                    continue

        # Extract shares owned
        shares_owned = None
        for pattern in self.SHARES_OWNED_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    shares_str = match.group(1).replace(",", "")
                    shares_owned = int(shares_str)
                    break
                except (ValueError, IndexError):
                    continue

        # Extract acquirer name
        acquirer_name = None
        for pattern in self.ACQUIRER_NAME_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                acquirer_name = match.group(1).strip()
                break

        # Extract issuer name
        issuer_name = None
        for pattern in self.ISSUER_NAME_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                issuer_name = match.group(1).strip()
                break

        # Require at least one party name
        if not acquirer_name and not issuer_name:
            self.logger.debug("Could not extract party names from document")
            return None

        # Calculate confidence based on what we extracted
        confidence = extraction_confidence
        if not ownership_pct:
            confidence *= 0.8
        if not shares_owned:
            confidence *= 0.9
        if not acquirer_name or not issuer_name:
            confidence *= 0.7

        return SEDAROwnership(
            owner_name=acquirer_name or "Unknown Acquirer",
            owner_sedar_profile=None,
            subject_name=issuer_name or "Unknown Issuer",
            subject_sedar_profile=None,
            ownership_percentage=ownership_pct,
            shares_owned=shares_owned,
            share_class="Common",
            filing_document_id="",  # Set by caller
            filing_date=date.today(),  # Set by caller
            filing_type="early_warning",
            extraction_confidence=confidence,
        )


# =============================================================================
# Ingester Class (T032-T037)
# =============================================================================


class SEDARIngester(BaseIngester[SEDARFiling]):
    """Ingester for SEDAR+ Canadian securities filings (T032).

    Supports:
    - Manual CSV export ingestion
    - Direct document URL parsing
    - Future: Third-party API integration
    """

    def __init__(self):
        """Initialize the SEDAR+ ingester."""
        super().__init__(source_name="sedar")
        self._http_client: httpx.AsyncClient | None = None
        self._parser = EarlyWarningReportParser(logger=self.logger)

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client with rate limiting (T033).

        Uses 1.0s delay between requests (conservative rate limiting).
        """
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "User-Agent": "MITDS Research (contact@mitds.org)",
                    "Accept": "text/html,application/pdf,application/json",
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
        """Get timestamp of last successful sync (T036)."""
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
        """Save sync timestamp (T036)."""
        pass  # Managed by IngestionResult in base class

    async def ingest_single(
        self,
        identifier: str,
        identifier_type: str,
    ) -> SingleIngestionResult | None:
        """Ingest a single Canadian company from SEDAR+/SEDI.

        Args:
            identifier: The company name, BN, or SEDAR profile ID
            identifier_type: One of "name", "bn", "sedar_profile"

        Returns:
            SingleIngestionResult if found and processed, None otherwise
        """
        print(f"[SEDAR] ingest_single called: identifier={identifier}, type={identifier_type}")
        self.logger.info(f"ingest_single called: identifier={identifier}, type={identifier_type}")
        try:
            filings_processed = 0
            entity_id = None
            entity_name = None
            is_new = False

            if identifier_type == "sedar_profile":
                # Fetch by SEDI issuer number
                async for filing in self._fetch_sedi_insiders(identifier, limit=1):
                    result = await self.process_record(filing)
                    if result.get("entity_id"):
                        entity_id = UUID(result["entity_id"])
                        entity_name = filing.subject_company
                        is_new = result.get("created", False)
                        filings_processed += 1
                        break

            elif identifier_type == "name":
                # Search by company name
                print(f"[SEDAR] Starting name search for: {identifier}")
                self.logger.info(f"Starting name search for: {identifier}")
                async for filing in self._search_sedi_by_name(identifier, limit=1):
                    result = await self.process_record(filing)
                    if result.get("entity_id"):
                        entity_id = UUID(result["entity_id"])
                        entity_name = filing.subject_company
                        is_new = result.get("created", False)
                        filings_processed += 1
                        break

            elif identifier_type == "bn":
                # BN lookup - search by associated company name via our database
                # SEDAR doesn't directly support BN lookup
                async with get_db_session() as db:
                    from sqlalchemy import text
                    result = await db.execute(
                        text("""
                            SELECT name FROM entities
                            WHERE external_ids->>'bn' = :bn
                            OR external_ids->>'business_number' = :bn
                            LIMIT 1
                        """),
                        {"bn": identifier},
                    )
                    row = result.fetchone()
                    if row:
                        # Recurse with name lookup
                        print(f"[SEDAR] BN {identifier} resolved to name: {row.name}, recursing...")
                        self.logger.info(f"BN {identifier} resolved to name: {row.name}, recursing...")
                        return await self.ingest_single(row.name, "name")

                self.logger.info(f"BN not found in database for SEDAR lookup: {identifier}")
                print(f"[SEDAR] BN not found in database: {identifier}")
                return None

            if entity_id:
                return SingleIngestionResult(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    entity_type="organization",
                    is_new=is_new,
                    relationships_created=filings_processed,
                    source="sedar",
                )

            self.logger.info(f"No SEDAR data found for: {identifier}")
            return None

        except Exception as e:
            import traceback
            print(f"[SEDAR] ERROR ingesting {identifier}: {e}")
            print(f"[SEDAR] Traceback: {traceback.format_exc()}")
            self.logger.error(f"Error ingesting from SEDAR {identifier}: {e}")
            return SingleIngestionResult(
                source="sedar",
                error=str(e),
            )

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[SEDARFiling]:
        """Fetch SEDAR+ filing records (T034).

        Supports multiple ingestion modes:
        1. Target entities: Fetch specific companies by name or SEDAR profile
        2. CSV file: Process exported CSV from SEDAR+ web interface
        3. Document URLs: Parse specific document URLs

        Args:
            config: Ingestion configuration

        Yields:
            Parsed SEDAR filing records
        """
        # Mode 1: Process target entities (search SEDAR+ for specific companies)
        if config.target_entities:
            self.logger.info(
                f"Targeted ingestion for {len(config.target_entities)} entities"
            )
            for i, target in enumerate(config.target_entities):
                try:
                    self.logger.debug(
                        f"Processing target: {target} ({i+1}/{len(config.target_entities)})"
                    )

                    # If target looks like a URL, fetch and parse it
                    if target.startswith("http"):
                        filing = await self._fetch_and_parse_document(target)
                        if filing:
                            yield filing
                    elif target.isdigit() or (len(target) == 8 and target[0] == "0"):
                        # Target is a SEDAR/SEDI issuer number - fetch from SEDI
                        self.logger.info(f"Fetching SEDI insider profiles for issuer {target}")
                        async for filing in self._fetch_sedi_insiders(target, config.limit):
                            yield filing
                    else:
                        # Target is a company name - search SEDI by name
                        self.logger.info(f"Searching SEDI for issuer: {target}")
                        async for filing in self._search_sedi_by_name(target, config.limit):
                            yield filing

                    # Rate limiting
                    await asyncio.sleep(1.0)

                except Exception as e:
                    self.logger.warning(f"Error processing target {target}: {e}")
                    continue

            return

        # Mode 2: Process CSV file from extra_params
        csv_path = config.extra_params.get("csv_path")
        if csv_path:
            async for filing in self._process_csv_file(csv_path, config.limit):
                yield filing
            return

        # Mode 3: No targets specified - query Neo4j for Canadian companies
        self.logger.info("No targets specified - querying database for Canadian companies...")

        companies = await self._get_canadian_companies_from_db()

        if not companies:
            self.logger.info(
                "No Canadian companies found in database. Options:\n"
                "1. Run: mitds ingest sedar --target 'Company Name'\n"
                "2. Export CSV from https://www.sedarplus.ca/\n"
                "3. Run: mitds ingest sedar --csv-path /path/to/export.csv"
            )
            return

        self.logger.info(f"Found {len(companies)} Canadian companies in database to search")

        total_processed = 0
        for i, company_name in enumerate(companies):
            if config.limit and total_processed >= config.limit:
                self.logger.info(f"Limit of {config.limit} reached")
                break

            try:
                self.logger.info(f"Searching SEDI for: {company_name} ({i+1}/{len(companies)})")

                # Calculate remaining limit for this company
                remaining_limit = None
                if config.limit:
                    remaining_limit = config.limit - total_processed
                    if remaining_limit <= 0:
                        break

                company_count = 0
                async for filing in self._search_sedi_by_name(company_name, remaining_limit):
                    yield filing
                    company_count += 1
                    total_processed += 1

                    if config.limit and total_processed >= config.limit:
                        break

                self.logger.info(f"Found {company_count} insiders for {company_name}")

                # Rate limiting between companies
                await asyncio.sleep(2.0)

            except Exception as e:
                self.logger.warning(f"Error searching SEDI for {company_name}: {e}")
                continue

        self.logger.info(f"Total: processed {total_processed} insider records from {len(companies)} companies")

    async def _get_canadian_companies_from_db(self) -> list[str]:
        """Query Neo4j for Canadian companies to search in SEDI.

        Returns a list of company names that:
        1. Have Canadian jurisdiction (CA or CA-XX)
        2. Are issuers (targets of OWNS relationships) with SEDAR profiles
        3. Have "Canada" or "Canadian" in their name

        Returns:
            List of company names to search in SEDI
        """
        from ..db import get_neo4j_session

        companies: list[str] = []

        try:
            async with get_neo4j_session() as session:
                # Query 1: Get issuers with SEDAR profiles (companies we already know about from SEDI)
                # Focus on ones that are targets of OWNS relationships
                result = await session.run("""
                    MATCH (issuer:Organization)
                    WHERE issuer.sedar_profile IS NOT NULL
                        AND issuer.sedar_profile STARTS WITH '0'
                    RETURN DISTINCT issuer.name AS name
                    ORDER BY issuer.name
                    LIMIT 100
                """)

                records = await result.data()
                for record in records:
                    name = record["name"]
                    if name and len(name) > 3:
                        # Extract core company name for SEDI search
                        # Remove common suffixes that might cause search issues
                        search_name = name
                        for suffix in [" Inc.", " Corp.", " Ltd.", " L.P.", " LP"]:
                            if search_name.endswith(suffix):
                                search_name = search_name[:-len(suffix)]
                                break
                        if search_name not in companies:
                            companies.append(search_name)

                # Query 2: Get Canadian media companies by jurisdiction
                result2 = await session.run("""
                    MATCH (o:Organization)
                    WHERE (o.jurisdiction = 'CA' OR o.jurisdiction STARTS WITH 'CA-')
                        AND (o.name CONTAINS 'Media' OR o.name CONTAINS 'News'
                             OR o.name CONTAINS 'Broadcast' OR o.name CONTAINS 'Television'
                             OR o.name CONTAINS 'Communications')
                    RETURN DISTINCT o.name AS name
                    ORDER BY o.name
                    LIMIT 50
                """)

                records2 = await result2.data()
                for record in records2:
                    name = record["name"]
                    if name and name not in companies and len(name) > 5:
                        companies.append(name)

                self.logger.debug(f"Found {len(companies)} Canadian companies from Neo4j")

        except Exception as e:
            self.logger.warning(f"Error querying Neo4j for Canadian companies: {e}")

        return companies

    async def _fetch_and_parse_document(
        self, url: str
    ) -> SEDARFiling | None:
        """Fetch a document from URL and parse it.

        Args:
            url: Document URL

        Returns:
            Parsed filing or None
        """
        try:
            async def _fetch():
                response = await self.http_client.get(url)
                response.raise_for_status()
                return response

            response = await with_retry(_fetch, logger=self.logger)
            content_type = response.headers.get("content-type", "text/html").split(";")[0]

            ownership = self._parser.parse(response.content, content_type)
            if not ownership:
                return None

            # Generate document ID from URL hash
            doc_id = hashlib.sha256(url.encode()).hexdigest()[:16]

            return SEDARFiling(
                document_id=doc_id,
                document_type="early_warning",
                filing_date=ownership.filing_date,
                acquirer_name=ownership.owner_name,
                acquirer_sedar_profile=ownership.owner_sedar_profile,
                issuer_name=ownership.subject_name,
                issuer_sedar_profile=ownership.subject_sedar_profile,
                ownership_percentage=ownership.ownership_percentage,
                shares_owned=ownership.shares_owned,
                share_class=ownership.share_class,
                document_url=url,
                content_type=content_type,
                raw_content_hash=compute_content_hash(response.content),
            )

        except Exception as e:
            self.logger.warning(f"Failed to fetch/parse document {url}: {e}")
            return None

    async def _process_csv_file(
        self, csv_path: str, limit: int | None = None
    ) -> AsyncIterator[SEDARFiling]:
        """Process a CSV export from SEDAR+ web interface.

        Expected CSV columns:
        - Document ID
        - Filing Date
        - Acquirer Name
        - Issuer Name
        - Document Type
        - Document URL

        Args:
            csv_path: Path to CSV file
            limit: Maximum records to process

        Yields:
            Parsed filing records
        """
        import csv
        from pathlib import Path

        csv_file = Path(csv_path)
        if not csv_file.exists():
            self.logger.error(f"CSV file not found: {csv_path}")
            return

        count = 0
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if limit and count >= limit:
                    break

                try:
                    # Parse date from CSV
                    filing_date_str = row.get("Filing Date", row.get("Date", ""))
                    try:
                        filing_date = datetime.strptime(
                            filing_date_str, "%Y-%m-%d"
                        ).date()
                    except ValueError:
                        try:
                            filing_date = datetime.strptime(
                                filing_date_str, "%m/%d/%Y"
                            ).date()
                        except ValueError:
                            filing_date = date.today()

                    # Create filing record
                    filing = SEDARFiling(
                        document_id=row.get("Document ID", str(uuid4())[:16]),
                        document_type=row.get("Document Type", "early_warning").lower(),
                        filing_date=filing_date,
                        acquirer_name=row.get("Acquirer Name", row.get("Filer", "Unknown")),
                        acquirer_sedar_profile=row.get("Acquirer Profile"),
                        issuer_name=row.get("Issuer Name", row.get("Subject", "Unknown")),
                        issuer_sedar_profile=row.get("Issuer Profile"),
                        ownership_percentage=self._parse_percentage(
                            row.get("Ownership %", row.get("Percentage"))
                        ),
                        shares_owned=self._parse_int(row.get("Shares Owned")),
                        share_class=row.get("Share Class", "Common"),
                        document_url=row.get("Document URL"),
                        content_type="text/html",
                    )

                    count += 1
                    yield filing

                except Exception as e:
                    self.logger.warning(f"Error parsing CSV row: {e}")
                    continue

    def _parse_percentage(self, value: str | None) -> float | None:
        """Parse a percentage value from string."""
        if not value:
            return None
        try:
            # Remove % sign and whitespace
            clean = value.replace("%", "").strip()
            return float(clean)
        except (ValueError, AttributeError):
            return None

    def _parse_int(self, value: str | None) -> int | None:
        """Parse an integer value from string."""
        if not value:
            return None
        try:
            # Remove commas and whitespace
            clean = value.replace(",", "").strip()
            return int(clean)
        except (ValueError, AttributeError):
            return None

    # =========================================================================
    # SEDI (System for Electronic Disclosure by Insiders) Scraping
    # =========================================================================

    async def _fetch_sedi_insiders(
        self, issuer_number: str, limit: int | None = None
    ) -> AsyncIterator[SEDARFiling]:
        """Fetch insider profiles for an issuer from SEDI.

        SEDI provides insider ownership data including 10%+ holders.
        Uses session-based authentication with CSRF tokens.

        Args:
            issuer_number: SEDI issuer number (e.g., "00031322")
            limit: Maximum number of insiders to process

        Yields:
            SEDARFiling records for each insider relationship
        """
        from lxml import html

        client = None
        try:
            # Initialize SEDI session
            client, csrf_token = await self._get_sedi_session()

            # Search for insiders by issuer number
            search_url = f"{SEDI_INSIDER_SEARCH_URL}?menukey=15.01.00&locale=en_CA"

            # Form data to search by issuer number
            form_data = {
                "jspSynchronizerToken": csrf_token,
                "ISSUER_NAME_SEARCH_TYPE": "1",  # Starts with
                "issuer_name": "",
                "INSIDER_NAME_SEARCH_TYPE": "1",  # Starts with
                "insider_name": "",
                "INSIDER_CIK_ID": "",
                "DATE_RANGE_TYPE": "3",  # All dates
                "date_from": "",
                "date_to": "",
                "SECURITY_ROLE_LIST": "",
                "ISSUER_TYPE_ID": "",
                "DATE_OF_FILING": "1",
                "TRANSACTION_TYPE_ID": "",
                # Add issuer number as a hidden filter if the form supports it
                "selectedIssuerNumber": issuer_number,
            }

            self.logger.info(f"Fetching SEDI insiders for issuer number: {issuer_number}")

            response = await client.post(
                search_url,
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://www.sedi.ca",
                    "Referer": search_url,
                },
            )
            response.raise_for_status()

            tree = html.fromstring(response.content)

            # Try to extract issuer name from page
            issuer_name_elem = tree.xpath("//span[@id='selectedIssuerName']/text() | //td[contains(text(), 'Issuer')]/following-sibling::td/text()")
            issuer_name = issuer_name_elem[0].strip() if issuer_name_elem else f"Issuer {issuer_number}"

            # Look for insider results table
            result_tables = tree.xpath("//table[@class='sediTable'] | //table[contains(@class, 'results')] | //table")

            count = 0
            for table in result_tables:
                rows = table.xpath(".//tr[position()>1]")

                for row in rows:
                    if limit and count >= limit:
                        return

                    try:
                        cells = row.xpath(".//td")
                        if len(cells) < 2:
                            continue

                        insider_name = cells[0].text_content().strip()
                        relationship = cells[1].text_content().strip() if len(cells) > 1 else ""
                        date_became_insider = cells[2].text_content().strip() if len(cells) > 2 else ""

                        # Check if this is a 10%+ security holder
                        is_10_percent_holder = "10%" in relationship.lower() or "security holder" in relationship.lower()

                        if not insider_name:
                            continue

                        # Parse date
                        try:
                            filing_date = datetime.strptime(date_became_insider, "%Y-%m-%d").date()
                        except ValueError:
                            filing_date = date.today()

                        # Create filing record
                        filing = SEDARFiling(
                            document_id=f"sedi_{issuer_number}_{hashlib.sha256(insider_name.encode()).hexdigest()[:8]}",
                            document_type="sedi_insider",
                            filing_date=filing_date,
                            acquirer_name=insider_name,
                            acquirer_sedar_profile=None,
                            issuer_name=issuer_name,
                            issuer_sedar_profile=issuer_number,
                            ownership_percentage=10.0 if is_10_percent_holder else None,
                            shares_owned=None,
                            share_class="Common",
                            document_url=f"https://www.sedi.ca/sedi/SVTSelectSediInsider?menukey=15.01.00&locale=en_CA",
                            content_type="text/html",
                        )

                        self.logger.info(
                            f"Found SEDI insider: {insider_name} -> {issuer_name} "
                            f"({relationship})"
                        )

                        count += 1
                        yield filing

                        # Rate limiting
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        self.logger.debug(f"Error parsing insider row: {e}")
                        continue

            if count == 0:
                self.logger.info(f"No insider records found for issuer {issuer_number}")

        except Exception as e:
            self.logger.warning(f"Failed to fetch SEDI insiders for {issuer_number}: {e}")
            import traceback
            self.logger.debug(f"SEDI fetch traceback: {traceback.format_exc()}")

        finally:
            if client:
                await client.aclose()

    async def _simulate_human_behavior(self, page) -> None:
        """Simulate human-like behavior to avoid bot detection.

        Performs random mouse movements, scrolling, and delays.
        """
        import random

        try:
            # Random mouse movements
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                await page.mouse.move(x, y)
                await page.wait_for_timeout(random.randint(50, 200))

            # Small random scroll
            await page.evaluate(f"window.scrollBy(0, {random.randint(50, 200)})")
            await page.wait_for_timeout(random.randint(300, 800))

            # Scroll back up
            await page.evaluate(f"window.scrollBy(0, -{random.randint(30, 100)})")
            await page.wait_for_timeout(random.randint(200, 500))

        except Exception as e:
            self.logger.debug(f"Human simulation error (non-fatal): {e}")

    async def _bypass_captcha(self, page) -> bool:
        """Attempt to bypass ShieldSquare/Radware captcha.

        Uses multiple strategies:
        1. Wait for JavaScript challenge to auto-resolve
        2. Simulate human behavior (mouse movements, scrolling)
        3. Trigger form interactions
        4. Wait for redirect

        Returns:
            True if captcha was bypassed, False otherwise
        """
        import random

        self.logger.info("Attempting captcha bypass strategies...")

        for attempt in range(8):
            # Strategy 1: Wait with human-like behavior
            await self._simulate_human_behavior(page)
            await page.wait_for_timeout(random.randint(4000, 8000))

            # Check if captcha resolved
            page_content = await page.content()
            if "captcha" not in page_content.lower() and "shieldsquare" not in page_content.lower() and "perfdrive" not in page_content.lower():
                self.logger.info(f"Captcha bypassed on attempt {attempt + 1}!")
                return True

            self.logger.debug(f"Captcha still present (attempt {attempt + 1}/8)")

            # Strategy 2: Try clicking in random page areas
            if attempt == 2:
                try:
                    await page.mouse.click(random.randint(400, 600), random.randint(300, 500))
                    await page.wait_for_timeout(2000)
                except Exception:
                    pass

            # Strategy 3: Try pressing keys
            if attempt == 4:
                try:
                    await page.keyboard.press("Tab")
                    await page.wait_for_timeout(500)
                    await page.keyboard.press("Tab")
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass

            # Strategy 4: Reload page (sometimes helps reset the challenge)
            if attempt == 6:
                try:
                    self.logger.debug("Attempting page reload...")
                    await page.reload(wait_until="load", timeout=30000)
                    await page.wait_for_timeout(5000)
                    await self._simulate_human_behavior(page)
                except Exception as e:
                    self.logger.debug(f"Reload failed: {e}")

        return False

    async def _search_sedi_with_playwright(
        self, company_name: str, limit: int | None = None
    ) -> AsyncIterator[SEDARFiling]:
        """Search SEDI using Playwright browser automation.

        SEDI has bot protection (ShieldSquare/Radware) that requires JavaScript
        execution. This method uses Playwright to automate a real browser.

        Args:
            company_name: Company name to search for
            limit: Maximum number of results to process

        Yields:
            SEDARFiling records for insider relationships found
        """
        if not PLAYWRIGHT_AVAILABLE:
            self.logger.warning(
                "Playwright not available. Install with: pip install playwright && playwright install"
            )
            return

        import random
        from playwright.async_api import async_playwright

        self.logger.info(f"Searching SEDI with Playwright for: {company_name}")
        print(f"[SEDI] Starting Playwright search for: {company_name}")

        async with async_playwright() as p:
            print("[SEDI] Playwright context created, launching browser...")
            # Try Firefox first (better captcha bypass), fall back to Chromium
            try:
                print("[SEDI] Attempting Firefox launch...")
                browser = await p.firefox.launch(
                    headless=False,  # Non-headless is much better for captcha bypass
                    firefox_user_prefs={
                        "dom.webdriver.enabled": False,
                        "useAutomationExtension": False,
                    }
                )
                print("[SEDI] Firefox launched successfully")
                self.logger.debug("Using Firefox browser (non-headless)")
            except Exception as e:
                print(f"[SEDI] Firefox launch failed: {e}, falling back to Chromium...")
                self.logger.debug(f"Firefox launch failed, falling back to Chromium: {e}")
                browser = await p.chromium.launch(
                    headless=False,  # Non-headless mode
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-infobars",
                        "--disable-background-timer-throttling",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-renderer-backgrounding",
                        "--window-size=1920,1080",
                        "--start-maximized",
                    ]
                )
                print("[SEDI] Chromium launched successfully")

            # Randomize viewport slightly to appear more human
            viewport_width = 1920 + random.randint(-100, 100)
            viewport_height = 1080 + random.randint(-50, 50)

            # Create context with realistic settings
            print("[SEDI] Creating browser context...")
            context = await browser.new_context(
                user_agent=SEDI_USER_AGENT,
                viewport={"width": viewport_width, "height": viewport_height},
                locale="en-CA",
                timezone_id="America/Toronto",
                # Add more realistic browser properties
                java_script_enabled=True,
                has_touch=False,
                is_mobile=False,
                device_scale_factor=1,
            )
            print("[SEDI] Browser context created")

            # Try to apply stealth if available
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
                print("[SEDI] Page created with stealth")
                self.logger.debug("Applied playwright-stealth")
            except ImportError:
                page = await context.new_page()
                print("[SEDI] Page created (no stealth)")
                self.logger.debug("playwright-stealth not available")

            # Inject additional anti-detection JavaScript
            print("[SEDI] Adding init script...")
            await page.add_init_script("""
                // Override webdriver detection
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

                // Override plugins to look more realistic
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-CA', 'en-US', 'en']
                });

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );

                // Add chrome object if missing
                if (!window.chrome) {
                    window.chrome = { runtime: {} };
                }
            """)
            print("[SEDI] Init script added")

            try:
                # Navigate to SEDI insider search page
                url = f"{SEDI_INSIDER_SEARCH_URL}?menukey=15.01.00&locale=en_CA"
                print(f"[SEDI] Navigating to: {url}")
                self.logger.debug(f"Navigating to: {url}")

                # Random delay before navigation (human-like)
                print("[SEDI] Waiting before navigation...")
                await page.wait_for_timeout(random.randint(500, 1500))

                # Use 'load' instead of 'networkidle' to avoid timeout on captcha pages
                print("[SEDI] Starting page.goto...")
                await page.goto(url, wait_until="load", timeout=30000)
                print("[SEDI] Navigation complete!")

                # Human-like random delay
                print("[SEDI] Waiting after navigation...")
                await page.wait_for_timeout(random.randint(2000, 4000))
                print("[SEDI] Wait complete, proceeding...")

                # Simulate human mouse movement
                print("[SEDI] Simulating human behavior...")
                await self._simulate_human_behavior(page)
                print("[SEDI] Human simulation done")

                # Check if we hit an actual captcha blocking us (not just captcha JS in background)
                # The real test is whether we can find the SEDI form elements
                print("[SEDI] Checking for SEDI form elements...")
                issuer_input_check = page.locator("input[name='ISSUER_NAME'], input[id*='ISSUER'], input[name*='issuer']")
                form_count = await issuer_input_check.count()
                print(f"[SEDI] Found {form_count} form elements")

                if form_count == 0:
                    # No form found - might be captcha or still loading
                    print("[SEDI] No form found, checking page content...")
                    page_content = await page.content()
                    print(f"[SEDI] Got page content ({len(page_content)} chars)")

                    # Check if it's a full captcha block page (very short content with captcha)
                    captcha_blocking = len(page_content) < 5000 and ("captcha" in page_content.lower() or "shieldsquare" in page_content.lower())
                    print(f"[SEDI] Captcha blocking page: {captcha_blocking}")

                    if captcha_blocking:
                        print("[SEDI] Attempting captcha bypass...")
                        self.logger.info("SEDI captcha detected, attempting bypass...")
                        captcha_bypassed = await self._bypass_captcha(page)
                        if not captcha_bypassed:
                            print("[SEDI] Captcha bypass FAILED")
                            self.logger.warning("Captcha bypass failed after all attempts")
                        else:
                            print("[SEDI] Captcha bypass SUCCEEDED")
                    else:
                        # Wait a bit more for page to fully load
                        print("[SEDI] Page loaded but no form yet, waiting for dynamic content...")
                        await page.wait_for_timeout(3000)
                else:
                    print("[SEDI] Form elements found - no captcha blocking us!")

                # Try to find and fill the issuer name field
                print("[SEDI] Looking for issuer input field...")
                issuer_input = page.locator("input[name='ISSUER_NAME'], input[id*='ISSUER'], input[name*='issuer'], input[name='issuer_name']")
                input_count = await issuer_input.count()
                print(f"[SEDI] Found {input_count} issuer input fields")
                self.logger.info(f"Found {input_count} issuer name input fields")

                if input_count > 0:
                    print(f"[SEDI] Filling issuer name: {company_name}")
                    await issuer_input.first.fill(company_name)
                    self.logger.info(f"Filled issuer name: {company_name}")

                    # Set search type to "Contains" if available
                    print("[SEDI] Looking for search type selector...")
                    search_type = page.locator("select[name='ISSUER_NAME_SEARCH_TYPE']")
                    search_type_count = await search_type.count()
                    print(f"[SEDI] Found {search_type_count} search type selectors")
                    if search_type_count > 0:
                        await search_type.select_option("2")  # 2 = Contains
                        print("[SEDI] Set search type to 'Contains'")
                        self.logger.debug("Set search type to 'Contains'")

                    # Click search/submit button
                    print("[SEDI] Looking for submit button...")
                    submit_btn = page.locator("input[type='submit'], button[type='submit'], input[value='Search']")
                    submit_count = await submit_btn.count()
                    print(f"[SEDI] Found {submit_count} submit buttons")
                    self.logger.info(f"Found {submit_count} submit buttons")

                    if submit_count > 0:
                        print("[SEDI] Clicking search button...")
                        self.logger.info("Clicking search button...")
                        await submit_btn.first.click()
                        print("[SEDI] Waiting for search results (networkidle)...")
                        await page.wait_for_load_state("networkidle", timeout=30000)
                        print("[SEDI] Search completed!")
                        self.logger.info("Search completed")
                    else:
                        print("[SEDI] WARNING: No submit button found!")
                        self.logger.warning("No submit button found on SEDI page")
                else:
                    page_title = await page.title()
                    print(f"[SEDI] No issuer input found! Page title: {page_title}")
                    self.logger.warning(
                        f"No issuer name input found on SEDI page. "
                        f"Page might still be loading or captcha blocking. "
                        f"Page title: {page_title}"
                    )
                    # Log available inputs for debugging
                    all_inputs = await page.locator("input").all()
                    input_names = []
                    for inp in all_inputs[:10]:  # First 10 inputs
                        name = await inp.get_attribute("name")
                        inp_id = await inp.get_attribute("id")
                        input_names.append(f"{name or inp_id}")
                    print(f"[SEDI] Available inputs: {input_names}")
                    self.logger.debug(f"Available inputs: {input_names}")

                # Parse results
                print("[SEDI] Waiting before parsing results...")
                await page.wait_for_timeout(2000)
                print("[SEDI] Getting page content for parsing...")
                page_content = await page.content()
                print(f"[SEDI] Got results page content ({len(page_content)} chars)")

                # Check for Postmedia or Chatham in the results
                if "Postmedia" in page_content:
                    print("[SEDI] Found 'Postmedia' in results!")
                    self.logger.info("Found 'Postmedia' in SEDI results!")
                if "Chatham" in page_content:
                    print("[SEDI] Found 'Chatham' in results!")
                    self.logger.info("Found 'Chatham' in SEDI results!")

                # Parse the page with lxml to find insider IDs
                from lxml import html
                tree = html.fromstring(page_content)

                # Get all radio button values (insider IDs)
                # SEDI uses name='UID_INSPR' with numeric values for insider selection
                insider_ids = tree.xpath("//input[@type='RADIO' or @type='radio'][@name='UID_INSPR']/@value")
                print(f"[SEDI] Found {len(insider_ids)} insider radio buttons")
                self.logger.debug(f"Found {len(insider_ids)} insider IDs")

                count = 0

                # For each insider ID, submit form to get their profile
                for insider_id in insider_ids:
                    if limit and count >= limit:
                        break

                    try:
                        # Select the radio button for this insider
                        radio = page.locator(f"input[name='UID_INSPR'][value='{insider_id}']")
                        if await radio.count() > 0:
                            await radio.click()
                            await page.wait_for_timeout(300)

                            # Click Next button to view profile
                            next_btn = page.locator("input[name='Next']")
                            if await next_btn.count() > 0:
                                await next_btn.click()
                                await page.wait_for_load_state("load", timeout=15000)
                                await page.wait_for_timeout(500)

                                # Parse the profile page
                                profile_content = await page.content()
                                profile_tree = html.fromstring(profile_content)

                                # Extract insider info from profile page
                                filing = self._parse_sedi_profile(profile_tree, company_name, url)

                                if filing:
                                    self.logger.info(
                                        f"Found SEDI insider (profile): {filing.acquirer_name} -> {filing.issuer_name} "
                                        f"({filing.ownership_percentage or '?'}%)"
                                    )
                                    count += 1
                                    yield filing

                                # Navigate back to search results by re-searching
                                # This is more reliable than page.go_back()
                                await page.goto(url, wait_until="load", timeout=15000)
                                await page.wait_for_timeout(3000)

                                # Re-fill the search form
                                issuer_input = page.locator("input[name='issuer_name']")
                                if await issuer_input.count() > 0:
                                    await issuer_input.first.fill(company_name)
                                    search_type = page.locator("select[name='ISSUER_NAME_SEARCH_TYPE']")
                                    if await search_type.count() > 0:
                                        await search_type.select_option("2")
                                    submit = page.locator("input[type='submit']")
                                    if await submit.count() > 0:
                                        await submit.first.click()
                                        await page.wait_for_load_state("load", timeout=15000)
                                        await page.wait_for_timeout(1000)

                    except Exception as e:
                        self.logger.debug(f"Error processing insider {insider_id}: {e}")
                        # Try to recover by navigating back to search
                        try:
                            await page.goto(url, wait_until="load", timeout=15000)
                            await page.wait_for_timeout(2000)
                        except Exception:
                            pass
                        continue

                # Fallback: parse from the search results page directly if no radio buttons
                if count == 0:
                    print("[SEDI] No radio buttons found, trying fallback parsing...")
                    self.logger.debug("No radio buttons found, falling back to direct parsing")

                    # SEDI uses nested tables with font tags for data
                    insider_fonts = tree.xpath("//td//font[string-length(normalize-space(text())) > 5]")
                    print(f"[SEDI] Fallback: Found {len(insider_fonts)} font elements to check")
                    self.logger.debug(f"Found {len(insider_fonts)} font elements")

                    seen_insiders = set()

                    for font in insider_fonts:
                        if limit and count >= limit:
                            break

                        text = font.text_content().strip()

                        # Skip navigation/UI elements
                        if not text or len(text) < 3:
                            continue
                        if text.lower() in ["view insider information", "view issuer information", "view summary reports"]:
                            continue
                        if text.startswith("Insider") or text.startswith("Issuer name"):
                            continue
                        if "search criteria" in text.lower():
                            continue

                        # Look for corporate entities (LLC, Ltd, Corp, LP, Fund, etc.)
                        is_corporate = any(suffix in text for suffix in [
                            "LLC", "Ltd", "Corp", "LP", "Fund", "Inc", "Asset", "Capital", "Management"
                        ])

                        # Also include if it's a person name (two+ words, capitalized)
                        words = text.split()
                        is_person = len(words) >= 2 and all(w[0].isupper() for w in words if w)

                        if not is_corporate and not is_person:
                            continue

                        # Skip duplicates
                        if text in seen_insiders:
                            continue
                        seen_insiders.add(text)

                        # Try to find the insider ID
                        insider_id = None
                        parent_row = font.getparent()
                        while parent_row is not None and parent_row.tag != "tr":
                            parent_row = parent_row.getparent()

                        if parent_row is not None:
                            row_text = parent_row.text_content()
                            id_match = re.search(r'\b([A-Z]+\d{3,}[A-Z0-9]*)\b', row_text)
                            if id_match:
                                insider_id = id_match.group(1)

                        filing = SEDARFiling(
                            document_id=f"sedi_pw_{hashlib.sha256(text.encode()).hexdigest()[:8]}",
                            document_type="sedi_insider",
                            filing_date=date.today(),
                            acquirer_name=text,
                            acquirer_sedar_profile=insider_id,
                            issuer_name=company_name,
                            issuer_sedar_profile=None,
                            ownership_percentage=None,
                            shares_owned=None,
                            share_class="Common",
                            document_url=url,
                            content_type="text/html",
                        )

                        print(f"[SEDI] Found insider (fallback): {text}")
                        self.logger.info(
                            f"Found SEDI insider (fallback): {text} ({insider_id or 'no ID'}) -> {company_name}"
                        )

                        count += 1
                        yield filing

                print(f"[SEDI] Total insiders found: {count}")
                if count == 0:
                    print(f"[SEDI] No insiders found! Page preview: {tree.text_content()[:300]}")
                    self.logger.debug(f"No insider records found. Page text preview: {tree.text_content()[:500]}")

            except Exception as e:
                self.logger.warning(f"Playwright SEDI search failed: {e}")
                import traceback
                self.logger.debug(f"Traceback: {traceback.format_exc()}")

            finally:
                await browser.close()

    def _parse_sedi_profile(self, tree, default_issuer: str, url: str) -> SEDARFiling | None:
        """Parse a SEDI insider profile page.

        Extracts detailed ownership information including:
        - Insider name (family name + given names)
        - Issuer name and number
        - Relationship to issuer (e.g., "10% Security Holder of Issuer")
        - Date became insider
        - Date ceased being insider (if applicable)

        Args:
            tree: lxml HTML tree of the profile page
            default_issuer: Default issuer name if not found on page
            url: Source URL

        Returns:
            SEDARFiling or None if parsing fails
        """
        try:
            # Extract text helper
            def get_field(label: str) -> str | None:
                """Find a field value by its label."""
                # Look for td with label followed by td with value
                xpath = f"//td[contains(., '{label}')]/following-sibling::td[1]//font/text()"
                values = tree.xpath(xpath)
                if values:
                    return values[0].strip()
                # Try without font tag
                xpath2 = f"//td[contains(., '{label}')]/following-sibling::td[1]/text()"
                values2 = tree.xpath(xpath2)
                if values2:
                    return values2[0].strip()
                return None

            # Extract insider name
            family_name = get_field("Family name")
            given_names = get_field("Given names")

            insider_name = None
            if family_name and given_names:
                insider_name = f"{given_names} {family_name}"
            elif family_name:
                insider_name = family_name
            else:
                # Try to find corporate name (for non-individuals)
                # Corporate insiders have "Insider name" field instead of Family/Given names
                corp_name = get_field("Insider name")
                if corp_name:
                    insider_name = corp_name
                else:
                    # Try "Corporate name" field
                    corp_name = get_field("Corporate name")
                    if corp_name:
                        insider_name = corp_name
                    else:
                        # Look for any font text that looks like a corporate name
                        # (contains LLC, Inc, Corp, LP, Fund, Management, Capital, etc.)
                        corp_fonts = tree.xpath("//td//font/text()")
                        corp_suffixes = ["LLC", "Inc", "Corp", "LP", "Fund", "Management", "Capital", "Asset", "Partners", "Holdings"]
                        for text in corp_fonts:
                            text = text.strip()
                            if text and len(text) > 5 and any(suffix in text for suffix in corp_suffixes):
                                insider_name = text
                                self.logger.debug(f"Found corporate insider via suffix match: {text}")
                                break

                        if not insider_name:
                            # Last resort: look in page title or header
                            title_fonts = tree.xpath("//b/font/text() | //h1//text() | //h2//text()")
                            for text in title_fonts:
                                text = text.strip()
                                if text and len(text) > 10 and "Insider" not in text and "Profile" not in text:
                                    insider_name = text
                                    self.logger.debug(f"Found insider name in header: {text}")
                                    break

            if not insider_name:
                self.logger.debug("Could not find insider name on profile page")
                return None

            # Extract issuer info
            issuer_name = get_field("Issuer name") or default_issuer
            issuer_number = get_field("Issuer number")

            # Extract relationship (key field!)
            relationship = None
            rel_xpath = "//td[contains(., 'relationship to issuer')]/following-sibling::td//font/text()"
            rel_values = tree.xpath(rel_xpath)
            for val in rel_values:
                val = val.strip()
                if val and len(val) > 3:
                    relationship = val
                    break

            # Check if 10% security holder
            is_10_percent = relationship and "10%" in relationship.lower()
            ownership_pct = 10.0 if is_10_percent else None

            # Extract dates
            date_became = get_field("Date the insider became")
            date_ceased = get_field("Date the insider ceased")

            # Parse date
            filing_date = date.today()
            if date_became:
                try:
                    filing_date = datetime.strptime(date_became, "%Y-%m-%d").date()
                except ValueError:
                    pass

            # Create filing record
            filing = SEDARFiling(
                document_id=f"sedi_profile_{hashlib.sha256(insider_name.encode()).hexdigest()[:8]}",
                document_type="sedi_insider",
                filing_date=filing_date,
                acquirer_name=insider_name,
                acquirer_sedar_profile=None,
                issuer_name=issuer_name,
                issuer_sedar_profile=issuer_number,
                ownership_percentage=ownership_pct,
                shares_owned=None,
                share_class="Common",
                document_url=url,
                content_type="text/html",
            )

            # Log the relationship found
            if relationship:
                self.logger.debug(f"Profile: {insider_name} - {relationship}")

            return filing

        except Exception as e:
            self.logger.debug(f"Error parsing SEDI profile: {e}")
            return None

    async def _get_sedi_session(self) -> tuple[httpx.AsyncClient, str]:
        """Initialize a SEDI session and get the CSRF token.

        SEDI requires a session cookie (JSESSIONID) and CSRF token (jspSynchronizerToken)
        for all form submissions.

        Returns:
            Tuple of (client with session cookies, CSRF token)
        """
        from lxml import html

        # Create a new client for this session (with cookies enabled)
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "User-Agent": SEDI_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-CA,en-US;q=0.7,en;q=0.3",
            },
            follow_redirects=True,
        )

        # GET the search page to obtain session cookie and CSRF token
        url = f"{SEDI_INSIDER_SEARCH_URL}?menukey=15.01.00&locale=en_CA"
        self.logger.debug(f"Getting SEDI session from: {url}")

        response = await client.get(url)
        response.raise_for_status()

        # Extract CSRF token from the form
        tree = html.fromstring(response.content)
        csrf_inputs = tree.xpath("//input[@name='jspSynchronizerToken']/@value")

        if not csrf_inputs:
            # Try alternate extraction from script or hidden field
            csrf_inputs = tree.xpath("//*[@id='jspSynchronizerToken']/@value")

        if not csrf_inputs:
            # Search in page text for the token
            csrf_match = re.search(r'jspSynchronizerToken["\s]*(?:value)?[=:]["\s]*([A-Za-z0-9_-]+)', response.text)
            csrf_token = csrf_match.group(1) if csrf_match else ""
        else:
            csrf_token = csrf_inputs[0]

        self.logger.debug(f"Got SEDI session, CSRF token: {csrf_token[:20]}..." if csrf_token else "No CSRF token found")

        return client, csrf_token

    async def _search_sedi_by_name(
        self, company_name: str, limit: int | None = None
    ) -> AsyncIterator[SEDARFiling]:
        """Search SEDI for a company by name and fetch insider profiles.

        SEDI has bot protection (ShieldSquare/Radware Captcha) that requires
        JavaScript execution. This method uses Playwright (if available) for
        browser automation, falling back to HTTP requests.

        Args:
            company_name: Company name to search for
            limit: Maximum number of results to process

        Yields:
            SEDARFiling records for insider relationships found
        """
        print(f"[SEDI] _search_sedi_by_name called for: {company_name}")
        # Use Playwright if available (required due to bot protection)
        if PLAYWRIGHT_AVAILABLE:
            print(f"[SEDI] Playwright IS available, proceeding...")
            self.logger.info("Using Playwright for SEDI search (bot protection bypass)")
            async for filing in self._search_sedi_with_playwright(company_name, limit):
                yield filing
            return

        # Fallback to HTTP (likely will be blocked by bot protection)
        self.logger.warning(
            "Playwright not available - SEDI has bot protection. "
            "Install Playwright: pip install playwright && playwright install chromium"
        )

        from lxml import html

        client = None
        try:
            # Step 1: Initialize SEDI session
            client, csrf_token = await self._get_sedi_session()

            # Step 2: POST the search form
            search_url = f"{SEDI_INSIDER_SEARCH_URL}?menukey=15.01.00&locale=en_CA"

            # Form data based on actual browser request
            form_data = {
                "jspSynchronizerToken": csrf_token,
                "ISSUER_NAME_SEARCH_TYPE": "2",  # Contains search
                "issuer_name": company_name,
                "INSIDER_NAME_SEARCH_TYPE": "1",  # Starts with
                "insider_name": "",
                "INSIDER_CIK_ID": "",
                "DATE_RANGE_TYPE": "3",  # All dates
                "date_from": "",
                "date_to": "",
                "SECURITY_ROLE_LIST": "",  # All roles
                "ISSUER_TYPE_ID": "",  # All issuer types
                "DATE_OF_FILING": "1",  # Any
                "TRANSACTION_TYPE_ID": "",  # All transaction types
            }

            self.logger.info(f"Searching SEDI for issuer: {company_name}")

            response = await client.post(
                search_url,
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://www.sedi.ca",
                    "Referer": search_url,
                },
            )
            response.raise_for_status()

            # Step 3: Parse search results
            tree = html.fromstring(response.content)

            # Check for error messages
            error_msgs = tree.xpath("//div[@class='error']//text() | //span[@class='error']//text()")
            if error_msgs:
                error_text = " ".join(e.strip() for e in error_msgs if e.strip())
                if error_text:
                    self.logger.warning(f"SEDI search error: {error_text}")

            # Find insider results table
            # SEDI results are typically in a table with class 'sediTable' or similar
            result_tables = tree.xpath("//table[@class='sediTable'] | //table[contains(@class, 'results')]")

            if not result_tables:
                # Check if we got a "no results" message
                no_results = tree.xpath("//*[contains(text(), 'No records found') or contains(text(), 'no results')]")
                if no_results:
                    self.logger.info(f"No SEDI results found for: {company_name}")
                    return

                # Log page content for debugging
                page_text = tree.text_content()[:500]
                self.logger.debug(f"SEDI response (first 500 chars): {page_text}")

                # Try to find any table with insider data
                all_tables = tree.xpath("//table")
                self.logger.debug(f"Found {len(all_tables)} tables on page")

            # Process results
            count = 0
            for table in result_tables:
                rows = table.xpath(".//tr[position()>1]")  # Skip header row

                for row in rows:
                    if limit and count >= limit:
                        return

                    try:
                        cells = row.xpath(".//td")
                        if len(cells) < 3:
                            continue

                        # Extract insider information from row
                        insider_name = cells[0].text_content().strip()
                        issuer_name_cell = cells[1].text_content().strip() if len(cells) > 1 else ""
                        relationship = cells[2].text_content().strip() if len(cells) > 2 else ""
                        filing_date_str = cells[3].text_content().strip() if len(cells) > 3 else ""

                        if not insider_name:
                            continue

                        # Check if this is a 10%+ security holder
                        is_10_percent = "10%" in relationship.lower() or "security holder" in relationship.lower()

                        # Parse date
                        try:
                            filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            filing_date = date.today()

                        # Extract issuer number from any links in the row
                        issuer_links = row.xpath(".//a[contains(@href, 'issuer')]/@href")
                        issuer_number = None
                        for href in issuer_links:
                            match = re.search(r"(?:issuer|Issuer)(?:Number|Id)?[=:]?(\d{6,8})", href)
                            if match:
                                issuer_number = match.group(1)
                                break

                        # Create filing record
                        filing = SEDARFiling(
                            document_id=f"sedi_search_{hashlib.sha256((insider_name + issuer_name_cell).encode()).hexdigest()[:8]}",
                            document_type="sedi_insider",
                            filing_date=filing_date,
                            acquirer_name=insider_name,
                            acquirer_sedar_profile=None,
                            issuer_name=issuer_name_cell or company_name,
                            issuer_sedar_profile=issuer_number,
                            ownership_percentage=10.0 if is_10_percent else None,
                            shares_owned=None,
                            share_class="Common",
                            document_url=f"https://www.sedi.ca/sedi/SVTSelectSediInsider?menukey=15.01.00&locale=en_CA",
                            content_type="text/html",
                        )

                        self.logger.info(
                            f"Found SEDI insider: {insider_name} -> {issuer_name_cell} "
                            f"({relationship})"
                        )

                        count += 1
                        yield filing

                    except Exception as e:
                        self.logger.debug(f"Error parsing SEDI result row: {e}")
                        continue

            # If no results from table parsing, try alternate parsing
            if count == 0:
                # Look for any links containing insider information
                insider_links = tree.xpath("//a[contains(@href, 'Insider') or contains(@href, 'insider')]")
                self.logger.debug(f"Found {len(insider_links)} insider-related links")

                # Try to extract issuer info from the page
                issuer_info = tree.xpath("//td[contains(text(), 'Issuer')]/following-sibling::td/text()")
                if issuer_info:
                    self.logger.debug(f"Found issuer info: {issuer_info[:3]}")

        except Exception as e:
            self.logger.warning(f"Failed to search SEDI for {company_name}: {e}")
            import traceback
            self.logger.debug(f"SEDI search traceback: {traceback.format_exc()}")

        finally:
            if client:
                await client.aclose()

    async def process_record(self, record: SEDARFiling) -> dict[str, Any]:
        """Process a SEDAR+ filing record (T035).

        Creates/updates:
        - Acquirer organization in PostgreSQL and Neo4j
        - Issuer organization in PostgreSQL and Neo4j
        - OWNS relationship between them
        - Evidence record linking to source document

        Args:
            record: Parsed SEDAR+ filing

        Returns:
            Processing result with entity IDs and status
        """
        import json

        result = {"created": False, "updated": False, "entity_id": None, "cross_source_match": False}

        # --- Step 1: PostgreSQL entity and evidence upsert with entity resolution (T049) ---
        async with get_db_session() as db:
            from sqlalchemy import text

            # --- Upsert acquirer (owner) with entity resolution ---
            acquirer_ext_ids = {}
            if record.acquirer_sedar_profile:
                acquirer_ext_ids["sedar_profile"] = record.acquirer_sedar_profile

            # First, try to find by SEDAR profile (highest priority)
            acquirer_id = None
            acquirer_match_confidence = 0.0
            acquirer_match_type = None

            if record.acquirer_sedar_profile:
                existing_by_profile = await db.execute(
                    text("""
                        SELECT id FROM entities
                        WHERE external_ids->>'sedar_profile' = :profile
                    """),
                    {"profile": record.acquirer_sedar_profile},
                )
                row = existing_by_profile.fetchone()
                if row:
                    acquirer_id = row.id
                    acquirer_match_confidence = 1.0
                    acquirer_match_type = "identifier"

            # Second, try fuzzy name matching if no identifier match
            if not acquirer_id:
                found_id, confidence = await find_existing_entity_by_name(
                    db, record.acquirer_name, "organization", threshold=0.85
                )
                if found_id:
                    acquirer_id = found_id
                    acquirer_match_confidence = confidence
                    acquirer_match_type = "fuzzy" if confidence < 1.0 else "exact"

                    # Check if this entity has SEC CIK (cross-source match!)
                    check_cross = await db.execute(
                        text("""
                            SELECT external_ids->>'sec_cik' as cik
                            FROM entities WHERE id = :id
                        """),
                        {"id": found_id},
                    )
                    cross_row = check_cross.fetchone()
                    if cross_row and cross_row.cik:
                        result["cross_source_match"] = True
                        self.logger.info(
                            f"Cross-source match: {record.acquirer_name} "
                            f"(SEDAR) -> CIK {cross_row.cik} (SEC)"
                        )

            if acquirer_id:
                # Update existing entity with SEDAR profile
                if acquirer_ext_ids:
                    await db.execute(
                        text("""
                            UPDATE entities
                            SET external_ids = COALESCE(external_ids, '{}'::jsonb) || CAST(:ext_ids AS jsonb),
                                updated_at = NOW()
                            WHERE id = :id
                        """),
                        {"id": acquirer_id, "ext_ids": json.dumps(acquirer_ext_ids)},
                    )
                result["updated"] = True
            else:
                # Create new entity
                acquirer_id = uuid4()
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, 'organization', CAST(:ext_ids AS jsonb), '{}'::jsonb, NOW())
                    """),
                    {
                        "id": acquirer_id,
                        "name": record.acquirer_name,
                        "ext_ids": json.dumps(acquirer_ext_ids),
                    },
                )
                result["created"] = True
                acquirer_match_confidence = 1.0  # New entity

            # --- Upsert issuer (subject) with entity resolution ---
            issuer_ext_ids = {}
            if record.issuer_sedar_profile:
                issuer_ext_ids["sedar_profile"] = record.issuer_sedar_profile

            issuer_id = None
            issuer_match_confidence = 0.0

            # First, try to find by SEDAR profile
            if record.issuer_sedar_profile:
                existing_by_profile = await db.execute(
                    text("""
                        SELECT id FROM entities
                        WHERE external_ids->>'sedar_profile' = :profile
                    """),
                    {"profile": record.issuer_sedar_profile},
                )
                row = existing_by_profile.fetchone()
                if row:
                    issuer_id = row.id
                    issuer_match_confidence = 1.0

            # Second, try fuzzy name matching
            if not issuer_id:
                found_id, confidence = await find_existing_entity_by_name(
                    db, record.issuer_name, "organization", threshold=0.85
                )
                if found_id:
                    issuer_id = found_id
                    issuer_match_confidence = confidence

                    # Check for cross-source match
                    check_cross = await db.execute(
                        text("""
                            SELECT external_ids->>'sec_cik' as cik
                            FROM entities WHERE id = :id
                        """),
                        {"id": found_id},
                    )
                    cross_row = check_cross.fetchone()
                    if cross_row and cross_row.cik:
                        if not result["cross_source_match"]:
                            result["cross_source_match"] = True
                        self.logger.info(
                            f"Cross-source match: {record.issuer_name} "
                            f"(SEDAR) -> CIK {cross_row.cik} (SEC)"
                        )

            if issuer_id:
                if issuer_ext_ids:
                    await db.execute(
                        text("""
                            UPDATE entities
                            SET external_ids = COALESCE(external_ids, '{}'::jsonb) || CAST(:ext_ids AS jsonb),
                                updated_at = NOW()
                            WHERE id = :id
                        """),
                        {"id": issuer_id, "ext_ids": json.dumps(issuer_ext_ids)},
                    )
            else:
                issuer_id = uuid4()
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, 'organization', CAST(:ext_ids AS jsonb), '{}'::jsonb, NOW())
                    """),
                    {
                        "id": issuer_id,
                        "name": record.issuer_name,
                        "ext_ids": json.dumps(issuer_ext_ids),
                    },
                )
                if not result["created"]:
                    result["created"] = True
                issuer_match_confidence = 1.0

            result["entity_id"] = str(acquirer_id)
            result["acquirer_confidence"] = acquirer_match_confidence
            result["issuer_confidence"] = issuer_match_confidence

            await db.commit()

            # Evidence record
            evidence_id = uuid4()
            source_url = record.document_url or f"sedar://document/{record.document_id}"
            await db.execute(
                text("""
                    INSERT INTO evidence (id, evidence_type, source_url, retrieved_at, extractor, extractor_version, raw_data_ref, extraction_confidence, content_hash)
                    VALUES (:id, :evidence_type, :source_url, NOW(), :extractor, :version, :raw_ref, :confidence, :hash)
                """),
                {
                    "id": evidence_id,
                    "evidence_type": EvidenceType.SEDAR_FILING.value,
                    "source_url": source_url,
                    "extractor": "sedar_ingester",
                    "version": "1.0.0",
                    "raw_ref": f"sedar/{record.document_id}",
                    "confidence": 0.85,
                    "hash": record.raw_content_hash or compute_content_hash(
                        record.model_dump_json().encode("utf-8")
                    ),
                },
            )
            await db.commit()

        # --- Step 2: Neo4j nodes and OWNS relationship with entity resolution (T050) ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Create/update acquirer node (Canadian owner)
                # Use entity resolution: try SEDAR profile first, then name match
                acquirer_props = {
                    "id": str(acquirer_id),
                    "name": record.acquirer_name,
                    "entity_type": "ORGANIZATION",
                    "org_type": "corporation",
                    "status": "active",
                    "jurisdiction": "CA",
                    "is_canadian": True,
                    "confidence": acquirer_match_confidence,
                    "updated_at": now,
                }
                if record.acquirer_sedar_profile:
                    acquirer_props["sedar_profile"] = record.acquirer_sedar_profile

                # First try to match by SEDAR profile if available
                if record.acquirer_sedar_profile:
                    await session.run(
                        """
                        MERGE (o:Organization {sedar_profile: $sedar_profile})
                        ON CREATE SET o += $props, o.created_at = $now
                        ON MATCH SET o.updated_at = $now,
                                     o.name = COALESCE(o.name, $name),
                                     o.is_canadian = true,
                                     o.jurisdiction = COALESCE(o.jurisdiction, 'CA')
                        """,
                        sedar_profile=record.acquirer_sedar_profile,
                        props=acquirer_props,
                        now=now,
                        name=record.acquirer_name,
                    )
                else:
                    # Fall back to name-based merge with cross-source linking
                    await session.run(
                        """
                        MERGE (o:Organization {name: $name})
                        ON CREATE SET o += $props, o.created_at = $now
                        ON MATCH SET o.updated_at = $now,
                                     o.sedar_profile = COALESCE($sedar_profile, o.sedar_profile),
                                     o.is_canadian = true,
                                     o.jurisdiction = COALESCE(o.jurisdiction, 'CA')
                        """,
                        name=record.acquirer_name,
                        props=acquirer_props,
                        now=now,
                        sedar_profile=record.acquirer_sedar_profile,
                    )

                # Create/update issuer node (Canadian subject company) with entity resolution
                issuer_props = {
                    "id": str(issuer_id),
                    "name": record.issuer_name,
                    "entity_type": "ORGANIZATION",
                    "org_type": "corporation",
                    "status": "active",
                    "jurisdiction": "CA",
                    "is_canadian": True,
                    "confidence": issuer_match_confidence,
                    "updated_at": now,
                }
                if record.issuer_sedar_profile:
                    issuer_props["sedar_profile"] = record.issuer_sedar_profile

                # First try to match by SEDAR profile if available
                if record.issuer_sedar_profile:
                    await session.run(
                        """
                        MERGE (o:Organization {sedar_profile: $sedar_profile})
                        ON CREATE SET o += $props, o.created_at = $now
                        ON MATCH SET o.updated_at = $now,
                                     o.name = COALESCE(o.name, $name),
                                     o.is_canadian = true,
                                     o.jurisdiction = COALESCE(o.jurisdiction, 'CA')
                        """,
                        sedar_profile=record.issuer_sedar_profile,
                        props=issuer_props,
                        now=now,
                        name=record.issuer_name,
                    )
                else:
                    # Fall back to name-based merge with cross-source linking
                    await session.run(
                        """
                        MERGE (o:Organization {name: $name})
                        ON CREATE SET o += $props, o.created_at = $now
                        ON MATCH SET o.updated_at = $now,
                                     o.sedar_profile = COALESCE($sedar_profile, o.sedar_profile),
                                     o.is_canadian = true,
                                     o.jurisdiction = COALESCE(o.jurisdiction, 'CA')
                        """,
                        name=record.issuer_name,
                        props=issuer_props,
                        now=now,
                        sedar_profile=record.issuer_sedar_profile,
                    )

                # Create OWNS relationship: acquirer -> issuer
                # Use flexible matching to handle cross-source entities
                owns_props = {
                    "id": str(uuid4()),
                    "source": "sedar",
                    "confidence": min(acquirer_match_confidence, issuer_match_confidence),
                    "filing_document_id": record.document_id,
                    "form_type": record.document_type,
                    "filing_date": record.filing_date.isoformat(),
                    "updated_at": now,
                }

                if record.ownership_percentage is not None:
                    owns_props["ownership_percentage"] = record.ownership_percentage
                if record.shares_owned is not None:
                    owns_props["shares_owned"] = record.shares_owned
                if record.share_class:
                    owns_props["share_class"] = record.share_class

                # Match by SEDAR profile if available, otherwise by name
                if record.acquirer_sedar_profile and record.issuer_sedar_profile:
                    await session.run(
                        """
                        MATCH (owner:Organization {sedar_profile: $owner_profile})
                        MATCH (subject:Organization {sedar_profile: $subject_profile})
                        MERGE (owner)-[r:OWNS]->(subject)
                        SET r += $props
                        """,
                        owner_profile=record.acquirer_sedar_profile,
                        subject_profile=record.issuer_sedar_profile,
                        props=owns_props,
                    )
                else:
                    await session.run(
                        """
                        MATCH (owner:Organization {name: $owner_name})
                        MATCH (subject:Organization {name: $subject_name})
                        MERGE (owner)-[r:OWNS]->(subject)
                        SET r += $props
                        """,
                        owner_name=record.acquirer_name,
                        subject_name=record.issuer_name,
                        props=owns_props,
                    )

                # Log with cross-source indicator
                cross_source_marker = " [CROSS-SOURCE]" if result.get("cross_source_match") else ""
                self.logger.info(
                    f"OWNS: {record.acquirer_name} -> {record.issuer_name}{cross_source_marker} "
                    f"({record.ownership_percentage or '?'}% via {record.document_type})"
                )

        except Exception as e:
            self.logger.warning(
                f"Neo4j write failed for {record.acquirer_name} -> {record.issuer_name}: {e}"
            )

        return result


# =============================================================================
# Entry Point (T037)
# =============================================================================


async def run_sedar_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    csv_path: str | None = None,
    document_types: list[str] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run SEDAR+ ingestion (T037).

    Args:
        incremental: Whether to do incremental sync
        limit: Maximum number of filings to process
        target_entities: List of company names, SEDAR profiles, or document URLs
        csv_path: Path to CSV export from SEDAR+ web interface
        document_types: Types of documents to process (early_warning, alternative_monthly)
        date_from: Start date for filing search
        date_to: End date for filing search
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = SEDARIngester()

    try:
        extra_params = {}
        if csv_path:
            extra_params["csv_path"] = csv_path
        if document_types:
            extra_params["document_types"] = document_types

        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
            date_from=datetime.combine(date_from, datetime.min.time()) if date_from else None,
            date_to=datetime.combine(date_to, datetime.max.time()) if date_to else None,
            extra_params=extra_params,
        )

        result = await ingester.run(config, run_id=run_id)
        return result.model_dump()
    finally:
        await ingester.close()
