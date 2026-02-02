"""LinkedIn member ingestion for network research.

This module ingests LinkedIn company member data to map out organizational
networks. It supports multiple ingestion methods:

1. CSV Import: Import exported LinkedIn data (Sales Navigator, LinkedIn exports)
2. Browser Automation: Use Playwright to scrape public company pages (with authentication)
3. Manual Entry: API-based addition of member data

Due to LinkedIn's Terms of Service and anti-scraping measures:
- Browser automation requires valid LinkedIn session cookies
- Rate limiting is aggressive to avoid account restrictions
- Public profile data only (no InMail, connection requests, etc.)

Usage:
    # Import from CSV
    mitds ingest linkedin --company "Postmedia Network" --from-csv members.csv

    # Browser scraping (requires authentication)
    mitds ingest linkedin --company "Postmedia Network" --scrape --headless

    # Target specific company URL
    mitds ingest linkedin --company-url "https://www.linkedin.com/company/postmedia"
"""

import asyncio
import csv
import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import (
    BaseIngester,
    IngestionConfig,
    IngestionResult,
    Neo4jHelper,
    PostgresHelper,
)

logger = get_context_logger(__name__)


# =========================
# Data Models
# =========================


class LinkedInPosition(BaseModel):
    """A position/role held at a company."""

    title: str
    company_name: str
    company_linkedin_id: str | None = None
    company_linkedin_url: str | None = None
    is_current: bool = True
    start_date: date | None = None
    end_date: date | None = None
    location: str | None = None
    description: str | None = None


class LinkedInProfile(BaseModel):
    """LinkedIn member profile record.

    Represents a person from LinkedIn with their professional history
    and organizational affiliations.
    """

    # Core identity
    linkedin_id: str = Field(..., description="LinkedIn member URN or vanity URL slug")
    name: str = Field(..., min_length=1, description="Full name")
    headline: str | None = Field(None, description="Professional headline")

    # Profile URLs
    profile_url: str | None = Field(None, description="Full LinkedIn profile URL")
    public_profile_url: str | None = Field(None, description="Public profile URL if available")

    # Current position (primary company affiliation)
    current_company: str | None = Field(None, description="Current company name")
    current_title: str | None = Field(None, description="Current job title")
    current_company_linkedin_id: str | None = Field(None, description="LinkedIn company ID")

    # Location
    location: str | None = Field(None, description="Location from profile")
    country: str | None = Field(None, description="Country code")

    # Additional profile data
    connections: int | None = Field(None, description="Number of connections (if visible)")
    summary: str | None = Field(None, description="Profile summary/about section")
    industry: str | None = Field(None, description="Industry from profile")

    # Position history (for building employment timeline)
    positions: list[LinkedInPosition] = Field(default_factory=list, description="Employment history")

    # Metadata
    scraped_at: datetime | None = Field(None, description="When profile was scraped")
    source: str = Field(default="linkedin", description="Data source")

    @field_validator("linkedin_id")
    @classmethod
    def normalize_linkedin_id(cls, v: str) -> str:
        """Normalize LinkedIn ID to consistent format."""
        # Extract ID from URL if provided
        if "linkedin.com" in v:
            # Extract from /in/username or /company/name
            match = re.search(r"/in/([^/?]+)", v)
            if match:
                return match.group(1)
            match = re.search(r"urn:li:member:(\d+)", v)
            if match:
                return match.group(1)
        return v.strip()

    @property
    def profile_hash(self) -> str:
        """Generate hash for change detection."""
        data = f"{self.name}|{self.current_company or ''}|{self.current_title or ''}"
        return hashlib.md5(data.encode()).hexdigest()

    @property
    def is_executive(self) -> bool:
        """Check if person appears to be an executive."""
        if not self.current_title:
            return False
        title_lower = self.current_title.lower()
        exec_keywords = [
            "ceo", "cfo", "coo", "cto", "cio", "cmo", "cpo", "cso",
            "chief", "president", "vice president", "vp",
            "director", "managing director", "executive",
            "partner", "principal", "founder", "owner",
            "general manager", "head of", "svp", "evp",
        ]
        return any(kw in title_lower for kw in exec_keywords)

    @property
    def is_board_member(self) -> bool:
        """Check if person appears to be a board member."""
        if not self.current_title:
            return False
        title_lower = self.current_title.lower()
        board_keywords = [
            "board member", "board director", "board of directors",
            "chairman", "chairwoman", "chairperson", "chair",
            "non-executive director", "independent director",
            "advisory board", "trustee",
        ]
        return any(kw in title_lower for kw in board_keywords)


class LinkedInCompany(BaseModel):
    """LinkedIn company profile record."""

    linkedin_id: str = Field(..., description="LinkedIn company ID")
    name: str = Field(..., description="Company name")
    vanity_name: str | None = Field(None, description="URL-friendly company name")
    description: str | None = Field(None, description="Company description")
    website: str | None = Field(None, description="Company website")
    industry: str | None = Field(None, description="Primary industry")
    company_size: str | None = Field(None, description="Employee count range")
    company_type: str | None = Field(None, description="Public, Private, etc.")
    headquarters: str | None = Field(None, description="Headquarters location")
    founded_year: int | None = Field(None, description="Year founded")
    specialties: list[str] = Field(default_factory=list, description="Company specialties")
    logo_url: str | None = Field(None, description="Company logo URL")

    # Relationship to existing entities
    matched_entity_id: UUID | None = Field(None, description="Matched organization entity")


# =========================
# CSV Parser
# =========================


def parse_linkedin_csv(csv_path: str | Path, company_filter: str | None = None) -> list[LinkedInProfile]:
    """Parse LinkedIn export CSV file.

    Supports common LinkedIn export formats:
    - Sales Navigator exports
    - LinkedIn Recruiter exports
    - Manual CSV with standard columns

    Expected columns (case-insensitive):
    - name / full_name / first_name + last_name
    - linkedin_url / profile_url / public_identifier
    - title / job_title / current_title / headline
    - company / current_company / company_name
    - location
    - connections (optional)

    Args:
        csv_path: Path to CSV file
        company_filter: Optional company name to filter results

    Returns:
        List of LinkedInProfile records
    """
    profiles = []
    csv_path = Path(csv_path)

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        # Try to detect delimiter
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)

        # Normalize column names
        if reader.fieldnames:
            normalized_fields = {col.lower().strip().replace(" ", "_"): col for col in reader.fieldnames}
        else:
            normalized_fields = {}

        def get_value(row: dict, *possible_keys: str) -> str | None:
            """Get value from row trying multiple possible column names."""
            for key in possible_keys:
                # Try exact match first
                if key in row and row[key]:
                    return row[key].strip()
                # Try normalized match
                key_lower = key.lower().replace(" ", "_")
                if key_lower in normalized_fields:
                    original_col = normalized_fields[key_lower]
                    if original_col in row and row[original_col]:
                        return row[original_col].strip()
            return None

        for row in reader:
            # Extract name
            name = get_value(row, "name", "full_name", "fullname")
            if not name:
                first = get_value(row, "first_name", "firstname", "first")
                last = get_value(row, "last_name", "lastname", "last")
                if first and last:
                    name = f"{first} {last}"

            if not name:
                continue

            # Extract LinkedIn ID from URL or direct field
            linkedin_url = get_value(
                row, "linkedin_url", "profile_url", "linkedin_profile_url",
                "url", "public_identifier", "linkedin"
            )
            linkedin_id = None
            if linkedin_url:
                # Extract ID from URL
                match = re.search(r"/in/([^/?]+)", linkedin_url)
                if match:
                    linkedin_id = match.group(1)
                else:
                    linkedin_id = linkedin_url

            if not linkedin_id:
                # Generate a hash-based ID
                linkedin_id = f"manual_{hashlib.md5(name.encode()).hexdigest()[:12]}"

            # Extract company
            company = get_value(
                row, "company", "current_company", "company_name",
                "organization", "employer"
            )

            # Apply company filter if specified
            if company_filter and company:
                if company_filter.lower() not in company.lower():
                    continue

            # Extract title
            title = get_value(
                row, "title", "job_title", "current_title",
                "headline", "position", "role"
            )

            # Extract location
            location = get_value(row, "location", "city", "region", "geography")

            # Extract connections
            connections_str = get_value(row, "connections", "connection_count")
            connections = None
            if connections_str:
                try:
                    connections = int(re.sub(r"[^\d]", "", connections_str))
                except ValueError:
                    pass

            profile = LinkedInProfile(
                linkedin_id=linkedin_id,
                name=name,
                headline=title,
                profile_url=linkedin_url,
                current_company=company,
                current_title=title,
                location=location,
                connections=connections,
                scraped_at=datetime.utcnow(),
                source="csv_import",
            )

            profiles.append(profile)

    logger.info(f"Parsed {len(profiles)} profiles from CSV")
    return profiles


# =========================
# Browser Automation
# =========================


class LinkedInScraper:
    """Playwright-based LinkedIn scraper.

    Requires valid LinkedIn session cookies for authenticated access.
    Implements aggressive rate limiting to avoid account restrictions.

    WARNING: Use at your own risk. Scraping LinkedIn may violate their ToS.
    """

    def __init__(
        self,
        headless: bool = True,
        cookies_file: str | None = None,
        session_cookie: str | None = None,
    ):
        self.headless = headless
        self.cookies_file = cookies_file
        self.session_cookie = session_cookie
        self._browser = None
        self._context = None
        self._page = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Initialize browser with LinkedIn session."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright is required for LinkedIn scraping. "
                "Install with: pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)

        # Create context with cookies
        self._context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )

        # Load cookies if provided
        if self.cookies_file:
            cookies = self._load_cookies_from_file(self.cookies_file)
            await self._context.add_cookies(cookies)
        elif self.session_cookie:
            await self._context.add_cookies([{
                "name": "li_at",
                "value": self.session_cookie,
                "domain": ".linkedin.com",
                "path": "/",
            }])

        self._page = await self._context.new_page()

    async def close(self):
        """Clean up browser resources."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if hasattr(self, "_playwright"):
            await self._playwright.stop()

    def _load_cookies_from_file(self, path: str) -> list[dict]:
        """Load cookies from JSON file (e.g., exported from browser extension)."""
        with open(path, "r") as f:
            cookies = json.load(f)
        # Normalize cookie format
        normalized = []
        for cookie in cookies:
            normalized.append({
                "name": cookie.get("name"),
                "value": cookie.get("value"),
                "domain": cookie.get("domain", ".linkedin.com"),
                "path": cookie.get("path", "/"),
            })
        return normalized

    async def is_authenticated(self) -> bool:
        """Check if session is authenticated."""
        if not self._page:
            return False

        await self._page.goto("https://www.linkedin.com/feed/", wait_until="networkidle")
        await asyncio.sleep(2)

        # Check for login redirect
        url = self._page.url
        return "login" not in url and "checkpoint" not in url

    async def get_company_employees(
        self,
        company_url: str | None = None,
        company_name: str | None = None,
        limit: int | None = None,
        titles_filter: list[str] | None = None,
    ) -> AsyncIterator[LinkedInProfile]:
        """Scrape company employees from LinkedIn.

        Args:
            company_url: LinkedIn company URL (e.g., linkedin.com/company/postmedia)
            company_name: Company name to search for
            limit: Maximum profiles to scrape
            titles_filter: Optional list of title keywords to filter

        Yields:
            LinkedInProfile records
        """
        if not self._page:
            raise RuntimeError("Scraper not started. Use async context manager.")

        # Navigate to company page
        if company_url:
            people_url = f"{company_url.rstrip('/')}/people/"
        elif company_name:
            # Search for company first
            search_url = f"https://www.linkedin.com/company/{company_name.lower().replace(' ', '-')}/people/"
            people_url = search_url
        else:
            raise ValueError("Either company_url or company_name required")

        logger.info(f"Navigating to: {people_url}")
        await self._page.goto(people_url, wait_until="networkidle")
        await asyncio.sleep(3)

        # Check if we can access the page
        if "login" in self._page.url:
            raise RuntimeError("Not authenticated. Provide valid LinkedIn session cookie.")

        count = 0
        page_num = 1
        max_pages = 50  # Safety limit

        while True:
            # Scroll to load more profiles
            await self._scroll_page()

            # Extract employee cards
            employee_cards = await self._page.query_selector_all(
                'div[data-view-name="search-entity-result-universal-template"]'
            )

            if not employee_cards:
                # Try alternative selector
                employee_cards = await self._page.query_selector_all(
                    'li.org-people-profile-card__profile-card-spacing'
                )

            for card in employee_cards:
                try:
                    profile = await self._parse_employee_card(card)
                    if profile:
                        # Apply title filter if specified
                        if titles_filter and profile.current_title:
                            title_lower = profile.current_title.lower()
                            if not any(t.lower() in title_lower for t in titles_filter):
                                continue

                        yield profile
                        count += 1

                        if limit and count >= limit:
                            return

                except Exception as e:
                    logger.warning(f"Failed to parse employee card: {e}")

            # Try to go to next page
            page_num += 1
            if page_num > max_pages:
                break

            next_button = await self._page.query_selector('button[aria-label="Next"]')
            if not next_button:
                break

            is_disabled = await next_button.get_attribute("disabled")
            if is_disabled:
                break

            await next_button.click()
            await asyncio.sleep(2)  # Rate limiting

    async def _scroll_page(self):
        """Scroll page to load lazy content."""
        for _ in range(3):
            await self._page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(0.5)
        await self._page.evaluate("window.scrollTo(0, 0)")

    async def _parse_employee_card(self, card) -> LinkedInProfile | None:
        """Parse an employee card element into a profile."""
        try:
            # Extract name
            name_elem = await card.query_selector('span[aria-hidden="true"]')
            if not name_elem:
                name_elem = await card.query_selector('.org-people-profile-card__profile-title')
            name = await name_elem.inner_text() if name_elem else None

            if not name or name == "LinkedIn Member":
                return None

            # Extract profile URL
            link_elem = await card.query_selector('a[href*="/in/"]')
            profile_url = await link_elem.get_attribute("href") if link_elem else None

            # Extract LinkedIn ID from URL
            linkedin_id = None
            if profile_url:
                match = re.search(r"/in/([^/?]+)", profile_url)
                if match:
                    linkedin_id = match.group(1)

            if not linkedin_id:
                linkedin_id = f"scraped_{hashlib.md5(name.encode()).hexdigest()[:12]}"

            # Extract title
            title_elem = await card.query_selector('.org-people-profile-card__profile-info')
            if not title_elem:
                title_elem = await card.query_selector('div.t-14')
            title = await title_elem.inner_text() if title_elem else None

            # Extract location
            location_elem = await card.query_selector('.t-black--light')
            location = await location_elem.inner_text() if location_elem else None

            return LinkedInProfile(
                linkedin_id=linkedin_id,
                name=name.strip(),
                profile_url=profile_url,
                current_title=title.strip() if title else None,
                headline=title.strip() if title else None,
                location=location.strip() if location else None,
                scraped_at=datetime.utcnow(),
                source="linkedin_scrape",
            )

        except Exception as e:
            logger.debug(f"Error parsing card: {e}")
            return None


# =========================
# Ingester Implementation
# =========================


class LinkedInIngester(BaseIngester[LinkedInProfile]):
    """LinkedIn member data ingester.

    Supports multiple data sources:
    - CSV import (recommended for bulk data)
    - Browser scraping (requires authentication)
    """

    def __init__(self):
        super().__init__("linkedin")
        self._neo4j = Neo4jHelper(logger)
        self._postgres = PostgresHelper(logger)

        # Configuration
        self._csv_path: str | None = None
        self._company_name: str | None = None
        self._company_url: str | None = None
        self._company_entity_id: UUID | None = None
        self._scrape_mode: bool = False
        self._headless: bool = True
        self._session_cookie: str | None = None
        self._cookies_file: str | None = None
        self._titles_filter: list[str] | None = None

    def configure(
        self,
        csv_path: str | None = None,
        company_name: str | None = None,
        company_url: str | None = None,
        company_entity_id: UUID | None = None,
        scrape: bool = False,
        headless: bool = True,
        session_cookie: str | None = None,
        cookies_file: str | None = None,
        titles_filter: list[str] | None = None,
    ):
        """Configure the ingester for a specific run."""
        self._csv_path = csv_path
        self._company_name = company_name
        self._company_url = company_url
        self._company_entity_id = company_entity_id
        self._scrape_mode = scrape
        self._headless = headless
        self._session_cookie = session_cookie
        self._cookies_file = cookies_file
        self._titles_filter = titles_filter

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[LinkedInProfile]:
        """Fetch LinkedIn profiles from configured source."""
        if self._csv_path:
            # CSV import mode
            profiles = parse_linkedin_csv(self._csv_path, self._company_name)
            for profile in profiles:
                # Apply target entity filter
                if config.target_entities:
                    if not any(
                        t.lower() in profile.name.lower()
                        for t in config.target_entities
                    ):
                        continue
                yield profile

        elif self._scrape_mode:
            # Browser scraping mode
            if not self._session_cookie and not self._cookies_file:
                raise ValueError(
                    "LinkedIn scraping requires authentication. "
                    "Provide --session-cookie or --cookies-file"
                )

            async with LinkedInScraper(
                headless=self._headless,
                session_cookie=self._session_cookie,
                cookies_file=self._cookies_file,
            ) as scraper:
                if not await scraper.is_authenticated():
                    raise RuntimeError(
                        "LinkedIn authentication failed. "
                        "Check your session cookie or login credentials."
                    )

                async for profile in scraper.get_company_employees(
                    company_url=self._company_url,
                    company_name=self._company_name,
                    limit=config.limit,
                    titles_filter=self._titles_filter,
                ):
                    # Set company context
                    if self._company_name and not profile.current_company:
                        profile.current_company = self._company_name
                    yield profile

        else:
            raise ValueError(
                "No data source configured. Use --from-csv or --scrape"
            )

    async def process_record(self, record: LinkedInProfile) -> dict[str, Any]:
        """Process a LinkedIn profile - store in PostgreSQL and Neo4j."""
        result = {"created": False, "updated": False, "duplicate": False, "entity_id": None}

        async with get_db_session() as db:
            # Check for existing person by LinkedIn ID
            check_query = text("""
                SELECT id, name FROM entities
                WHERE external_ids->>'linkedin_id' = :linkedin_id
                   OR external_ids->>'linkedin_url' = :profile_url
            """)
            check_result = await db.execute(check_query, {
                "linkedin_id": record.linkedin_id,
                "profile_url": record.profile_url or "",
            })
            existing = check_result.fetchone()

            if existing:
                entity_id = existing.id
                # Update existing record
                update_query = text("""
                    UPDATE entities
                    SET metadata = metadata || :metadata,
                        external_ids = external_ids || :external_ids,
                        updated_at = NOW()
                    WHERE id = :id
                """)
                await db.execute(update_query, {
                    "id": entity_id,
                    "metadata": json.dumps({
                        "linkedin_headline": record.headline,
                        "linkedin_title": record.current_title,
                        "linkedin_company": record.current_company,
                        "linkedin_location": record.location,
                        "linkedin_connections": record.connections,
                        "linkedin_source": record.source,
                        "linkedin_scraped_at": record.scraped_at.isoformat() if record.scraped_at else None,
                        "is_executive": record.is_executive,
                        "is_board_member": record.is_board_member,
                    }),
                    "external_ids": json.dumps({
                        "linkedin_id": record.linkedin_id,
                        "linkedin_url": record.profile_url,
                    }),
                })
                result["updated"] = True
            else:
                # Create new person entity
                entity_id = uuid4()
                insert_query = text("""
                    INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                    VALUES (:id, :name, 'person', :external_ids, :metadata, NOW())
                """)
                await db.execute(insert_query, {
                    "id": entity_id,
                    "name": record.name,
                    "external_ids": json.dumps({
                        "linkedin_id": record.linkedin_id,
                        "linkedin_url": record.profile_url,
                    }),
                    "metadata": json.dumps({
                        "source": "linkedin",
                        "linkedin_headline": record.headline,
                        "linkedin_title": record.current_title,
                        "linkedin_company": record.current_company,
                        "linkedin_location": record.location,
                        "linkedin_connections": record.connections,
                        "linkedin_source": record.source,
                        "linkedin_scraped_at": record.scraped_at.isoformat() if record.scraped_at else None,
                        "is_executive": record.is_executive,
                        "is_board_member": record.is_board_member,
                    }),
                })
                result["created"] = True

            result["entity_id"] = str(entity_id)

        # Neo4j operations
        try:
            await self._sync_to_neo4j(record, UUID(result["entity_id"]))
        except Exception as e:
            logger.warning(f"Neo4j sync failed for {record.name}: {e}")

        return result

    async def _sync_to_neo4j(self, record: LinkedInProfile, entity_id: UUID):
        """Sync profile and relationships to Neo4j."""
        async with get_neo4j_session() as session:
            # Create/update Person node
            person_query = """
            MERGE (p:Person {linkedin_id: $linkedin_id})
            ON CREATE SET
                p.id = $id,
                p.name = $name,
                p.created_at = datetime()
            SET
                p.headline = $headline,
                p.location = $location,
                p.current_title = $current_title,
                p.current_company = $current_company,
                p.is_executive = $is_executive,
                p.is_board_member = $is_board_member,
                p.linkedin_url = $linkedin_url,
                p.linkedin_connections = $connections,
                p.source = $source,
                p.updated_at = datetime()
            RETURN p.id as id
            """
            await session.run(person_query, {
                "id": str(entity_id),
                "linkedin_id": record.linkedin_id,
                "name": record.name,
                "headline": record.headline,
                "location": record.location,
                "current_title": record.current_title,
                "current_company": record.current_company,
                "is_executive": record.is_executive,
                "is_board_member": record.is_board_member,
                "linkedin_url": record.profile_url,
                "connections": record.connections,
                "source": record.source,
            })

            # Create employment relationship if company is known
            if record.current_company:
                # Try to find/create company node
                company_query = """
                MERGE (o:Organization {name: $company_name})
                ON CREATE SET
                    o.id = randomUUID(),
                    o.created_at = datetime(),
                    o.source = 'linkedin'
                RETURN o.id as id
                """
                await session.run(company_query, {"company_name": record.current_company})

                # Create relationship based on role type
                if record.is_board_member:
                    rel_type = "DIRECTOR_OF"
                else:
                    rel_type = "EMPLOYED_BY"

                rel_query = f"""
                MATCH (p:Person {{linkedin_id: $linkedin_id}})
                MATCH (o:Organization {{name: $company_name}})
                MERGE (p)-[r:{rel_type}]->(o)
                ON CREATE SET
                    r.id = randomUUID(),
                    r.created_at = datetime()
                SET
                    r.title = $title,
                    r.source = 'linkedin',
                    r.is_current = true,
                    r.updated_at = datetime()
                """
                await session.run(rel_query, {
                    "linkedin_id": record.linkedin_id,
                    "company_name": record.current_company,
                    "title": record.current_title,
                })

            # Link to specific company entity if provided
            if self._company_entity_id:
                link_query = """
                MATCH (p:Person {linkedin_id: $linkedin_id})
                MATCH (o:Organization {id: $org_id})
                MERGE (p)-[r:EMPLOYED_BY]->(o)
                ON CREATE SET
                    r.id = randomUUID(),
                    r.created_at = datetime()
                SET
                    r.title = $title,
                    r.source = 'linkedin',
                    r.is_current = true,
                    r.updated_at = datetime()
                """
                await session.run(link_query, {
                    "linkedin_id": record.linkedin_id,
                    "org_id": str(self._company_entity_id),
                    "title": record.current_title,
                })

    async def get_last_sync_time(self) -> datetime | None:
        """Get timestamp of last successful sync."""
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
        """Save sync timestamp (handled by base class)."""
        pass


# =========================
# Convenience Functions
# =========================


async def run_linkedin_ingestion(
    csv_path: str | None = None,
    company_name: str | None = None,
    company_url: str | None = None,
    company_entity_id: str | None = None,
    scrape: bool = False,
    headless: bool = True,
    session_cookie: str | None = None,
    cookies_file: str | None = None,
    titles_filter: list[str] | None = None,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
    **extra_params,
) -> dict[str, Any]:
    """Run LinkedIn member ingestion.

    Args:
        csv_path: Path to CSV file with LinkedIn data
        company_name: Company name to filter/search
        company_url: LinkedIn company URL for scraping
        company_entity_id: UUID of existing organization entity to link members to
        scrape: Enable browser scraping mode
        headless: Run browser in headless mode (default: True)
        session_cookie: LinkedIn li_at session cookie
        cookies_file: Path to JSON file with LinkedIn cookies
        titles_filter: List of title keywords to filter (e.g., ["CEO", "Director"])
        limit: Maximum profiles to process
        target_entities: Filter by person names
        run_id: Optional run ID for tracking

    Returns:
        Ingestion result dictionary

    Examples:
        # Import from CSV
        result = await run_linkedin_ingestion(
            csv_path="members.csv",
            company_name="Postmedia Network"
        )

        # Scrape company page (requires auth)
        result = await run_linkedin_ingestion(
            company_url="https://www.linkedin.com/company/postmedia",
            scrape=True,
            session_cookie="your_li_at_cookie",
            titles_filter=["CEO", "Director", "VP"]
        )

        # Link to existing organization
        result = await run_linkedin_ingestion(
            csv_path="members.csv",
            company_entity_id="550e8400-e29b-41d4-a716-446655440000"
        )
    """
    ingester = LinkedInIngester()

    # Parse company_entity_id if string
    entity_uuid = None
    if company_entity_id:
        entity_uuid = UUID(company_entity_id) if isinstance(company_entity_id, str) else company_entity_id

    ingester.configure(
        csv_path=csv_path,
        company_name=company_name,
        company_url=company_url,
        company_entity_id=entity_uuid,
        scrape=scrape,
        headless=headless,
        session_cookie=session_cookie,
        cookies_file=cookies_file,
        titles_filter=titles_filter,
    )

    config = IngestionConfig(
        incremental=True,
        limit=limit,
        target_entities=target_entities,
        run_id=run_id,
        extra_params=extra_params,
    )

    result = await ingester.run(config)
    return result.model_dump()


async def search_linkedin_companies(
    query: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search for companies on LinkedIn (placeholder for future API integration).

    This is a placeholder for future LinkedIn API integration.
    Currently returns an error directing users to manual lookup.
    """
    raise NotImplementedError(
        "LinkedIn company search is not yet implemented. "
        "Please use the company URL directly: "
        "https://www.linkedin.com/company/COMPANY_NAME"
    )
