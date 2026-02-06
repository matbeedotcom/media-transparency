"""Elections Ontario Third Party ingester.

Ingests data from Elections Ontario about registered third party advertisers,
including their financial statements and contributor lists.

Data sources:
- Financial Statements: https://www.finances.elections.on.ca/en/third-party-advertisers
- SPA (Single Page Application) data endpoints or Playwright scraping

Key data points:
- Third party registration details
- Financial statements
- Contributor lists (>$100 threshold)
"""

import asyncio
import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import BaseIngester, IngestionConfig
from .elections_canada import ThirdPartyContributor

logger = get_context_logger(__name__)

# Elections Ontario URLs
EO_BASE_URL = "https://www.finances.elections.on.ca"
EO_THIRD_PARTY_URL = f"{EO_BASE_URL}/en/third-party-advertisers"

# Check for Playwright availability
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class ElectionThirdParty(BaseModel):
    """A third party registered for an Ontario election."""

    third_party_name: str = Field(..., description="Name of the third party")
    election_id: str | None = Field(default=None, description="Election identifier")
    election_name: str | None = None
    registration_id: str | None = None

    # Contact info
    city: str | None = None
    province: str = Field(default="ON", description="Province code")
    postal_code: str | None = None

    # Registration details
    registered_date: date | None = None

    # Contributors (>$100 threshold)
    contributors: list[ThirdPartyContributor] = Field(default_factory=list)

    # PDF/document links
    document_links: list[str] = Field(default_factory=list)


class ElectionsOntarioIngester(BaseIngester[ElectionThirdParty]):
    """Ingester for Elections Ontario third party data."""

    def __init__(self):
        super().__init__("elections_ontario")
        self._http_client: httpx.AsyncClient | None = None
        self._playwright = None
        self._browser = None
        self._context = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
                headers={
                    "User-Agent": "MITDS/1.0 (Media Influence Transparency; research)",
                    "Accept": "application/json, text/html, */*",
                },
            )
        return self._http_client

    async def close(self):
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

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
    ) -> AsyncIterator[ElectionThirdParty]:
        """Fetch third party records from Elections Ontario."""

        self.logger.info("Fetching third parties from Elections Ontario")

        # Try to reverse-engineer SPA endpoints first
        try:
            records = await self._fetch_via_api()
            if records:
                for record in records:
                    yield record
                return
        except Exception as e:
            self.logger.warning(f"API fetch failed, falling back to Playwright: {e}")

        # Fall back to Playwright scraping
        if PLAYWRIGHT_AVAILABLE:
            try:
                async for record in self._fetch_via_playwright(config):
                    yield record
            except Exception as e:
                self.logger.error(f"Playwright fetch failed: {e}")
        else:
            self.logger.warning(
                "Playwright not available. Install with: pip install playwright && playwright install chromium"
            )

    async def _fetch_via_api(self) -> list[ElectionThirdParty]:
        """Attempt to fetch data via reverse-engineered API endpoints."""
        # Try common SPA endpoint patterns
        api_endpoints = [
            f"{EO_BASE_URL}/api/third-party-advertisers",
            f"{EO_BASE_URL}/api/v1/third-party-advertisers",
            f"{EO_BASE_URL}/api/data/third-party-advertisers",
        ]

        for endpoint in api_endpoints:
            try:
                response = await self.http_client.get(endpoint)
                if response.status_code == 200:
                    data = response.json()
                    # Parse JSON response (structure depends on actual API)
                    return self._parse_api_response(data)
            except Exception:
                continue

        return []

    async def _fetch_via_playwright(
        self, config: IngestionConfig
    ) -> AsyncIterator[ElectionThirdParty]:
        """Fetch third party data using Playwright browser automation."""
        await self._init_playwright()

        try:
            page = await self._context.new_page()
            await page.goto(EO_THIRD_PARTY_URL, wait_until="networkidle")

            # Wait for SPA to load
            await asyncio.sleep(2)

            # Try to extract data from the page
            # This will need to be adapted based on the actual page structure
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")

            # Look for third party listings
            # Structure depends on actual Elections Ontario site
            listings = soup.find_all(["div", "tr"], class_=re.compile(r"third.*party|advertiser", re.I))

            for listing in listings:
                name_elem = listing.find(["a", "span", "td"], class_=re.compile(r"name|title", re.I))
                if not name_elem:
                    continue

                name = name_elem.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Extract registration ID if available
                reg_id = None
                reg_elem = listing.find(string=re.compile(r"TP-\d+|Registration.*\d+", re.I))
                if reg_elem:
                    match = re.search(r"TP-(\d+)", reg_elem)
                    if match:
                        reg_id = match.group(1)

                # Extract document links
                doc_links = []
                for link in listing.find_all("a", href=True):
                    href = link["href"]
                    if href.endswith(".pdf") or "document" in href.lower():
                        if not href.startswith("http"):
                            href = f"{EO_BASE_URL}{href}"
                        doc_links.append(href)

                tp = ElectionThirdParty(
                    third_party_name=name,
                    registration_id=reg_id,
                    province="ON",
                    document_links=doc_links,
                )

                # Fetch contributors from financial statements if available
                if doc_links:
                    contributors = await self._extract_contributors_from_docs(doc_links)
                    tp.contributors.extend(contributors)

                yield tp

        finally:
            await page.close()

    async def _init_playwright(self):
        """Initialize Playwright browser."""
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is required. Install with: pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        self._context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 800},
        )

    def _parse_api_response(self, data: dict | list) -> list[ElectionThirdParty]:
        """Parse API response into ElectionThirdParty records."""
        records = []

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and "data" in data:
            items = data["data"]
        elif isinstance(data, dict) and "results" in data:
            items = data["results"]
        else:
            return records

        for item in items:
            name = item.get("name") or item.get("third_party_name") or item.get("organization_name")
            if not name:
                continue

            tp = ElectionThirdParty(
                third_party_name=name,
                registration_id=item.get("registration_id") or item.get("id"),
                province="ON",
            )

            # Parse contributors if present
            if "contributors" in item:
                for contrib_data in item["contributors"]:
                    contrib = self._parse_contributor(contrib_data)
                    if contrib:
                        tp.contributors.append(contrib)

            records.append(tp)

        return records

    def _parse_contributor(self, data: dict) -> ThirdPartyContributor | None:
        """Parse contributor data from API response."""
        name = data.get("name") or data.get("contributor_name")
        if not name:
            return None

        amount_str = data.get("amount") or data.get("contribution_amount") or "0"
        try:
            amount = Decimal(str(amount_str).replace(",", "").replace("$", "").strip() or "0")
        except:
            amount = Decimal("0")

        # Only include contributors >$100
        if amount <= 100:
            return None

        return ThirdPartyContributor(
            name=name,
            contributor_class=data.get("contributor_class", "individual"),
            amount=amount,
            address=data.get("address"),
            city=data.get("city"),
            province=data.get("province", "ON"),
            postal_code=data.get("postal_code"),
            jurisdiction="ontario",
            date_received=self._parse_date(data.get("date_received")),
        )

    async def _extract_contributors_from_docs(
        self, doc_links: list[str]
    ) -> list[ThirdPartyContributor]:
        """Extract contributors from financial statement documents."""
        contributors = []

        for doc_url in doc_links[:3]:  # Limit to first 3 documents
            try:
                response = await self.http_client.get(doc_url)
                if response.status_code != 200:
                    continue

                # Try to parse PDF if it's a PDF
                if doc_url.lower().endswith(".pdf"):
                    pdf_contributors = await self._parse_pdf_contributors(response.content)
                    contributors.extend(pdf_contributors)
                else:
                    # Try HTML parsing
                    soup = BeautifulSoup(response.text, "html.parser")
                    html_contributors = self._parse_html_contributors(soup)
                    contributors.extend(html_contributors)

            except Exception as e:
                self.logger.warning(f"Failed to parse document {doc_url}: {e}")

        return contributors

    async def _parse_pdf_contributors(self, pdf_content: bytes) -> list[ThirdPartyContributor]:
        """Parse contributors from PDF financial statement."""
        contributors = []

        try:
            import pdfplumber
        except ImportError:
            self.logger.warning("pdfplumber not installed - skipping PDF parsing")
            return contributors

        try:
            import io
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    tables = page.extract_tables()

                    # Look for contributor tables
                    for table in tables:
                        if not table or len(table) < 2:
                            continue

                        # Check if this looks like a contributor table
                        header = table[0] if table else []
                        header_str = str(header).lower()

                        if any(keyword in header_str for keyword in ["contributor", "donor", "name", "amount"]):
                            for row in table[1:]:
                                if not row or len(row) < 2:
                                    continue

                                name = row[0] if row else ""
                                if not name or name.strip() == "":
                                    continue

                                # Find amount column
                                amount = Decimal("0")
                                for cell in row[1:]:
                                    if cell:
                                        try:
                                            val = Decimal(str(cell).replace(",", "").replace("$", "").strip() or "0")
                                            if val > amount:
                                                amount = val
                                        except:
                                            pass

                                # Only include >$100
                                if amount > 100:
                                    contrib = ThirdPartyContributor(
                                        name=name.strip(),
                                        amount=amount,
                                        contributor_class="individual",  # Default, may need refinement
                                        jurisdiction="ontario",
                                    )
                                    contributors.append(contrib)

        except Exception as e:
            self.logger.warning(f"PDF parsing failed: {e}")

        return contributors

    def _parse_html_contributors(self, soup: BeautifulSoup) -> list[ThirdPartyContributor]:
        """Parse contributors from HTML financial statement."""
        contributors = []

        # Look for contributor tables
        tables = soup.find_all("table")
        for table in tables:
            headers = table.find_all(["th", "td"], limit=5)
            header_text = " ".join([h.get_text() for h in headers]).lower()

            if any(keyword in header_text for keyword in ["contributor", "donor", "name", "amount"]):
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue

                    name = cells[0].get_text(strip=True)
                    if not name:
                        continue

                    # Find amount
                    amount = Decimal("0")
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True)
                        try:
                            val = Decimal(text.replace(",", "").replace("$", "").strip() or "0")
                            if val > amount:
                                amount = val
                        except:
                            pass

                    if amount > 100:
                        contrib = ThirdPartyContributor(
                            name=name,
                            amount=amount,
                            contributor_class="individual",
                            jurisdiction="ontario",
                        )
                        contributors.append(contrib)

        return contributors

    def _parse_date(self, date_str: str | None) -> date | None:
        """Parse date string to date object."""
        if not date_str:
            return None

        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except:
                continue

        return None

    async def process_record(self, record: ElectionThirdParty) -> dict[str, Any]:
        """Process a third party registration record."""
        result = {"created": False, "updated": False, "entity_id": None}

        self.logger.info(f"Processing: {record.third_party_name}")

        # --- PostgreSQL: Create/Update entity ---
        async with get_db_session() as db:
            check_result = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                    AND entity_type = 'organization'
                """),
                {"name": record.third_party_name},
            )
            existing = check_result.fetchone()

            entity_data = {
                "name": record.third_party_name,
                "entity_type": "organization",
                "external_ids": {},
                "metadata": {
                    "source": "elections_ontario",
                    "province": record.province,
                    "is_election_third_party": True,
                },
            }

            if record.registration_id:
                entity_data["external_ids"]["eo_registration_id"] = record.registration_id

            if existing:
                await db.execute(
                    text("""
                        UPDATE entities
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                            external_ids = COALESCE(external_ids, '{}'::jsonb) || CAST(:external_ids AS jsonb),
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
                        "name": record.third_party_name,
                        "entity_type": "organization",
                        "external_ids": json.dumps(entity_data["external_ids"]),
                        "metadata": json.dumps(entity_data["metadata"]),
                    },
                )
                result["created"] = True
                result["entity_id"] = str(new_id)

        # --- Neo4j: Create nodes and relationships ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Create/update Organization node
                await session.run(
                    """
                    MERGE (o:Organization {name: $name})
                    ON CREATE SET o.id = $id,
                                  o.entity_type = 'ORGANIZATION',
                                  o.is_election_third_party = true,
                                  o.province = $province,
                                  o.created_at = $now
                    ON MATCH SET o.is_election_third_party = true,
                                 o.province = COALESCE(o.province, $province),
                                 o.updated_at = $now
                    """,
                    name=record.third_party_name,
                    id=result["entity_id"],
                    province=record.province,
                    now=now,
                )

                # Create contributors and CONTRIBUTED_TO relationships
                if record.contributors:
                    self.logger.info(f"  Processing {len(record.contributors)} contributors")
                    for contributor in record.contributors[:50]:  # Limit to top 50
                        if contributor.amount < 100:
                            continue

                        is_corporate = contributor.contributor_class in (
                            "corporation",
                            "business",
                            "trade_union",
                            "unincorporated_association",
                        )

                        if is_corporate:
                            await session.run(
                                """
                                MERGE (o:Organization {name: $name})
                                ON CREATE SET o.id = $id,
                                              o.entity_type = 'ORGANIZATION',
                                              o.contributor_class = $contributor_class,
                                              o.city = $city,
                                              o.postal_code = $postal_code,
                                              o.created_at = $now
                                ON MATCH SET o.updated_at = $now
                                """,
                                name=contributor.name,
                                id=str(uuid4()),
                                contributor_class=contributor.contributor_class,
                                city=contributor.city,
                                postal_code=contributor.postal_code,
                                now=now,
                            )
                        else:
                            await session.run(
                                """
                                MERGE (p:Person {name: $name})
                                ON CREATE SET p.id = $id,
                                              p.entity_type = 'PERSON',
                                              p.city = $city,
                                              p.postal_code = $postal_code,
                                              p.created_at = $now
                                ON MATCH SET p.updated_at = $now
                                """,
                                name=contributor.name,
                                id=str(uuid4()),
                                city=contributor.city,
                                postal_code=contributor.postal_code,
                                now=now,
                            )

                        # Create CONTRIBUTED_TO relationship
                        node_label = "Organization" if is_corporate else "Person"
                        await session.run(
                            f"""
                            MATCH (c:{node_label} {{name: $contributor_name}})
                            MATCH (tp:Organization {{name: $third_party_name}})
                            MERGE (c)-[r:CONTRIBUTED_TO]->(tp)
                            ON CREATE SET r.created_at = $now
                            SET r.amount = $amount,
                                r.contributor_class = $contributor_class,
                                r.jurisdiction = $jurisdiction,
                                r.source = 'elections_ontario',
                                r.updated_at = $now
                            """,
                            contributor_name=contributor.name,
                            third_party_name=record.third_party_name,
                            amount=float(contributor.amount),
                            contributor_class=contributor.contributor_class,
                            jurisdiction=contributor.jurisdiction,
                            now=now,
                        )

        except Exception as e:
            self.logger.warning(f"  Neo4j: FAILED - {e}")

        return result


async def run_elections_ontario_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run Elections Ontario third party ingestion.

    Args:
        limit: Maximum number of third parties to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of third party names to filter
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = ElectionsOntarioIngester()

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
