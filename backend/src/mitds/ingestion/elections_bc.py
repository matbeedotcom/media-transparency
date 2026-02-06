"""Elections BC Third Party ingester.

Ingests data from Elections BC about registered third party sponsors,
including their sponsorship contributions (>$250 threshold).

Data sources:
- FRPC System: https://contributions.electionsbc.gov.bc.ca/pcs/
- ASP.NET application with ViewState handling required

Key data points:
- Third party sponsor registration details
- Sponsorship contributions (>$250 threshold)
"""

import asyncio
import base64
import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import uuid4
from urllib.parse import urljoin, urlparse, parse_qs

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

# Elections BC URLs
EBC_BASE_URL = "https://contributions.electionsbc.gov.bc.ca"
EBC_FRPC_URL = f"{EBC_BASE_URL}/pcs/"


class ElectionThirdParty(BaseModel):
    """A third party sponsor registered with Elections BC."""

    third_party_name: str = Field(..., description="Name of the third party sponsor")
    registration_id: str | None = None
    province: str = Field(default="BC", description="Province code")

    # Contact info
    city: str | None = None
    postal_code: str | None = None

    # Registration details
    registered_date: date | None = None

    # Sponsorship contributions (>$250 threshold)
    contributors: list[ThirdPartyContributor] = Field(default_factory=list)


class ElectionsBCIngester(BaseIngester[ElectionThirdParty]):
    """Ingester for Elections BC third party sponsor data."""

    def __init__(self):
        super().__init__("elections_bc")
        self._http_client: httpx.AsyncClient | None = None
        self._session_cookies: dict[str, str] = {}

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-CA,en;q=0.9",
                },
                cookies=self._session_cookies,
            )
        return self._http_client

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
    ) -> AsyncIterator[ElectionThirdParty]:
        """Fetch third party sponsor records from Elections BC."""

        self.logger.info("Fetching third party sponsors from Elections BC")

        # Initialize session and get initial page
        await self._initialize_session()

        # Search for third party sponsors
        # The search interface may require navigating through forms
        async for record in self._search_third_parties(config):
            yield record

    async def _initialize_session(self):
        """Initialize session and get ViewState from initial page."""
        try:
            response = await self.http_client.get(EBC_FRPC_URL)
            response.raise_for_status()

            # Extract cookies
            for cookie in response.cookies.jar:
                self._session_cookies[cookie.name] = cookie.value

            # Parse ViewState from initial page
            soup = BeautifulSoup(response.text, "html.parser")
            viewstate_input = soup.find("input", {"name": "__VIEWSTATE"})
            if viewstate_input:
                self._viewstate = viewstate_input.get("value", "")
            else:
                self._viewstate = ""

            self.logger.info("Session initialized")

        except Exception as e:
            self.logger.warning(f"Failed to initialize session: {e}")
            self._viewstate = ""

    def _extract_viewstate(self, html: str) -> str:
        """Extract ViewState from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        viewstate_input = soup.find("input", {"name": "__VIEWSTATE"})
        if viewstate_input:
            return viewstate_input.get("value", "")
        return ""

    def _extract_eventvalidation(self, html: str) -> str:
        """Extract EventValidation from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        eventvalidation_input = soup.find("input", {"name": "__EVENTVALIDATION"})
        if eventvalidation_input:
            return eventvalidation_input.get("value", "")
        return ""

    async def _search_third_parties(
        self, config: IngestionConfig
    ) -> AsyncIterator[ElectionThirdParty]:
        """Search for third party sponsors."""
        # Navigate to search page
        search_url = urljoin(EBC_FRPC_URL, "Search.aspx")  # Common ASP.NET pattern

        try:
            # Get search page
            response = await self.http_client.get(search_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            viewstate = self._extract_viewstate(response.text)
            eventvalidation = self._extract_eventvalidation(response.text)

            # Find search form and submit search
            # This will need to be adapted based on actual form structure
            form_data = {
                "__VIEWSTATE": viewstate,
                "__EVENTVALIDATION": eventvalidation,
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
            }

            # Look for search input fields
            search_inputs = soup.find_all("input", {"type": ["text", "search"]})
            for inp in search_inputs:
                name = inp.get("name", "")
                if "search" in name.lower() or "name" in name.lower():
                    form_data[name] = ""  # Empty search to get all results

            # Submit search
            search_response = await self.http_client.post(search_url, data=form_data)
            search_response.raise_for_status()

            # Parse results
            results_soup = BeautifulSoup(search_response.text, "html.parser")

            # Extract third party listings from results table
            # Structure depends on actual Elections BC site
            results_table = results_soup.find("table", {"id": re.compile(r"results|grid|data", re.I)})
            if not results_table:
                # Try alternative selectors
                results_table = results_soup.find("table", class_=re.compile(r"results|data|grid", re.I))

            if results_table:
                rows = results_table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:
                        continue

                    # Extract third party name (usually first column)
                    name_elem = cells[0].find("a") or cells[0]
                    name = name_elem.get_text(strip=True)
                    if not name or len(name) < 2:
                        continue

                    # Extract registration ID if available
                    reg_id = None
                    link = cells[0].find("a", href=True)
                    if link:
                        href = link["href"]
                        # Extract ID from query params or href
                        match = re.search(r"id=(\d+)", href)
                        if match:
                            reg_id = match.group(1)

                    tp = ElectionThirdParty(
                        third_party_name=name,
                        registration_id=reg_id,
                        province="BC",
                    )

                    # Fetch contributors for this third party
                    if link and link.get("href"):
                        detail_url = urljoin(EBC_FRPC_URL, link["href"])
                        contributors = await self._fetch_contributors(detail_url, tp.third_party_name)
                        tp.contributors.extend(contributors)

                    yield tp

            else:
                # Fallback: try to find any links that look like third party sponsors
                links = results_soup.find_all("a", href=re.compile(r"third.*party|sponsor", re.I))
                for link in links:
                    name = link.get_text(strip=True)
                    if name and len(name) > 2:
                        tp = ElectionThirdParty(
                            third_party_name=name,
                            province="BC",
                        )
                        yield tp

        except Exception as e:
            self.logger.error(f"Search failed: {e}")

    async def _fetch_contributors(
        self, detail_url: str, third_party_name: str
    ) -> list[ThirdPartyContributor]:
        """Fetch sponsorship contributions for a third party."""
        contributors = []

        try:
            # Get detail page
            response = await self.http_client.get(detail_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Look for contributions table
            # Elections BC typically shows contributions >$250
            contrib_tables = soup.find_all("table", {"id": re.compile(r"contrib|donation|sponsor", re.I)})

            if not contrib_tables:
                # Try alternative selectors
                contrib_tables = soup.find_all("table", class_=re.compile(r"contrib|donation|sponsor", re.I))

            for table in contrib_tables:
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:
                        continue

                    # Extract contributor name (usually first column)
                    name = cells[0].get_text(strip=True)
                    if not name:
                        continue

                    # Extract amount (usually last column or marked column)
                    amount = Decimal("0")
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True)
                        # Look for dollar amounts
                        match = re.search(r"\$?\s*([\d,]+\.?\d*)", text)
                        if match:
                            try:
                                val = Decimal(match.group(1).replace(",", ""))
                                if val > amount:
                                    amount = val
                            except:
                                pass

                    # Only include contributions >$250
                    if amount > 250:
                        # Extract additional fields if available
                        city = None
                        postal_code = None
                        date_received = None

                        if len(cells) > 1:
                            # Try to find city/postal code
                            for cell in cells[1:]:
                                text = cell.get_text(strip=True)
                                # Check if it looks like a postal code
                                if re.match(r"[A-Z]\d[A-Z]\s?\d[A-Z]\d", text):
                                    postal_code = text
                                # Check if it looks like a city
                                elif len(text) > 2 and text[0].isupper() and not re.search(r"\d", text):
                                    city = text

                        # Try to find date
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            date_match = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})", text)
                            if date_match:
                                date_received = self._parse_date(date_match.group(1))

                        contrib = ThirdPartyContributor(
                            name=name,
                            amount=amount,
                            contributor_class="individual",  # Default, may need refinement
                            city=city,
                            postal_code=postal_code,
                            jurisdiction="bc",
                            date_received=date_received,
                        )
                        contributors.append(contrib)

        except Exception as e:
            self.logger.warning(f"Failed to fetch contributors from {detail_url}: {e}")

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
        """Process a third party sponsor record."""
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
                    "source": "elections_bc",
                    "province": record.province,
                    "is_election_third_party": True,
                },
            }

            if record.registration_id:
                entity_data["external_ids"]["ebc_registration_id"] = record.registration_id

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
                        if contributor.amount < 250:
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
                                r.source = 'elections_bc',
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


async def run_elections_bc_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run Elections BC third party sponsor ingestion.

    Args:
        limit: Maximum number of third parties to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of third party names to filter
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = ElectionsBCIngester()

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
