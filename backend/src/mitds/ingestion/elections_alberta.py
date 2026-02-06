"""Elections Alberta Third Party ingester.

Ingests data from Elections Alberta about registered third party advertisers (TPAs),
including their financial statements and contributor lists.

Data sources:
- Contributions Data Extract: https://efpublic.elections.ab.ca/efCXDataExtract.cfm
- TPA Pages: https://efpublic.elections.ab.ca/efTPAs.cfm
- Contributor Search: https://efpublic.elections.ab.ca/efContributorSearch.cfm

Key data points:
- Third party advertiser registration details
- Financial statements
- Contributor lists (>$250 threshold)
"""

import asyncio
import csv
import io
import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import uuid4
from urllib.parse import urljoin

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

# Elections Alberta URLs
EA_BASE_URL = "https://efpublic.elections.ab.ca"
EA_DATA_EXTRACT_URL = f"{EA_BASE_URL}/efCXDataExtract.cfm"
EA_TPA_LIST_URL = f"{EA_BASE_URL}/efTPAs.cfm"
EA_CONTRIBUTOR_SEARCH_URL = f"{EA_BASE_URL}/efContributorSearch.cfm"


class ElectionThirdParty(BaseModel):
    """A third party advertiser registered with Elections Alberta."""

    third_party_name: str = Field(..., description="Name of the third party advertiser")
    registration_id: str | None = None
    tpa_type: str | None = Field(
        default=None, description="Political Advertiser, Election Advertiser, etc."
    )
    province: str = Field(default="AB", description="Province code")

    # Contact info
    city: str | None = None
    postal_code: str | None = None

    # Registration details
    registered_date: date | None = None
    registration_status: str | None = None

    # Contributors (>$250 threshold)
    contributors: list[ThirdPartyContributor] = Field(default_factory=list)

    # Document links
    document_links: list[str] = Field(default_factory=list)


class ElectionsAlbertaIngester(BaseIngester[ElectionThirdParty]):
    """Ingester for Elections Alberta third party advertiser data."""

    def __init__(self):
        super().__init__("elections_alberta")
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
                headers={
                    "User-Agent": "MITDS/1.0 (Media Influence Transparency; research)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-CA,en;q=0.9",
                },
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
        """Fetch third party advertiser records from Elections Alberta."""

        self.logger.info("Fetching third party advertisers from Elections Alberta")

        # Try data extract tool first (CSV export)
        try:
            async for record in self._fetch_via_data_extract(config):
                yield record
                return  # If successful, don't fall back to scraping
        except Exception as e:
            self.logger.warning(f"Data extract failed, falling back to scraping: {e}")

        # Fall back to scraping TPA pages
        try:
            async for record in self._fetch_via_scraping(config):
                yield record
        except Exception as e:
            self.logger.error(f"Scraping fetch failed: {e}")

    async def _fetch_via_data_extract(
        self, config: IngestionConfig
    ) -> AsyncIterator[ElectionThirdParty]:
        """Fetch data using the Contributions Data Extract tool (ColdFusion form)."""
        try:
            # Get the data extract form page
            response = await self.http_client.get(EA_DATA_EXTRACT_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Find the form
            form = soup.find("form")
            if not form:
                raise ValueError("Data extract form not found")

            # Extract form action and method
            form_action = form.get("action", "")
            if not form_action.startswith("http"):
                form_action = urljoin(EA_BASE_URL, form_action)

            # Build form data for TPA extract
            # Common ColdFusion form fields for data extracts
            form_data = {
                "extractType": "TPA",  # Third Party Advertiser
                "format": "CSV",
                "includeContributors": "true",
                "minAmount": "250",  # Only contributions >$250
            }

            # Submit form to generate CSV
            extract_response = await self.http_client.post(form_action, data=form_data)
            extract_response.raise_for_status()

            # Check if response is CSV
            content_type = extract_response.headers.get("content-type", "").lower()
            if "csv" in content_type or extract_response.text.strip().startswith("TPA"):
                # Parse CSV
                csv_content = extract_response.text
                records = self._parse_csv_extract(csv_content)
                for record in records:
                    yield record
            else:
                # May need to follow redirect or check for download link
                # Try to find CSV download link in response
                soup = BeautifulSoup(extract_response.text, "html.parser")
                download_link = soup.find("a", href=re.compile(r"\.csv|download", re.I))
                if download_link:
                    csv_url = urljoin(EA_BASE_URL, download_link["href"])
                    csv_response = await self.http_client.get(csv_url)
                    csv_response.raise_for_status()
                    records = self._parse_csv_extract(csv_response.text)
                    for record in records:
                        yield record
                else:
                    raise ValueError("CSV extract not available")

        except Exception as e:
            self.logger.warning(f"Data extract method failed: {e}")
            raise

    def _parse_csv_extract(self, csv_content: str) -> list[ElectionThirdParty]:
        """Parse CSV extract into ElectionThirdParty records."""
        records = []
        seen_tpas: dict[str, ElectionThirdParty] = {}

        try:
            # Try to detect delimiter
            delimiter = ","
            if "\t" in csv_content[:100]:
                delimiter = "\t"

            reader = csv.DictReader(io.StringIO(csv_content), delimiter=delimiter)

            for row in reader:
                # Extract TPA name (column names may vary)
                tpa_name = (
                    row.get("TPA Name")
                    or row.get("Third Party Advertiser")
                    or row.get("Organization Name")
                    or row.get("Name")
                )

                if not tpa_name or len(tpa_name) < 2:
                    continue

                # Get or create TPA record
                if tpa_name not in seen_tpas:
                    tpa = ElectionThirdParty(
                        third_party_name=tpa_name,
                        registration_id=row.get("Registration ID") or row.get("ID"),
                        tpa_type=row.get("TPA Type") or row.get("Type"),
                        province="AB",
                        registration_status=row.get("Status"),
                    )
                    seen_tpas[tpa_name] = tpa
                else:
                    tpa = seen_tpas[tpa_name]

                # Extract contributor if present
                contrib_name = (
                    row.get("Contributor Name")
                    or row.get("Donor Name")
                    or row.get("Name")
                )
                contrib_amount_str = (
                    row.get("Amount")
                    or row.get("Contribution Amount")
                    or row.get("Donation Amount")
                    or "0"
                )

                if contrib_name and contrib_amount_str:
                    try:
                        amount = Decimal(
                            str(contrib_amount_str).replace(",", "").replace("$", "").strip()
                            or "0"
                        )
                    except:
                        amount = Decimal("0")

                    # Only include contributions >$250
                    if amount > 250:
                        contrib = ThirdPartyContributor(
                            name=contrib_name.strip(),
                            amount=amount,
                            contributor_class=row.get("Contributor Class", "individual"),
                            city=row.get("City"),
                            province=row.get("Province", "AB"),
                            postal_code=row.get("Postal Code"),
                            jurisdiction="alberta",
                            date_received=self._parse_date(row.get("Date") or row.get("Contribution Date")),
                        )
                        tpa.contributors.append(contrib)

            records = list(seen_tpas.values())

        except Exception as e:
            self.logger.warning(f"CSV parsing failed: {e}")

        return records

    async def _fetch_via_scraping(
        self, config: IngestionConfig
    ) -> AsyncIterator[ElectionThirdParty]:
        """Fetch data by scraping TPA pages."""
        try:
            # Get TPA list page
            response = await self.http_client.get(EA_TPA_LIST_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Find TPA listings (structure depends on actual site)
            tpa_listings = soup.find_all(
                ["tr", "div"], class_=re.compile(r"tpa|third.*party|advertiser", re.I)
            )

            # If no class-based matches, try table rows
            if not tpa_listings:
                table = soup.find("table")
                if table:
                    tpa_listings = table.find_all("tr")[1:]  # Skip header

            for listing in tpa_listings:
                # Extract TPA name
                name_elem = listing.find(["a", "td", "span"], class_=re.compile(r"name|title", re.I))
                if not name_elem:
                    # Try first link or first cell
                    name_elem = listing.find("a") or (
                        listing.find_all("td")[0] if listing.find_all("td") else None
                    )

                if not name_elem:
                    continue

                name = name_elem.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Extract registration ID
                reg_id = None
                link = listing.find("a", href=True)
                if link:
                    href = link["href"]
                    match = re.search(r"id=(\d+)|tpa[_-]?(\d+)", href, re.I)
                    if match:
                        reg_id = match.group(1) or match.group(2)

                # Extract TPA type
                tpa_type = None
                type_elem = listing.find(string=re.compile(r"Political|Election", re.I))
                if type_elem:
                    if "Political" in type_elem:
                        tpa_type = "Political Advertiser"
                    elif "Election" in type_elem:
                        tpa_type = "Election Advertiser"

                tp = ElectionThirdParty(
                    third_party_name=name,
                    registration_id=reg_id,
                    tpa_type=tpa_type,
                    province="AB",
                )

                # Fetch contributors from detail page or contributor search
                if link and link.get("href"):
                    detail_url = urljoin(EA_BASE_URL, link["href"])
                    contributors = await self._fetch_contributors_from_detail(
                        detail_url, name
                    )
                    tp.contributors.extend(contributors)

                # Also try contributor search
                search_contributors = await self._search_contributors(name)
                tp.contributors.extend(search_contributors)

                # Deduplicate contributors by name
                seen_names = set()
                unique_contributors = []
                for contrib in tp.contributors:
                    if contrib.name not in seen_names:
                        seen_names.add(contrib.name)
                        unique_contributors.append(contrib)
                tp.contributors = unique_contributors

                yield tp

        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")

    async def _fetch_contributors_from_detail(
        self, detail_url: str, tpa_name: str
    ) -> list[ThirdPartyContributor]:
        """Fetch contributors from a TPA detail page."""
        contributors = []

        try:
            response = await self.http_client.get(detail_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Look for contributor tables
            contrib_tables = soup.find_all(
                "table", {"id": re.compile(r"contrib|donation|donor", re.I)}
            )

            if not contrib_tables:
                # Try alternative selectors
                contrib_tables = soup.find_all(
                    "table", class_=re.compile(r"contrib|donation|donor", re.I)
                )

            # If still not found, look for any table with contribution-like headers
            if not contrib_tables:
                all_tables = soup.find_all("table")
                for table in all_tables:
                    headers = table.find_all(["th", "td"], limit=5)
                    header_text = " ".join([h.get_text() for h in headers]).lower()
                    if any(
                        keyword in header_text
                        for keyword in ["contributor", "donor", "donation", "amount"]
                    ):
                        contrib_tables.append(table)

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

                    # Extract amount (look for dollar amounts)
                    amount = Decimal("0")
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True)
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
                        # Extract additional fields
                        city = None
                        postal_code = None
                        date_received = None

                        for cell in cells[1:]:
                            text = cell.get_text(strip=True)
                            # Check if it looks like a postal code
                            if re.match(r"[A-Z]\d[A-Z]\s?\d[A-Z]\d", text):
                                postal_code = text
                            # Check if it looks like a city
                            elif len(text) > 2 and text[0].isupper() and not re.search(r"\d", text):
                                city = text
                            # Check if it looks like a date
                            date_match = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})", text)
                            if date_match:
                                date_received = self._parse_date(date_match.group(1))

                        contrib = ThirdPartyContributor(
                            name=name,
                            amount=amount,
                            contributor_class="individual",  # Default, may need refinement
                            city=city,
                            postal_code=postal_code,
                            jurisdiction="alberta",
                            date_received=date_received,
                        )
                        contributors.append(contrib)

        except Exception as e:
            self.logger.warning(f"Failed to fetch contributors from {detail_url}: {e}")

        return contributors

    async def _search_contributors(self, tpa_name: str) -> list[ThirdPartyContributor]:
        """Search for contributors using the contributor search page."""
        contributors = []

        try:
            # Get contributor search page
            response = await self.http_client.get(EA_CONTRIBUTOR_SEARCH_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Find search form
            form = soup.find("form")
            if not form:
                return contributors

            form_action = form.get("action", "")
            if not form_action.startswith("http"):
                form_action = urljoin(EA_BASE_URL, form_action)

            # Build search query for this TPA
            form_data = {
                "tpaName": tpa_name,
                "minAmount": "250",
            }

            # Submit search
            search_response = await self.http_client.post(form_action, data=form_data)
            search_response.raise_for_status()

            # Parse results
            results_soup = BeautifulSoup(search_response.text, "html.parser")
            results_table = results_soup.find("table", {"id": re.compile(r"results|data", re.I)})

            if results_table:
                rows = results_table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:
                        continue

                    name = cells[0].get_text(strip=True)
                    if not name:
                        continue

                    # Extract amount
                    amount = Decimal("0")
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True)
                        match = re.search(r"\$?\s*([\d,]+\.?\d*)", text)
                        if match:
                            try:
                                val = Decimal(match.group(1).replace(",", ""))
                                if val > amount:
                                    amount = val
                            except:
                                pass

                    if amount > 250:
                        contrib = ThirdPartyContributor(
                            name=name,
                            amount=amount,
                            contributor_class="individual",
                            jurisdiction="alberta",
                        )
                        contributors.append(contrib)

        except Exception as e:
            self.logger.warning(f"Contributor search failed for {tpa_name}: {e}")

        return contributors

    def _parse_date(self, date_str: str | None) -> date | None:
        """Parse date string to date object."""
        if not date_str:
            return None

        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except:
                continue

        return None

    async def process_record(self, record: ElectionThirdParty) -> dict[str, Any]:
        """Process a third party advertiser record."""
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
                    "source": "elections_alberta",
                    "province": record.province,
                    "is_election_third_party": True,
                },
            }

            if record.registration_id:
                entity_data["external_ids"]["ea_registration_id"] = record.registration_id

            if record.tpa_type:
                entity_data["metadata"]["tpa_type"] = record.tpa_type

            if record.registration_status:
                entity_data["metadata"]["registration_status"] = record.registration_status

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
                                r.source = 'elections_alberta',
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


async def run_elections_alberta_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run Elections Alberta third party advertiser ingestion.

    Args:
        limit: Maximum number of third parties to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of third party names to filter
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = ElectionsAlbertaIngester()

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
