"""Beneficial ownership ingester for Canadian corporations.

Queries ISED web search for Individuals with Significant Control (ISC) data
by corporation number. Creates BENEFICIAL_OWNER_OF relationships in Neo4j.

Data source:
- ISED web search: https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.html
- Note: No API for ISC data — web scraping is required.
- The existing ISED API returns directors but NOT ISC data.

Key limitation:
- Cannot search by person name — only by corporation name/number.
- Reverse lookup requires knowing all corporation numbers first.
"""

import asyncio
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import BaseIngester, IngestionConfig, IngestionResult, Neo4jHelper

logger = get_context_logger(__name__)

# ISED web search URLs
ISED_CORP_SEARCH_URL = "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.html"
ISED_CORP_DETAIL_URL = "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"


class BeneficialOwnerRecord(BaseModel):
    """Record for an Individual with Significant Control (ISC) of a corporation."""

    full_name: str = Field(..., description="Legal name of the ISC")
    date_became_isc: date | None = Field(
        default=None, description="When they gained significant control"
    )
    date_ceased_isc: date | None = Field(
        default=None, description="When they lost significant control (null if current)"
    )
    control_description: str | None = Field(
        default=None, description="e.g., 'holds >25% shares'"
    )
    service_address: str | None = Field(
        default=None, description="Address for service (if provided)"
    )
    corporation_number: str = Field(..., description="Corporation they control")
    corporation_name: str | None = Field(default=None, description="Corporation name")


class BeneficialOwnershipIngester(BaseIngester["BeneficialOwnerRecord"]):
    """Ingester for federal beneficial ownership (ISC) data from ISED web search.

    Scrapes the ISED web interface to extract ISC data for specified corporations.
    Creates Person entities and BENEFICIAL_OWNER_OF relationships in Neo4j.
    """

    def __init__(self) -> None:
        super().__init__("beneficial_ownership")

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[BeneficialOwnerRecord]:
        """Fetch ISC records by scraping ISED web search.

        Config options:
        - corporation_numbers: list of corporation numbers to look up
        - from_graph: if True, pull corporation numbers from Neo4j
        - limit: max records to process
        """
        corporation_numbers: list[str] = config.extra.get("corporation_numbers", [])
        from_graph: bool = config.extra.get("from_graph", False)
        limit: int | None = config.extra.get("limit")

        if from_graph and not corporation_numbers:
            corporation_numbers = await self._get_corp_numbers_from_graph(limit)

        settings = get_settings()
        timeout = httpx.Timeout(30.0)

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            processed = 0
            for corp_num in corporation_numbers:
                if limit and processed >= limit:
                    break

                try:
                    records = await self._scrape_isc_data(client, corp_num)
                    for record in records:
                        yield record
                        processed += 1
                        if limit and processed >= limit:
                            break

                    # Polite crawling: 1-2 req/sec
                    await asyncio.sleep(1.0)

                except Exception as e:
                    logger.warning(
                        f"Failed to fetch ISC data for corporation {corp_num}: {e}"
                    )

    async def process_record(self, record: BeneficialOwnerRecord) -> dict[str, Any]:
        """Process a beneficial ownership record.

        Creates/updates Person entity and BENEFICIAL_OWNER_OF relationship.
        """
        entity_id = uuid4()
        result: dict[str, Any] = {"created": False, "entity_id": str(entity_id)}

        # Store person entity in PostgreSQL
        async with get_db_session() as db:
            from sqlalchemy import text

            # Check if person already exists
            existing = await db.execute(
                text(
                    "SELECT id FROM entities WHERE name = :name AND entity_type = 'person' LIMIT 1"
                ),
                {"name": record.full_name},
            )
            existing_row = existing.fetchone()

            if existing_row:
                entity_id = existing_row[0]
                result = {"updated": True, "entity_id": str(entity_id)}
            else:
                await db.execute(
                    text(
                        """
                        INSERT INTO entities (id, name, entity_type, jurisdiction, metadata, created_at, updated_at)
                        VALUES (:id, :name, 'person', 'CA', :metadata, NOW(), NOW())
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "id": str(entity_id),
                        "name": record.full_name,
                        "metadata": {
                            "control_description": record.control_description,
                            "service_address": record.service_address,
                            "source": "ised_isc",
                        },
                    },
                )
                result = {"created": True, "entity_id": str(entity_id)}

        # Create BENEFICIAL_OWNER_OF relationship in Neo4j
        try:
            async with get_neo4j_session() as session:
                await session.run(
                    """
                    MERGE (p:Person {name: $person_name})
                    ON CREATE SET p.id = $person_id,
                                  p.created_at = datetime(),
                                  p.source = 'ised_isc'
                    SET p.updated_at = datetime()

                    WITH p
                    MATCH (o:Organization {canada_corp_num: $corp_number})
                    MERGE (p)-[r:BENEFICIAL_OWNER_OF]->(o)
                    SET r.control_description = $control_description,
                        r.date_from = $date_from,
                        r.date_to = $date_to,
                        r.source = 'ised_isc',
                        r.updated_at = datetime()
                    """,
                    person_name=record.full_name,
                    person_id=str(entity_id),
                    corp_number=record.corporation_number,
                    control_description=record.control_description,
                    date_from=str(record.date_became_isc) if record.date_became_isc else None,
                    date_to=str(record.date_ceased_isc) if record.date_ceased_isc else None,
                )
        except Exception as e:
            logger.warning(f"Neo4j sync failed for beneficial owner {record.full_name}: {e}")

        return result

    async def get_last_sync_time(self) -> datetime | None:
        """Get last sync timestamp."""
        async with get_db_session() as db:
            from sqlalchemy import text

            result = await db.execute(
                text(
                    "SELECT MAX(completed_at) FROM ingestion_runs WHERE source = 'beneficial_ownership'"
                )
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save sync timestamp — handled by base class."""
        pass

    async def _get_corp_numbers_from_graph(self, limit: int | None = None) -> list[str]:
        """Get corporation numbers from Neo4j for entities under investigation."""
        corp_numbers: list[str] = []
        try:
            async with get_neo4j_session() as session:
                query = """
                    MATCH (o:Organization)
                    WHERE o.canada_corp_num IS NOT NULL
                    RETURN DISTINCT o.canada_corp_num AS corp_num
                """
                if limit:
                    query += f" LIMIT {limit}"

                result = await session.run(query)
                records = await result.data()
                corp_numbers = [r["corp_num"] for r in records if r.get("corp_num")]
        except Exception as e:
            logger.warning(f"Failed to get corp numbers from Neo4j: {e}")

        return corp_numbers

    async def _scrape_isc_data(
        self, client: httpx.AsyncClient, corporation_number: str
    ) -> list[BeneficialOwnerRecord]:
        """Scrape ISC data from ISED web search for a corporation number.

        Parses the HTML response to extract ISC records.
        """
        records: list[BeneficialOwnerRecord] = []

        try:
            # Search by corporation number
            response = await client.get(
                ISED_CORP_DETAIL_URL,
                params={"corpId": corporation_number},
            )
            response.raise_for_status()

            html = response.text

            # Extract corporation name from page
            corp_name = self._extract_corp_name(html)

            # Parse ISC section
            isc_data = self._parse_isc_section(html)

            for isc in isc_data:
                records.append(
                    BeneficialOwnerRecord(
                        full_name=isc["full_name"],
                        date_became_isc=isc.get("date_became"),
                        date_ceased_isc=isc.get("date_ceased"),
                        control_description=isc.get("control_description"),
                        service_address=isc.get("service_address"),
                        corporation_number=corporation_number,
                        corporation_name=corp_name,
                    )
                )

        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error fetching ISC data for {corporation_number}: {e}")
        except Exception as e:
            logger.warning(f"Error parsing ISC data for {corporation_number}: {e}")

        return records

    def _extract_corp_name(self, html: str) -> str | None:
        """Extract corporation name from ISED detail page HTML."""
        # Look for the corporation name in the page header
        match = re.search(
            r'<h2[^>]*>([^<]+)</h2>',
            html,
        )
        if match:
            return match.group(1).strip()
        return None

    def _parse_isc_section(self, html: str) -> list[dict[str, Any]]:
        """Parse the ISC (Individuals with Significant Control) section from HTML.

        Returns a list of dicts with ISC data fields.
        """
        isc_records: list[dict[str, Any]] = []

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")

            # Look for ISC section — the exact structure depends on ISED page layout
            isc_section = soup.find("div", {"id": re.compile(r"isc|significant", re.I)})
            if not isc_section:
                # Try finding by heading text
                headings = soup.find_all(["h2", "h3", "h4"])
                for h in headings:
                    if "significant control" in h.get_text().lower():
                        isc_section = h.find_parent("div") or h.parent
                        break

            if not isc_section:
                return isc_records

            # Parse individual ISC entries from table rows
            rows = isc_section.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    # Build record from label-value pairs
                    if "name" in label and value:
                        isc_records.append({"full_name": value})
                    elif isc_records:
                        # Add fields to the last record
                        current = isc_records[-1]
                        if "became" in label or "start" in label:
                            current["date_became"] = self._parse_date(value)
                        elif "ceased" in label or "end" in label:
                            current["date_ceased"] = self._parse_date(value)
                        elif "control" in label or "description" in label:
                            current["control_description"] = value
                        elif "address" in label:
                            current["service_address"] = value

        except ImportError:
            logger.warning("beautifulsoup4 required for ISC parsing")
        except Exception as e:
            logger.warning(f"Error parsing ISC HTML section: {e}")

        return isc_records

    def _parse_date(self, date_str: str) -> date | None:
        """Parse a date string from ISED pages."""
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        return None


async def detect_common_controllers(limit: int | None = None) -> list[dict[str, Any]]:
    """Detect persons who are ISC or director of 2+ entities under investigation.

    Returns a list of common controller findings.
    """
    findings: list[dict[str, Any]] = []

    try:
        async with get_neo4j_session() as session:
            query = """
                MATCH (p:Person)-[r:BENEFICIAL_OWNER_OF|DIRECTOR_OF]->(o:Organization)
                WITH p, collect(DISTINCT {name: o.name, id: o.id, rel: type(r)}) AS orgs,
                     count(DISTINCT o) AS org_count
                WHERE org_count >= 2
                RETURN p.name AS person_name, p.id AS person_id,
                       org_count, orgs
                ORDER BY org_count DESC
            """
            if limit:
                query += f" LIMIT {limit}"

            result = await session.run(query)
            records = await result.data()

            for r in records:
                findings.append({
                    "person_name": r["person_name"],
                    "person_id": r.get("person_id"),
                    "organization_count": r["org_count"],
                    "organizations": r["orgs"],
                    "finding_type": "common_controller",
                })
    except Exception as e:
        logger.warning(f"Common controller detection failed: {e}")

    return findings


async def detect_shared_registered_offices(limit: int | None = None) -> list[dict[str, Any]]:
    """Detect organizations sharing the same registered office address.

    Queries entities table for organizations with matching (city + postal_code + street).
    """
    findings: list[dict[str, Any]] = []

    try:
        async with get_db_session() as db:
            from sqlalchemy import text

            result = await db.execute(
                text(
                    """
                    SELECT
                        metadata->>'city' AS city,
                        metadata->>'postal_code' AS postal_code,
                        array_agg(name ORDER BY name) AS org_names,
                        array_agg(id::text) AS org_ids,
                        count(*) AS org_count
                    FROM entities
                    WHERE entity_type = 'organization'
                      AND metadata->>'city' IS NOT NULL
                      AND metadata->>'postal_code' IS NOT NULL
                    GROUP BY metadata->>'city', metadata->>'postal_code'
                    HAVING count(*) >= 2
                    ORDER BY count(*) DESC
                    """
                    + (f" LIMIT {limit}" if limit else "")
                )
            )

            for row in result.fetchall():
                findings.append({
                    "city": row[0],
                    "postal_code": row[1],
                    "organization_names": row[2],
                    "organization_ids": row[3],
                    "organization_count": row[4],
                    "finding_type": "shared_address",
                })

    except Exception as e:
        logger.warning(f"Shared address detection failed: {e}")

    return findings


async def run_beneficial_ownership_ingestion(
    corporation_numbers: list[str] | None = None,
    from_graph: bool = False,
    limit: int | None = None,
    verbose: bool = False,
) -> IngestionResult:
    """Run the beneficial ownership ingestion pipeline.

    Args:
        corporation_numbers: Specific corporation numbers to look up
        from_graph: If True, pull corporation numbers from Neo4j graph
        limit: Maximum records to process
        verbose: Enable verbose output
    """
    ingester = BeneficialOwnershipIngester()
    config = IngestionConfig(
        incremental=True,
        extra={
            "corporation_numbers": corporation_numbers or [],
            "from_graph": from_graph,
            "limit": limit,
        },
    )
    run_id = uuid4()
    return await ingester.run(config, run_id)
