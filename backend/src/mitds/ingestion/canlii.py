"""CanLII (Canadian Legal Information Institute) ingester.

Ingests case law data from CanLII to identify legal relationships between entities.
Uses CanLII REST API for case metadata and supplements with website scraping
for full-text keyword search.

Data source:
- API: https://api.canlii.org/v1/ (requires API key)
- Website: https://www.canlii.org/ (for keyword search scraping)

Key features:
- Case metadata via REST API
- Full-text search via website scraping
- Party name matching using rapidfuzz
- LITIGATED_WITH relationships in Neo4j
- Evidence records for case details
"""

import hashlib
import json
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from rapidfuzz import fuzz
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import BaseIngester, IngestionConfig, Neo4jHelper, PostgresHelper

logger = get_context_logger(__name__)

# CanLII API base URL
CANLII_API_BASE = "https://api.canlii.org/v1"
CANLII_SEARCH_URL = "https://www.canlii.org/en/search"


class CanLIICaseRecord(BaseModel):
    """A CanLII case record."""

    case_id: str = Field(..., description="CanLII case identifier")
    citation: str | None = Field(default=None, description="Case citation (e.g., '2024 SCC 1')")
    title: str = Field(..., description="Case title")
    court: str | None = Field(default=None, description="Court name")
    decision_date: date | None = Field(default=None, description="Decision date")
    url: str | None = Field(default=None, description="CanLII case URL")
    parties: list[str] = Field(default_factory=list, description="Party names from case title")
    summary: str | None = Field(default=None, description="Case summary")
    keywords: list[str] = Field(default_factory=list, description="Keywords/topics")


class CanLIIIngester(BaseIngester[CanLIICaseRecord]):
    """Ingester for CanLII case law data.

    Fetches case metadata via REST API and supplements with website scraping
    for full-text keyword search. Matches party names against entities
    and creates LITIGATED_WITH relationships.
    """

    def __init__(self):
        super().__init__("canlii")
        self._http_client: httpx.AsyncClient | None = None
        self._neo4j = Neo4jHelper(self.logger)
        self._postgres = PostgresHelper(self.logger)
        self._settings = get_settings()

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            api_key = self._settings.canlii_api_key
            headers = {
                "User-Agent": "MITDS/1.0 (Media Influence Transparency; research)",
                "Accept": "application/json",
            }
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=60.0),
                follow_redirects=True,
                headers=headers,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_last_sync_time(self) -> datetime | None:
        """Get last successful sync timestamp."""
        async with get_db_session() as db:
            result = await db.execute(
                text("""
                    SELECT MAX(retrieved_at) as last_sync
                    FROM evidence
                    WHERE evidence_type = 'CANLII_CASE'
                    AND extractor = 'canlii_ingester'
                """),
            )
            row = result.fetchone()
            return row.last_sync if row else None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save sync time (handled by base class)."""
        pass

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[CanLIICaseRecord]:
        """Fetch CanLII case records.

        Uses API for metadata and supplements with website scraping
        for keyword-based search.
        """
        # Check for API key
        api_key = self._settings.canlii_api_key
        if not api_key:
            self.logger.warning(
                "CANLII_API_KEY not configured. "
                "Only website scraping will be available."
            )

        # Fetch via API if available
        if api_key:
            async for record in self._fetch_via_api(config):
                yield record

        # Supplement with website scraping for keyword search
        if config.extra_params.get("enable_scraping", True):
            async for record in self._fetch_via_scraping(config):
                yield record

    async def _fetch_via_api(
        self, config: IngestionConfig
    ) -> AsyncIterator[CanLIICaseRecord]:
        """Fetch cases via CanLII REST API."""
        api_key = self._settings.canlii_api_key
        if not api_key:
            return

        try:
            # Build query parameters
            params: dict[str, Any] = {}
            if config.date_from:
                params["date_from"] = config.date_from.isoformat()
            if config.date_to:
                params["date_to"] = config.date_to.isoformat()
            if config.limit:
                params["limit"] = config.limit

            # Search for cases
            # Note: Actual API endpoint structure may vary - adjust as needed
            url = f"{CANLII_API_BASE}/cases"
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            cases = data.get("results", []) or data.get("cases", []) or []

            for case_data in cases:
                try:
                    record = self._parse_api_case(case_data)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning(f"Failed to parse API case: {e}")
                    continue

        except httpx.HTTPStatusError as e:
            self.logger.error(f"CanLII API error: {e.response.status_code}")
            if e.response.status_code == 401:
                self.logger.error("Invalid API key or unauthorized")
        except Exception as e:
            self.logger.warning(f"API fetch failed, falling back to scraping: {e}")

    async def _fetch_via_scraping(
        self, config: IngestionConfig
    ) -> AsyncIterator[CanLIICaseRecord]:
        """Fetch cases via website scraping."""
        # Extract search keywords from config
        keywords = config.extra_params.get("keywords", [])
        if not keywords and config.target_entities:
            # Use entity names as keywords
            keywords = config.target_entities

        if not keywords:
            self.logger.info("No keywords provided for scraping")
            return

        for keyword in keywords[:10]:  # Limit to 10 keywords
            try:
                # Search CanLII website
                search_params = {
                    "q": keyword,
                    "type": "cases",
                }
                response = await self.http_client.get(CANLII_SEARCH_URL, params=search_params)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                # Parse search results - adjust selectors based on actual HTML structure
                case_links = soup.select("a[href*='/en/cas/']") or soup.select(
                    ".case-result a"
                )

                for link in case_links[:20]:  # Limit to 20 results per keyword
                    case_url = link.get("href", "")
                    if not case_url.startswith("http"):
                        case_url = f"https://www.canlii.org{case_url}"

                    try:
                        record = await self._scrape_case_page(case_url)
                        if record:
                            yield record
                    except Exception as e:
                        self.logger.debug(f"Failed to scrape {case_url}: {e}")
                        continue

            except Exception as e:
                self.logger.warning(f"Scraping failed for keyword '{keyword}': {e}")
                continue

    async def _scrape_case_page(self, url: str) -> CanLIICaseRecord | None:
        """Scrape a single case page."""
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Extract case details - adjust selectors based on actual structure
            title_elem = soup.select_one("h1, .case-title, .title")
            title = title_elem.get_text(strip=True) if title_elem else ""

            citation_elem = soup.select_one(".citation, .case-citation")
            citation = citation_elem.get_text(strip=True) if citation_elem else None

            court_elem = soup.select_one(".court, .court-name")
            court = court_elem.get_text(strip=True) if court_elem else None

            date_elem = soup.select_one(".decision-date, .date")
            decision_date = None
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                # Try to parse date
                for fmt in ["%Y-%m-%d", "%B %d, %Y", "%d %B %Y"]:
                    try:
                        decision_date = datetime.strptime(date_text, fmt).date()
                        break
                    except ValueError:
                        continue

            # Extract parties from title (typically "Party A v. Party B")
            parties = self._extract_parties_from_title(title)

            # Extract summary
            summary_elem = soup.select_one(".summary, .case-summary, .abstract")
            summary = summary_elem.get_text(strip=True) if summary_elem else None

            # Generate case ID from URL or citation
            case_id = url.split("/")[-1] or citation or hashlib.md5(url.encode()).hexdigest()[:12]

            return CanLIICaseRecord(
                case_id=case_id,
                citation=citation,
                title=title,
                court=court,
                decision_date=decision_date,
                url=url,
                parties=parties,
                summary=summary,
            )

        except Exception as e:
            self.logger.debug(f"Failed to scrape case page {url}: {e}")
            return None

    def _parse_api_case(self, case_data: dict[str, Any]) -> CanLIICaseRecord | None:
        """Parse a case from API response."""
        try:
            case_id = case_data.get("id") or case_data.get("case_id") or ""
            if not case_id:
                return None

            title = case_data.get("title") or case_data.get("name") or ""
            if not title:
                return None

            # Parse decision date
            decision_date = None
            date_str = case_data.get("decision_date") or case_data.get("date")
            if date_str:
                try:
                    if isinstance(date_str, str):
                        decision_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                    elif isinstance(date_str, date):
                        decision_date = date_str
                except Exception:
                    pass

            parties = self._extract_parties_from_title(title)

            return CanLIICaseRecord(
                case_id=str(case_id),
                citation=case_data.get("citation"),
                title=title,
                court=case_data.get("court") or case_data.get("court_name"),
                decision_date=decision_date,
                url=case_data.get("url") or case_data.get("canlii_url"),
                parties=parties,
                summary=case_data.get("summary") or case_data.get("abstract"),
                keywords=case_data.get("keywords", []) or case_data.get("topics", []),
            )

        except Exception as e:
            self.logger.warning(f"Failed to parse API case data: {e}")
            return None

    def _extract_parties_from_title(self, title: str) -> list[str]:
        """Extract party names from case title.

        Common patterns:
        - "Party A v. Party B"
        - "Party A v Party B"
        - "Party A vs. Party B"
        """
        parties = []
        title = title.strip()

        # Split on common separators
        separators = [" v. ", " v ", " vs. ", " vs ", " against "]
        for sep in separators:
            if sep in title:
                parts = title.split(sep, 1)
                if len(parts) == 2:
                    parties = [p.strip() for p in parts]
                    break

        # If no separator found, try to extract from patterns like "In re: Party Name"
        if not parties:
            if "In re: " in title or "In re " in title:
                party = title.split("In re")[-1].strip(": ").strip()
                if party:
                    parties = [party]

        return parties

    async def process_record(self, record: CanLIICaseRecord) -> dict[str, Any]:
        """Process a CanLII case record.

        Creates evidence record and LITIGATED_WITH relationships
        between matched entities.
        """
        result: dict[str, Any] = {"created": False}

        # Store evidence record
        evidence_id = uuid4()
        async with get_db_session() as db:
            # Check if case already exists
            existing = await db.execute(
                text("""
                    SELECT id FROM evidence
                    WHERE evidence_type = 'CANLII_CASE'
                    AND raw_data_ref = :case_id
                    LIMIT 1
                """),
                {"case_id": f"canlii/{record.case_id}"},
            )
            existing_row = existing.fetchone()

            if existing_row:
                evidence_id = existing_row[0]
                result = {"updated": True, "evidence_id": str(evidence_id)}
            else:
                # Create content hash
                content = json.dumps(
                    {
                        "case_id": record.case_id,
                        "citation": record.citation,
                        "title": record.title,
                        "court": record.court,
                        "decision_date": record.decision_date.isoformat()
                        if record.decision_date
                        else None,
                        "parties": record.parties,
                        "summary": record.summary,
                    },
                    sort_keys=True,
                )
                content_hash = hashlib.sha256(content.encode()).hexdigest()

                await db.execute(
                    text("""
                        INSERT INTO evidence (
                            id, evidence_type, source_url, retrieved_at,
                            extractor, extractor_version, raw_data_ref,
                            extraction_confidence, content_hash
                        )
                        VALUES (
                            :id, :evidence_type, :source_url, NOW(),
                            :extractor, :version, :raw_ref,
                            :confidence, :hash
                        )
                    """),
                    {
                        "id": evidence_id,
                        "evidence_type": "CANLII_CASE",
                        "source_url": record.url or f"https://www.canlii.org/en/cas/{record.case_id}",
                        "extractor": "canlii_ingester",
                        "version": "1.0.0",
                        "raw_ref": f"canlii/{record.case_id}",
                        "confidence": 0.85,
                        "hash": content_hash,
                    },
                )
                result = {"created": True, "evidence_id": str(evidence_id)}

        # Match parties against entities and create relationships
        if len(record.parties) >= 2:
            try:
                async with get_db_session() as db:
                    # Find entities matching party names
                    matched_entities: list[UUID] = []
                    for party_name in record.parties:
                        entity_id = await self._postgres.find_entity_by_name(db, party_name)
                        if entity_id:
                            matched_entities.append(entity_id)
                        else:
                            # Try fuzzy matching
                            entity_id = await self._fuzzy_match_entity(db, party_name)
                            if entity_id:
                                matched_entities.append(entity_id)

                    # Create LITIGATED_WITH relationships between matched entities
                    if len(matched_entities) >= 2:
                        await self._create_litigation_relationships(
                            matched_entities, record, evidence_id
                        )

            except Exception as e:
                self.logger.warning(f"Failed to create relationships: {e}")

        return result

    async def _fuzzy_match_entity(
        self, db, party_name: str, threshold: float = 0.85
    ) -> UUID | None:
        """Find entity using fuzzy name matching."""
        from sqlalchemy import text

        # Get all organization names
        result = await db.execute(
            text("""
                SELECT id, name FROM entities
                WHERE entity_type = 'organization'
            """),
        )
        rows = result.fetchall()

        best_match = None
        best_score = 0.0

        for row in rows:
            score = fuzz.ratio(party_name.lower(), row.name.lower())
            if score > best_score and score >= threshold * 100:
                best_score = score
                best_match = row.id

        return best_match

    async def _create_litigation_relationships(
        self,
        entity_ids: list[UUID],
        record: CanLIICaseRecord,
        evidence_id: UUID,
    ):
        """Create LITIGATED_WITH relationships between entities."""
        try:
            async with get_neo4j_session() as session:
                # Create relationships between all pairs
                for i, entity_a_id in enumerate(entity_ids):
                    for entity_b_id in entity_ids[i + 1 :]:
                        await self._neo4j.create_relationship(
                            session,
                            "LITIGATED_WITH",
                            "Organization",
                            "id",
                            str(entity_a_id),
                            "Organization",
                            "id",
                            str(entity_b_id),
                            properties={
                                "case_citation": record.citation,
                                "decision_date": record.decision_date.isoformat()
                                if record.decision_date
                                else None,
                                "court": record.court,
                                "case_title": record.title,
                                "evidence_id": str(evidence_id),
                            },
                            merge_on=["case_citation"],
                        )

        except Exception as e:
            self.logger.warning(f"Neo4j relationship creation failed: {e}")


async def run_canlii_ingestion(
    config: IngestionConfig | None = None,
) -> Any:
    """Run CanLII ingestion.

    Args:
        config: Optional ingestion configuration

    Returns:
        Ingestion result
    """
    ingester = CanLIIIngester()
    try:
        return await ingester.run(config)
    finally:
        await ingester.close()
