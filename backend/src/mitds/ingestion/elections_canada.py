"""Elections Canada Third Party ingester.

Ingests data from Elections Canada about registered third party advertisers,
including their advertising expenses by media type during election periods.

Data sources:
- Third Party Registry: https://www.elections.ca/WPAPPS/WPR/EN/TP
- Financial Returns: https://www.elections.ca/content.aspx?section=fin&dir=oth/thi/advert
- Open Data: https://www.elections.ca/content.aspx?section=fin&dir=oda&document=index

Key data points:
- Third party registration details
- Election participation
- Advertising expenses by media type (TV, radio, digital, print, etc.)
- Financial agents and auditors
"""

import asyncio
import csv
import io
import json
import re
from datetime import datetime, date
from typing import Any, AsyncIterator
from uuid import uuid4, UUID
from decimal import Decimal

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..storage import StorageClient, generate_storage_key, get_storage
from .base import BaseIngester, IngestionConfig, with_retry, Neo4jHelper
from .search import search_all_sources
from ..resolution.matcher import normalize_organization_name, FuzzyMatcher, MatchCandidate
from rapidfuzz import fuzz

logger = get_context_logger(__name__)

# Elections Canada URLs
# Third Party Registry export endpoint
EC_THIRD_PARTY_SEARCH_URL = "https://www.elections.ca/WPAPPS/WPR/EN/TP"
EC_THIRD_PARTY_EXPORT_URL = "https://www.elections.ca/WPAPPS/WPR/EN/TP/ExportReport"

# Financial returns pages by election
EC_FINANCIAL_RETURNS_BASE = "https://www.elections.ca/content.aspx?section=fin&dir=oth/thi/advert"

# Election identifiers - maps user-friendly IDs to Elections Canada database IDs
# The registry uses database IDs (like "53" for 44th GE), but we expose friendly IDs ("44")
ELECTIONS = {
    "45": {"name": "45th General Election", "date": "2025-04-28", "writ": "2025-03-23", "db_id": "62"},
    "44": {"name": "44th General Election", "date": "2021-09-20", "writ": "2021-08-15", "db_id": "53"},
    "43": {"name": "43rd General Election", "date": "2019-10-21", "writ": "2019-09-11", "db_id": "51"},
    "42": {"name": "42nd General Election", "date": "2015-10-19", "writ": "2015-08-02", "db_id": "41"},
    "41": {"name": "41st General Election", "date": "2011-05-02", "writ": "2011-03-26", "db_id": "34"},
}

# Media type categories for advertising expenses
MEDIA_TYPES = [
    "television",
    "radio",
    "print_media",
    "social_media",
    "digital",
    "signs",
    "mailouts",
    "content_boosting",
    "design_development",
    "production_costs",
    "placement_costs",
    "other",
]


# =========================
# Vendor Resolution Functions
# =========================


async def resolve_vendor_to_organization(
    session,  # Neo4j session
    vendor_name: str,
    log,
) -> dict | None:
    """Try to match a vendor name against existing Organization nodes.

    Returns match info if found with confidence >= 0.7, None otherwise.
    """
    normalized = normalize_organization_name(vendor_name)
    if not normalized:
        return None

    # Check existing Neo4j Organization nodes by normalized name
    result = await session.run(
        """
        MATCH (o:Organization)
        WHERE toUpper(o.name) CONTAINS $normalized
           OR toUpper(o.name) STARTS WITH $prefix
        RETURN o.id as id, o.name as name,
               o.canada_corp_num as corp_num,
               o.bn as bn,
               o.ein as ein,
               o.cik as cik
        LIMIT 10
        """,
        normalized=normalized,
        prefix=normalized[: min(10, len(normalized))],
    )

    existing_orgs = await result.data()

    if existing_orgs:
        # Use FuzzyMatcher to find best match
        matcher = FuzzyMatcher(min_score=70)

        source = MatchCandidate(
            entity_id=UUID("00000000-0000-0000-0000-000000000000"),
            entity_type="vendor",
            name=vendor_name,
            identifiers={},
            attributes={},
        )

        candidates = [
            MatchCandidate(
                entity_id=(
                    UUID(org["id"])
                    if org["id"]
                    else UUID("00000000-0000-0000-0000-000000000000")
                ),
                entity_type="organization",
                name=org["name"],
                identifiers={
                    k: v
                    for k, v in [
                        ("bn", org.get("bn")),
                        ("ein", org.get("ein")),
                        ("corp_num", org.get("corp_num")),
                        ("cik", org.get("cik")),
                    ]
                    if v
                },
                attributes={},
            )
            for org in existing_orgs
        ]

        matches = matcher.find_matches(source, candidates, threshold=0.7)

        if matches and matches[0].confidence >= 0.7:
            best = matches[0]
            return {
                "org_id": str(best.target.entity_id),
                "org_name": best.target.name,
                "confidence": best.confidence,
                "match_type": "neo4j_existing",
                "identifiers": best.target.identifiers,
            }

    return None


async def search_vendor_in_sources(
    vendor_name: str,
    log,
) -> dict | None:
    """Search external sources for vendor information.

    Returns best match if found, None otherwise.
    """
    try:
        response = await search_all_sources(vendor_name, limit=5)

        if not response.results:
            return None

        # Find best result using fuzzy matching
        normalized_vendor = normalize_organization_name(vendor_name)

        best_match = None
        best_score = 0

        for result in response.results:
            normalized_result = normalize_organization_name(result.name)
            score = fuzz.WRatio(normalized_vendor, normalized_result)

            if score > best_score and score >= 75:
                best_score = score
                best_match = result

        if best_match:
            return {
                "source": best_match.source,
                "identifier": best_match.identifier,
                "identifier_type": best_match.identifier_type,
                "name": best_match.name,
                "confidence": best_score / 100,
                "details": best_match.details,
            }

    except Exception as e:
        log.warning(f"External search failed for {vendor_name}: {e}")

    return None


class AdvertisingExpense(BaseModel):
    """Advertising expense by media type."""

    media_type: str
    amount: Decimal = Decimal("0")
    description: str | None = None


class ExpenseLineItem(BaseModel):
    """A single expense line item from the financial return."""

    supplier: str
    expense_type: str | None = None
    expense_category: str | None = None
    expense_subcategory: str | None = None
    amount: Decimal = Decimal("0")
    date_incurred: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    place: str | None = None


class Contributor(BaseModel):
    """A contributor to a third party."""

    name: str
    city: str | None = None
    province: str | None = None
    postal_code: str | None = None
    date_received: date | None = None
    amount: Decimal = Decimal("0")
    contributor_type: str = "individual"  # individual, business, corporation, union, etc.


class ElectionThirdParty(BaseModel):
    """A third party registered for an election."""

    # Registration info
    third_party_name: str = Field(..., description="Name of the third party")
    election_id: str = Field(..., description="Election identifier (e.g., '44' for 44th GE)")
    election_name: str | None = None

    # Contact info
    city: str | None = None
    province: str | None = None
    postal_code: str | None = None

    # Registration details
    registered_date: date | None = None
    applicant_name: str | None = None

    # Financial agent
    financial_agent_name: str | None = None
    financial_agent_city: str | None = None
    financial_agent_province: str | None = None

    # Auditor
    auditor_name: str | None = None
    auditor_city: str | None = None

    # Advertising expenses (from financial returns)
    total_expenses: Decimal = Decimal("0")
    expenses_by_media: list[AdvertisingExpense] = Field(default_factory=list)

    # Pre-election vs election period
    pre_election_expenses: Decimal = Decimal("0")
    election_period_expenses: Decimal = Decimal("0")

    # Detailed expense line items with suppliers
    expense_items: list[ExpenseLineItem] = Field(default_factory=list)

    # Contributors
    contributors: list[Contributor] = Field(default_factory=list)

    # PDF links (stored during fetch, used for PDF parsing)
    pdf_links: list[str] = Field(default_factory=list)


class ElectionsCanadaIngester(BaseIngester[ElectionThirdParty]):
    """Ingester for Elections Canada third party data."""

    def __init__(self):
        super().__init__("elections_canada")
        self._http_client: httpx.AsyncClient | None = None
        self._storage: StorageClient | None = None
        self._enrich_vendors: bool = False

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

    @property
    def storage(self) -> StorageClient:
        if self._storage is None:
            self._storage = get_storage()
        return self._storage

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
        """Fetch third party records from Elections Canada."""

        # Determine which elections to process
        election_ids = config.extra_params.get("elections", list(ELECTIONS.keys()))
        if isinstance(election_ids, str):
            election_ids = [e.strip() for e in election_ids.split(",")]

        self.logger.info(f"Processing elections: {election_ids}")

        for election_id in election_ids:
            election_info = ELECTIONS.get(election_id)
            if not election_info:
                self.logger.warning(f"Unknown election: {election_id}")
                continue

            self.logger.info(f"Fetching third parties for {election_info['name']}")

            # Fetch third party list for this election
            third_parties = await self._fetch_third_parties_for_election(election_id)

            self.logger.info(f"Found {len(third_parties)} third parties for election {election_id}")

            # Filter by target entities if specified
            if config.target_entities:
                target_patterns = [n.lower() for n in config.target_entities]
                filtered = []
                for tp in third_parties:
                    name_lower = tp.third_party_name.lower()
                    for pattern in target_patterns:
                        if pattern in name_lower:
                            filtered.append(tp)
                            break
                third_parties = filtered
                self.logger.info(f"Filtered to {len(third_parties)} matching target entities")

            # Check if we should parse PDFs for expense details
            parse_pdfs = config.extra_params.get("parse_pdfs", False)

            # Store enrich_vendors flag for use in process_record
            self._enrich_vendors = config.extra_params.get("enrich_vendors", False)

            for tp in third_parties:
                # Optionally parse PDFs for expense and contributor details
                if parse_pdfs and tp.pdf_links:
                    self.logger.info(f"Parsing PDFs for {tp.third_party_name}")

                    # Select the best available report (most complete/recent)
                    # Priority: ECR (final) > due7 (7-day interim) > due21 (21-day interim)
                    # PDF naming: TP-XXXX_ecr.pdf, TP-XXXX_due7.pdf, TP-XXXX_due21.pdf
                    ecr_pdf = next((url for url in tp.pdf_links if "_ecr.pdf" in url.lower()), None)
                    due7_pdf = next((url for url in tp.pdf_links if "_due7.pdf" in url.lower()), None)
                    due21_pdf = next((url for url in tp.pdf_links if "_due21.pdf" in url.lower()), None)

                    # Use best available report
                    best_pdf = ecr_pdf or due7_pdf or due21_pdf
                    report_type = "ecr" if ecr_pdf else ("due7" if due7_pdf else "due21")

                    if best_pdf:
                        self.logger.info(f"  Using {report_type} report: {best_pdf.split('/')[-1]}")
                        expenses, contributors = await self._parse_pdf_expenses(best_pdf)
                        tp.expense_items.extend(expenses)
                        tp.contributors.extend(contributors)
                    else:
                        self.logger.warning(f"  No recognized PDF reports found")

                    # Calculate totals
                    tp.total_expenses = sum(e.amount for e in tp.expense_items)

                yield tp
                # Note: Limit checking is handled by base class run() method

    async def _fetch_third_parties_for_election(
        self, election_id: str
    ) -> list[ElectionThirdParty]:
        """Fetch third party list from the financial returns page for an election."""
        from bs4 import BeautifulSoup

        election_info = ELECTIONS.get(election_id, {})
        results = []

        # Financial returns page URL
        # e.g., https://www.elections.ca/content.aspx?section=fin&dir=oth/thi/advert/tp45&document=index&lang=e
        url = f"https://www.elections.ca/content.aspx?section=fin&dir=oth/thi/advert/tp{election_id}&document=index&lang=e"

        self.logger.info(f"Fetching financial returns from: {url}")

        try:
            response = await self.http_client.get(url)

            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch financial returns page: {response.status_code}")
                return results

            soup = BeautifulSoup(response.text, "html.parser")

            # Find the main table with third party data
            table = soup.find("table")
            if not table:
                self.logger.warning("No table found on financial returns page")
                return results

            rows = table.find_all("tr")
            self.logger.info(f"Found {len(rows)} rows in table")

            # Skip header row
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue

                # First cell is the third party name
                name = cells[0].get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Extract PDF links for financial returns
                pdf_links = []
                for cell in cells[1:]:
                    links = cell.find_all("a", href=True)
                    for link in links:
                        href = link["href"]
                        if ".pdf" in href.lower():
                            # Make absolute URL
                            if not href.startswith("http"):
                                href = f"https://www.elections.ca/{href.lstrip('/')}"
                            pdf_links.append(href)

                tp = ElectionThirdParty(
                    third_party_name=name,
                    election_id=election_id,
                    election_name=election_info.get("name"),
                    pdf_links=pdf_links,
                )

                results.append(tp)

            self.logger.info(f"Parsed {len(results)} third parties from financial returns")

        except Exception as e:
            self.logger.warning(f"Failed to fetch/parse financial returns: {e}")

        return results

    async def _parse_pdf_expenses(
        self, pdf_url: str
    ) -> tuple[list[ExpenseLineItem], list[Contributor]]:
        """Download and parse a financial return PDF to extract expenses and contributors."""
        import io

        try:
            import pdfplumber
        except ImportError:
            self.logger.warning("pdfplumber not installed - skipping PDF parsing")
            return [], []

        expenses = []
        contributors = []

        try:
            # Download PDF
            response = await self.http_client.get(pdf_url)
            if response.status_code != 200:
                self.logger.warning(f"Failed to download PDF: {response.status_code}")
                return [], []

            pdf_content = response.content
            self.logger.info(f"Downloaded PDF: {len(pdf_content)} bytes")

            # Parse PDF
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    tables = page.extract_tables()

                    # Check if this is an expense page (Part 3a or 3b)
                    if "statementofexpenses" in text.lower().replace(" ", "") or "part3" in text.lower().replace(" ", ""):
                        for table in tables:
                            if not table or len(table) < 2:
                                continue

                            # Check if this looks like an expense table (has Supplier column)
                            header = table[0] if table else []
                            header_str = str(header).lower()

                            if "supplier" in header_str:
                                # Parse expense rows
                                for row in table[1:]:
                                    if not row or len(row) < 4:
                                        continue

                                    # Skip empty rows
                                    supplier = row[3] if len(row) > 3 else ""
                                    if not supplier or supplier.strip() == "":
                                        continue

                                    # Parse amount (last column)
                                    amount_str = row[-1] if row else "0"
                                    try:
                                        amount = Decimal(
                                            amount_str.replace(",", "").replace("$", "").strip() or "0"
                                        )
                                    except:
                                        amount = Decimal("0")

                                    if amount > 0:
                                        expense = ExpenseLineItem(
                                            supplier=supplier.strip(),
                                            expense_type=row[4] if len(row) > 4 else None,
                                            expense_category=row[5] if len(row) > 5 else None,
                                            expense_subcategory=row[6] if len(row) > 6 else None,
                                            amount=amount,
                                            place=row[9] if len(row) > 9 else None,
                                        )

                                        # Parse dates if present
                                        if len(row) > 1 and row[1]:
                                            try:
                                                expense.date_incurred = datetime.strptime(
                                                    row[1].strip(), "%Y/%m/%d"
                                                ).date()
                                            except:
                                                pass

                                        expenses.append(expense)
                                        self.logger.debug(
                                            f"  Expense: {expense.supplier} - ${expense.amount}"
                                        )

                    # Check if this is a contributions page (Part 2a)
                    elif "statementofmonetarycontributions" in text.lower().replace(" ", ""):
                        for table in tables:
                            if not table or len(table) < 2:
                                continue

                            header = table[0] if table else []
                            header_str = str(header).lower()

                            if "fullname" in header_str and "individual" in header_str:
                                # Parse contributor rows
                                for row in table[1:]:
                                    if not row or len(row) < 10:
                                        continue

                                    # First column is row number, second is name
                                    name = row[1] if len(row) > 1 else ""
                                    if not name or name.strip() == "":
                                        continue

                                    # Find amount from contribution columns (indices 9-14)
                                    # Columns: Individual, Business/Commercial, Government,
                                    #          Trade union, Corporation without share capital,
                                    #          Unincorporated organization
                                    amount = Decimal("0")
                                    contributor_type = "individual"
                                    contrib_types = [
                                        (9, "individual"),
                                        (10, "business"),
                                        (11, "government"),
                                        (12, "union"),
                                        (13, "corporation"),
                                        (14, "association"),
                                    ]
                                    for col_idx, ctype in contrib_types:
                                        if col_idx < len(row) and row[col_idx]:
                                            cell = row[col_idx].strip()
                                            if cell:
                                                try:
                                                    val = Decimal(
                                                        cell.replace(",", "").replace("$", "").strip()
                                                    )
                                                    if val > 0:
                                                        amount = val
                                                        contributor_type = ctype
                                                        break
                                                except:
                                                    pass

                                    if amount > 0:
                                        contributor = Contributor(
                                            name=name.strip(),
                                            city=row[5] if len(row) > 5 else None,
                                            postal_code=row[7] if len(row) > 7 else None,
                                            amount=amount,
                                            contributor_type=contributor_type,
                                        )
                                        contributors.append(contributor)

            self.logger.info(
                f"Parsed PDF: {len(expenses)} expenses, {len(contributors)} contributors"
            )

        except Exception as e:
            self.logger.warning(f"Failed to parse PDF {pdf_url}: {e}")

        return expenses, contributors

    async def process_record(self, record: ElectionThirdParty) -> dict[str, Any]:
        """Process a third party registration record."""
        result = {"created": False, "updated": False, "entity_id": None}

        self.logger.info(
            f"Processing: {record.third_party_name} ({record.election_name})"
        )

        # --- PostgreSQL: Create/Update entity ---
        async with get_db_session() as db:
            # Check if entity exists
            check_result = await db.execute(
                text("""
                    SELECT id FROM entities
                    WHERE LOWER(name) = LOWER(:name)
                    AND entity_type = 'organization'
                """),
                {"name": record.third_party_name},
            )
            existing = check_result.fetchone()

            # Build election registration data
            election_reg = {
                "election_id": record.election_id,
                "election_name": record.election_name,
                "registered_date": record.registered_date.isoformat() if record.registered_date else None,
                "applicant": record.applicant_name,
                "financial_agent": record.financial_agent_name,
                "auditor": record.auditor_name,
            }

            entity_data = {
                "name": record.third_party_name,
                "entity_type": "organization",
                "external_ids": {
                    f"ec_third_party_{record.election_id}": record.third_party_name,
                },
                "metadata": {
                    "source": "elections_canada",
                    "city": record.city,
                    "province": record.province,
                    "election_registrations": [election_reg],
                    "is_election_third_party": True,
                },
            }

            if existing:
                # Update existing entity, merging election registrations
                current_result = await db.execute(
                    text("SELECT metadata FROM entities WHERE id = :id"),
                    {"id": existing.id},
                )
                current = current_result.fetchone()
                current_metadata = current.metadata if current and current.metadata else {}

                # Merge election registrations
                existing_regs = current_metadata.get("election_registrations", [])
                # Check if this election is already registered
                existing_election_ids = {r.get("election_id") for r in existing_regs}
                if record.election_id not in existing_election_ids:
                    existing_regs.append(election_reg)

                merged_metadata = {**current_metadata, **entity_data["metadata"]}
                merged_metadata["election_registrations"] = existing_regs

                await db.execute(
                    text("""
                        UPDATE entities
                        SET metadata = CAST(:metadata AS jsonb),
                            external_ids = external_ids || CAST(:external_ids AS jsonb),
                            updated_at = NOW()
                        WHERE id = :id
                    """),
                    {
                        "id": existing.id,
                        "metadata": json.dumps(merged_metadata),
                        "external_ids": json.dumps(entity_data["external_ids"]),
                    },
                )
                result["updated"] = True
                result["entity_id"] = str(existing.id)
                self.logger.info(f"  PostgreSQL: updated entity {existing.id}")
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
                self.logger.info(f"  PostgreSQL: created entity {new_id}")

            # Note: db.commit() is handled automatically by get_db_session() context manager

        # --- Neo4j: Create nodes and relationships ---
        try:
            async with get_neo4j_session() as session:
                now = datetime.utcnow().isoformat()

                # Create/update Organization node for the third party
                org_props = {
                    "id": result["entity_id"],
                    "name": record.third_party_name,
                    "entity_type": "ORGANIZATION",
                    "is_election_third_party": True,
                    "city": record.city,
                    "province": record.province,
                    "updated_at": now,
                }

                await session.run(
                    """
                    MERGE (o:Organization {name: $name})
                    ON CREATE SET o += $props
                    ON MATCH SET o.is_election_third_party = true,
                                 o.city = COALESCE(o.city, $props.city),
                                 o.province = COALESCE(o.province, $props.province),
                                 o.updated_at = $props.updated_at
                    """,
                    name=record.third_party_name,
                    props=org_props,
                )

                # Create Election node
                election_info = ELECTIONS.get(record.election_id, {})
                election_props = {
                    "id": f"election_{record.election_id}",
                    "name": election_info.get("name", f"Election {record.election_id}"),
                    "election_id": record.election_id,
                    "election_date": election_info.get("date"),
                    "writ_date": election_info.get("writ"),
                    "jurisdiction": "federal",
                    "country": "CA",
                    "updated_at": now,
                }

                await session.run(
                    """
                    MERGE (e:Election {election_id: $election_id})
                    ON CREATE SET e += $props
                    ON MATCH SET e.updated_at = $props.updated_at
                    """,
                    election_id=record.election_id,
                    props=election_props,
                )

                # Create REGISTERED_FOR relationship between Organization and Election
                reg_props = {
                    "source": "elections_canada",
                    "confidence": 1.0,
                    "updated_at": now,
                }
                if record.registered_date:
                    reg_props["registered_date"] = record.registered_date.isoformat()
                if record.applicant_name:
                    reg_props["applicant"] = record.applicant_name
                if record.financial_agent_name:
                    reg_props["financial_agent"] = record.financial_agent_name
                if record.auditor_name:
                    reg_props["auditor"] = record.auditor_name

                await session.run(
                    """
                    MATCH (o:Organization {name: $org_name})
                    MATCH (e:Election {election_id: $election_id})
                    MERGE (o)-[r:REGISTERED_FOR]->(e)
                    SET r += $props
                    """,
                    org_name=record.third_party_name,
                    election_id=record.election_id,
                    props=reg_props,
                )

                self.logger.info(
                    f"  Neo4j: {record.third_party_name} -[REGISTERED_FOR]-> Election {record.election_id}"
                )

                # Create Person nodes for financial agent and auditor
                if record.financial_agent_name:
                    await session.run(
                        """
                        MERGE (p:Person {name: $name})
                        ON CREATE SET p.id = $id,
                                      p.entity_type = 'PERSON',
                                      p.updated_at = $now
                        ON MATCH SET p.updated_at = $now
                        """,
                        name=record.financial_agent_name,
                        id=str(uuid4()),
                        now=now,
                    )

                    # Create FINANCIAL_AGENT_FOR relationship
                    await session.run(
                        """
                        MATCH (p:Person {name: $person_name})
                        MATCH (o:Organization {name: $org_name})
                        MERGE (p)-[r:FINANCIAL_AGENT_FOR]->(o)
                        SET r.election_id = $election_id,
                            r.source = 'elections_canada',
                            r.updated_at = $now
                        """,
                        person_name=record.financial_agent_name,
                        org_name=record.third_party_name,
                        election_id=record.election_id,
                        now=now,
                    )

                if record.auditor_name:
                    # Auditor is usually a firm (Organization)
                    await session.run(
                        """
                        MERGE (a:Organization {name: $name})
                        ON CREATE SET a.id = $id,
                                      a.entity_type = 'ORGANIZATION',
                                      a.is_auditor = true,
                                      a.updated_at = $now
                        ON MATCH SET a.is_auditor = true,
                                     a.updated_at = $now
                        """,
                        name=record.auditor_name,
                        id=str(uuid4()),
                        now=now,
                    )

                    # Create AUDITED_BY relationship
                    await session.run(
                        """
                        MATCH (o:Organization {name: $org_name})
                        MATCH (a:Organization {name: $auditor_name})
                        MERGE (o)-[r:AUDITED_BY]->(a)
                        SET r.election_id = $election_id,
                            r.source = 'elections_canada',
                            r.updated_at = $now
                        """,
                        org_name=record.third_party_name,
                        auditor_name=record.auditor_name,
                        election_id=record.election_id,
                        now=now,
                    )

                # Create advertising expense nodes if available
                if record.expenses_by_media:
                    for expense in record.expenses_by_media:
                        if expense.amount > 0:
                            # Create AdExpense relationship to MediaType
                            await session.run(
                                """
                                MERGE (m:MediaType {name: $media_type})
                                ON CREATE SET m.id = $media_id
                                WITH m
                                MATCH (o:Organization {name: $org_name})
                                MERGE (o)-[r:ADVERTISED_ON]->(m)
                                SET r.amount = COALESCE(r.amount, 0) + $amount,
                                    r.election_id = $election_id,
                                    r.source = 'elections_canada',
                                    r.updated_at = $now
                                """,
                                media_type=expense.media_type,
                                media_id=f"media_{expense.media_type}",
                                org_name=record.third_party_name,
                                amount=float(expense.amount),
                                election_id=record.election_id,
                                now=now,
                            )

                # Create Vendor nodes and PAID_BY relationships for expense items
                if record.expense_items:
                    # Group expenses by supplier to aggregate amounts
                    supplier_totals: dict[str, Decimal] = {}
                    supplier_details: dict[str, list[ExpenseLineItem]] = {}

                    for expense in record.expense_items:
                        supplier = expense.supplier
                        if supplier not in supplier_totals:
                            supplier_totals[supplier] = Decimal("0")
                            supplier_details[supplier] = []
                        supplier_totals[supplier] += expense.amount
                        supplier_details[supplier].append(expense)

                    for supplier, total_amount in supplier_totals.items():
                        if total_amount <= 0:
                            continue

                        # Get expense types for this supplier
                        expense_types = list(set(
                            e.expense_type for e in supplier_details[supplier]
                            if e.expense_type
                        ))

                        # --- Try to resolve vendor to existing Organization ---
                        vendor_match = await resolve_vendor_to_organization(
                            session, supplier, self.logger
                        )

                        # If no local match and enrich_vendors is enabled, search external sources
                        if not vendor_match and self._enrich_vendors:
                            external_match = await search_vendor_in_sources(
                                supplier, self.logger
                            )
                            if external_match:
                                self.logger.info(
                                    f"  Vendor '{supplier}' found in {external_match['source']} "
                                    f"({external_match['identifier_type']}: {external_match['identifier']})"
                                )
                                # Store external match info for the Vendor node
                                vendor_match = {
                                    "org_id": None,  # Not linked yet, just metadata
                                    "org_name": external_match["name"],
                                    "confidence": external_match["confidence"],
                                    "match_type": "external_search",
                                    "external_source": external_match["source"],
                                    "external_id": external_match["identifier"],
                                    "external_id_type": external_match["identifier_type"],
                                }

                        if vendor_match and vendor_match["confidence"] >= 0.9 and vendor_match.get("org_id"):
                            # HIGH CONFIDENCE: Link directly to existing Organization
                            self.logger.info(
                                f"  Vendor '{supplier}' matched to Organization "
                                f"'{vendor_match['org_name']}' (confidence: {vendor_match['confidence']:.0%})"
                            )

                            # Create PAID_BY from existing Org instead of new Vendor
                            await session.run(
                                """
                                MATCH (tp:Organization {name: $third_party_name})
                                MATCH (o:Organization {id: $org_id})
                                MERGE (o)-[r:PAID_BY {election_id: $election_id}]->(tp)
                                ON CREATE SET r.created_at = $now
                                SET r.amount = $amount,
                                    r.expense_types = $expense_types,
                                    r.source = 'elections_canada',
                                    r.vendor_name_original = $vendor_name,
                                    r.updated_at = $now
                                """,
                                third_party_name=record.third_party_name,
                                org_id=vendor_match["org_id"],
                                amount=float(total_amount),
                                election_id=record.election_id,
                                expense_types=expense_types[:5],
                                vendor_name=supplier,
                                now=now,
                            )
                            continue  # Skip Vendor node creation

                        # Determine vendor type based on name
                        vendor_type = "advertising"
                        if any(kw in supplier.lower() for kw in ["facebook", "meta", "google", "twitter", "x.com"]):
                            vendor_type = "digital_platform"
                        elif any(kw in supplier.lower() for kw in ["radio", "cftr", "cfrb", "cknw"]):
                            vendor_type = "radio"
                        elif any(kw in supplier.lower() for kw in ["tv", "television", "ctv", "cbc", "global"]):
                            vendor_type = "television"
                        elif any(kw in supplier.lower() for kw in ["newspaper", "star", "globe", "post", "sun"]):
                            vendor_type = "print"

                        # Build extra props for potential matches
                        vendor_props_extra = {}
                        if vendor_match and vendor_match["confidence"] >= 0.7:
                            # MEDIUM CONFIDENCE: Create Vendor but add potential_match metadata
                            if vendor_match.get("match_type") == "external_search":
                                # External match - store source info for future enrichment
                                self.logger.info(
                                    f"  Vendor '{supplier}' may match '{vendor_match['org_name']}' "
                                    f"from {vendor_match['external_source']} (confidence: {vendor_match['confidence']:.0%})"
                                )
                                vendor_props_extra = {
                                    "potential_org_match": vendor_match["org_name"],
                                    "match_confidence": vendor_match["confidence"],
                                    "external_source": vendor_match["external_source"],
                                    "external_id": vendor_match["external_id"],
                                    "external_id_type": vendor_match["external_id_type"],
                                }
                            else:
                                # Local match - store org ID for linking
                                self.logger.info(
                                    f"  Vendor '{supplier}' may match Organization "
                                    f"'{vendor_match['org_name']}' (confidence: {vendor_match['confidence']:.0%}) - needs review"
                                )
                                vendor_props_extra = {
                                    "potential_org_match": vendor_match["org_name"],
                                    "potential_org_id": vendor_match["org_id"],
                                    "match_confidence": vendor_match["confidence"],
                                }

                        # Create Vendor node (with optional match metadata)
                        await session.run(
                            """
                            MERGE (v:Vendor {name: $name})
                            ON CREATE SET v.id = $id,
                                          v.entity_type = 'VENDOR',
                                          v.vendor_type = $vendor_type,
                                          v.normalized_name = $normalized,
                                          v.created_at = $now
                            ON MATCH SET v.updated_at = $now
                            SET v += $extra_props
                            """,
                            name=supplier,
                            id=str(uuid4()),
                            vendor_type=vendor_type,
                            normalized=normalize_organization_name(supplier),
                            extra_props=vendor_props_extra,
                            now=now,
                        )

                        # Create PAID_BY relationship (Third Party paid the Vendor)
                        # Use election_id in the match to allow different amounts per election
                        # Replace (not accumulate) amounts to support interim -> final updates
                        await session.run(
                            """
                            MATCH (tp:Organization {name: $third_party_name})
                            MATCH (v:Vendor {name: $vendor_name})
                            MERGE (v)-[r:PAID_BY {election_id: $election_id}]->(tp)
                            ON CREATE SET r.created_at = $now
                            SET r.amount = $amount,
                                r.expense_types = $expense_types,
                                r.source = 'elections_canada',
                                r.updated_at = $now
                            """,
                            third_party_name=record.third_party_name,
                            vendor_name=supplier,
                            amount=float(total_amount),
                            election_id=record.election_id,
                            expense_types=expense_types[:5],  # Limit to first 5
                            now=now,
                        )

                        self.logger.info(
                            f"  Neo4j: {supplier} -[PAID_BY ${total_amount}]-> {record.third_party_name}"
                        )

                # Create Person nodes for contributors and CONTRIBUTED_TO relationships
                if record.contributors:
                    self.logger.info(f"  Processing {len(record.contributors)} contributors")
                    for contributor in record.contributors[:50]:  # Limit to top 50
                        if contributor.amount < 200:  # Only contributors over $200
                            continue

                        # Create Person node
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
                        # Use election_id in the match to allow different amounts per election
                        # Replace (not accumulate) amounts to support interim -> final updates
                        await session.run(
                            """
                            MATCH (p:Person {name: $person_name})
                            MATCH (tp:Organization {name: $third_party_name})
                            MERGE (p)-[r:CONTRIBUTED_TO {election_id: $election_id}]->(tp)
                            ON CREATE SET r.created_at = $now
                            SET r.amount = $amount,
                                r.source = 'elections_canada',
                                r.updated_at = $now
                            """,
                            person_name=contributor.name,
                            third_party_name=record.third_party_name,
                            amount=float(contributor.amount),
                            election_id=record.election_id,
                            now=now,
                        )

                self.logger.info(f"  Neo4j: completed for {record.third_party_name}")

        except Exception as e:
            self.logger.warning(f"  Neo4j: FAILED - {e}")

        return result


async def run_elections_canada_ingestion(
    limit: int | None = None,
    incremental: bool = True,
    target_entities: list[str] | None = None,
    elections: list[str] | None = None,
    parse_pdfs: bool = False,
    enrich_vendors: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run Elections Canada third party ingestion.

    Args:
        limit: Maximum number of third parties to process
        incremental: Whether to do incremental sync
        target_entities: Optional list of third party names to filter
        elections: Optional list of election IDs to process (e.g., ["44", "45"])
        parse_pdfs: Whether to download and parse PDF financial returns for
                    detailed expense and contributor data
        enrich_vendors: Whether to search external sources for vendor matches
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary
    """
    ingester = ElectionsCanadaIngester()

    try:
        extra_params = {
            "parse_pdfs": parse_pdfs,
            "enrich_vendors": enrich_vendors,
        }
        if elections:
            extra_params["elections"] = elections

        config = IngestionConfig(
            incremental=incremental,
            limit=limit,
            target_entities=target_entities,
            extra_params=extra_params,
        )
        result = await ingester.run(config, run_id=run_id)
        return result.model_dump()
    finally:
        await ingester.close()
