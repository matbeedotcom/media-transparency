"""OpenCorporates API client for MITDS.

Provides company and officer data ingestion from OpenCorporates API.
Implements rate limiting, caching, and retry logic for reliable data collection.

API Documentation: https://api.opencorporates.com/documentation/API-Reference
"""

from datetime import datetime, timedelta
from typing import Any, AsyncIterator
from uuid import UUID, uuid4
import asyncio
import json
import hashlib

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from .base import BaseIngester, IngestionResult
from ..config import get_settings
from ..db import get_db_session, get_redis
from ..logging import get_context_logger
from ..worker import app as celery_app

logger = get_context_logger(__name__)


# =========================
# Data Models
# =========================


class OpenCorpCompany(BaseModel):
    """Company record from OpenCorporates."""

    jurisdiction_code: str
    company_number: str
    name: str
    company_type: str | None = None
    incorporation_date: str | None = None
    dissolution_date: str | None = None
    current_status: str | None = None
    registered_address: dict[str, Any] | None = None
    registered_address_in_full: str | None = None
    industry_codes: list[dict[str, Any]] = Field(default_factory=list)
    identifiers: list[dict[str, Any]] = Field(default_factory=list)
    previous_names: list[dict[str, Any]] = Field(default_factory=list)
    source: dict[str, Any] | None = None
    opencorporates_url: str | None = None


class OpenCorpOfficer(BaseModel):
    """Officer record from OpenCorporates."""

    id: str
    name: str
    position: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    inactive: bool = False
    current_status: str | None = None
    occupation: str | None = None
    nationality: str | None = None
    address: str | None = None


class OpenCorpFiling(BaseModel):
    """Company filing record from OpenCorporates."""

    id: str
    title: str
    filing_type: str | None = None
    filing_date: str | None = None
    filing_code: str | None = None
    url: str | None = None
    opencorporates_url: str | None = None


class CompanyWithOfficers(BaseModel):
    """Complete company record with officers."""

    company: OpenCorpCompany
    officers: list[OpenCorpOfficer] = Field(default_factory=list)
    filings: list[OpenCorpFiling] = Field(default_factory=list)


# =========================
# API Client
# =========================


class OpenCorporatesClient:
    """Client for OpenCorporates API with rate limiting and caching.

    Rate limits:
    - Free tier: 200 requests/day, 20 requests/minute
    - API key: 500 requests/day, 50 requests/minute (varies by plan)

    Uses Redis for:
    - Request rate tracking
    - Response caching (24h default)
    """

    BASE_URL = "https://api.opencorporates.com/v0.4"

    # Rate limiting defaults (free tier)
    DEFAULT_REQUESTS_PER_MINUTE = 18  # Stay under 20/min limit
    DEFAULT_REQUESTS_PER_DAY = 180  # Stay under 200/day limit

    # Cache TTL
    CACHE_TTL_SECONDS = 86400  # 24 hours

    def __init__(
        self,
        api_token: str | None = None,
        requests_per_minute: int | None = None,
        requests_per_day: int | None = None,
    ):
        """Initialize the client.

        Args:
            api_token: OpenCorporates API token (optional, increases limits)
            requests_per_minute: Rate limit per minute
            requests_per_day: Rate limit per day
        """
        settings = get_settings()
        self.api_token = api_token or settings.opencorporates_api_key

        # Set rate limits based on whether we have an API key
        if self.api_token:
            self.requests_per_minute = requests_per_minute or 45
            self.requests_per_day = requests_per_day or 450
        else:
            self.requests_per_minute = requests_per_minute or self.DEFAULT_REQUESTS_PER_MINUTE
            self.requests_per_day = requests_per_day or self.DEFAULT_REQUESTS_PER_DAY

        self._http_client: httpx.AsyncClient | None = None
        self._redis = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client with retry logic."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={
                    "Accept": "application/json",
                    "User-Agent": "MITDS/1.0 (Media Influence Transparency Detection System)",
                },
            )
        return self._http_client

    async def get_redis(self):
        """Get Redis client for caching and rate limiting."""
        if self._redis is None:
            self._redis = await get_redis()
        return self._redis

    async def close(self):
        """Close the client connections."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits.

        Returns:
            True if request is allowed, False if rate limited
        """
        redis = await self.get_redis()
        now = datetime.utcnow()

        # Check minute limit
        minute_key = f"opencorp:rate:minute:{now.strftime('%Y%m%d%H%M')}"
        minute_count = await redis.incr(minute_key)
        if minute_count == 1:
            await redis.expire(minute_key, 120)  # 2 minute expiry

        if minute_count > self.requests_per_minute:
            logger.warning(
                f"Rate limited: {minute_count}/{self.requests_per_minute} requests/minute"
            )
            return False

        # Check daily limit
        day_key = f"opencorp:rate:day:{now.strftime('%Y%m%d')}"
        day_count = await redis.incr(day_key)
        if day_count == 1:
            await redis.expire(day_key, 90000)  # 25 hour expiry

        if day_count > self.requests_per_day:
            logger.warning(
                f"Rate limited: {day_count}/{self.requests_per_day} requests/day"
            )
            return False

        return True

    async def _get_cached(self, cache_key: str) -> dict[str, Any] | None:
        """Get cached response.

        Args:
            cache_key: Cache key for the request

        Returns:
            Cached response or None
        """
        redis = await self.get_redis()
        cached = await redis.get(cache_key)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return json.loads(cached)
        return None

    async def _set_cached(self, cache_key: str, data: dict[str, Any]):
        """Cache response.

        Args:
            cache_key: Cache key
            data: Response data to cache
        """
        redis = await self.get_redis()
        await redis.setex(
            cache_key,
            self.CACHE_TTL_SECONDS,
            json.dumps(data),
        )

    def _make_cache_key(self, endpoint: str, params: dict[str, Any]) -> str:
        """Generate cache key for request.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Cache key string
        """
        param_str = json.dumps(params, sort_keys=True)
        hash_str = hashlib.md5(f"{endpoint}:{param_str}".encode()).hexdigest()
        return f"opencorp:cache:{hash_str}"

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        use_cache: bool = True,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """Make API request with rate limiting, caching, and retries.

        Args:
            method: HTTP method
            endpoint: API endpoint (e.g., "/companies/search")
            params: Query parameters
            use_cache: Whether to use response caching
            max_retries: Maximum retry attempts

        Returns:
            API response data

        Raises:
            httpx.HTTPStatusError: On API errors
            ValueError: On rate limit exhaustion
        """
        params = params or {}

        # Add API token if available
        if self.api_token:
            params["api_token"] = self.api_token

        # Check cache first
        cache_key = self._make_cache_key(endpoint, params)
        if use_cache:
            cached = await self._get_cached(cache_key)
            if cached:
                return cached

        # Check rate limit
        for attempt in range(max_retries):
            if not await self._check_rate_limit():
                if attempt < max_retries - 1:
                    # Wait and retry
                    wait_time = 65  # Wait over a minute
                    logger.info(f"Rate limited, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise ValueError("Rate limit exceeded after retries")

            try:
                response = await self.http_client.request(
                    method,
                    endpoint,
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                # Cache successful response
                if use_cache:
                    await self._set_cached(cache_key, data)

                return data

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limited by API - wait and retry
                    if attempt < max_retries - 1:
                        wait_time = 120
                        logger.warning(f"API rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                elif e.response.status_code >= 500:
                    # Server error - retry
                    if attempt < max_retries - 1:
                        wait_time = 5 * (attempt + 1)
                        logger.warning(
                            f"Server error {e.response.status_code}, "
                            f"retrying in {wait_time}s"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                raise

            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"Request timeout, retrying (attempt {attempt + 1})")
                    await asyncio.sleep(5)
                    continue
                raise

        raise ValueError("Request failed after all retries")

    # =========================
    # Company Search
    # =========================

    async def search_companies(
        self,
        query: str,
        jurisdiction_code: str | None = None,
        company_type: str | None = None,
        current_status: str | None = None,
        page: int = 1,
        per_page: int = 30,
    ) -> dict[str, Any]:
        """Search for companies.

        Args:
            query: Search query (company name, number, etc.)
            jurisdiction_code: Filter by jurisdiction (e.g., "us_de", "gb")
            company_type: Filter by company type
            current_status: Filter by status (e.g., "Active", "Dissolved")
            page: Page number for pagination
            per_page: Results per page (max 100)

        Returns:
            Search results with pagination info
        """
        params = {
            "q": query,
            "page": page,
            "per_page": min(per_page, 100),
        }

        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        if company_type:
            params["company_type"] = company_type
        if current_status:
            params["current_status"] = current_status

        return await self._make_request("GET", "/companies/search", params)

    async def get_company(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> OpenCorpCompany | None:
        """Get company by jurisdiction and number.

        Args:
            jurisdiction_code: Jurisdiction code (e.g., "us_de")
            company_number: Company registration number

        Returns:
            Company record or None if not found
        """
        try:
            response = await self._make_request(
                "GET",
                f"/companies/{jurisdiction_code}/{company_number}",
            )

            if "company" in response.get("results", {}):
                return OpenCorpCompany(**response["results"]["company"])
            return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_company_officers(
        self,
        jurisdiction_code: str,
        company_number: str,
        page: int = 1,
        per_page: int = 100,
    ) -> list[OpenCorpOfficer]:
        """Get officers for a company.

        Args:
            jurisdiction_code: Jurisdiction code
            company_number: Company registration number
            page: Page number
            per_page: Results per page

        Returns:
            List of officer records
        """
        try:
            response = await self._make_request(
                "GET",
                f"/companies/{jurisdiction_code}/{company_number}/officers",
                {"page": page, "per_page": per_page},
            )

            officers = []
            for item in response.get("results", {}).get("officers", []):
                if "officer" in item:
                    officers.append(OpenCorpOfficer(**item["officer"]))

            return officers

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

    async def get_company_filings(
        self,
        jurisdiction_code: str,
        company_number: str,
        page: int = 1,
        per_page: int = 30,
    ) -> list[OpenCorpFiling]:
        """Get filings for a company.

        Args:
            jurisdiction_code: Jurisdiction code
            company_number: Company registration number
            page: Page number
            per_page: Results per page

        Returns:
            List of filing records
        """
        try:
            response = await self._make_request(
                "GET",
                f"/companies/{jurisdiction_code}/{company_number}/filings",
                {"page": page, "per_page": per_page},
            )

            filings = []
            for item in response.get("results", {}).get("filings", []):
                if "filing" in item:
                    filings.append(OpenCorpFiling(**item["filing"]))

            return filings

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

    async def get_company_with_officers(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> CompanyWithOfficers | None:
        """Get complete company record with officers and filings.

        Args:
            jurisdiction_code: Jurisdiction code
            company_number: Company registration number

        Returns:
            Complete company record or None
        """
        company = await self.get_company(jurisdiction_code, company_number)
        if not company:
            return None

        # Fetch officers and filings in parallel
        officers_task = asyncio.create_task(
            self.get_company_officers(jurisdiction_code, company_number)
        )
        filings_task = asyncio.create_task(
            self.get_company_filings(jurisdiction_code, company_number)
        )

        officers, filings = await asyncio.gather(officers_task, filings_task)

        return CompanyWithOfficers(
            company=company,
            officers=officers,
            filings=filings,
        )

    # =========================
    # Officer Search
    # =========================

    async def search_officers(
        self,
        query: str,
        jurisdiction_code: str | None = None,
        position: str | None = None,
        inactive: bool | None = None,
        page: int = 1,
        per_page: int = 30,
    ) -> dict[str, Any]:
        """Search for officers/directors.

        Args:
            query: Search query (name)
            jurisdiction_code: Filter by jurisdiction
            position: Filter by position (e.g., "director")
            inactive: Filter by active/inactive status
            page: Page number
            per_page: Results per page

        Returns:
            Search results with pagination info
        """
        params = {
            "q": query,
            "page": page,
            "per_page": min(per_page, 100),
        }

        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        if position:
            params["position"] = position
        if inactive is not None:
            params["inactive"] = str(inactive).lower()

        return await self._make_request("GET", "/officers/search", params)


# =========================
# Data Parser
# =========================


class OpenCorporatesParser:
    """Parser for OpenCorporates data into MITDS entities."""

    def parse_company_to_entity(
        self,
        company: OpenCorpCompany,
    ) -> dict[str, Any]:
        """Convert OpenCorporates company to MITDS entity format.

        Args:
            company: OpenCorporates company record

        Returns:
            Entity dictionary for database insertion
        """
        # Map company type to MITDS org type
        org_type = self._map_company_type(company.company_type)

        # Parse dates
        incorporation_date = None
        if company.incorporation_date:
            try:
                incorporation_date = datetime.strptime(
                    company.incorporation_date, "%Y-%m-%d"
                ).date()
            except ValueError:
                pass

        dissolution_date = None
        if company.dissolution_date:
            try:
                dissolution_date = datetime.strptime(
                    company.dissolution_date, "%Y-%m-%d"
                ).date()
            except ValueError:
                pass

        # Determine status
        status = "ACTIVE"
        if company.current_status:
            status_lower = company.current_status.lower()
            if any(s in status_lower for s in ["dissolved", "inactive", "struck"]):
                status = "INACTIVE"
            elif any(s in status_lower for s in ["liquidation", "administration"]):
                status = "DISSOLVED"

        return {
            "entity_type": "ORGANIZATION",
            "name": company.name,
            "org_type": org_type,
            "status": status,
            "jurisdiction": company.jurisdiction_code,
            "registration_number": company.company_number,
            "incorporation_date": incorporation_date,
            "dissolution_date": dissolution_date,
            "address": company.registered_address_in_full,
            "external_ids": {
                "opencorporates_url": company.opencorporates_url,
                "jurisdiction_code": company.jurisdiction_code,
                "company_number": company.company_number,
                **{
                    id_rec["identifier_system_code"]: id_rec["identifier"]
                    for id_rec in company.identifiers
                    if "identifier_system_code" in id_rec
                },
            },
            "metadata": {
                "company_type": company.company_type,
                "industry_codes": company.industry_codes,
                "previous_names": company.previous_names,
                "source": company.source,
            },
        }

    def parse_officer_to_entity(
        self,
        officer: OpenCorpOfficer,
    ) -> dict[str, Any]:
        """Convert OpenCorporates officer to MITDS person entity.

        Args:
            officer: OpenCorporates officer record

        Returns:
            Entity dictionary for database insertion
        """
        return {
            "entity_type": "PERSON",
            "name": officer.name,
            "status": "INACTIVE" if officer.inactive else "ACTIVE",
            "external_ids": {
                "opencorporates_officer_id": officer.id,
            },
            "metadata": {
                "occupation": officer.occupation,
                "nationality": officer.nationality,
                "address": officer.address,
            },
        }

    def parse_officer_relationship(
        self,
        officer: OpenCorpOfficer,
        company_entity_id: UUID,
        person_entity_id: UUID,
    ) -> dict[str, Any]:
        """Create relationship from officer to company.

        Args:
            officer: OpenCorporates officer record
            company_entity_id: ID of the company entity
            person_entity_id: ID of the person entity

        Returns:
            Relationship dictionary for database insertion
        """
        # Map position to relationship type
        rel_type = self._map_position_to_rel_type(officer.position)

        # Parse dates
        start_date = None
        if officer.start_date:
            try:
                start_date = datetime.strptime(officer.start_date, "%Y-%m-%d").date()
            except ValueError:
                pass

        end_date = None
        if officer.end_date:
            try:
                end_date = datetime.strptime(officer.end_date, "%Y-%m-%d").date()
            except ValueError:
                pass

        return {
            "relationship_type": rel_type,
            "from_entity_id": str(person_entity_id),
            "to_entity_id": str(company_entity_id),
            "start_date": start_date,
            "end_date": end_date,
            "is_current": not officer.inactive,
            "properties": {
                "position": officer.position,
                "status": officer.current_status,
            },
        }

    def _map_company_type(self, company_type: str | None) -> str:
        """Map OpenCorporates company type to MITDS org type.

        Args:
            company_type: OpenCorporates company type

        Returns:
            MITDS organization type
        """
        if not company_type:
            return "COMPANY"

        type_lower = company_type.lower()

        # Non-profit types
        if any(t in type_lower for t in ["nonprofit", "non-profit", "charity", "501c"]):
            return "NONPROFIT"

        # Foundation types
        if any(t in type_lower for t in ["foundation", "trust"]):
            return "FOUNDATION"

        # Government types
        if any(t in type_lower for t in ["government", "public body", "state"]):
            return "GOVERNMENT"

        # LLC types
        if "llc" in type_lower or "limited liability" in type_lower:
            return "COMPANY"

        # Corporation types
        if any(t in type_lower for t in ["corporation", "inc", "corp", "plc", "ltd"]):
            return "COMPANY"

        # Partnership types
        if any(t in type_lower for t in ["partnership", "lp", "llp"]):
            return "COMPANY"

        return "COMPANY"

    def _map_position_to_rel_type(self, position: str | None) -> str:
        """Map officer position to relationship type.

        Args:
            position: Officer position title

        Returns:
            Relationship type
        """
        if not position:
            return "EMPLOYED_BY"

        pos_lower = position.lower()

        # Director/board positions
        if any(p in pos_lower for p in ["director", "board", "trustee", "governor"]):
            return "DIRECTOR_OF"

        # Ownership positions
        if any(p in pos_lower for p in ["owner", "shareholder", "member", "partner"]):
            return "OWNS"

        # Executive positions (still employed by)
        if any(p in pos_lower for p in ["ceo", "cfo", "coo", "president", "secretary", "treasurer"]):
            return "EMPLOYED_BY"

        # Default to employed
        return "EMPLOYED_BY"


# =========================
# Ingestion Service
# =========================


class OpenCorporatesIngester:
    """Ingestion service for OpenCorporates data."""

    def __init__(self, api_token: str | None = None):
        """Initialize the ingester.

        Args:
            api_token: OpenCorporates API token
        """
        self.source_name = "opencorporates"
        self.client = OpenCorporatesClient(api_token=api_token)
        self.parser = OpenCorporatesParser()

    async def close(self):
        """Close client connections."""
        await self.client.close()

    async def ingest_company(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> IngestionResult:
        """Ingest a single company with officers.

        Args:
            jurisdiction_code: Jurisdiction code
            company_number: Company registration number

        Returns:
            Ingestion result
        """
        result = IngestionResult(
            run_id=uuid4(),
            source=self.source_name,
            status="running",
            started_at=datetime.utcnow(),
        )

        try:
            # Fetch company with officers
            company_data = await self.client.get_company_with_officers(
                jurisdiction_code, company_number
            )

            if not company_data:
                result.errors.append({
                    "error": f"Company not found: {jurisdiction_code}/{company_number}"
                })
                return result

            result.records_processed = 1

            async with get_db_session() as db:
                # Create or update company entity
                company_entity = self.parser.parse_company_to_entity(company_data.company)
                company_id = await self._upsert_entity(db, company_entity)

                if company_id:
                    result.records_created += 1

                    # Process officers
                    for officer in company_data.officers:
                        result.records_processed += 1

                        # Create person entity
                        person_entity = self.parser.parse_officer_to_entity(officer)
                        person_id = await self._upsert_entity(db, person_entity)

                        if person_id:
                            result.records_created += 1

                            # Create relationship
                            relationship = self.parser.parse_officer_relationship(
                                officer, company_id, person_id
                            )
                            await self._create_relationship(db, relationship)

                await db.commit()

            result.status = "completed"

        except Exception as e:
            logger.exception(f"Error ingesting company {jurisdiction_code}/{company_number}")
            result.errors.append({"error": str(e), "fatal": True})
            result.status = "failed"

        return result

    async def ingest_companies_by_search(
        self,
        query: str,
        jurisdiction_code: str | None = None,
        max_companies: int = 100,
    ) -> IngestionResult:
        """Ingest companies matching search criteria.

        Args:
            query: Search query
            jurisdiction_code: Optional jurisdiction filter
            max_companies: Maximum companies to ingest

        Returns:
            Aggregated ingestion result
        """
        result = IngestionResult(
            run_id=uuid4(),
            source=self.source_name,
            status="running",
            started_at=datetime.utcnow(),
        )
        companies_ingested = 0
        page = 1

        try:
            while companies_ingested < max_companies:
                # Search for companies
                search_results = await self.client.search_companies(
                    query=query,
                    jurisdiction_code=jurisdiction_code,
                    page=page,
                    per_page=min(30, max_companies - companies_ingested),
                )

                companies = search_results.get("results", {}).get("companies", [])
                if not companies:
                    break

                # Ingest each company
                for item in companies:
                    if companies_ingested >= max_companies:
                        break

                    company = item.get("company", {})
                    jur = company.get("jurisdiction_code")
                    num = company.get("company_number")

                    if jur and num:
                        sub_result = await self.ingest_company(jur, num)
                        result.records_processed += sub_result.records_processed
                        result.records_created += sub_result.records_created
                        result.errors.extend(sub_result.errors)
                        companies_ingested += 1

                page += 1

                # Check if there are more pages
                total_pages = search_results.get("results", {}).get("total_pages", 1)
                if page > total_pages:
                    break

            result.status = "completed" if not result.errors else "partial"

        except Exception as e:
            logger.exception(f"Error in company search ingestion: {query}")
            result.errors.append({"error": str(e), "fatal": True})
            result.status = "failed"

        return result

    async def ingest_related_companies(
        self,
        entity_names: list[str],
        jurisdiction_codes: list[str] | None = None,
    ) -> IngestionResult:
        """Ingest companies related to known entities.

        Args:
            entity_names: List of entity names to search for
            jurisdiction_codes: Optional jurisdiction filter

        Returns:
            Aggregated ingestion result
        """
        result = IngestionResult(source=self.source_name)

        for name in entity_names:
            try:
                jurisdictions = jurisdiction_codes or [None]

                for jur in jurisdictions:
                    sub_result = await self.ingest_companies_by_search(
                        query=name,
                        jurisdiction_code=jur,
                        max_companies=10,  # Limit per entity
                    )

                    result.records_processed += sub_result.records_processed
                    result.records_created += sub_result.records_created
                    result.duplicates_found += sub_result.duplicates_found
                    result.errors.extend(sub_result.errors)

            except Exception as e:
                logger.warning(f"Error ingesting related companies for {name}: {e}")
                result.errors.append({"entity": name, "error": str(e)})

        result.status = "completed" if not any(
            e.get("fatal") for e in result.errors
        ) else "partial"

        return result

    async def _upsert_entity(
        self,
        db,
        entity_data: dict[str, Any],
    ) -> UUID | None:
        """Create or update an entity.

        Args:
            db: Database session
            entity_data: Entity data dictionary

        Returns:
            Entity ID or None on error
        """
        # Check for existing entity by external IDs
        external_ids = entity_data.get("external_ids", {})

        if external_ids.get("opencorporates_url"):
            check_query = text("""
                SELECT id FROM entities
                WHERE external_ids->>'opencorporates_url' = :url
            """)
            result = await db.execute(
                check_query,
                {"url": external_ids["opencorporates_url"]},
            )
            existing = result.fetchone()
            if existing:
                # Update existing
                return existing.id

        # Create new entity
        entity_id = uuid4()

        insert_query = text("""
            INSERT INTO entities (
                id, entity_type, name, status, jurisdiction,
                external_ids, metadata, created_at, updated_at
            ) VALUES (
                :id, :entity_type, :name, :status, :jurisdiction,
                :external_ids, :metadata, :created_at, :updated_at
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
            RETURNING id
        """)

        result = await db.execute(
            insert_query,
            {
                "id": entity_id,
                "entity_type": entity_data["entity_type"],
                "name": entity_data["name"],
                "status": entity_data.get("status", "ACTIVE"),
                "jurisdiction": entity_data.get("jurisdiction"),
                "external_ids": json.dumps(entity_data.get("external_ids", {})),
                "metadata": json.dumps(entity_data.get("metadata", {})),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
        )

        row = result.fetchone()
        return row.id if row else None

    async def _create_relationship(
        self,
        db,
        relationship: dict[str, Any],
    ):
        """Create a relationship in the graph.

        Args:
            db: Database session
            relationship: Relationship data dictionary
        """
        insert_query = text("""
            INSERT INTO relationships (
                id, relationship_type, from_entity_id, to_entity_id,
                start_date, end_date, is_current, properties,
                created_at
            ) VALUES (
                :id, :relationship_type, :from_entity_id, :to_entity_id,
                :start_date, :end_date, :is_current, :properties,
                :created_at
            )
            ON CONFLICT (from_entity_id, to_entity_id, relationship_type) DO UPDATE SET
                start_date = COALESCE(EXCLUDED.start_date, relationships.start_date),
                end_date = COALESCE(EXCLUDED.end_date, relationships.end_date),
                is_current = EXCLUDED.is_current,
                properties = EXCLUDED.properties
        """)

        await db.execute(
            insert_query,
            {
                "id": uuid4(),
                "relationship_type": relationship["relationship_type"],
                "from_entity_id": relationship["from_entity_id"],
                "to_entity_id": relationship["to_entity_id"],
                "start_date": relationship.get("start_date"),
                "end_date": relationship.get("end_date"),
                "is_current": relationship.get("is_current", True),
                "properties": json.dumps(relationship.get("properties", {})),
                "created_at": datetime.utcnow(),
            },
        )


# =========================
# Celery Tasks
# =========================


@celery_app.task(bind=True, max_retries=3)
def run_opencorporates_ingestion_task(
    self,
    entity_names: list[str] | None = None,
    jurisdiction_codes: list[str] | None = None,
    search_query: str | None = None,
    max_companies: int = 100,
) -> dict[str, Any]:
    """Celery task for OpenCorporates ingestion.

    Args:
        entity_names: List of entity names to search for
        jurisdiction_codes: Jurisdiction filter
        search_query: Direct search query (alternative to entity_names)
        max_companies: Maximum companies to ingest

    Returns:
        Ingestion result dictionary
    """
    import asyncio

    async def run():
        ingester = OpenCorporatesIngester()

        try:
            if entity_names:
                result = await ingester.ingest_related_companies(
                    entity_names=entity_names,
                    jurisdiction_codes=jurisdiction_codes,
                )
            elif search_query:
                result = await ingester.ingest_companies_by_search(
                    query=search_query,
                    jurisdiction_code=jurisdiction_codes[0] if jurisdiction_codes else None,
                    max_companies=max_companies,
                )
            else:
                return {
                    "status": "failed",
                    "errors": [{"error": "No entity_names or search_query provided"}],
                }

            return result.model_dump() if hasattr(result, "model_dump") else result.__dict__

        finally:
            await ingester.close()

    return asyncio.run(run())


@celery_app.task
def schedule_weekly_opencorporates_enrichment():
    """Weekly task to enrich existing entities with OpenCorporates data.

    Queries for entities that haven't been enriched recently and
    fetches updated information from OpenCorporates.
    """
    import asyncio

    async def run():
        async with get_db_session() as db:
            # Get entities that need enrichment
            query = text("""
                SELECT name, jurisdiction
                FROM entities
                WHERE entity_type = 'ORGANIZATION'
                AND (
                    external_ids->>'opencorporates_url' IS NULL
                    OR updated_at < NOW() - INTERVAL '7 days'
                )
                ORDER BY updated_at ASC NULLS FIRST
                LIMIT 50
            """)

            result = await db.execute(query)
            entities = result.fetchall()

            if not entities:
                return {"status": "completed", "message": "No entities need enrichment"}

            # Extract names and jurisdictions
            names = [e.name for e in entities]
            jurisdictions = list(set(
                e.jurisdiction for e in entities
                if e.jurisdiction
            ))

            # Run ingestion
            ingester = OpenCorporatesIngester()
            try:
                result = await ingester.ingest_related_companies(
                    entity_names=names,
                    jurisdiction_codes=jurisdictions or None,
                )
                return result.model_dump() if hasattr(result, "model_dump") else result.__dict__
            finally:
                await ingester.close()

    return asyncio.run(run())


# =========================
# Convenience Functions
# =========================


async def run_opencorporates_ingestion(
    entity_names: list[str] | None = None,
    search_query: str | None = None,
    jurisdiction_codes: list[str] | None = None,
    max_companies: int = 100,
) -> dict[str, Any]:
    """Run OpenCorporates ingestion directly (non-Celery).

    Args:
        entity_names: List of entity names to search for
        search_query: Direct search query
        jurisdiction_codes: Jurisdiction filter
        max_companies: Maximum companies to ingest

    Returns:
        Ingestion result dictionary
    """
    ingester = OpenCorporatesIngester()

    try:
        if entity_names:
            result = await ingester.ingest_related_companies(
                entity_names=entity_names,
                jurisdiction_codes=jurisdiction_codes,
            )
        elif search_query:
            result = await ingester.ingest_companies_by_search(
                query=search_query,
                jurisdiction_code=jurisdiction_codes[0] if jurisdiction_codes else None,
                max_companies=max_companies,
            )
        else:
            return {
                "status": "failed",
                "errors": [{"error": "No entity_names or search_query provided"}],
            }

        return {
            "status": result.status,
            "records_processed": result.records_processed,
            "records_created": result.records_created,
            "duplicates_found": result.duplicates_found,
            "errors": result.errors,
        }

    finally:
        await ingester.close()
