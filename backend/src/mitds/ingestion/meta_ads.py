"""Meta Ad Library API ingester.

Ingests political and social issue ads from Meta's Ad Library API.
Key data points:
- Ad ID, creation time, delivery start/end
- Page name and ID (advertiser)
- Funding entity (disclaimer)
- Spend range (lower/upper bounds)
- Impressions range
- Demographic breakdown

Data source: https://graph.facebook.com/v24.0/ads_archive
Coverage: Political/social issue ads in US and Canada
Rate limits: 200 calls/hour per app

API Docs: https://developers.facebook.com/docs/graph-api/reference/ads_archive/
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..storage import get_storage
from .base import BaseIngester, IngestionConfig, RetryConfig, with_retry

logger = get_context_logger(__name__)

# Meta Graph API base URL (v24.0 as of Jan 2026)
META_GRAPH_API_BASE = "https://graph.facebook.com/v24.0"
META_ADS_ARCHIVE_ENDPOINT = f"{META_GRAPH_API_BASE}/ads_archive"

# Supported countries for political ads
SUPPORTED_COUNTRIES = ["US", "CA"]

# Minimal fields for basic functionality (use if full fields cause issues)
MINIMAL_AD_FIELDS = [
    "id",
    "ad_creation_time",
    "ad_delivery_start_time",
    "page_id",
    "page_name",
]

# Default fields to request (per ArchivedAd schema)
# Note: estimated_audience_size is a query PARAMETER, not a return field
DEFAULT_AD_FIELDS = [
    "id",
    "ad_creation_time",
    "ad_creative_bodies",
    "ad_creative_link_captions",
    "ad_creative_link_descriptions",
    "ad_creative_link_titles",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_snapshot_url",
    "bylines",
    "currency",
    "delivery_by_region",
    "demographic_distribution",
    "impressions",
    "languages",
    "page_id",
    "page_name",
    "publisher_platforms",
    "spend",
]


class MetaAdRecord(BaseModel):
    """Parsed Meta Ad record."""

    # Ad identification
    ad_id: str = Field(..., description="Meta Ad ID")
    ad_archive_id: str | None = Field(None, description="Ad Archive ID if available")

    # Timing
    ad_creation_time: datetime | None = None
    ad_delivery_start_time: datetime | None = None
    ad_delivery_stop_time: datetime | None = None

    # Advertiser info
    page_id: str | None = None
    page_name: str | None = None
    funding_entity: str | None = Field(None, description="Funding disclaimer/byline")

    # Content
    ad_creative_bodies: list[str] = Field(default_factory=list)
    ad_creative_link_titles: list[str] = Field(default_factory=list)
    ad_creative_link_descriptions: list[str] = Field(default_factory=list)
    ad_snapshot_url: str | None = None

    # Reach and spend
    spend_lower: float | None = None
    spend_upper: float | None = None
    currency: str = "USD"
    impressions_lower: int | None = None
    impressions_upper: int | None = None

    # Targeting
    delivery_by_region: list[dict[str, Any]] = Field(default_factory=list)
    demographic_distribution: list[dict[str, Any]] = Field(default_factory=list)
    estimated_audience_size_lower: int | None = None
    estimated_audience_size_upper: int | None = None

    # Platforms
    publisher_platforms: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)

    # Metadata
    country: str = "US"


class MetaAdIngester(BaseIngester[MetaAdRecord]):
    """Ingester for Meta Ad Library API.

    Fetches political and social issue ads and extracts:
    - Ad timing and delivery information
    - Advertiser/funder identification
    - Spend and impression data
    - Demographic targeting
    """

    def __init__(self):
        super().__init__("meta_ads")
        self._http_client: httpx.AsyncClient | None = None
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._storage = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                follow_redirects=True,
            )
        return self._http_client

    @property
    def storage(self):
        """Get storage client."""
        if self._storage is None:
            self._storage = get_storage()
        return self._storage

    async def close(self):
        """Close the HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Meta access tokens can be:
        - Short-lived: 1-2 hours (from OAuth flow)
        - Long-lived: 60 days (exchanged from short-lived)
        - System user tokens: Can be long-lived or never expire

        For production, use a long-lived system user token and
        implement refresh logic before expiry.
        """
        settings = get_settings()

        # Check if we have a cached valid token
        if self._access_token and self._token_expires_at:
            if datetime.utcnow() < self._token_expires_at - timedelta(minutes=5):
                return self._access_token

        # Check if we have a configured token
        if settings.meta_access_token:
            self._access_token = settings.meta_access_token
            # Assume token is valid for 60 days if not refreshed
            self._token_expires_at = datetime.utcnow() + timedelta(days=60)
            return self._access_token

        # Try to exchange for a long-lived token if we have app credentials
        if settings.meta_app_id and settings.meta_app_secret:
            try:
                token = await self._refresh_access_token()
                return token
            except Exception as e:
                self.logger.error(f"Failed to refresh Meta access token: {e}")
                raise

        raise ValueError(
            "No Meta access token configured. Set META_ACCESS_TOKEN or "
            "META_APP_ID and META_APP_SECRET in environment."
        )

    async def _refresh_access_token(self) -> str:
        """Refresh the access token using app credentials.

        Uses the OAuth App Access Token flow:
        https://developers.facebook.com/docs/facebook-login/access-tokens/#apptokens

        For the Ad Library API, you typically need a User or System User token
        with the ads_read permission, not an App token.

        This implementation supports:
        1. App Access Token (limited functionality)
        2. Exchange short-lived for long-lived token
        """
        settings = get_settings()

        # Option 1: Get App Access Token (limited)
        token_url = f"{META_GRAPH_API_BASE}/oauth/access_token"
        params = {
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "grant_type": "client_credentials",
        }

        response = await self.http_client.get(token_url, params=params)
        response.raise_for_status()

        data = response.json()
        self._access_token = data["access_token"]

        # App tokens don't have an explicit expiry, but we set a reasonable refresh period
        self._token_expires_at = datetime.utcnow() + timedelta(days=30)

        self.logger.info("Successfully refreshed Meta access token")
        return self._access_token

    async def exchange_for_long_lived_token(
        self, short_lived_token: str
    ) -> tuple[str, datetime]:
        """Exchange a short-lived token for a long-lived token.

        Args:
            short_lived_token: A short-lived user access token

        Returns:
            Tuple of (long_lived_token, expiry_datetime)
        """
        settings = get_settings()

        exchange_url = f"{META_GRAPH_API_BASE}/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "fb_exchange_token": short_lived_token,
        }

        response = await self.http_client.get(exchange_url, params=params)
        response.raise_for_status()

        data = response.json()
        access_token = data["access_token"]
        expires_in = data.get("expires_in", 5184000)  # Default 60 days
        expiry = datetime.utcnow() + timedelta(seconds=expires_in)

        self.logger.info(f"Exchanged for long-lived token, expires: {expiry}")
        return access_token, expiry

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[MetaAdRecord]:
        """Fetch ads from Meta Ad Library API.

        Queries for political/social issue ads in supported countries
        and yields parsed ad records.
        """
        access_token = await self.get_access_token()

        # Determine date range
        if config.date_from:
            start_date = config.date_from
        else:
            # Default to last 7 days for incremental
            start_date = datetime.utcnow() - timedelta(days=7)

        end_date = config.date_to or datetime.utcnow()

        # Build search parameters
        countries = config.extra_params.get("countries", SUPPORTED_COUNTRIES)
        search_terms = config.extra_params.get("search_terms")
        page_ids = config.extra_params.get("page_ids")
        minimal_fields = config.extra_params.get("minimal_fields", False)

        self.logger.info(
            f"Fetching ads from {start_date.date()} to {end_date.date()} "
            f"for countries: {countries}"
        )

        for country in countries:
            async for ad in self._fetch_country_ads(
                access_token,
                country,
                start_date,
                end_date,
                search_terms=search_terms,
                page_ids=page_ids,
                config=config,
                minimal_fields=minimal_fields,
            ):
                yield ad

    async def _fetch_country_ads(
        self,
        access_token: str,
        country: str,
        start_date: datetime,
        end_date: datetime,
        search_terms: list[str] | None = None,
        page_ids: list[str] | None = None,
        config: IngestionConfig | None = None,
        minimal_fields: bool = False,
    ) -> AsyncIterator[MetaAdRecord]:
        """Fetch ads for a specific country."""

        # Rate limiting: 200 calls/hour = ~3 calls/minute
        # We'll use a conservative delay between pagination calls
        rate_limit_delay = 0.5  # seconds

        # Use minimal fields if requested (for debugging permission issues)
        fields = MINIMAL_AD_FIELDS if minimal_fields else DEFAULT_AD_FIELDS
        if minimal_fields:
            self.logger.info("Using minimal fields for debugging")

        params = {
            "access_token": access_token,
            # Meta API expects array format like ['US'] with single quotes inside
            # Using Python list repr gives us this format
            "ad_reached_countries": f"['{country}']",
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "ad_active_status": "ALL",
            "fields": ",".join(fields),
            "limit": 100,  # Max per page
        }

        # Meta Ad Library API requires EITHER search_terms OR search_page_ids
        # Cannot query all ads without search criteria
        # Note: Meta's API expects search_terms with single quotes: 'california'
        if search_terms:
            params["search_terms"] = f"'{search_terms[0]}'"  # API expects single quotes
        elif page_ids:
            # search_page_ids expects comma-separated list of IDs
            params["search_page_ids"] = ",".join(page_ids)
        else:
            # No search criteria provided - this will fail
            # Meta requires either search_terms or search_page_ids
            raise ValueError(
                "Meta Ad Library API requires either --search-terms or --page-ids. "
                "Example: mitds ingest meta-ads --search-terms 'election'"
            )

        # Date filter using ad_delivery_date_min/max
        params["ad_delivery_date_min"] = start_date.strftime("%Y-%m-%d")
        params["ad_delivery_date_max"] = end_date.strftime("%Y-%m-%d")

        next_url = META_ADS_ARCHIVE_ENDPOINT
        page_count = 0
        records_count = 0

        while next_url:
            try:
                async def _do_fetch():
                    if next_url == META_ADS_ARCHIVE_ENDPOINT:
                        response = await self.http_client.get(next_url, params=params)
                    else:
                        # Pagination URL already includes params
                        response = await self.http_client.get(next_url)
                    
                    # Check for errors and log Meta's response before raising
                    if response.status_code >= 400:
                        try:
                            error_data = response.json()
                            error_info = error_data.get("error", {})
                            self.logger.error(
                                f"Meta API returned {response.status_code}: "
                                f"code={error_info.get('code')}, "
                                f"type={error_info.get('type')}, "
                                f"message={error_info.get('message')}"
                            )
                        except Exception:
                            self.logger.error(f"Meta API returned {response.status_code}: {response.text[:500]}")
                    
                    response.raise_for_status()
                    return response.json()

                data = await with_retry(
                    _do_fetch,
                    config=RetryConfig(max_retries=3, base_delay=2.0),
                    logger=self.logger,
                )

                page_count += 1
                ads = data.get("data", [])

                self.logger.debug(
                    f"Page {page_count}: received {len(ads)} ads for {country}"
                )

                for ad_data in ads:
                    try:
                        ad = self._parse_ad(ad_data, country)
                        records_count += 1
                        yield ad

                        # Check limit
                        if config and config.limit and records_count >= config.limit:
                            self.logger.info(f"Reached limit of {config.limit} records")
                            return

                    except Exception as e:
                        self.logger.warning(f"Failed to parse ad: {e}")
                        continue

                # Get next page URL
                paging = data.get("paging", {})
                next_url = paging.get("next")

                # Rate limiting delay
                if next_url:
                    await asyncio.sleep(rate_limit_delay)

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limited - wait longer
                    self.logger.warning("Rate limited by Meta API, waiting 60s...")
                    await asyncio.sleep(60)
                    continue
                elif e.response.status_code == 400:
                    # Bad request - log full error details from Meta
                    try:
                        error_data = e.response.json() if e.response.content else {}
                        error_msg = error_data.get("error", {}).get("message", str(e))
                        error_code = error_data.get("error", {}).get("code", "unknown")
                        error_subcode = error_data.get("error", {}).get("error_subcode", "")
                        self.logger.error(
                            f"Meta API error (code={error_code}, subcode={error_subcode}): {error_msg}"
                        )
                    except Exception:
                        self.logger.error(f"Meta API error: {e}")
                    break
                else:
                    raise

        self.logger.info(
            f"Completed fetching {records_count} ads for {country} "
            f"across {page_count} pages"
        )

    def _parse_ad(self, ad_data: dict[str, Any], country: str) -> MetaAdRecord:
        """Parse raw API response into MetaAdRecord."""

        # Parse spend range
        spend = ad_data.get("spend", {})
        spend_lower = None
        spend_upper = None
        if isinstance(spend, dict):
            spend_lower = float(spend.get("lower_bound", 0)) if spend.get("lower_bound") else None
            spend_upper = float(spend.get("upper_bound", 0)) if spend.get("upper_bound") else None

        # Parse impressions range
        impressions = ad_data.get("impressions", {})
        impressions_lower = None
        impressions_upper = None
        if isinstance(impressions, dict):
            impressions_lower = int(impressions.get("lower_bound", 0)) if impressions.get("lower_bound") else None
            impressions_upper = int(impressions.get("upper_bound", 0)) if impressions.get("upper_bound") else None

        # Parse audience size range
        audience = ad_data.get("estimated_audience_size", {})
        audience_lower = None
        audience_upper = None
        if isinstance(audience, dict):
            audience_lower = int(audience.get("lower_bound", 0)) if audience.get("lower_bound") else None
            audience_upper = int(audience.get("upper_bound", 0)) if audience.get("upper_bound") else None

        # Parse timestamps
        creation_time = None
        if ad_data.get("ad_creation_time"):
            try:
                creation_time = datetime.fromisoformat(
                    ad_data["ad_creation_time"].replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        delivery_start = None
        if ad_data.get("ad_delivery_start_time"):
            try:
                delivery_start = datetime.fromisoformat(
                    ad_data["ad_delivery_start_time"].replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        delivery_stop = None
        if ad_data.get("ad_delivery_stop_time"):
            try:
                delivery_stop = datetime.fromisoformat(
                    ad_data["ad_delivery_stop_time"].replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        # Extract funding entity from bylines
        bylines = ad_data.get("bylines", [])
        funding_entity = bylines[0] if bylines else None

        return MetaAdRecord(
            ad_id=ad_data["id"],
            ad_archive_id=ad_data.get("ad_archive_id"),
            ad_creation_time=creation_time,
            ad_delivery_start_time=delivery_start,
            ad_delivery_stop_time=delivery_stop,
            page_id=ad_data.get("page_id"),
            page_name=ad_data.get("page_name"),
            funding_entity=funding_entity,
            ad_creative_bodies=ad_data.get("ad_creative_bodies", []),
            ad_creative_link_titles=ad_data.get("ad_creative_link_titles", []),
            ad_creative_link_descriptions=ad_data.get("ad_creative_link_descriptions", []),
            ad_snapshot_url=ad_data.get("ad_snapshot_url"),
            spend_lower=spend_lower,
            spend_upper=spend_upper,
            currency=ad_data.get("currency", "USD"),
            impressions_lower=impressions_lower,
            impressions_upper=impressions_upper,
            delivery_by_region=ad_data.get("delivery_by_region", []),
            demographic_distribution=ad_data.get("demographic_distribution", []),
            estimated_audience_size_lower=audience_lower,
            estimated_audience_size_upper=audience_upper,
            publisher_platforms=ad_data.get("publisher_platforms", []),
            languages=ad_data.get("languages", []),
            country=country,
        )

    async def process_record(self, record: MetaAdRecord) -> dict[str, Any]:
        """Process a parsed Meta ad record.

        Creates/updates:
        - Sponsor entity (from funding_entity/page)
        - Ad event record
        - SPONSORED_BY relationship
        """
        result = {"created": False, "updated": False, "duplicate": False}

        async with get_neo4j_session() as session:
            # Check if ad already exists
            check_query = """
            MATCH (a:Ad {meta_ad_id: $ad_id})
            RETURN a.id as id
            """
            check_result = await session.run(check_query, ad_id=record.ad_id)
            existing = await check_result.single()

            if existing:
                result["updated"] = True
                ad_node_id = existing["id"]
            else:
                result["created"] = True
                ad_node_id = str(uuid4())

            # Create/update ad node
            ad_props = {
                "id": ad_node_id,
                "meta_ad_id": record.ad_id,
                "entity_type": "AD",
                "ad_creation_time": record.ad_creation_time.isoformat() if record.ad_creation_time else None,
                "ad_delivery_start_time": record.ad_delivery_start_time.isoformat() if record.ad_delivery_start_time else None,
                "ad_delivery_stop_time": record.ad_delivery_stop_time.isoformat() if record.ad_delivery_stop_time else None,
                "page_id": record.page_id,
                "page_name": record.page_name,
                "funding_entity": record.funding_entity,
                "spend_lower": record.spend_lower,
                "spend_upper": record.spend_upper,
                "currency": record.currency,
                "impressions_lower": record.impressions_lower,
                "impressions_upper": record.impressions_upper,
                "country": record.country,
                "publisher_platforms": record.publisher_platforms,
                "languages": record.languages,
                "ad_snapshot_url": record.ad_snapshot_url,
                "updated_at": datetime.utcnow().isoformat(),
            }

            # Store ad creative content
            if record.ad_creative_bodies:
                ad_props["creative_body"] = record.ad_creative_bodies[0]
            if record.ad_creative_link_titles:
                ad_props["creative_title"] = record.ad_creative_link_titles[0]

            if not existing:
                ad_props["created_at"] = datetime.utcnow().isoformat()

            upsert_query = """
            MERGE (a:Ad {meta_ad_id: $ad_id})
            SET a += $props
            RETURN a.id as id
            """
            await session.run(upsert_query, ad_id=record.ad_id, props=ad_props)

            # Create sponsor entity if we have funding information
            if record.funding_entity or record.page_name:
                sponsor_name = record.funding_entity or record.page_name
                sponsor_id = str(uuid4())

                # Create/update sponsor node
                sponsor_query = """
                MERGE (s:Sponsor {name: $name})
                ON CREATE SET
                    s.id = $sponsor_id,
                    s.entity_type = 'SPONSOR',
                    s.meta_page_id = $page_id,
                    s.confidence = 0.8,
                    s.created_at = $now
                SET s.updated_at = $now,
                    s.meta_page_id = COALESCE(s.meta_page_id, $page_id)
                RETURN s.id as id
                """
                sponsor_result = await session.run(
                    sponsor_query,
                    name=sponsor_name,
                    sponsor_id=sponsor_id,
                    page_id=record.page_id,
                    now=datetime.utcnow().isoformat(),
                )
                sponsor_record = await sponsor_result.single()

                # Create SPONSORED_BY relationship
                rel_query = """
                MATCH (a:Ad {meta_ad_id: $ad_id})
                MATCH (s:Sponsor {name: $sponsor_name})
                MERGE (a)-[r:SPONSORED_BY]->(s)
                SET r.spend_lower = $spend_lower,
                    r.spend_upper = $spend_upper,
                    r.currency = $currency,
                    r.country = $country,
                    r.updated_at = $now
                """
                await session.run(
                    rel_query,
                    ad_id=record.ad_id,
                    sponsor_name=sponsor_name,
                    spend_lower=record.spend_lower,
                    spend_upper=record.spend_upper,
                    currency=record.currency,
                    country=record.country,
                    now=datetime.utcnow().isoformat(),
                )

        # Store demographic and regional data in PostgreSQL for analysis
        async with get_db_session() as db:
            from sqlalchemy import text

            # Store as an event for temporal analysis
            event_query = text("""
                INSERT INTO events (
                    id, event_type, entity_id, event_time, metadata
                )
                VALUES (
                    :id, 'AD_DELIVERY', :entity_id, :event_time, :metadata
                )
                ON CONFLICT (id) DO UPDATE SET
                    metadata = :metadata
            """)

            await db.execute(
                event_query,
                {
                    "id": record.ad_id,
                    "entity_id": record.page_id or record.ad_id,
                    "event_time": record.ad_delivery_start_time or record.ad_creation_time,
                    "metadata": {
                        "spend_lower": record.spend_lower,
                        "spend_upper": record.spend_upper,
                        "impressions_lower": record.impressions_lower,
                        "impressions_upper": record.impressions_upper,
                        "delivery_by_region": record.delivery_by_region,
                        "demographic_distribution": record.demographic_distribution,
                        "country": record.country,
                        "funding_entity": record.funding_entity,
                    },
                },
            )
            await db.commit()

        return result

    async def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last successful sync."""
        async with get_db_session() as session:
            from sqlalchemy import text

            query = text("""
                SELECT MAX(completed_at) as last_sync
                FROM ingestion_runs
                WHERE source = :source AND status IN ('completed', 'partial')
            """)
            result = await session.execute(query, {"source": self.source_name})
            row = result.first()
            if row and row.last_sync:
                return row.last_sync
        return None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save the timestamp of a successful sync."""
        # Sync time is saved implicitly via ingestion_runs table
        pass


# Celery task for scheduled ingestion
def get_meta_ads_celery_task():
    """Get the Celery task for Meta Ad Library ingestion.

    Returns the task function to be registered with Celery.
    """
    from ..worker import celery_app

    @celery_app.task(name="mitds.ingestion.meta_ads.ingest")
    def ingest_meta_ads_task(
        countries: list[str] | None = None,
        days_back: int = 7,
        incremental: bool = True,
    ):
        """Celery task for Meta Ad Library ingestion.

        Args:
            countries: Countries to fetch ads for (default: US, CA)
            days_back: Number of days to look back (default: 7)
            incremental: Whether to do incremental sync
        """
        import asyncio

        async def run_ingestion():
            ingester = MetaAdIngester()
            try:
                date_from = datetime.utcnow() - timedelta(days=days_back)
                config = IngestionConfig(
                    incremental=incremental,
                    date_from=date_from,
                    extra_params={
                        "countries": countries or SUPPORTED_COUNTRIES,
                    },
                )
                result = await ingester.run(config)
                return result.model_dump()
            finally:
                await ingester.close()

        return asyncio.run(run_ingestion())

    return ingest_meta_ads_task


async def run_meta_ads_ingestion(
    countries: list[str] | None = None,
    days_back: int = 7,
    incremental: bool = True,
    limit: int | None = None,
    search_terms: list[str] | None = None,
    page_ids: list[str] | None = None,
    minimal_fields: bool = False,
) -> dict[str, Any]:
    """Run Meta Ad Library ingestion directly (not via Celery).

    Args:
        countries: Countries to fetch ads for (default: US, CA)
        days_back: Number of days to look back (default: 7)
        incremental: Whether to do incremental sync
        limit: Maximum number of records to process
        search_terms: Optional search terms to filter ads
        page_ids: Optional page IDs to filter ads
        minimal_fields: Use minimal fields for debugging permission issues

    Returns:
        Ingestion result dictionary
    """
    ingester = MetaAdIngester()
    try:
        date_from = datetime.utcnow() - timedelta(days=days_back)
        config = IngestionConfig(
            incremental=incremental,
            date_from=date_from,
            limit=limit,
            extra_params={
                "countries": countries or SUPPORTED_COUNTRIES,
                "search_terms": search_terms,
                "page_ids": page_ids,
                "minimal_fields": minimal_fields,
            },
        )
        result = await ingester.run(config)
        return result.model_dump()
    finally:
        await ingester.close()
