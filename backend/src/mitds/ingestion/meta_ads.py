"""Meta Ad Library API ingester.

Ingests political and social issue ads from Meta's Ad Library API.
Key data points:
- Ad ID, creation time, delivery start/end
- Page name and ID (advertiser)
- Funding entity (disclaimer)
- Spend range (lower/upper bounds)
- Impressions range
- Demographic breakdown

Additionally fetches Facebook Page details:
- Page transparency info (who manages the page)
- Contact information (website, email, phone, WhatsApp)
- Social links (Instagram, external URLs)
- Page category and verification status

Data source: https://graph.facebook.com/v24.0/ads_archive
Coverage: Political/social issue ads in US and Canada
Rate limits: 200 calls/hour per app

API Docs: 
- https://developers.facebook.com/docs/graph-api/reference/ads_archive/
- https://developers.facebook.com/docs/graph-api/reference/page/
"""

import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from ..storage import get_storage
from .base import BaseIngester, IngestionConfig, RetryConfig, SingleIngestionResult, with_retry

logger = get_context_logger(__name__)

# Meta Graph API base URL (v24.0 as of Jan 2026)
META_GRAPH_API_BASE = "https://graph.facebook.com/v24.0"
META_ADS_ARCHIVE_ENDPOINT = f"{META_GRAPH_API_BASE}/ads_archive"

# Supported countries for political ads
SUPPORTED_COUNTRIES = ["US", "CA"]

# Corporate suffixes to strip for search variations
CORPORATE_SUFFIXES = [
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
]


def generate_search_name_variations(name: str) -> list[str]:
    """Generate variations of an organization name for Meta Ad Library search.
    
    The Meta API search is sensitive to special characters, corporate suffixes,
    and spacing. Many Facebook pages use concatenated names (e.g., "NationalCitizensCoalition").
    
    This generates variations to try in order of preference:
    1. Original name
    2. Without special characters (apostrophes, periods, commas)
    3. Without corporate suffixes (Inc., Corp., Ltd., etc.)
    4. Without both special characters and suffixes
    5. Concatenated (no spaces) - matches Facebook page handles
    6. Core name words
    
    Args:
        name: Original organization name
        
    Returns:
        List of unique name variations to try (most specific first)
    """
    if not name:
        return []
    
    variations: list[str] = []
    seen: set[str] = set()
    
    def add_variation(v: str, preserve_case: bool = False) -> None:
        """Add a variation if it's non-empty and not already seen."""
        if not preserve_case:
            v = " ".join(v.split()).strip()  # Normalize whitespace
        else:
            v = v.strip()
        if v and v not in seen:
            variations.append(v)
            seen.add(v)
    
    # 1. Original name
    add_variation(name)
    
    # 2. Without special characters (apostrophes, periods, commas)
    # Keep alphanumeric and spaces only
    clean_name = re.sub(r"[''`]", "", name)  # Remove apostrophes specifically
    clean_name = re.sub(r"[^\w\s-]", " ", clean_name)  # Remove other punctuation
    add_variation(clean_name)
    
    # 3. Without corporate suffixes (on original name, uppercase for matching)
    stripped_name = name.upper()
    for suffix in CORPORATE_SUFFIXES:
        stripped_name = re.sub(suffix, "", stripped_name, flags=re.IGNORECASE)
    # Convert back to title case for readability
    stripped_name = stripped_name.title()
    add_variation(stripped_name)
    
    # 4. Clean name without suffixes
    # Note: Must normalize whitespace first since punctuation removal may create trailing spaces
    stripped_clean = " ".join(clean_name.upper().split())  # Normalize whitespace
    for suffix in CORPORATE_SUFFIXES:
        stripped_clean = re.sub(suffix, "", stripped_clean, flags=re.IGNORECASE)
    stripped_clean = stripped_clean.strip().title()
    add_variation(stripped_clean)
    
    # 5. Concatenated versions (no spaces) - Facebook pages often use this format
    # e.g., "National Citizens Coalition" -> "NationalCitizensCoalition"
    words = stripped_clean.split()
    if len(words) >= 2:
        # PascalCase concatenation (most common for Facebook pages)
        pascal_case = "".join(w.title() for w in words if w)
        add_variation(pascal_case, preserve_case=True)
        
        # Also try lowercase concatenation
        lowercase_concat = "".join(w.lower() for w in words if w)
        add_variation(lowercase_concat, preserve_case=True)
    
    # 6. Additional variation: just the core name words (first 2-3 significant words)
    # This helps with very long names like "National Citizens' Coalition Inc."
    if len(words) >= 2:
        # Try first 2-3 words (skip very short words at the end)
        core_words = [w for w in words if len(w) > 2][:3]
        if core_words:
            add_variation(" ".join(core_words))
    
    return variations


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


# Fields to request from Page API for enrichment
PAGE_DETAIL_FIELDS = [
    "id",
    "name",
    "username",
    "about",
    "description",
    "category",
    "category_list",
    "website",
    "emails",
    "phone",
    "whatsapp_number",
    "link",
    "fan_count",
    "followers_count",
    "verification_status",
    "single_line_address",
    "location",
    "founded",
    "company_overview",
    "mission",
    "general_info",
]

# Fields for Page Transparency (requires special permissions)
PAGE_TRANSPARENCY_FIELDS = [
    "id",
    "name",
    # Note: Page transparency data requires additional permissions
    # and may not be available via standard API access
]


class FacebookPageContact(BaseModel):
    """Contact information from a Facebook Page."""
    
    website: str | None = Field(default=None, description="Primary website URL")
    websites: list[str] = Field(default_factory=list, description="All website URLs")
    emails: list[str] = Field(default_factory=list, description="Contact emails")
    phone: str | None = Field(default=None, description="Phone number")
    whatsapp_number: str | None = Field(default=None, description="WhatsApp number")
    address: str | None = Field(default=None, description="Single line address")
    

class FacebookPageSocialLinks(BaseModel):
    """Social media links associated with a Facebook Page."""
    
    facebook_url: str | None = Field(default=None, description="Page's Facebook URL")
    instagram_username: str | None = Field(default=None, description="Linked Instagram username")
    instagram_id: str | None = Field(default=None, description="Linked Instagram account ID")
    twitter_handle: str | None = Field(default=None, description="Twitter/X handle if found in about/description")
    youtube_url: str | None = Field(default=None, description="YouTube URL if found")
    external_links: list[str] = Field(default_factory=list, description="Other external links found")


class FacebookPageCategory(BaseModel):
    """Category information for a Facebook Page."""
    
    id: str
    name: str


class ManagingOrganization(BaseModel):
    """Organization that manages a Facebook Page."""
    
    id: str | None = Field(default=None, description="Organization/Business ID")
    name: str | None = Field(default=None, description="Organization/Business name")
    type: str | None = Field(default=None, description="Type: business, agency, etc.")


class FacebookPageDetails(BaseModel):
    """Comprehensive details about a Facebook Page.
    
    Captures page information including contact details, social links,
    managing organizations, and transparency information.
    """
    
    # Identification
    page_id: str = Field(..., description="Meta/Facebook Page ID")
    page_name: str = Field(..., description="Page display name")
    username: str | None = Field(default=None, description="Page username (handle)")
    
    # Basic info
    about: str | None = Field(default=None, description="Short about text (100 char limit)")
    description: str | None = Field(default=None, description="Longer description")
    company_overview: str | None = Field(default=None, description="Company overview")
    mission: str | None = Field(default=None, description="Mission statement")
    general_info: str | None = Field(default=None, description="General information")
    founded: str | None = Field(default=None, description="When the company was founded")
    
    # Categories
    category: str | None = Field(default=None, description="Primary category")
    category_list: list[FacebookPageCategory] = Field(default_factory=list)
    
    # Contact information
    contact: FacebookPageContact = Field(default_factory=FacebookPageContact)
    
    # Social links
    social_links: FacebookPageSocialLinks = Field(default_factory=FacebookPageSocialLinks)
    
    # Managing organizations
    managing_organizations: list[ManagingOrganization] = Field(
        default_factory=list, 
        description="Organizations that manage this page"
    )
    
    # Stats and verification
    followers_count: int | None = Field(default=None, description="Number of followers")
    fan_count: int | None = Field(default=None, description="Number of likes/fans")
    verification_status: str | None = Field(
        default=None, 
        description="Verification status: blue_verified, gray_verified, or not_verified"
    )
    
    # Metadata
    fetched_at: datetime | None = Field(default=None, description="When this data was fetched")
    raw_data: dict[str, Any] = Field(default_factory=dict, description="Raw API response")


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
        self._enrich_page_details: bool = False  # Set by run() from config
        self._failed_page_ids: set[str] = set()  # Cache of page IDs that failed enrichment
        self._enriched_page_ids: set[str] = set()  # Cache of already enriched page IDs

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
        """Get a valid access token, generating one automatically if needed.

        Meta access tokens can be:
        - App Access Token: Fetched via OAuth from app_id + app_secret
        - Short-lived User Token: 1-2 hours (from OAuth flow)
        - Long-lived User Token: 60 days (exchanged from short-lived)
        - System User Token: Can be long-lived or never expire

        For the Ad Library API (public political ads data), an App Access Token
        is sufficient and can be auto-fetched using META_APP_ID + META_APP_SECRET.
        
        Token priority:
        1. Cached valid token
        2. META_ACCESS_TOKEN environment variable (for User/System tokens)
        3. Fetch App Access Token via OAuth from META_APP_ID + META_APP_SECRET
        """
        settings = get_settings()

        # Check if we have a cached valid token
        if self._access_token and self._token_expires_at:
            if datetime.utcnow() < self._token_expires_at - timedelta(minutes=5):
                return self._access_token

        # Priority 1: Use explicit META_ACCESS_TOKEN if set
        # (This is for User tokens or System User tokens with extra permissions)
        if settings.meta_access_token:
            self._access_token = settings.meta_access_token
            # Assume token is valid for 60 days if not refreshed
            self._token_expires_at = datetime.utcnow() + timedelta(days=60)
            self.logger.debug("Using configured META_ACCESS_TOKEN")
            return self._access_token

        # Priority 2: Fetch App Access Token via OAuth from app credentials
        if settings.meta_app_id and settings.meta_app_secret:
            try:
                token = await self._fetch_app_access_token()
                return token
            except Exception as e:
                self.logger.error(f"Failed to fetch App Access Token: {e}")
                raise

        raise ValueError(
            "No Meta access token configured. Set either:\n"
            "  - META_APP_ID and META_APP_SECRET (recommended for Ad Library API)\n"
            "  - META_ACCESS_TOKEN (for User/System User tokens with extra permissions)"
        )

    async def _fetch_app_access_token(self) -> str:
        """Fetch an App Access Token via OAuth client_credentials flow.

        This fetches a proper App Access Token from Meta's OAuth endpoint.
        Works for the Ad Library API since it's public data.
        
        https://developers.facebook.com/docs/facebook-login/access-tokens/#apptokens
        """
        settings = get_settings()

        token_url = f"{META_GRAPH_API_BASE}/oauth/access_token"
        params = {
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "grant_type": "client_credentials",
        }

        self.logger.info("Fetching App Access Token from Meta OAuth endpoint...")
        
        response = await self.http_client.get(token_url, params=params)
        
        if response.status_code != 200:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", str(response.text))
            except Exception:
                error_msg = response.text[:500] if response.text else "No response"
            raise ValueError(f"Failed to get App Access Token: {error_msg}")
        
        data = response.json()
        self._access_token = data["access_token"]

        # App tokens don't have an explicit expiry, but we set a reasonable refresh period
        self._token_expires_at = datetime.utcnow() + timedelta(days=30)

        self.logger.info("Successfully fetched App Access Token via OAuth")
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

    async def run(
        self, config: IngestionConfig | None = None, run_id: UUID | None = None
    ) -> "IngestionResult":
        """Override run to capture enrich_page_details setting from config."""
        from .base import IngestionResult
        
        # Reset caches for new run
        self._failed_page_ids = set()
        self._enriched_page_ids = set()
        
        # Capture enrich_page_details from config before running
        if config and config.extra_params:
            self._enrich_page_details = config.extra_params.get("enrich_page_details", False)
            if self._enrich_page_details:
                self.logger.info("Page details enrichment enabled")
                self.logger.warning(
                    "Note: Page details require 'Page Public Metadata Access' feature. "
                    "Apply at: https://developers.facebook.com/docs/apps/review/feature#page-public-metadata-access"
                )
        
        # Call parent run
        return await super().run(config, run_id)

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[MetaAdRecord]:
        """Fetch ads from Meta Ad Library API.

        Queries for political/social issue ads in supported countries
        and yields parsed ad records.
        """
        access_token = await self.get_access_token()

        # Determine date range
        # Note: Meta API rejects future dates, so we cap end_date at yesterday to be timezone-safe
        if config.date_from:
            start_date = config.date_from
        else:
            # Default to last 7 days for incremental
            start_date = datetime.utcnow() - timedelta(days=7)

        # Use yesterday as max date to avoid timezone issues with Meta's servers
        yesterday = datetime.utcnow() - timedelta(days=1)
        end_date = config.date_to if config.date_to else yesterday
        if end_date > yesterday:
            end_date = yesterday

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
            # Meta API expects JSON array format: ["US"]
            "ad_reached_countries": json.dumps([country]),
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "ad_active_status": "ALL",
            "fields": ",".join(fields),
            "limit": 100,  # Max per page
        }

        # Meta Ad Library API requires EITHER search_terms OR search_page_ids
        # Cannot query all ads without search criteria
        if search_terms:
            params["search_terms"] = search_terms[0]  # No quotes needed
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
        # bylines can be a string or a list depending on API version
        bylines = ad_data.get("bylines")
        funding_entity = None
        if bylines:
            if isinstance(bylines, str):
                funding_entity = bylines
            elif isinstance(bylines, list) and len(bylines) > 0:
                # Could be list of strings or list of dicts
                first = bylines[0]
                if isinstance(first, str):
                    funding_entity = first
                elif isinstance(first, dict):
                    funding_entity = first.get("byline") or first.get("name") or str(first)

        # Sanity check: if funding_entity is suspiciously short, it's likely a parsing error
        if funding_entity and len(funding_entity) <= 2:
            logger.warning(f"Suspicious funding_entity '{funding_entity}' for ad {ad_data.get('id')}, raw bylines: {bylines}")
            funding_entity = None  # Will fall back to page_name in process_record

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

    async def process_record(
        self, 
        record: MetaAdRecord,
    ) -> dict[str, Any]:
        """Process a parsed Meta ad record.

        Creates/updates:
        - Sponsor entity (from funding_entity/page)
        - Ad event record
        - SPONSORED_BY relationship
        - Optionally fetches and stores Facebook Page details (if _enrich_page_details is set)
        
        Args:
            record: The parsed Meta ad record
        """
        result = {
            "created": False, 
            "updated": False, 
            "duplicate": False, 
            "entity_id": None,
            "page_details_enriched": False,
        }

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
            # Use page_name or funding_entity as the display name
            ad_name = record.page_name or record.funding_entity or f"Ad {record.ad_id[:8]}"
            
            ad_props = {
                "id": ad_node_id,
                "meta_ad_id": record.ad_id,
                "name": ad_name,  # Generic name for display in reports
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
                "jurisdiction": record.country,  # Map country to jurisdiction for cross-border detection
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
                # Also add Organization label for cross-border detection queries
                # Infer jurisdiction from country (CA=Canada, US=United States)
                jurisdiction = record.country if record.country in ("CA", "US") else None
                
                sponsor_query = """
                MERGE (s:Sponsor {name: $name})
                ON CREATE SET
                    s.id = $sponsor_id,
                    s.entity_type = 'organization',
                    s.meta_page_id = $page_id,
                    s.confidence = 0.8,
                    s.jurisdiction = $jurisdiction,
                    s.created_at = $now
                ON MATCH SET
                    s.jurisdiction = COALESCE(s.jurisdiction, $jurisdiction)
                SET s.updated_at = $now,
                    s.meta_page_id = COALESCE(s.meta_page_id, $page_id)
                WITH s
                SET s:Organization
                RETURN s.id as id
                """
                sponsor_result = await session.run(
                    sponsor_query,
                    name=sponsor_name,
                    sponsor_id=sponsor_id,
                    page_id=record.page_id,
                    jurisdiction=jurisdiction,
                    now=datetime.utcnow().isoformat(),
                )
                sponsor_record = await sponsor_result.single()

                # Capture the sponsor entity ID for the research system
                if sponsor_record and sponsor_record.get("id"):
                    result["entity_id"] = sponsor_record["id"]
                    
                    # Also create the entity in PostgreSQL for session_entities FK constraint
                    await self._ensure_entity_in_postgres(
                        entity_id=sponsor_record["id"],
                        name=sponsor_name,
                        entity_type="organization",
                        external_ids={"meta_page_id": record.page_id} if record.page_id else {},
                    )

                # Create SPONSORED_BY relationship
                # Calculate amount as average of spend range for reporting
                amount = None
                if record.spend_lower is not None and record.spend_upper is not None:
                    amount = (record.spend_lower + record.spend_upper) / 2
                elif record.spend_lower is not None:
                    amount = record.spend_lower
                elif record.spend_upper is not None:
                    amount = record.spend_upper
                
                rel_query = """
                MATCH (a:Ad {meta_ad_id: $ad_id})
                MATCH (s:Sponsor {name: $sponsor_name})
                MERGE (a)-[r:SPONSORED_BY]->(s)
                SET r.spend_lower = $spend_lower,
                    r.spend_upper = $spend_upper,
                    r.amount = $amount,
                    r.confidence = 0.9,
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
                    amount=amount,
                    currency=record.currency,
                    country=record.country,
                    now=datetime.utcnow().isoformat(),
                )

        # Note: PostgreSQL events storage skipped - the events table schema requires
        # evidence_ref FK and uses different column names (entity_ids array, occurred_at, properties).
        # The Neo4j graph is the primary storage for influence detection.
        # TODO: Implement proper events storage with evidence creation if needed for temporal analysis.

        # Optionally enrich with Facebook Page details
        if self._enrich_page_details and record.page_id:
            # Skip if we already tried and failed for this page ID
            if record.page_id in self._failed_page_ids:
                self.logger.debug(f"Skipping page {record.page_id} (previously failed)")
            elif record.page_id in self._enriched_page_ids:
                self.logger.debug(f"Skipping page {record.page_id} (already enriched)")
                result["page_details_enriched"] = True
            else:
                try:
                    self.logger.info(f"Enriching page details for {record.page_name} (ID: {record.page_id})")
                    enrichment = await self.enrich_sponsor_with_page_details(
                        page_id=record.page_id,
                        sponsor_name=record.funding_entity or record.page_name or "",
                    )
                    result["page_details_enriched"] = enrichment.get("enriched", False)
                    if enrichment.get("enriched"):
                        self._enriched_page_ids.add(record.page_id)
                        self.logger.info(
                            f"  Found {enrichment.get('social_links_found', 0)} social links, "
                            f"{enrichment.get('managing_orgs_found', 0)} managing orgs"
                        )
                    elif not enrichment.get("page_details_fetched"):
                        # API call failed (permission error, etc.)
                        self._failed_page_ids.add(record.page_id)
                except Exception as e:
                    self._failed_page_ids.add(record.page_id)
                    self.logger.warning(f"Failed to enrich page details for {record.page_id}: {e}")

        return result

    async def _ensure_entity_in_postgres(
        self,
        entity_id: str,
        name: str,
        entity_type: str = "organization",
        external_ids: dict[str, Any] | None = None,
    ) -> None:
        """Ensure an entity exists in PostgreSQL's entities table.
        
        This is needed because session_entities has a FK constraint to entities.
        Neo4j is the primary graph store, but PostgreSQL tracks entity metadata.
        
        Args:
            entity_id: Entity UUID (as string)
            name: Entity name
            entity_type: Entity type (organization, person, etc.)
            external_ids: External identifier mappings
        """
        from sqlalchemy import text
        
        try:
            async with get_db_session() as db:
                await db.execute(
                    text("""
                        INSERT INTO entities (id, name, entity_type, external_ids, metadata, created_at)
                        VALUES (:id, :name, :entity_type, CAST(:external_ids AS jsonb), '{}'::jsonb, NOW())
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {
                        "id": entity_id,
                        "name": name,
                        "entity_type": entity_type,
                        "external_ids": json.dumps(external_ids or {}),
                    },
                )
        except Exception as e:
            self.logger.debug(f"Could not ensure entity in PostgreSQL: {e}")

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

    async def ingest_single(
        self,
        identifier: str,
        identifier_type: str,
    ) -> SingleIngestionResult | None:
        """Ingest ads for a single sponsor/page from Meta Ad Library.

        Args:
            identifier: The page name, page ID, or funding entity name
            identifier_type: One of "name", "meta_page_id", "funding_entity"

        Returns:
            SingleIngestionResult if ads found and processed, None otherwise
        """
        try:
            access_token = await self.get_access_token()

            # Build search query based on identifier type
            page_ids = None

            if identifier_type == "meta_page_id":
                page_ids = [identifier]
                search_variations = []
            elif identifier_type in ("name", "funding_entity"):
                # Generate name variations to try (handles apostrophes, Inc., etc.)
                search_variations = generate_search_name_variations(identifier)
                self.logger.debug(
                    f"Generated {len(search_variations)} search variations for '{identifier}': {search_variations}"
                )
            else:
                search_variations = [identifier]

            if not search_variations and not page_ids:
                return None

            # Fetch ads matching the query
            ads_processed = 0
            entity_id = None
            entity_name = None
            is_new = False
            ads = []

            # Base params for all requests
            base_params = {
                "access_token": access_token,
                "ad_reached_countries": json.dumps(["CA", "US"]),
                "ad_type": "POLITICAL_AND_ISSUE_ADS",
                "ad_active_status": "ALL",
                "fields": ",".join(DEFAULT_AD_FIELDS),
                "limit": 10,  # Only fetch a few ads for single entity lookup
            }

            if page_ids:
                params = base_params.copy()
                params["search_page_ids"] = ",".join(page_ids)
                ads = await self._fetch_single_query(params, identifier)
            else:
                # Try each search variation until we find results
                for variation in search_variations:
                    params = base_params.copy()
                    params["search_terms"] = variation
                    
                    ads = await self._fetch_single_query(params, variation)
                    
                    if ads:
                        self.logger.info(
                            f"Found {len(ads)} ads using search variation: '{variation}' (original: '{identifier}')"
                        )
                        break
                    else:
                        self.logger.debug(f"No ads found for variation: '{variation}'")

            if not ads:
                self.logger.info(f"No ads found for: {identifier} (tried {len(search_variations)} variations)")
                return None

            # Process the first few ads to create/update the sponsor entity
            for ad_data in ads[:5]:
                try:
                    record = self._parse_ad(ad_data, "US")
                    result = await self.process_record(record)

                    if result.get("entity_id") and not entity_id:
                        entity_id = UUID(result["entity_id"])
                        entity_name = record.page_name or record.funding_entity
                        is_new = result.get("created", False)

                    ads_processed += 1

                except Exception as e:
                    self.logger.warning(f"Error processing ad: {e}")
                    continue

            if entity_id:
                return SingleIngestionResult(
                    entity_id=entity_id,
                    entity_name=entity_name,
                    entity_type="organization",
                    is_new=is_new,
                    relationships_created=ads_processed,
                    source="meta_ads",
                )

            return None

        except Exception as e:
            self.logger.error(f"Error ingesting from Meta Ads {identifier}: {e}")
            return SingleIngestionResult(
                source="meta_ads",
                error=str(e),
            )

    async def _fetch_single_query(
        self,
        params: dict[str, Any],
        query_description: str,
    ) -> list[dict[str, Any]]:
        """Execute a single Meta Ad Library query and return results.
        
        Args:
            params: Query parameters including access_token, search_terms, etc.
            query_description: Human-readable description of what we're searching for (for logging)
            
        Returns:
            List of ad data dictionaries, empty list if no results or error
        """
        try:
            response = await self.http_client.get(
                META_ADS_ARCHIVE_ENDPOINT, params=params
            )

            if response.status_code != 200:
                # Log detailed error information from Meta
                try:
                    error_data = response.json() if response.content else {}
                    error_info = error_data.get("error", {})
                    self.logger.warning(
                        f"Meta API error {response.status_code} for '{query_description}': "
                        f"code={error_info.get('code')}, "
                        f"type={error_info.get('type')}, "
                        f"message={error_info.get('message', 'No message')}, "
                        f"error_subcode={error_info.get('error_subcode', 'N/A')}"
                    )
                except Exception:
                    self.logger.warning(
                        f"Meta API error {response.status_code} for '{query_description}': "
                        f"{response.text[:500] if response.text else 'No response body'}"
                    )
                return []

            data = response.json()
            return data.get("data", [])

        except Exception as e:
            self.logger.warning(f"Error querying Meta API for '{query_description}': {e}")
            return []

    async def search_by_sponsor(
        self,
        sponsor_name: str,
        limit: int = 100,
    ) -> list[MetaAdRecord]:
        """Search Meta Ad Library by sponsor/page name.
        
        Tries multiple name variations to handle apostrophes, corporate suffixes, etc.
        
        Args:
            sponsor_name: The sponsor or page name to search for
            limit: Maximum number of records to return
            
        Returns:
            List of MetaAdRecord objects
        """
        try:
            access_token = await self.get_access_token()
            
            # Generate name variations
            search_variations = generate_search_name_variations(sponsor_name)
            self.logger.debug(
                f"search_by_sponsor: trying {len(search_variations)} variations for '{sponsor_name}'"
            )
            
            base_params = {
                "access_token": access_token,
                "ad_reached_countries": json.dumps(["CA", "US"]),
                "ad_type": "POLITICAL_AND_ISSUE_ADS",
                "ad_active_status": "ALL",
                "fields": ",".join(DEFAULT_AD_FIELDS),
                "limit": min(limit, 100),
            }
            
            # Try each variation until we find results
            for variation in search_variations:
                params = base_params.copy()
                params["search_terms"] = variation
                
                ads = await self._fetch_single_query(params, variation)
                
                if ads:
                    self.logger.info(
                        f"Found {len(ads)} ads for sponsor '{variation}' (original: '{sponsor_name}')"
                    )
                    # Parse and return records
                    records = []
                    for ad_data in ads[:limit]:
                        try:
                            record = self._parse_ad(ad_data, "CA")
                            records.append(record)
                        except Exception as e:
                            self.logger.warning(f"Failed to parse ad: {e}")
                            continue
                    return records
            
            self.logger.info(f"No ads found for sponsor '{sponsor_name}' (tried {len(search_variations)} variations)")
            return []
            
        except Exception as e:
            self.logger.error(f"Error searching by sponsor '{sponsor_name}': {e}")
            return []

    async def search_by_page_id(
        self,
        page_id: str,
        limit: int = 100,
    ) -> list[MetaAdRecord]:
        """Search Meta Ad Library by page ID.
        
        Args:
            page_id: The Meta page ID to search for
            limit: Maximum number of records to return
            
        Returns:
            List of MetaAdRecord objects
        """
        try:
            access_token = await self.get_access_token()
            
            params = {
                "access_token": access_token,
                "ad_reached_countries": json.dumps(["CA", "US"]),
                "ad_type": "POLITICAL_AND_ISSUE_ADS",
                "ad_active_status": "ALL",
                "fields": ",".join(DEFAULT_AD_FIELDS),
                "limit": min(limit, 100),
                "search_page_ids": page_id,
            }
            
            ads = await self._fetch_single_query(params, f"page_id:{page_id}")
            
            if not ads:
                self.logger.info(f"No ads found for page ID '{page_id}'")
                return []
            
            self.logger.info(f"Found {len(ads)} ads for page ID '{page_id}'")
            
            # Parse and return records
            records = []
            for ad_data in ads[:limit]:
                try:
                    record = self._parse_ad(ad_data, "CA")
                    records.append(record)
                except Exception as e:
                    self.logger.warning(f"Failed to parse ad: {e}")
                    continue
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error searching by page ID '{page_id}': {e}")
            return []

    # =========================================================================
    # Facebook Page Details Methods
    # =========================================================================

    async def fetch_page_details(
        self,
        page_id: str,
        include_agencies: bool = True,
    ) -> FacebookPageDetails | None:
        """Fetch detailed information about a Facebook Page.
        
        Retrieves page information including:
        - Basic info (name, about, description, category)
        - Contact info (website, emails, phone, WhatsApp)
        - Social links (Instagram, external URLs)
        - Managing organizations (agencies with access)
        - Stats (followers, verification status)
        
        Args:
            page_id: The Meta/Facebook Page ID
            include_agencies: Whether to fetch managing agencies (requires extra API call)
            
        Returns:
            FacebookPageDetails if successful, None otherwise
        """
        try:
            access_token = await self.get_access_token()
            
            # Fetch basic page details
            page_url = f"{META_GRAPH_API_BASE}/{page_id}"
            params = {
                "access_token": access_token,
                "fields": ",".join(PAGE_DETAIL_FIELDS),
            }
            
            response = await self.http_client.get(page_url, params=params)
            
            if response.status_code != 200:
                self._log_api_error(response, f"page details for {page_id}")
                return None
            
            page_data = response.json()
            
            # Parse page details
            details = self._parse_page_details(page_data)
            
            # Fetch linked Instagram accounts
            instagram_data = await self._fetch_page_instagram(page_id, access_token)
            if instagram_data:
                details.social_links.instagram_username = instagram_data.get("username")
                details.social_links.instagram_id = instagram_data.get("id")
            
            # Fetch managing agencies/businesses
            if include_agencies:
                agencies = await self._fetch_page_agencies(page_id, access_token)
                details.managing_organizations = agencies
            
            # Extract social links from text fields
            self._extract_social_links_from_text(details)
            
            details.fetched_at = datetime.utcnow()
            details.raw_data = page_data
            
            self.logger.info(
                f"Fetched page details for {page_id}: {details.page_name}, "
                f"{len(details.managing_organizations)} managing orgs"
            )
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error fetching page details for {page_id}: {e}")
            return None

    def _parse_page_details(self, page_data: dict[str, Any]) -> FacebookPageDetails:
        """Parse raw Page API response into FacebookPageDetails."""
        
        # Parse category list
        category_list = []
        for cat in page_data.get("category_list", []):
            if isinstance(cat, dict) and cat.get("id") and cat.get("name"):
                category_list.append(FacebookPageCategory(
                    id=str(cat["id"]),
                    name=cat["name"]
                ))
        
        # Parse location
        location = page_data.get("location", {})
        address = page_data.get("single_line_address")
        if not address and location:
            # Build address from location components
            addr_parts = []
            if location.get("street"):
                addr_parts.append(location["street"])
            if location.get("city"):
                addr_parts.append(location["city"])
            if location.get("state"):
                addr_parts.append(location["state"])
            if location.get("country"):
                addr_parts.append(location["country"])
            if addr_parts:
                address = ", ".join(addr_parts)
        
        # Parse websites - can be comma-separated or just one URL
        websites = []
        website_raw = page_data.get("website", "")
        if website_raw:
            # Split on common separators
            for sep in [",", "\n", " "]:
                if sep in website_raw:
                    websites = [w.strip() for w in website_raw.split(sep) if w.strip()]
                    break
            if not websites:
                websites = [website_raw.strip()]
        
        # Build contact info
        contact = FacebookPageContact(
            website=websites[0] if websites else None,
            websites=websites,
            emails=page_data.get("emails", []),
            phone=page_data.get("phone"),
            whatsapp_number=page_data.get("whatsapp_number"),
            address=address,
        )
        
        # Build social links
        social_links = FacebookPageSocialLinks(
            facebook_url=page_data.get("link"),
            external_links=websites,
        )
        
        return FacebookPageDetails(
            page_id=str(page_data.get("id", "")),
            page_name=page_data.get("name", ""),
            username=page_data.get("username"),
            about=page_data.get("about"),
            description=page_data.get("description"),
            company_overview=page_data.get("company_overview"),
            mission=page_data.get("mission"),
            general_info=page_data.get("general_info"),
            founded=page_data.get("founded"),
            category=page_data.get("category"),
            category_list=category_list,
            contact=contact,
            social_links=social_links,
            followers_count=page_data.get("followers_count"),
            fan_count=page_data.get("fan_count"),
            verification_status=page_data.get("verification_status"),
        )

    async def _fetch_page_instagram(
        self,
        page_id: str,
        access_token: str,
    ) -> dict[str, Any] | None:
        """Fetch Instagram account linked to a Facebook Page.
        
        Args:
            page_id: The Facebook Page ID
            access_token: Valid access token
            
        Returns:
            Instagram account data dict or None
        """
        try:
            # Try connected_instagram_account first
            url = f"{META_GRAPH_API_BASE}/{page_id}"
            params = {
                "access_token": access_token,
                "fields": "connected_instagram_account{id,username,name,biography}",
            }
            
            response = await self.http_client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if "connected_instagram_account" in data:
                    return data["connected_instagram_account"]
            
            # Try instagram_accounts edge as fallback
            url = f"{META_GRAPH_API_BASE}/{page_id}/instagram_accounts"
            params = {
                "access_token": access_token,
                "fields": "id,username,name",
            }
            
            response = await self.http_client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                accounts = data.get("data", [])
                if accounts:
                    return accounts[0]  # Return first linked account
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Could not fetch Instagram for page {page_id}: {e}")
            return None

    async def _fetch_page_agencies(
        self,
        page_id: str,
        access_token: str,
    ) -> list[ManagingOrganization]:
        """Fetch agencies/businesses that manage a Facebook Page.
        
        Note: This requires the Page to be associated with a Business Manager
        and appropriate permissions. May return empty for public pages.
        
        Args:
            page_id: The Facebook Page ID
            access_token: Valid access token
            
        Returns:
            List of ManagingOrganization objects
        """
        organizations = []
        
        try:
            # Fetch agencies edge (businesses with agency permissions on the Page)
            url = f"{META_GRAPH_API_BASE}/{page_id}/agencies"
            params = {
                "access_token": access_token,
                "fields": "id,name",
            }
            
            response = await self.http_client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                for agency in data.get("data", []):
                    organizations.append(ManagingOrganization(
                        id=str(agency.get("id", "")),
                        name=agency.get("name"),
                        type="agency",
                    ))
            
            # Also try to get the primary business association
            url = f"{META_GRAPH_API_BASE}/{page_id}"
            params = {
                "access_token": access_token,
                "fields": "business{id,name}",
            }
            
            response = await self.http_client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if "business" in data:
                    business = data["business"]
                    # Avoid duplicates
                    if not any(org.id == str(business.get("id")) for org in organizations):
                        organizations.append(ManagingOrganization(
                            id=str(business.get("id", "")),
                            name=business.get("name"),
                            type="business",
                        ))
            
        except Exception as e:
            self.logger.debug(f"Could not fetch agencies for page {page_id}: {e}")
        
        return organizations

    def _extract_social_links_from_text(self, details: FacebookPageDetails) -> None:
        """Extract social media handles from page text fields.
        
        Parses about, description, and general_info fields to find
        Twitter/X handles, YouTube links, and other social media references.
        """
        # Combine all text fields to search
        text_fields = [
            details.about or "",
            details.description or "",
            details.general_info or "",
            details.company_overview or "",
        ]
        combined_text = " ".join(text_fields)
        
        # Extract Twitter/X handles (@username pattern)
        twitter_pattern = r"(?:twitter\.com/|x\.com/|@)([a-zA-Z0-9_]{1,15})(?:\s|$|[^a-zA-Z0-9_])"
        twitter_matches = re.findall(twitter_pattern, combined_text, re.IGNORECASE)
        if twitter_matches and not details.social_links.twitter_handle:
            details.social_links.twitter_handle = twitter_matches[0]
        
        # Extract YouTube URLs
        youtube_pattern = r"(?:youtube\.com/(?:channel/|c/|user/|@)?|youtu\.be/)([a-zA-Z0-9_-]+)"
        youtube_matches = re.findall(youtube_pattern, combined_text, re.IGNORECASE)
        if youtube_matches and not details.social_links.youtube_url:
            details.social_links.youtube_url = f"https://youtube.com/{youtube_matches[0]}"
        
        # Also check website list for social links
        for url in details.contact.websites:
            url_lower = url.lower()
            if "youtube.com" in url_lower or "youtu.be" in url_lower:
                if not details.social_links.youtube_url:
                    details.social_links.youtube_url = url
            elif "twitter.com" in url_lower or "x.com" in url_lower:
                if not details.social_links.twitter_handle:
                    # Extract handle from URL
                    match = re.search(r"(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)", url)
                    if match:
                        details.social_links.twitter_handle = match.group(1)

    def _log_api_error(self, response: httpx.Response, context: str) -> None:
        """Log detailed error information from a failed API response."""
        try:
            error_data = response.json() if response.content else {}
            error_info = error_data.get("error", {})
            self.logger.warning(
                f"Meta API error {response.status_code} for {context}: "
                f"code={error_info.get('code')}, "
                f"type={error_info.get('type')}, "
                f"message={error_info.get('message', 'No message')}"
            )
        except Exception:
            self.logger.warning(
                f"Meta API error {response.status_code} for {context}: "
                f"{response.text[:500] if response.text else 'No response body'}"
            )

    async def enrich_sponsor_with_page_details(
        self,
        page_id: str,
        sponsor_name: str,
    ) -> dict[str, Any]:
        """Enrich a sponsor entity with Facebook Page details.
        
        Fetches page details and updates the sponsor node in Neo4j with
        additional information including contact details, social links,
        and managing organizations.
        
        Args:
            page_id: The Meta/Facebook Page ID
            sponsor_name: Name of the sponsor entity
            
        Returns:
            Dict with enrichment results
        """
        result = {
            "enriched": False,
            "page_details_fetched": False,
            "social_links_found": 0,
            "managing_orgs_found": 0,
        }
        
        # Fetch page details
        details = await self.fetch_page_details(page_id)
        
        if not details:
            self.logger.debug(f"Could not fetch page details for {page_id}")
            return result
        
        result["page_details_fetched"] = True
        
        # Count social links found
        social = details.social_links
        if social.instagram_username:
            result["social_links_found"] += 1
        if social.twitter_handle:
            result["social_links_found"] += 1
        if social.youtube_url:
            result["social_links_found"] += 1
        
        result["managing_orgs_found"] = len(details.managing_organizations)
        
        # Update Neo4j
        try:
            async with get_neo4j_session() as session:
                # Update sponsor node with page details
                update_query = """
                MATCH (s:Sponsor {meta_page_id: $page_id})
                SET s.page_username = $username,
                    s.page_about = $about,
                    s.page_category = $category,
                    s.page_website = $website,
                    s.page_emails = $emails,
                    s.page_phone = $phone,
                    s.page_whatsapp = $whatsapp,
                    s.page_facebook_url = $facebook_url,
                    s.page_instagram_username = $instagram_username,
                    s.page_twitter_handle = $twitter_handle,
                    s.page_youtube_url = $youtube_url,
                    s.page_followers_count = $followers_count,
                    s.page_verification_status = $verification_status,
                    s.page_address = $address,
                    s.page_details_fetched_at = $fetched_at
                RETURN s.id as id
                """
                
                await session.run(
                    update_query,
                    page_id=page_id,
                    username=details.username,
                    about=details.about,
                    category=details.category,
                    website=details.contact.website,
                    emails=details.contact.emails,
                    phone=details.contact.phone,
                    whatsapp=details.contact.whatsapp_number,
                    facebook_url=details.social_links.facebook_url,
                    instagram_username=details.social_links.instagram_username,
                    twitter_handle=details.social_links.twitter_handle,
                    youtube_url=details.social_links.youtube_url,
                    followers_count=details.followers_count,
                    verification_status=details.verification_status,
                    address=details.contact.address,
                    fetched_at=datetime.utcnow().isoformat(),
                )
                
                # Create relationships for managing organizations
                for org in details.managing_organizations:
                    if org.id and org.name:
                        org_query = """
                        MERGE (mo:Organization {meta_business_id: $org_id})
                        ON CREATE SET
                            mo.id = $uuid,
                            mo.name = $org_name,
                            mo.entity_type = 'organization',
                            mo.org_type = $org_type,
                            mo.created_at = $now
                        ON MATCH SET
                            mo.name = COALESCE(mo.name, $org_name)
                        WITH mo
                        MATCH (s:Sponsor {meta_page_id: $page_id})
                        MERGE (mo)-[r:MANAGES]->(s)
                        SET r.relationship_type = $org_type,
                            r.updated_at = $now
                        """
                        
                        await session.run(
                            org_query,
                            org_id=org.id,
                            uuid=str(uuid4()),
                            org_name=org.name,
                            org_type=org.type or "unknown",
                            page_id=page_id,
                            now=datetime.utcnow().isoformat(),
                        )
                
                result["enriched"] = True
                
        except Exception as e:
            self.logger.error(f"Error enriching sponsor with page details: {e}")
        
        return result


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
    enrich_page_details: bool = False,
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
        enrich_page_details: Fetch Facebook Page details (contact, social links, etc.)

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
                "enrich_page_details": enrich_page_details,
            },
        )
        result = await ingester.run(config)
        return result.model_dump()
    finally:
        await ingester.close()


async def fetch_facebook_page_details(
    page_id: str,
    include_agencies: bool = True,
) -> FacebookPageDetails | None:
    """Fetch detailed information about a Facebook Page.
    
    Standalone function to get page details including:
    - Basic info (name, about, description, category)
    - Contact info (website, emails, phone, WhatsApp)  
    - Social links (Instagram, YouTube, Twitter/X)
    - Managing organizations (agencies with access)
    - Stats (followers, verification status)
    
    Args:
        page_id: The Meta/Facebook Page ID
        include_agencies: Whether to fetch managing agencies
        
    Returns:
        FacebookPageDetails if successful, None otherwise
        
    Example:
        >>> details = await fetch_facebook_page_details("123456789")
        >>> print(details.contact.website)
        >>> print(details.social_links.twitter_handle)
        >>> for org in details.managing_organizations:
        ...     print(f"Managed by: {org.name}")
    """
    ingester = MetaAdIngester()
    try:
        return await ingester.fetch_page_details(page_id, include_agencies)
    finally:
        await ingester.close()


async def enrich_sponsor_page_details(
    page_id: str,
    sponsor_name: str = "",
) -> dict[str, Any]:
    """Enrich an existing sponsor with Facebook Page details.
    
    Fetches page details and updates the sponsor node in Neo4j with
    contact information, social links, and managing organizations.
    
    Args:
        page_id: The Meta/Facebook Page ID
        sponsor_name: Name of the sponsor entity (optional)
        
    Returns:
        Dict with enrichment results including:
        - enriched: bool - Whether enrichment succeeded
        - page_details_fetched: bool - Whether page details were fetched
        - social_links_found: int - Number of social links found
        - managing_orgs_found: int - Number of managing orgs found
        
    Example:
        >>> result = await enrich_sponsor_page_details("123456789", "National Citizens Coalition")
        >>> print(f"Enriched: {result['enriched']}")
        >>> print(f"Found {result['social_links_found']} social links")
    """
    ingester = MetaAdIngester()
    try:
        return await ingester.enrich_sponsor_with_page_details(page_id, sponsor_name)
    finally:
        await ingester.close()


async def search_and_enrich_sponsor(
    sponsor_name: str,
    limit: int = 5,
) -> dict[str, Any]:
    """Search for a sponsor and enrich with Facebook Page details.
    
    Searches the Meta Ad Library for ads by the sponsor, then fetches
    and stores detailed Facebook Page information.
    
    Args:
        sponsor_name: Name of the sponsor to search for
        limit: Maximum ads to process
        
    Returns:
        Dict with results including:
        - ads_found: int - Number of ads found
        - page_id: str | None - Facebook Page ID if found
        - page_details: FacebookPageDetails | None - Page details if fetched
        - enriched: bool - Whether enrichment succeeded
    """
    ingester = MetaAdIngester()
    result = {
        "ads_found": 0,
        "page_id": None,
        "page_details": None,
        "enriched": False,
    }
    
    try:
        # Search for ads by sponsor
        ads = await ingester.search_by_sponsor(sponsor_name, limit=limit)
        result["ads_found"] = len(ads)
        
        if not ads:
            return result
        
        # Get page ID from first ad
        page_id = ads[0].page_id
        if not page_id:
            return result
            
        result["page_id"] = page_id
        
        # Process the ads to create/update sponsor (without page enrichment, we do it manually below)
        for ad in ads[:limit]:
            await ingester.process_record(ad)
        
        # Fetch page details
        details = await ingester.fetch_page_details(page_id)
        if details:
            result["page_details"] = details.model_dump()
        
        # Enrich the sponsor
        enrichment = await ingester.enrich_sponsor_with_page_details(
            page_id=page_id,
            sponsor_name=sponsor_name,
        )
        result["enriched"] = enrichment.get("enriched", False)
        
        return result
        
    finally:
        await ingester.close()
