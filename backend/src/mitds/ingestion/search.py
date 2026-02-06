"""Cross-source company search for targeted ingestion.

Provides search functions that query each data source's index/listing
to find companies matching a text query. Returns structured results
with source-specific identifiers that can be used for targeted ingestion.
"""

import asyncio
import csv
import io
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import httpx
from pydantic import BaseModel, Field

from ..logging import get_context_logger
from .base import RetryConfig, with_retry

logger = get_context_logger(__name__)

# Cache for downloaded data (module-level, persists across requests)
_cache: dict[str, Any] = {}

# Disk cache for persistence across server restarts
_DISK_CACHE_DIR = Path(
    os.environ.get(
        "MITDS_CACHE_DIR",
        str(Path(__file__).resolve().parents[4] / ".cache" / "mitds" / "search"),
    )
)
_DISK_CACHE_TTL_HOURS = 24


def _load_disk_cache(key: str) -> Any | None:
    """Load data from disk cache if it exists and is fresh."""
    cache_file = _DISK_CACHE_DIR / f"{key}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        fetched_at = data.get("_fetched_at", 0)
        age_hours = (time.time() - fetched_at) / 3600
        if age_hours > _DISK_CACHE_TTL_HOURS:
            logger.info(f"Disk cache expired for {key} ({age_hours:.1f}h old)")
            return None
        logger.info(f"Loaded {key} from disk cache ({age_hours:.1f}h old)")
        return data.get("payload")
    except Exception as e:
        logger.warning(f"Failed to load disk cache for {key}: {e}")
        return None


def _save_disk_cache(key: str, payload: Any) -> None:
    """Save data to disk cache."""
    try:
        _DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = _DISK_CACHE_DIR / f"{key}.json"
        cache_file.write_text(
            json.dumps({"_fetched_at": time.time(), "payload": payload}),
            encoding="utf-8",
        )
        logger.info(f"Saved {key} to disk cache")
    except Exception as e:
        logger.warning(f"Failed to save disk cache for {key}: {e}")


class CompanySearchResult(BaseModel):
    """A single company search result from any data source."""

    source: str = Field(..., description="Data source name")
    identifier: str = Field(..., description="Source-specific identifier")
    identifier_type: str = Field(..., description="Type of identifier (CIK, EIN, BN, etc.)")
    name: str = Field(..., description="Company/organization name")
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific details",
    )


class CompanySearchResponse(BaseModel):
    """Response from cross-source company search."""

    query: str
    results: list[CompanySearchResult]
    sources_searched: list[str]
    sources_failed: list[str] = Field(default_factory=list)


# =========================
# SEC EDGAR Search
# =========================

EDGAR_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT = "MITDS Research contact@mitds.org"


async def _get_edgar_tickers() -> dict[str, dict[str, Any]]:
    """Get cached SEC EDGAR company tickers mapping."""
    if "edgar_tickers" in _cache:
        return _cache["edgar_tickers"]

    # Try disk cache
    disk = _load_disk_cache("edgar_tickers")
    if disk is not None:
        _cache["edgar_tickers"] = disk
        return disk

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    ) as client:

        async def _fetch():
            response = await client.get(EDGAR_COMPANY_TICKERS_URL)
            if response.status_code == 404:
                logger.warning("SEC EDGAR company tickers file not found")
                return None
            response.raise_for_status()
            return response.json()

        data = await with_retry(
            _fetch, config=RetryConfig(max_retries=2), logger=logger
        )

    if data is None:
        _cache["edgar_tickers"] = {}
        return {}

    # Build CIK-keyed dict with name and tickers
    result = {}
    for item in data.values():
        cik = str(item["cik_str"]).zfill(10)
        if cik not in result:
            result[cik] = {
                "cik": cik,
                "tickers": [],
                "name": item["title"],
            }
        result[cik]["tickers"].append(item["ticker"])

    _cache["edgar_tickers"] = result
    _save_disk_cache("edgar_tickers", result)
    return result


async def search_sec_edgar(query: str, limit: int = 10) -> list[CompanySearchResult]:
    """Search SEC EDGAR for companies by name or ticker.

    Downloads company_tickers.json (~2MB) and searches in-memory.
    Results are cached for subsequent searches.
    """
    tickers_map = await _get_edgar_tickers()
    query_lower = query.lower()
    query_parts = query_lower.split()

    results = []
    scored: list[tuple[int, CompanySearchResult]] = []

    for cik, info in tickers_map.items():
        name = info["name"]
        name_lower = name.lower()
        tickers = info.get("tickers", [])
        tickers_lower = [t.lower() for t in tickers]

        # Exact ticker match (highest priority)
        if query_lower in tickers_lower:
            score = 100
        # Name starts with query
        elif name_lower.startswith(query_lower):
            score = 90
        # All query parts found in name
        elif all(part in name_lower for part in query_parts):
            score = 70
        # Partial name match
        elif query_lower in name_lower:
            score = 60
        else:
            continue

        scored.append((score, CompanySearchResult(
            source="sec_edgar",
            identifier=cik,
            identifier_type="CIK",
            name=name,
            details={
                "tickers": tickers,
                "cik_formatted": f"CIK{cik}",
            },
        )))

    # Sort by score descending, then name
    scored.sort(key=lambda x: (-x[0], x[1].name))
    return [r for _, r in scored[:limit]]


# =========================
# IRS 990 Search
# =========================

IRS_990_INDEX_URL = "https://s3.amazonaws.com/irs-form-990/index_{year}.json"


async def _get_irs990_index(year: int) -> list[dict[str, str]]:
    """Get cached IRS 990 index for a given year."""
    cache_key = f"irs990_index_{year}"
    if cache_key in _cache:
        return _cache[cache_key]

    # Try disk cache
    disk = _load_disk_cache(cache_key)
    if disk is not None:
        _cache[cache_key] = disk
        return disk

    url = IRS_990_INDEX_URL.format(year=year)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, read=120.0),
        follow_redirects=True,
    ) as client:

        async def _fetch():
            response = await client.get(url)
            if response.status_code == 404:
                # Index for this year doesn't exist yet — not a retryable error
                return None
            response.raise_for_status()
            return response.json()

        data = await with_retry(
            _fetch, config=RetryConfig(max_retries=2), logger=logger
        )

    if data is None:
        _cache[cache_key] = []
        return []

    entries = []
    for item in data.get(f"Filings{year}", data.get("Filings", [])):
        if item.get("RETURN_TYPE") in ("990", "990EZ", "990PF"):
            entries.append(item)

    _cache[cache_key] = entries
    _save_disk_cache(cache_key, entries)
    return entries


async def search_irs990(query: str, limit: int = 10) -> list[CompanySearchResult]:
    """Search IRS 990 index for organizations by name.

    Downloads available year indexes from S3 and searches TAXPAYER_NAME.
    Tries previous two years since current year index may not exist yet.
    """
    current_year = datetime.now().year

    # Try previous two years (current year index often doesn't exist early in the year)
    all_entries: list[dict[str, str]] = []
    for year in [current_year - 1, current_year - 2, current_year]:
        try:
            entries = await _get_irs990_index(year)
            all_entries.extend(entries)
        except Exception as e:
            logger.warning(f"Failed to fetch IRS 990 index for {year}: {e}")

    if not all_entries:
        return []

    query_lower = query.lower()
    query_parts = query_lower.split()

    # Deduplicate by EIN (keep latest entry)
    seen_eins: dict[str, dict] = {}
    for entry in all_entries:
        ein = entry.get("EIN", "")
        if ein and ein not in seen_eins:
            seen_eins[ein] = entry

    scored: list[tuple[int, CompanySearchResult]] = []

    for ein, entry in seen_eins.items():
        name = entry.get("TAXPAYER_NAME", "")
        name_lower = name.lower()

        # Name starts with query
        if name_lower.startswith(query_lower):
            score = 90
        # All query parts found in name
        elif all(part in name_lower for part in query_parts):
            score = 70
        # Partial match
        elif query_lower in name_lower:
            score = 60
        else:
            continue

        # Format EIN
        formatted_ein = ein
        if len(ein) == 9:
            formatted_ein = f"{ein[:2]}-{ein[2:]}"

        scored.append((score, CompanySearchResult(
            source="irs990",
            identifier=ein,
            identifier_type="EIN",
            name=name,
            details={
                "ein_formatted": formatted_ein,
                "form_type": entry.get("RETURN_TYPE", ""),
                "tax_period": entry.get("TAX_PERIOD", ""),
            },
        )))

    scored.sort(key=lambda x: (-x[0], x[1].name))
    return [r for _, r in scored[:limit]]


# =========================
# CRA Charities Search
# =========================

CRA_IDENTIFICATION_URL = (
    "https://open.canada.ca/data/dataset/"
    "05b3abd0-e70f-4b3b-a9c5-acc436bd15b6/resource/"
    "31a52caf-fa79-4ab3-bded-1ccc7b61c17f/download/ident_2023_update.csv"
)


async def _get_cra_charities() -> list[dict[str, str]]:
    """Get cached CRA charity identification data."""
    if "cra_charities" in _cache:
        return _cache["cra_charities"]

    # Try disk cache
    disk = _load_disk_cache("cra_charities")
    if disk is not None:
        _cache["cra_charities"] = disk
        return disk

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, read=300.0),
        follow_redirects=True,
    ) as client:

        async def _fetch():
            response = await client.get(CRA_IDENTIFICATION_URL)
            if response.status_code == 404:
                logger.warning("CRA identification data not found at expected URL")
                return None
            response.raise_for_status()
            return response.content

        content = await with_retry(
            _fetch, config=RetryConfig(max_retries=2), logger=logger
        )

    if content is None:
        _cache["cra_charities"] = []
        return []

    # Parse CSV (direct download, not zipped)
    try:
        decoded = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(decoded))
        rows = list(reader)
    except Exception as e:
        # Fall back to trying ZIP format in case URL changes back
        try:
            with ZipFile(io.BytesIO(content)) as zf:
                csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_files:
                    return []
                csv_content = zf.read(csv_files[0])
                decoded = csv_content.decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(decoded))
                rows = list(reader)
        except Exception:
            logger.error(f"Failed to parse CRA identification data: {e}")
            return []

    _cache["cra_charities"] = rows
    _save_disk_cache("cra_charities", rows)
    return rows


async def search_cra(query: str, limit: int = 10) -> list[CompanySearchResult]:
    """Search CRA registered charities by name.

    Downloads the identification CSV (~few MB) and searches by legal/operating name.
    """
    charities = await _get_cra_charities()
    if not charities:
        return []

    query_lower = query.lower()
    query_parts = query_lower.split()

    scored: list[tuple[int, CompanySearchResult]] = []

    for row in charities:
        bn = row.get("BN", row.get("bn", "")).strip()
        legal_name = (
            row.get("Legal Name", row.get("legal_name", ""))
            or row.get("LegalNameEng", "")
            or row.get("LEGAL_NAME", "")
        ).strip()
        operating_name = (
            row.get("Operating Name", row.get("operating_name", ""))
            or row.get("Account Name", "")
            or row.get("OperatingNameEng", "")
        )
        if operating_name:
            operating_name = operating_name.strip()

        if not bn or not legal_name:
            continue

        legal_lower = legal_name.lower()
        operating_lower = (operating_name or "").lower()

        # Score based on match quality
        score = 0
        if legal_lower.startswith(query_lower):
            score = 90
        elif operating_lower and operating_lower.startswith(query_lower):
            score = 85
        elif all(part in legal_lower for part in query_parts):
            score = 70
        elif operating_lower and all(part in operating_lower for part in query_parts):
            score = 65
        elif query_lower in legal_lower:
            score = 60
        elif operating_lower and query_lower in operating_lower:
            score = 55
        else:
            continue

        province = row.get("Province", row.get("province", ""))
        city = row.get("City", row.get("city", ""))
        designation = row.get("Designation", row.get("designation", ""))
        category = row.get("Category", row.get("category", ""))

        scored.append((score, CompanySearchResult(
            source="cra",
            identifier=bn,
            identifier_type="BN",
            name=legal_name,
            details={
                "operating_name": operating_name or None,
                "province": province,
                "city": city,
                "designation": designation,
                "category": category,
            },
        )))

    scored.sort(key=lambda x: (-x[0], x[1].name))
    return [r for _, r in scored[:limit]]


# =========================
# Canada Corporations Search
# =========================

BULK_DATA_URL = "https://ised-isde.canada.ca/cc/lgcy/download/OPEN_DATA_SPLIT.zip"


async def _get_canada_corps() -> list[dict[str, Any]]:
    """Get cached Canada corporations data."""
    if "canada_corps" in _cache:
        return _cache["canada_corps"]

    # Try disk cache
    disk = _load_disk_cache("canada_corps")
    if disk is not None:
        _cache["canada_corps"] = disk
        return disk

    import xml.etree.ElementTree as ET
    import zipfile

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=30.0),
        headers={"User-Agent": "MITDS Research contact@mitds.org"},
        follow_redirects=True,
    ) as client:

        async def _fetch():
            response = await client.get(BULK_DATA_URL)
            if response.status_code == 404:
                logger.warning("Canada Corporations bulk data not found at expected URL")
                return None
            response.raise_for_status()
            return response.content

        zip_content = await with_retry(
            _fetch, config=RetryConfig(max_retries=2), logger=logger
        )

    if zip_content is None:
        _cache["canada_corps"] = []
        return []

    records = []
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        xml_files = [
            f for f in zf.namelist()
            if f.startswith("OPEN_DATA_") and f.endswith(".xml")
        ]
        for filename in xml_files:
            try:
                with zf.open(filename) as f:
                    content = f.read()
                    root = ET.fromstring(content)
                    corporations = root.find("corporations")
                    if corporations is None:
                        corporations = root
                    for corp_elem in corporations.findall("corporation"):
                        record = _extract_corp_for_search(corp_elem)
                        if record:
                            records.append(record)
            except Exception:
                continue

    _cache["canada_corps"] = records
    _save_disk_cache("canada_corps", records)
    return records


def _extract_corp_for_search(corp_elem: Any) -> dict[str, Any] | None:
    """Extract minimal corporation data for search."""
    corp_id = corp_elem.get("corporationId")
    if not corp_id:
        return None

    name = None
    names_elem = corp_elem.find("names")
    if names_elem is not None:
        for name_elem in names_elem.findall("name"):
            if name_elem.get("current") == "true":
                name = name_elem.text
                break
        if not name:
            first_name = names_elem.find("name")
            if first_name is not None and first_name.text:
                name = first_name.text

    if not name:
        return None

    # Status
    status = "Unknown"
    statuses_elem = corp_elem.find("statuses")
    if statuses_elem is not None:
        for status_elem in statuses_elem.findall("status"):
            if status_elem.get("current") == "true":
                status_map = {"1": "Active", "2": "Dissolved", "3": "Revoked", "4": "Amalgamated"}
                status = status_map.get(status_elem.get("code"), "Unknown")
                break

    # Act
    corp_type = None
    acts_elem = corp_elem.find("acts")
    if acts_elem is not None:
        for act_elem in acts_elem.findall("act"):
            if act_elem.get("current") == "true":
                act_map = {"6": "CBCA", "7": "NFP", "8": "BOTA", "9": "COOP", "10": "CNFPA"}
                corp_type = act_map.get(act_elem.get("code"))
                break

    return {
        "corporation_number": corp_id,
        "name": name,
        "status": status,
        "corporation_type": corp_type,
    }


async def search_canada_corps(query: str, limit: int = 10) -> list[CompanySearchResult]:
    """Search Canadian federal corporations by name.

    Downloads bulk data XML from ISED and searches corporation names.
    """
    corps = await _get_canada_corps()
    if not corps:
        return []

    query_lower = query.lower()
    query_parts = query_lower.split()

    scored: list[tuple[int, CompanySearchResult]] = []

    for corp in corps:
        name = corp["name"]
        name_lower = name.lower()

        if name_lower.startswith(query_lower):
            score = 90
        elif all(part in name_lower for part in query_parts):
            score = 70
        elif query_lower in name_lower:
            score = 60
        else:
            continue

        scored.append((score, CompanySearchResult(
            source="canada_corps",
            identifier=corp["corporation_number"],
            identifier_type="Corporation Number",
            name=name,
            details={
                "status": corp.get("status"),
                "corporation_type": corp.get("corporation_type"),
            },
        )))

    scored.sort(key=lambda x: (-x[0], x[1].name))
    return [r for _, r in scored[:limit]]


# =========================
# Meta Ads Search
# =========================


def _score_meta_page_name(page_name: str, query: str) -> int:
    """Score how well a Meta page name matches the search query.

    The Meta Ad Library API's search_terms parameter searches ad *content*,
    not advertiser names. This function scores returned page names against
    the original query so we can rank/filter results by advertiser relevance.

    Returns:
        Score 0-100 (0 means no match, higher is better).
        Returns 0 for pages that don't match at all — these are filtered out.
    """
    if not page_name or not query:
        return 0

    name_lower = page_name.lower()
    query_lower = query.lower()
    query_parts = query_lower.split()

    # Exact match (case-insensitive)
    if name_lower == query_lower:
        return 100
    # Page name starts with the full query
    if name_lower.startswith(query_lower):
        return 95
    # Full query found as substring in page name
    if query_lower in name_lower:
        return 85
    # All query words appear in page name (in any order)
    if all(part in name_lower for part in query_parts):
        return 75
    # Concatenated query matches page name (e.g. "Canada Proud" -> "canadaproud")
    query_concat = query_lower.replace(" ", "")
    name_concat = name_lower.replace(" ", "")
    if query_concat in name_concat or name_concat in query_concat:
        return 70
    # Most query words appear (at least 2/3 of words)
    matching_parts = sum(1 for part in query_parts if part in name_lower)
    if len(query_parts) >= 2 and matching_parts >= max(2, len(query_parts) * 2 // 3):
        return 60
    # Page name starts with any query word longer than 3 chars
    # (catches "CanadaProud" when searching "Canada Proud")
    for part in query_parts:
        if len(part) > 3 and name_lower.startswith(part):
            return 50

    # No meaningful match — filter this page out
    return 0


async def _search_meta_pages(
    query: str,
    access_token: str,
    limit: int,
) -> list[CompanySearchResult]:
    """Search Facebook Pages by name using the Graph API.

    This is what the Facebook Ad Library website does internally —
    a page-name typeahead search — which returns pages matching the
    query by name. Much more accurate than searching ad content text.

    Uses GET /v24.0/pages/search?q=... which requires a user access
    token (from OAuth). Returns empty list if the endpoint is unavailable
    (e.g., missing permissions), allowing the caller to fall back.

    Args:
        query: Page name to search for
        access_token: Meta API access token
        limit: Maximum results to return

    Returns:
        List of CompanySearchResult, or empty list if endpoint unavailable
    """
    from .meta_ads import META_GRAPH_API_BASE

    page_search_url = f"{META_GRAPH_API_BASE}/pages/search"
    params = {
        "q": query,
        "fields": "id,name,category,fan_count,verification_status,link",
        "access_token": access_token,
        "limit": min(limit, 25),
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(page_search_url, params=params)

            if response.status_code != 200:
                # The /pages/search endpoint requires the "Page Public Content
                # Access" feature (or "Page Public Metadata Access") to be
                # approved via Meta App Review.  The pages_read_engagement
                # *permission* only covers pages the user manages — it does
                # NOT unlock public page search.
                #
                # To enable this:
                #   1. Go to https://developers.facebook.com/apps/<APP_ID>/review/
                #   2. Request the "Page Public Content Access" feature
                #   3. Complete Meta's App Review process
                #
                # Until then, the caller falls back to ads_archive search.
                try:
                    error_data = response.json() if response.content else {}
                    error_info = error_data.get("error", {})
                    error_code = error_info.get("code")
                    error_msg = error_info.get("message", "unknown error")
                    if error_code == 10:
                        # Permission / feature error — expected until App Review
                        logger.info(
                            "Meta page search requires 'Page Public Content Access' "
                            "feature via App Review. Falling back to ads_archive search. "
                            "Apply at: https://developers.facebook.com/docs/apps/review/feature#reference-PAGES_ACCESS"
                        )
                    else:
                        logger.debug(
                            f"Meta page search unavailable ({response.status_code}): "
                            f"{error_msg}"
                        )
                except Exception:
                    logger.debug(
                        f"Meta page search unavailable ({response.status_code})"
                    )
                return []

            data = response.json()
            pages = data.get("data", [])

    except Exception as e:
        logger.debug(f"Meta page search request failed: {e}")
        return []

    if not pages:
        return []

    results: list[CompanySearchResult] = []
    for page in pages:
        page_id = str(page.get("id", ""))
        name = page.get("name", "")
        if not page_id or not name:
            continue

        results.append(CompanySearchResult(
            source="meta_ads",
            identifier=page_id,
            identifier_type="meta_page_id",
            name=name,
            details={
                "page_id": page_id,
                "category": page.get("category"),
                "fan_count": page.get("fan_count"),
                "verification_status": page.get("verification_status"),
                "link": page.get("link"),
            },
        ))

    logger.info(
        f"Meta page search returned {len(results)} results for '{query}'"
    )
    return results[:limit]


async def _search_meta_ads_archive(
    query: str,
    access_token: str,
    limit: int,
) -> list[CompanySearchResult]:
    """Fallback: search via the ads_archive API and score by page name.

    The ads_archive API's search_terms parameter searches ad creative text
    **and** funding entity disclaimers. We fetch a large batch, deduplicate
    by page_id, and then score each page by how well its name matches the
    original query. Pages with no name relevance are filtered out.

    Args:
        query: Search query (sponsor/page name)
        access_token: Meta API access token
        limit: Maximum results to return

    Returns:
        List of CompanySearchResult scored and filtered by page-name relevance
    """
    from .meta_ads import META_ADS_ARCHIVE_ENDPOINT

    # Request page_id, page_name, and bylines (funding entity / "Paid for by").
    # We use a custom minimal set rather than MINIMAL_AD_FIELDS to include
    # bylines for better sponsor matching.
    search_fields = ["id", "page_id", "page_name", "bylines"]

    # Fetch a large batch — search_terms matches ad content text, so many
    # results will be from irrelevant pages. We need enough volume to
    # catch the actual matching sponsors after dedup + scoring.
    fetch_limit = max(limit * 10, 100)
    params = {
        "access_token": access_token,
        "ad_reached_countries": json.dumps(["CA", "US"]),
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_active_status": "ALL",
        "fields": ",".join(search_fields),
        "search_terms": query,
        "limit": fetch_limit,
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, read=30.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(META_ADS_ARCHIVE_ENDPOINT, params=params)

            if response.status_code != 200:
                try:
                    error_data = response.json() if response.content else {}
                    error_info = error_data.get("error", {})
                    logger.warning(
                        f"Meta Ads archive search error {response.status_code}: "
                        f"code={error_info.get('code')}, "
                        f"message={error_info.get('message', 'No message')}"
                    )
                except Exception:
                    logger.warning(
                        f"Meta Ads archive search error {response.status_code}: "
                        f"{response.text[:300] if response.text else 'No response body'}"
                    )
                return []

            data = response.json()
            ads = data.get("data", [])

    except Exception as e:
        logger.warning(f"Meta Ads archive search request failed: {e}")
        return []

    if not ads:
        return []

    # Deduplicate by page_id — multiple ads from the same sponsor.
    # Also capture bylines (funding entity / "Paid for by") for scoring.
    seen_page_ids: dict[str, dict[str, Any]] = {}
    for ad in ads:
        page_id = str(ad.get("page_id", ""))
        page_name = ad.get("page_name", "")
        if not page_id:
            continue
        if page_id not in seen_page_ids:
            seen_page_ids[page_id] = {
                "page_id": page_id,
                "page_name": page_name,
                "ad_id": str(ad.get("id", "")),
                "bylines": set(),
            }
        # Collect bylines across all ads from this page
        bylines_str = ad.get("bylines", "")
        if bylines_str:
            seen_page_ids[page_id]["bylines"].add(bylines_str)

    # Score each unique page by how well its name (or funding entity)
    # matches the query, then filter out irrelevant pages (score == 0).
    scored: list[tuple[int, CompanySearchResult]] = []
    for page_info in seen_page_ids.values():
        name = page_info["page_name"]
        # Score against page name first
        score = _score_meta_page_name(name, query)
        # Also try scoring against bylines (funding entity names)
        # — use whichever gives the higher score
        for byline in page_info["bylines"]:
            byline_score = _score_meta_page_name(byline, query)
            if byline_score > score:
                score = byline_score
        if score == 0:
            continue

        bylines_list = sorted(page_info["bylines"]) if page_info["bylines"] else None
        scored.append((score, CompanySearchResult(
            source="meta_ads",
            identifier=page_info["page_id"],
            identifier_type="meta_page_id",
            name=name,
            details={
                "page_id": page_info["page_id"],
                "sample_ad_id": page_info["ad_id"],
                "funding_entity": bylines_list[0] if bylines_list else None,
            },
        )))

    # Sort by score descending, then alphabetically by name
    scored.sort(key=lambda x: (-x[0], x[1].name))
    return [r for _, r in scored[:limit]]


async def search_meta_ads(query: str, limit: int = 10) -> list[CompanySearchResult]:
    """Search for Facebook Pages / Meta Ad sponsors by name.

    Uses a two-strategy approach matching how the Facebook Ad Library
    website itself works:

    1. **Primary — Page Search API**: Searches Facebook Pages by name via
       GET /pages/search. This is the equivalent of the website's GraphQL
       typeahead (useAdLibraryTypeaheadSuggestionDataSourceQuery). Returns
       pages whose *names* match the query. Requires a user access token
       with page search capabilities.

    2. **Fallback — ads_archive with scoring**: If the page search endpoint
       is unavailable (e.g., token lacks permissions), falls back to the
       ads_archive API's search_terms (which searches ad *content* text),
       deduplicates by page, and scores/filters by page-name relevance.

    Gracefully returns empty list if no Meta token is configured.

    Args:
        query: Search query (sponsor/page name)
        limit: Maximum results to return

    Returns:
        List of CompanySearchResult with source="meta_ads", sorted by relevance
    """
    # Obtain access token — gracefully skip if not configured
    try:
        from .meta_ads import MetaAdIngester

        ingester = MetaAdIngester()
        try:
            access_token = await ingester.get_access_token()
        finally:
            await ingester.close()
    except (ValueError, Exception) as e:
        logger.debug(f"Meta Ads search skipped (no token): {e}")
        return []

    # Strategy 1: Try Facebook Page Search API (searches by page name)
    results = await _search_meta_pages(query, access_token, limit)
    if results:
        return results

    # Strategy 2: Fall back to ads_archive search (searches ad content text)
    logger.debug(
        "Page search unavailable, falling back to ads_archive search"
    )
    return await _search_meta_ads_archive(query, access_token, limit)


# =========================
# Cache Management
# =========================


async def warmup_search_cache() -> None:
    """Pre-load all search indexes into memory.

    Call this on server startup to avoid slow first searches.
    Loads from disk cache if available, otherwise downloads.
    """
    logger.info("Warming up search cache...")
    sources = {
        "sec_edgar": _get_edgar_tickers,
        "irs990": lambda: _warmup_irs990(),
        "cra": _get_cra_charities,
        "canada_corps": _get_canada_corps,
    }
    results = await asyncio.gather(
        *[fn() for fn in sources.values()],
        return_exceptions=True,
    )
    for name, result in zip(sources.keys(), results):
        if isinstance(result, Exception):
            logger.warning(f"Warmup failed for {name}: {result}")
        else:
            logger.info(f"Warmup complete for {name}")


async def _warmup_irs990() -> None:
    """Warmup IRS 990 indexes for relevant years."""
    current_year = datetime.now().year
    for year in [current_year - 1, current_year - 2, current_year]:
        try:
            await _get_irs990_index(year)
        except Exception as e:
            logger.warning(f"Warmup failed for IRS 990 {year}: {e}")


# =========================
# Unified Search
# =========================


async def search_all_sources(
    query: str,
    sources: list[str] | None = None,
    limit: int = 10,
) -> CompanySearchResponse:
    """Search for companies across all data sources.

    Args:
        query: Search query (company name, ticker, etc.)
        sources: Optional list of sources to search (default: all)
        limit: Maximum results per source

    Returns:
        Aggregated search results from all sources
    """
    active_sources = sources or ["sec_edgar", "irs990", "cra", "canada_corps", "meta_ads"]

    search_fns = {
        "sec_edgar": search_sec_edgar,
        "irs990": search_irs990,
        "cra": search_cra,
        "canada_corps": search_canada_corps,
        "meta_ads": search_meta_ads,
    }

    tasks = {}
    for source_name in active_sources:
        if source_name in search_fns:
            tasks[source_name] = search_fns[source_name](query, limit)

    # Run all searches in parallel
    task_results = await asyncio.gather(
        *tasks.values(), return_exceptions=True
    )

    all_results: list[CompanySearchResult] = []
    sources_searched: list[str] = []
    sources_failed: list[str] = []

    for source_name, result in zip(tasks.keys(), task_results):
        if isinstance(result, Exception):
            logger.warning(f"Search failed for {source_name}: {result}")
            sources_failed.append(source_name)
        else:
            sources_searched.append(source_name)
            all_results.extend(result)

    return CompanySearchResponse(
        query=query,
        results=all_results,
        sources_searched=sources_searched,
        sources_failed=sources_failed,
    )
