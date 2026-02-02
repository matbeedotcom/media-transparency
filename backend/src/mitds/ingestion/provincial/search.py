"""Search-based provincial registry scrapers using Playwright.

This module provides browser automation to search provincial corporate registries
that do not offer bulk data downloads. Each province has its own search interface
that requires browser automation to query.

Requires: pip install playwright && playwright install chromium

Usage:
    from mitds.ingestion.provincial.search import (
        OntarioRegistrySearch,
        SaskatchewanRegistrySearch,
        run_targeted_search,
    )

    # Search for a specific company
    results = await run_targeted_search(
        province="ON",
        search_terms=["Postmedia", "Corus Entertainment"],
    )
"""

import asyncio
import logging
import random
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from .models import (
    Address,
    ProvincialCorporationRecord,
    ProvincialCorpStatus,
    ProvincialCorpType,
)

# Check for Playwright availability
try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Result from a registry search."""

    name: str
    registration_number: str
    status: str
    corp_type: str | None = None
    incorporation_date: date | None = None
    jurisdiction: str | None = None
    address: Address | None = None
    raw_data: dict | None = None


class BaseRegistrySearch(ABC):
    """Abstract base class for provincial registry search scrapers.

    Uses Playwright for browser automation to search registries that
    only provide search interfaces (no bulk data downloads).
    """

    # Browser settings
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    VIEWPORT = {"width": 1280, "height": 720}
    DEFAULT_TIMEOUT = 30000  # 30 seconds

    def __init__(self):
        """Initialize the registry search scraper."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    @property
    @abstractmethod
    def province(self) -> str:
        """Return the province code (e.g., 'ON', 'SK')."""
        ...

    @property
    @abstractmethod
    def registry_url(self) -> str:
        """Return the URL of the registry search page."""
        ...

    @property
    def province_name(self) -> str:
        """Return the full province name."""
        names = {
            "ON": "Ontario",
            "SK": "Saskatchewan",
            "MB": "Manitoba",
            "BC": "British Columbia",
            "NB": "New Brunswick",
            "PE": "Prince Edward Island",
            "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories",
            "YT": "Yukon",
            "NU": "Nunavut",
        }
        return names.get(self.province, self.province)

    @property
    def requires_account(self) -> bool:
        """Return True if this registry requires account creation to search.

        Override in subclasses for registries that require login.
        """
        return False

    async def _ensure_playwright(self) -> None:
        """Ensure Playwright is available."""
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                f"Playwright is required for {self.province_name} registry search. "
                "Install with: pip install playwright && playwright install chromium"
            )

    async def _init_browser(self, headless: bool = True) -> None:
        """Initialize browser with anti-detection settings."""
        await self._ensure_playwright()

        p = await async_playwright().start()

        # Try chromium first
        try:
            self._browser = await p.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                ],
            )
        except Exception as e:
            self.logger.warning(f"Chromium launch failed: {e}, trying Firefox")
            self._browser = await p.firefox.launch(headless=headless)

        self._context = await self._browser.new_context(
            user_agent=self.USER_AGENT,
            viewport=self.VIEWPORT,
            locale="en-CA",
            timezone_id="America/Toronto",
        )

        # Add anti-detection scripts
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-CA', 'en-US', 'en'] });
            if (!window.chrome) { window.chrome = { runtime: {} }; }
        """)

    async def _close_browser(self) -> None:
        """Close browser resources."""
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None

    async def _simulate_human_behavior(self, page: "Page") -> None:
        """Simulate human-like behavior to avoid bot detection."""
        # Random mouse movements
        for _ in range(random.randint(2, 4)):
            x = random.randint(100, 800)
            y = random.randint(100, 500)
            await page.mouse.move(x, y)
            await page.wait_for_timeout(random.randint(50, 150))

        # Small random scroll
        await page.evaluate(f"window.scrollBy(0, {random.randint(30, 100)})")
        await page.wait_for_timeout(random.randint(200, 500))

    @abstractmethod
    async def search(self, search_term: str) -> list[SearchResult]:
        """Search the registry for a company name.

        Args:
            search_term: Company name or partial name to search for

        Returns:
            List of SearchResult objects matching the search
        """
        ...

    @abstractmethod
    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a specific corporation.

        Args:
            registration_number: The corporation's registration number

        Returns:
            ProvincialCorporationRecord with full details, or None if not found
        """
        ...

    async def search_batch(
        self,
        search_terms: list[str],
        delay_between: float = 2.0,
        headless: bool = True,
    ) -> AsyncIterator[SearchResult]:
        """Search for multiple terms with delays to avoid rate limiting.

        Args:
            search_terms: List of company names to search
            delay_between: Seconds to wait between searches
            headless: Whether to run browser in headless mode

        Yields:
            SearchResult for each match found
        """
        try:
            await self._init_browser(headless=headless)

            for i, term in enumerate(search_terms):
                self.logger.info(f"Searching {self.province}: '{term}' ({i+1}/{len(search_terms)})")

                try:
                    results = await self.search(term)
                    for result in results:
                        yield result
                except Exception as e:
                    self.logger.error(f"Search failed for '{term}': {e}")

                # Add delay between searches (with jitter)
                if i < len(search_terms) - 1:
                    delay = delay_between + random.uniform(-0.5, 0.5)
                    await asyncio.sleep(max(0.5, delay))

        finally:
            await self._close_browser()

    def _to_corporation_record(
        self, result: SearchResult, source_url: str
    ) -> ProvincialCorporationRecord:
        """Convert SearchResult to ProvincialCorporationRecord."""
        return ProvincialCorporationRecord(
            name=result.name,
            name_french=None,
            registration_number=result.registration_number,
            business_number=None,
            corp_type_raw=result.corp_type or "Business Corporation",
            status_raw=result.status,
            incorporation_date=result.incorporation_date,
            jurisdiction=self.province,
            registered_address=result.address,
            source_url=source_url,
        )


# =============================================================================
# Ontario Registry Search
# =============================================================================


class OntarioRegistrySearch(BaseRegistrySearch):
    """Search the Ontario Business Registry (OBR).

    URL: https://www.ontario.ca/page/ontario-business-registry
    Direct Search: https://www.appmybizaccount.gov.on.ca/onbis/master/entry.pub

    The Ontario Business Registry provides FREE public search for basic information
    about businesses and not-for-profit corporations including:
    - Corporation name
    - Ontario Corporation Number (OCN)
    - Incorporation date
    - Type and status
    - Registered office address

    For detailed reports (directors, articles), fees apply ($8 profile, $3 documents).
    """

    @property
    def province(self) -> str:
        return "ON"

    @property
    def registry_url(self) -> str:
        return "https://www.ontario.ca/page/ontario-business-registry"

    @property
    def search_url(self) -> str:
        # Direct OBR public search interface
        return "https://www.appmybizaccount.gov.on.ca/onbis/master/entry.pub?applicationCode=onbis-master&businessService=registerItemSearch"

    # Known header/label text to filter out from results
    SKIP_TEXTS = frozenset([
        "business names", "business name", "name", "corporation name",
        "corporations", "registration date", "date", "status", "type",
        "ontario corporation number", "ocn", "address", "registered office",
        "search results", "results", "no results", "no matches",
        "sort by", "ascending", "descending", "most recent", "least recent",
        "most relevant", "filter", "page", "next", "previous", "first", "last",
        "page size", "business type", "showing", "of", "entries", "show",
        "per page", "items", "total", "active", "inactive", "dissolved",
        "cancelled", "revoked", "view", "details", "more",
        "registrant", "agent", "director", "officer", "jurisdiction",
    ])

    # Month names for date detection
    MONTH_NAMES = frozenset([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
        "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ])

    @staticmethod
    def _extract_ocn_from_name(text: str) -> tuple[str, str]:
        """Extract Ontario Corporation Number from name if embedded.

        Handles patterns like:
        - "POSTMEDIA NETWORK INC. (1000171044)"
        - "ABC CORP - 1234567"

        Returns:
            Tuple of (clean_name, ocn) where ocn may be empty
        """
        import re

        text = text.strip()

        # Pattern 1: Name with OCN in parentheses - "COMPANY NAME (1234567)"
        match = re.match(r'^(.+?)\s*\((\d{7,10})\)\s*$', text)
        if match:
            return match.group(1).strip(), match.group(2)

        # Pattern 2: Name with OCN after dash - "COMPANY NAME - 1234567"
        match = re.match(r'^(.+?)\s*[-–]\s*(\d{7,10})\s*$', text)
        if match:
            return match.group(1).strip(), match.group(2)

        # Pattern 3: Name with OCN after colon - "COMPANY NAME: 1234567"
        match = re.match(r'^(.+?):\s*(\d{7,10})\s*$', text)
        if match:
            return match.group(1).strip(), match.group(2)

        return text, ""

    def _is_date_string(self, text: str) -> bool:
        """Check if text appears to be a date rather than a company name."""
        import re

        text_lower = text.lower().strip()

        # Check for month names
        words = text_lower.split()
        for word in words:
            # Remove punctuation for comparison
            clean_word = word.rstrip(',.')
            if clean_word in self.MONTH_NAMES:
                return True

        # Check for date patterns: YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # 2024-01-15
            r'^\d{1,2}/\d{1,2}/\d{2,4}$',  # 1/15/2024
            r'^\d{1,2}-\d{1,2}-\d{2,4}$',  # 15-01-2024
            r'^[a-zA-Z]+\s+\d{1,2},?\s+\d{4}$',  # January 15, 2024
            r'^\d{1,2}\s+[a-zA-Z]+,?\s+\d{4}$',  # 15 January 2024
        ]

        for pattern in date_patterns:
            if re.match(pattern, text):
                return True

        return False

    def _is_header_or_label(self, text: str) -> bool:
        """Check if text appears to be a header, label, or UI element rather than a result."""
        import re

        text_lower = text.lower().strip()

        # Direct match with known headers
        if text_lower in self.SKIP_TEXTS:
            return True

        # Check for partial matches - if the text starts with known skip terms
        skip_prefixes = [
            "page ", "sort ", "show ", "filter ", "view ", "search ",
            "business type", "page size", "showing ", "items per",
            "displaying ", "results per", "go to ",
        ]
        for prefix in skip_prefixes:
            if text_lower.startswith(prefix):
                return True

        # Check for pagination text pattern: "Displaying X-Y of Z results"
        if re.match(r'displaying\s+\d+.*of\s+\d+', text_lower):
            return True

        # Check for pagination link patterns: "1- 2- 3 Next", "1- 2-...- 20 Next"
        if re.match(r'^[\d\-\.\s]+\s*(next|prev|last|first|»|«)', text_lower):
            return True
        if re.match(r'^[\d\-\.\s»«]+$', text) and '-' in text:
            return True

        # Check for column header patterns: "Business Name - Corporation", "Name - Type"
        if re.match(r'^[a-zA-Z\s]+-\s*[a-zA-Z\s]+$', text) and len(text) < 40:
            # But allow company names that actually have " - " in them
            parts = text.split(' - ')
            if len(parts) == 2:
                first, second = parts[0].strip().lower(), parts[1].strip().lower()
                # If both parts look like headers, skip
                header_words = {"name", "type", "status", "date", "number", "business", "corporation", "address"}
                if first in header_words or second in header_words:
                    return True

        # Check for UI dropdown/sort text patterns
        if "sort by" in text_lower or text_lower.startswith("sort"):
            return True

        # Text containing multiple sort options concatenated
        if re.search(r'(ascending|descending|most recent|least recent)', text_lower):
            return True

        # Check if it's a date string
        if self._is_date_string(text):
            return True

        # Very short text is likely a header/label
        if len(text_lower) < 3:
            return True

        # Text that is all numbers but too short to be an OCN
        if text.isdigit() and len(text) < 7:
            return True

        # Pure numeric text with slashes (likely a date or page number)
        if re.match(r'^[\d/\-\s]+$', text) and len(text) < 15:
            return True

        # Short text that doesn't contain typical company name patterns
        # Real company names usually have INC, LTD, CORP, CO, or are longer
        if len(text_lower) < 15:
            if not any(suffix in text_lower for suffix in ["inc", "ltd", "corp", "co", "llc", "lp", "llp"]):
                # Also check if it's just a few short words (likely UI element)
                words = text_lower.split()
                if len(words) <= 2 and all(len(w) < 8 for w in words):
                    return True

        return False

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search Ontario Business Registry for a company name.

        Uses the public OBR search at appmybizaccount.gov.on.ca
        """
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []
        seen_reg_nums: set[str] = set()  # Track seen registration numbers for deduplication

        try:
            # Navigate directly to OBR search
            self.logger.info(f"Navigating to OBR search: {self.search_url}")
            await page.goto(self.search_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # OBR search has specific input fields
            # Look for name search input
            search_selectors = [
                "input[name*='name']",
                "input[name*='Name']",
                "input[id*='name']",
                "input[id*='Name']",
                "input[id*='search']",
                "input[placeholder*='name']",
                "input[placeholder*='Name']",
                "input[type='text']",
            ]

            search_input = None
            for selector in search_selectors:
                try:
                    elems = await page.locator(selector).all()
                    for elem in elems:
                        if await elem.is_visible(timeout=1500):
                            # Check if it's not a number-only field
                            placeholder = await elem.get_attribute("placeholder") or ""
                            name_attr = await elem.get_attribute("name") or ""
                            if "number" not in placeholder.lower() and "number" not in name_attr.lower():
                                search_input = elem
                                break
                    if search_input:
                        break
                except Exception:
                    continue

            if not search_input:
                self.logger.warning("Could not find name search input on OBR")
                # Try any visible text input as fallback
                search_input = page.locator("input[type='text']").first
                try:
                    if not await search_input.is_visible(timeout=2000):
                        return results
                except Exception:
                    return results

            # Enter search term
            await search_input.click()
            await page.wait_for_timeout(200)
            await search_input.fill(search_term)
            await page.wait_for_timeout(random.randint(400, 800))

            # Submit search - OBR may use button or form submit
            submit_selectors = [
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Search')",
                "button:has-text('Find')",
                "a:has-text('Search')",
                "[onclick*='search']",
            ]

            submitted = False
            for selector in submit_selectors:
                try:
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=1500):
                        await btn.click()
                        submitted = True
                        self.logger.info(f"Clicked submit button: {selector}")
                        break
                except Exception:
                    continue

            if not submitted:
                await page.keyboard.press("Enter")
                self.logger.info("Submitted via Enter key")

            # Wait for results to load
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_timeout(2000)

            # Parse results - OBR shows results in various formats
            # Try table first (most common)
            table = page.locator("table")
            if await table.count() > 0:
                rows = await page.locator("table tbody tr, table tr:not(:first-child)").all()
                self.logger.info(f"Found {len(rows)} table rows")

                for row in rows[:20]:
                    try:
                        cells = await row.locator("td").all()
                        if len(cells) >= 2:
                            # Try to extract data from cells
                            name_text = await cells[0].inner_text()
                            name = name_text.strip()

                            # Skip header-like rows
                            if not name or self._is_header_or_label(name):
                                continue

                            # Check if OCN is embedded in the name
                            name, embedded_ocn = self._extract_ocn_from_name(name)

                            reg_num = embedded_ocn
                            status = "Active"
                            corp_type = None

                            # Check other cells for registration number, status, type
                            for i, cell in enumerate(cells[1:], 1):
                                cell_text = (await cell.inner_text()).strip()
                                if not cell_text:
                                    continue

                                # Skip header/label text in cells
                                if self._is_header_or_label(cell_text):
                                    continue

                                # Ontario Corp Numbers are typically 7-10 digits
                                if cell_text.isdigit() and 7 <= len(cell_text) <= 10:
                                    if not reg_num:  # Prefer cell value over embedded
                                        reg_num = cell_text
                                elif any(kw in cell_text.lower() for kw in ["active", "inactive", "dissolved", "cancelled", "revoked"]):
                                    status = cell_text
                                elif any(kw in cell_text.lower() for kw in ["corporation", "inc", "ltd", "nonprofit", "co-op"]):
                                    corp_type = cell_text

                            # Skip if no valid name or search term not in name
                            if not name or len(name) < 3:
                                continue

                            # Generate fallback registration number if none found
                            if not reg_num:
                                reg_num = f"ON-{hash(name) & 0xFFFFFFFF:08X}"

                            # Deduplicate by registration number
                            if reg_num in seen_reg_nums:
                                continue
                            seen_reg_nums.add(reg_num)

                            results.append(SearchResult(
                                name=name,
                                registration_number=reg_num,
                                status=status,
                                corp_type=corp_type,
                                jurisdiction="ON",
                            ))
                    except Exception as e:
                        self.logger.debug(f"Failed to parse table row: {e}")
                        continue

            # If no table results, try other result formats
            if not results:
                result_selectors = [
                    ".search-result",
                    ".result-item",
                    "[class*='result']",
                    ".entity-item",
                    "li a",
                ]

                for selector in result_selectors:
                    items = await page.locator(selector).all()
                    if items:
                        for item in items[:20]:
                            try:
                                text = await item.inner_text()
                                if search_term.lower() in text.lower():
                                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                                    # Filter out header/label lines
                                    lines = [l for l in lines if not self._is_header_or_label(l)]

                                    if lines:
                                        raw_name = lines[0]
                                        name, embedded_ocn = self._extract_ocn_from_name(raw_name)
                                        reg_num = embedded_ocn
                                        status = "Active"

                                        for line in lines[1:]:
                                            if line.isdigit() and 7 <= len(line) <= 10:
                                                if not reg_num:
                                                    reg_num = line
                                            elif any(kw in line.lower() for kw in ["active", "inactive", "dissolved"]):
                                                status = line

                                        if not reg_num:
                                            reg_num = f"ON-{hash(name) & 0xFFFFFFFF:08X}"

                                        # Deduplicate
                                        if reg_num in seen_reg_nums:
                                            continue
                                        seen_reg_nums.add(reg_num)

                                        if name and name != search_term and len(name) >= 3:
                                            results.append(SearchResult(
                                                name=name,
                                                registration_number=reg_num,
                                                status=status,
                                                jurisdiction="ON",
                                            ))
                            except Exception:
                                continue
                        if results:
                            break

            self.logger.info(f"Found {len(results)} results for '{search_term}'")

        except Exception as e:
            self.logger.error(f"Ontario search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for an Ontario corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# Saskatchewan Registry Search
# =============================================================================


class SaskatchewanRegistrySearch(BaseRegistrySearch):
    """Search the Saskatchewan Corporate Registry (ISC).

    URL: https://corporateregistry.isc.ca/

    LIMITATION: ISC requires account creation to search. No public search available.
    This scraper cannot function without ISC credentials.

    Alternative: Use the cross-reference service with entities found through
    other sources (SEC, SEDAR, Elections Canada, etc.)
    """

    @property
    def province(self) -> str:
        return "SK"

    @property
    def registry_url(self) -> str:
        return "https://corporateregistry.isc.ca/"

    @property
    def requires_account(self) -> bool:
        return True

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search Saskatchewan Corporate Registry.

        NOTE: ISC requires account creation - no public search available.
        This method will raise an error.
        """
        raise NotImplementedError(
            "Saskatchewan Corporate Registry (ISC) requires an account to search. "
            "No public search interface is available.\n\n"
            "Alternatives:\n"
            "  - Create an ISC account at https://corporateregistry.isc.ca/\n"
            "  - Use cross-reference service with entities from other sources\n"
            "  - Manual lookup at https://corporateregistry.isc.ca/"
        )

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a Saskatchewan corporation."""
        raise NotImplementedError(
            "Saskatchewan Corporate Registry requires an account. See search() for details."
        )


# =============================================================================
# Manitoba Registry Search
# =============================================================================


class ManitobaRegistrySearch(BaseRegistrySearch):
    """Search the Manitoba Companies Office.

    URL: https://companiesoffice.gov.mb.ca/
    """

    @property
    def province(self) -> str:
        return "MB"

    @property
    def registry_url(self) -> str:
        return "https://companiesoffice.gov.mb.ca/"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search Manitoba Companies Office."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            await page.goto(self.registry_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Look for search functionality
            search_link = page.locator("a:has-text('Search'), a:has-text('Find'), a[href*='search']").first
            try:
                if await search_link.is_visible(timeout=3000):
                    await search_link.click()
                    await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                pass

            # Find search input
            search_input = page.locator("input[type='text'], input[name*='name'], input[id*='company']").first
            await search_input.fill(search_term)
            await page.wait_for_timeout(random.randint(300, 600))

            # Submit search
            submit_btn = page.locator("button[type='submit'], input[type='submit']").first
            try:
                await submit_btn.click()
            except Exception:
                await page.keyboard.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_timeout(1500)

            # Parse results
            result_elements = await page.locator("table tbody tr, .result, .company-result").all()

            for elem in result_elements[:20]:
                try:
                    text = await elem.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    if lines:
                        results.append(SearchResult(
                            name=lines[0],
                            registration_number=lines[1] if len(lines) > 1 else "",
                            status="Active",
                            jurisdiction="MB",
                        ))
                except Exception:
                    continue

        except Exception as e:
            self.logger.error(f"Manitoba search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a Manitoba corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# British Columbia Registry Search
# =============================================================================


class BCRegistrySearch(BaseRegistrySearch):
    """Search the BC Registry (BC OnLine).

    URL: https://www.bcregistry.gov.bc.ca/

    Note: Full access requires paid BC OnLine subscription. This scraper
    provides limited public search functionality.
    """

    @property
    def province(self) -> str:
        return "BC"

    @property
    def registry_url(self) -> str:
        return "https://www.bcregistry.gov.bc.ca/"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search BC Registry for a company name."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            # BC has a public name search at a different URL
            search_url = "https://www.bcregistry.gov.bc.ca/corporateonline/colin/search/name"
            await page.goto(search_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Fill search
            search_input = page.locator("input[type='text'], input[name*='name']").first
            await search_input.fill(search_term)
            await page.wait_for_timeout(random.randint(300, 600))

            # Submit
            submit_btn = page.locator("button[type='submit'], input[type='submit'], button:has-text('Search')").first
            try:
                await submit_btn.click()
            except Exception:
                await page.keyboard.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_timeout(1500)

            # Parse results - BC shows results in various formats
            result_elements = await page.locator("table tbody tr, .search-result, [data-cy='search-result']").all()

            for elem in result_elements[:20]:
                try:
                    text = await elem.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    if lines:
                        name = lines[0]
                        reg_num = ""
                        status = "Active"
                        corp_type = None

                        for line in lines[1:]:
                            # BC numbers often start with BC or are 7-8 digits
                            if line.startswith("BC") or (line.isdigit() and len(line) >= 7):
                                reg_num = line
                            elif any(kw in line.lower() for kw in ["active", "inactive", "dissolved"]):
                                status = line
                            elif any(kw in line.lower() for kw in ["corporation", "society", "cooperative"]):
                                corp_type = line

                        results.append(SearchResult(
                            name=name,
                            registration_number=reg_num,
                            status=status,
                            corp_type=corp_type,
                            jurisdiction="BC",
                        ))
                except Exception:
                    continue

        except Exception as e:
            self.logger.error(f"BC search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a BC corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# New Brunswick Registry Search
# =============================================================================


class NewBrunswickRegistrySearch(BaseRegistrySearch):
    """Search the New Brunswick Corporate Registry (SNB).

    URL: https://www.pxw2.snb.ca/card_online/cardsearch.aspx

    LIMITATION: The SNB Corporate Registry uses reCAPTCHA protection which
    blocks automated search. This scraper cannot function without solving
    the captcha challenge.

    Manual search available at: https://www.pxw2.snb.ca/card_online/cardsearch.aspx
    """

    @property
    def province(self) -> str:
        return "NB"

    @property
    def registry_url(self) -> str:
        return "https://www.pxw2.snb.ca/card_online/cardsearch.aspx"

    @property
    def requires_account(self) -> bool:
        return False  # No account required, but has captcha

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search New Brunswick Corporate Registry.

        NOTE: SNB uses reCAPTCHA protection - automated search is blocked.
        This method will raise an error.
        """
        raise NotImplementedError(
            "New Brunswick Corporate Registry uses reCAPTCHA protection. "
            "Automated search is not possible.\n\n"
            "Alternatives:\n"
            "  - Manual search at https://www.pxw2.snb.ca/card_online/cardsearch.aspx\n"
            "  - Use cross-reference service with entities from other sources"
        )

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a New Brunswick corporation."""
        raise NotImplementedError(
            "New Brunswick registry has reCAPTCHA protection. See search() for details."
        )


# =============================================================================
# Prince Edward Island Registry Search
# =============================================================================


class PEIRegistrySearch(BaseRegistrySearch):
    """Search the Prince Edward Island Corporate Registry.

    URL: https://www.princeedwardisland.ca/en/feature/corporate-registry

    LIMITATION: The PEI website uses Radware bot protection which blocks
    automated access. This scraper cannot function without bypassing the
    protection system.

    Manual search available at: https://www.princeedwardisland.ca/en/feature/corporate-registry
    """

    @property
    def province(self) -> str:
        return "PE"

    @property
    def registry_url(self) -> str:
        return "https://www.princeedwardisland.ca/en/feature/corporate-registry"

    @property
    def requires_account(self) -> bool:
        return False  # No account required, but has bot protection

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search PEI Corporate Registry.

        NOTE: PEI uses Radware bot protection - automated search is blocked.
        This method will raise an error.
        """
        raise NotImplementedError(
            "Prince Edward Island Corporate Registry uses Radware bot protection. "
            "Automated search is not possible.\n\n"
            "Alternatives:\n"
            "  - Manual search at https://www.princeedwardisland.ca/en/feature/corporate-registry\n"
            "  - Use cross-reference service with entities from other sources"
        )

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a PEI corporation."""
        raise NotImplementedError(
            "PEI registry has Radware bot protection. See search() for details."
        )


# =============================================================================
# Newfoundland and Labrador Registry Search
# =============================================================================


class NewfoundlandRegistrySearch(BaseRegistrySearch):
    """Search the Newfoundland and Labrador Registry of Companies (CADO).

    URL: https://cado.eservices.gov.nl.ca/
    Direct Search: https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx

    CADO (Companies and Deeds Online) provides free public search for:
    - Company name and number
    - Status (Active, Dissolved, Cancelled, etc.)
    - Corporation type
    - Registration date
    """

    @property
    def province(self) -> str:
        return "NL"

    @property
    def registry_url(self) -> str:
        return "https://cado.eservices.gov.nl.ca/"

    @property
    def search_url(self) -> str:
        return "https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search NL Registry of Companies via CADO."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            # Go directly to the name/number search page
            self.logger.info(f"Navigating to CADO search: {self.search_url}")
            await page.goto(self.search_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Fill the first keyword field
            search_input = page.locator("#txtNameKeywords1")
            await search_input.fill(search_term)
            await page.wait_for_timeout(random.randint(300, 600))

            # Click the image search button
            await page.locator('input[name="btnSearch"]').click()
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_timeout(2000)

            # Parse results - CADO returns tab-separated table data
            # Columns: Name | Status | Number | Corporation Type | Date
            body_text = await page.locator("body").inner_text()
            lines = body_text.split("\n")

            seen_numbers: set[str] = set()
            skip_headers = {"name", "status", "number", "corporation type", "date", "name/number search"}

            for line in lines:
                line = line.strip()
                if not line or len(line) < 5:
                    continue

                # Skip header lines
                if line.lower() in skip_headers:
                    continue

                # Split by tabs (CADO uses tab-separated values)
                parts = [p.strip() for p in line.split("\t") if p.strip()]

                # Also try splitting by multiple spaces for fallback
                if len(parts) < 2:
                    parts = [p.strip() for p in line.split("  ") if p.strip() and len(p.strip()) > 1]

                if len(parts) >= 3:
                    name = parts[0]
                    status = parts[1] if len(parts) > 1 else "Unknown"
                    reg_num = parts[2] if len(parts) > 2 else ""
                    corp_type = parts[3] if len(parts) > 3 else None

                    # Skip if this looks like a header row
                    if name.lower() in skip_headers or status.lower() in skip_headers:
                        continue

                    # Skip if number already seen (deduplication)
                    if reg_num and reg_num in seen_numbers:
                        continue
                    if reg_num:
                        seen_numbers.add(reg_num)

                    # Validate: name should contain search term or look like a company name
                    if search_term.lower() in name.lower() or any(kw in name.upper() for kw in ["LTD", "INC", "CORP", "LIMITED", "COMPANY"]):
                        results.append(SearchResult(
                            name=name,
                            registration_number=reg_num,
                            status=status,
                            corp_type=corp_type,
                            jurisdiction="NL",
                        ))

            self.logger.info(f"Found {len(results)} results for '{search_term}'")

        except Exception as e:
            self.logger.error(f"Newfoundland search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a NL corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# Northwest Territories Registry Search
# =============================================================================


class NWTRegistrySearch(BaseRegistrySearch):
    """Search the Northwest Territories MACA Registry.

    URL: https://www.maca.gov.nt.ca/
    """

    @property
    def province(self) -> str:
        return "NT"

    @property
    def registry_url(self) -> str:
        return "https://www.maca.gov.nt.ca/"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search NWT MACA Registry."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            # NWT has corporate registration info under MACA
            search_url = self.registry_url + "en/services/corporate-registries"
            await page.goto(search_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Look for online search link
            search_link = page.locator("a:has-text('search'), a:has-text('registry'), a[href*='search']").first
            try:
                if await search_link.is_visible(timeout=3000):
                    await search_link.click()
                    await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                pass

            search_input = page.locator("input[type='text'], input[name*='search']").first
            try:
                await search_input.fill(search_term)
                await page.wait_for_timeout(random.randint(300, 600))
                await page.keyboard.press("Enter")
                await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                # NWT may not have online search - return empty
                self.logger.info("NWT registry may not have online search functionality")
                return results

            # Parse any results
            result_elements = await page.locator("table tbody tr, .result").all()

            for elem in result_elements[:20]:
                try:
                    text = await elem.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    if lines:
                        results.append(SearchResult(
                            name=lines[0],
                            registration_number=lines[1] if len(lines) > 1 else "",
                            status="Active",
                            jurisdiction="NT",
                        ))
                except Exception:
                    continue

        except Exception as e:
            self.logger.error(f"NWT search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a NWT corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# Yukon Registry Search
# =============================================================================


class YukonRegistrySearch(BaseRegistrySearch):
    """Search the Yukon Corporate Affairs registry.

    URL: https://corporateonline.gov.yk.ca/
    """

    @property
    def province(self) -> str:
        return "YT"

    @property
    def registry_url(self) -> str:
        return "https://corporateonline.gov.yk.ca/"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search Yukon Corporate Online."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            await page.goto(self.registry_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Look for public search
            search_link = page.locator("a:has-text('Search'), a:has-text('Public'), a[href*='search']").first
            try:
                if await search_link.is_visible(timeout=3000):
                    await search_link.click()
                    await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                pass

            search_input = page.locator("input[type='text'], input[name*='name']").first
            await search_input.fill(search_term)
            await page.wait_for_timeout(random.randint(300, 600))

            # Submit
            submit_btn = page.locator("button[type='submit'], input[type='submit']").first
            try:
                await submit_btn.click()
            except Exception:
                await page.keyboard.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_timeout(1500)

            # Parse results
            result_elements = await page.locator("table tbody tr, .result, .search-result").all()

            for elem in result_elements[:20]:
                try:
                    text = await elem.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    if lines:
                        results.append(SearchResult(
                            name=lines[0],
                            registration_number=lines[1] if len(lines) > 1 else "",
                            status="Active",
                            jurisdiction="YT",
                        ))
                except Exception:
                    continue

        except Exception as e:
            self.logger.error(f"Yukon search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a Yukon corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# Nunavut Registry Search
# =============================================================================


class NunavutRegistrySearch(BaseRegistrySearch):
    """Search the Nunavut Legal Registries.

    URL: https://www.nunavutlegalregistries.ca/
    """

    @property
    def province(self) -> str:
        return "NU"

    @property
    def registry_url(self) -> str:
        return "https://www.nunavutlegalregistries.ca/"

    async def search(self, search_term: str) -> list[SearchResult]:
        """Search Nunavut Legal Registries."""
        if not self._context:
            await self._init_browser()

        page = await self._context.new_page()
        results = []

        try:
            await page.goto(self.registry_url, timeout=self.DEFAULT_TIMEOUT)
            await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            await self._simulate_human_behavior(page)

            # Look for corporate search
            corp_link = page.locator("a:has-text('Corporate'), a:has-text('Business'), a[href*='corporate']").first
            try:
                if await corp_link.is_visible(timeout=3000):
                    await corp_link.click()
                    await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                pass

            search_input = page.locator("input[type='text'], input[name*='search']").first
            try:
                await search_input.fill(search_term)
                await page.wait_for_timeout(random.randint(300, 600))
                await page.keyboard.press("Enter")
                await page.wait_for_load_state("networkidle", timeout=self.DEFAULT_TIMEOUT)
            except Exception:
                self.logger.info("Nunavut registry may not have online search functionality")
                return results

            # Parse results
            result_elements = await page.locator("table tbody tr, .result").all()

            for elem in result_elements[:20]:
                try:
                    text = await elem.inner_text()
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    if lines:
                        results.append(SearchResult(
                            name=lines[0],
                            registration_number=lines[1] if len(lines) > 1 else "",
                            status="Active",
                            jurisdiction="NU",
                        ))
                except Exception:
                    continue

        except Exception as e:
            self.logger.error(f"Nunavut search failed: {e}")
        finally:
            await page.close()

        return results

    async def get_details(self, registration_number: str) -> ProvincialCorporationRecord | None:
        """Get detailed information for a Nunavut corporation."""
        results = await self.search(registration_number)
        if results:
            return self._to_corporation_record(results[0], self.registry_url)
        return None


# =============================================================================
# Registry Factory and Utilities
# =============================================================================


# Map of province codes to search classes
SEARCH_REGISTRY_CLASSES: dict[str, type[BaseRegistrySearch]] = {
    "ON": OntarioRegistrySearch,
    "SK": SaskatchewanRegistrySearch,
    "MB": ManitobaRegistrySearch,
    "BC": BCRegistrySearch,
    "NB": NewBrunswickRegistrySearch,
    "PE": PEIRegistrySearch,
    "PEI": PEIRegistrySearch,  # Alias
    "NL": NewfoundlandRegistrySearch,
    "NT": NWTRegistrySearch,
    "YT": YukonRegistrySearch,
    "NU": NunavutRegistrySearch,
}


def get_registry_access_info() -> dict[str, dict[str, Any]]:
    """Get information about registry access for each province.

    Returns:
        Dictionary with province code -> access info
    """
    return {
        "ON": {
            "name": "Ontario",
            "url": "https://www.ontario.ca/page/ontario-business-registry",
            "requires_account": False,
            "public_search": True,
            "notes": "Limited public search via Ontario.ca",
        },
        "SK": {
            "name": "Saskatchewan",
            "url": "https://corporateregistry.isc.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "ISC requires account creation - no public search",
        },
        "MB": {
            "name": "Manitoba",
            "url": "https://companiesoffice.gov.mb.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "Companies Office requires registration",
        },
        "BC": {
            "name": "British Columbia",
            "url": "https://www.bcregistry.gov.bc.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "BC OnLine requires paid subscription ($100+)",
        },
        "NB": {
            "name": "New Brunswick",
            "url": "https://www.pxw2.snb.ca/card_online/cardsearch.aspx",
            "requires_account": False,
            "public_search": False,
            "has_captcha": True,
            "notes": "Service NB has reCAPTCHA protection - automated search blocked",
        },
        "PE": {
            "name": "Prince Edward Island",
            "url": "https://www.princeedwardisland.ca/en/feature/corporate-registry",
            "requires_account": False,
            "public_search": False,
            "has_bot_protection": True,
            "notes": "PEI has Radware bot protection - automated search blocked",
        },
        "NL": {
            "name": "Newfoundland and Labrador",
            "url": "https://cado.eservices.gov.nl.ca/",
            "search_url": "https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx",
            "requires_account": False,
            "public_search": True,
            "notes": "CADO has public company name/number search",
        },
        "NT": {
            "name": "Northwest Territories",
            "url": "https://www.maca.gov.nt.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "MACA has limited online access",
        },
        "YT": {
            "name": "Yukon",
            "url": "https://corporateonline.gov.yk.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "Corporate Online requires account",
        },
        "NU": {
            "name": "Nunavut",
            "url": "https://www.nunavutlegalregistries.ca/",
            "requires_account": True,
            "public_search": False,
            "notes": "Legal Registries has limited online access",
        },
    }


def get_public_search_provinces() -> list[str]:
    """Get provinces with public search (no account required)."""
    info = get_registry_access_info()
    return [code for code, data in info.items() if data.get("public_search")]


def get_account_required_provinces() -> list[str]:
    """Get provinces that require account creation."""
    info = get_registry_access_info()
    return [code for code, data in info.items() if data.get("requires_account")]


def get_registry_search(province: str) -> BaseRegistrySearch:
    """Get the appropriate registry search class for a province.

    Args:
        province: Province code (e.g., 'ON', 'SK')

    Returns:
        Instance of the registry search class

    Raises:
        ValueError: If province code is not supported
    """
    province_upper = province.upper()

    if province_upper not in SEARCH_REGISTRY_CLASSES:
        available = ", ".join(sorted(SEARCH_REGISTRY_CLASSES.keys()))
        raise ValueError(
            f"No search scraper available for province '{province}'. "
            f"Available: {available}"
        )

    return SEARCH_REGISTRY_CLASSES[province_upper]()


async def run_targeted_search(
    province: str,
    search_terms: list[str],
    headless: bool = True,
    delay_between: float = 2.0,
    save_to_db: bool = False,
) -> dict[str, Any]:
    """Run targeted search for corporations in a province.

    Args:
        province: Province code (e.g., 'ON', 'SK')
        search_terms: List of company names to search for
        headless: Whether to run browser in headless mode
        delay_between: Seconds to wait between searches
        save_to_db: Whether to save results to database

    Returns:
        Dictionary with search results and statistics
    """
    from ...db import get_db_session, get_neo4j_session
    from ..base import Neo4jHelper, PostgresHelper
    import json

    if not PLAYWRIGHT_AVAILABLE:
        raise ImportError(
            "Playwright is required for targeted search. "
            "Install with: pip install playwright && playwright install chromium"
        )

    searcher = get_registry_search(province)

    results = []
    errors = []

    print(f"Searching {searcher.province_name} registry for {len(search_terms)} terms...", file=sys.stderr)

    async for result in searcher.search_batch(search_terms, delay_between, headless):
        results.append(result)
        print(f"  Found: {result.name} ({result.registration_number})", file=sys.stderr)

    # Save to database if requested
    records_created = 0
    records_updated = 0

    if save_to_db and results:
        neo4j_helper = Neo4jHelper(logger)

        for result in results:
            record = searcher._to_corporation_record(result, searcher.registry_url)

            try:
                async with get_db_session() as db:
                    # Check if exists
                    existing = await db.execute(
                        text("SELECT id FROM entities WHERE provincial_registry_id = :reg_id"),
                        {"reg_id": record.provincial_registry_id},
                    )
                    row = existing.fetchone()

                    now = datetime.utcnow()
                    metadata = {
                        "provincial_corp_type": record.corp_type_parsed.value,
                        "provincial_status": record.status_parsed.value,
                        "source_url": record.source_url,
                        "search_source": "playwright_scraper",
                        "last_synced": now.isoformat(),
                    }

                    if row:
                        await db.execute(
                            text("""
                                UPDATE entities SET
                                    metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS jsonb),
                                    updated_at = :updated_at
                                WHERE id = :id
                            """),
                            {"id": row.id, "metadata": json.dumps(metadata), "updated_at": now},
                        )
                        records_updated += 1
                        entity_id = row.id
                    else:
                        entity_id = uuid4()
                        await db.execute(
                            text("""
                                INSERT INTO entities (
                                    id, name, entity_type, provincial_registry_id,
                                    external_ids, metadata, created_at, updated_at
                                ) VALUES (
                                    :id, :name, 'organization', :reg_id,
                                    CAST(:external_ids AS jsonb), CAST(:metadata AS jsonb),
                                    :created_at, :updated_at
                                )
                            """),
                            {
                                "id": entity_id,
                                "name": record.name,
                                "reg_id": record.provincial_registry_id,
                                "external_ids": json.dumps({
                                    "provincial_registry": province,
                                    f"{province.lower()}_corp_number": record.registration_number,
                                }),
                                "metadata": json.dumps(metadata),
                                "created_at": now,
                                "updated_at": now,
                            },
                        )
                        records_created += 1

                # Sync to Neo4j
                try:
                    async with get_neo4j_session() as session:
                        await neo4j_helper.merge_organization(
                            session,
                            id=str(entity_id),
                            name=record.name,
                            org_type=record.corp_type_parsed.value,
                            external_ids={"provincial_registry_id": record.provincial_registry_id},
                            properties={
                                "jurisdiction": f"CA-{province}",
                                "provincial_corp_type": record.corp_type_parsed.value,
                                "provincial_status": record.status_parsed.value,
                            },
                        )
                except Exception as e:
                    logger.warning(f"Neo4j sync failed for {record.name}: {e}")

            except Exception as e:
                errors.append(f"{record.name}: {str(e)}")

    return {
        "province": province,
        "province_name": searcher.province_name,
        "search_terms": search_terms,
        "results_found": len(results),
        "results": [
            {
                "name": r.name,
                "registration_number": r.registration_number,
                "status": r.status,
                "corp_type": r.corp_type,
            }
            for r in results
        ],
        "records_created": records_created,
        "records_updated": records_updated,
        "errors": errors,
    }
