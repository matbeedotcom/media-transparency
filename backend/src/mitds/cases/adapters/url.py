"""URL entry point adapter.

Handles URLs as entry points for case creation, fetching page content
and extracting entities.
"""

import json
import logging
from datetime import datetime
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx

from ...storage import store_evidence_content
from ..extraction.pipeline import ExtractionPipeline, get_extraction_pipeline
from ..models import (
    EntryPointType,
    Evidence,
    EvidenceType,
    ExtractedLead,
)
from .base import BaseEntryPointAdapter, SeedEntity, ValidationResult

logger = logging.getLogger(__name__)


class URLAdapter(BaseEntryPointAdapter):
    """Adapter for URL/webpage entry points.

    Fetches the page content, extracts the main content using trafilatura,
    and runs entity extraction to create leads.
    """

    # Rate limit: 1 request per second per domain
    REQUEST_DELAY_SECONDS = 1.0

    # Request timeout
    TIMEOUT_SECONDS = 30

    # Maximum content size (10MB)
    MAX_CONTENT_SIZE = 10 * 1024 * 1024

    def __init__(self, enable_llm: bool = False):
        """Initialize the adapter.

        Args:
            enable_llm: Whether to enable LLM extraction
        """
        self.enable_llm = enable_llm
        self._extraction_pipeline: ExtractionPipeline | None = None

    @property
    def entry_point_type(self) -> str:
        return EntryPointType.URL.value

    @property
    def extraction_pipeline(self) -> ExtractionPipeline:
        """Get the extraction pipeline."""
        if self._extraction_pipeline is None:
            self._extraction_pipeline = get_extraction_pipeline(
                enable_llm=self.enable_llm,
            )
        return self._extraction_pipeline

    async def validate(self, input_value: str) -> ValidationResult:
        """Validate a URL.

        Performs quick validation:
        - Non-empty value
        - Valid URL format
        - HTTP or HTTPS scheme
        """
        if not input_value or not input_value.strip():
            return ValidationResult(
                is_valid=False,
                error_message="URL is required",
            )

        url = input_value.strip()

        # Parse URL
        try:
            parsed = urlparse(url)
        except Exception:
            return ValidationResult(
                is_valid=False,
                error_message="Invalid URL format",
            )

        # Check scheme
        if parsed.scheme not in ("http", "https"):
            return ValidationResult(
                is_valid=False,
                error_message="URL must use HTTP or HTTPS",
            )

        # Check for hostname
        if not parsed.netloc:
            return ValidationResult(
                is_valid=False,
                error_message="URL must have a hostname",
            )

        return ValidationResult(
            is_valid=True,
            normalized_value=url,
            metadata={
                "domain": parsed.netloc,
                "path": parsed.path,
            },
        )

    async def create_evidence(
        self,
        case_id: UUID,
        input_value: str,
        validation_result: ValidationResult,
    ) -> Evidence:
        """Create evidence by fetching the URL content.

        Fetches the page, extracts main content, and stores both.
        """
        evidence_id = uuid4()
        now = datetime.utcnow()
        url = validation_result.normalized_value

        # Fetch the page
        raw_html = ""
        extracted_text = ""
        fetch_error = None

        try:
            async with httpx.AsyncClient(
                timeout=self.TIMEOUT_SECONDS,
                follow_redirects=True,
            ) as client:
                response = await client.get(url)
                response.raise_for_status()

                # Check content size
                if len(response.content) > self.MAX_CONTENT_SIZE:
                    raise ValueError(f"Content too large: {len(response.content)} bytes")

                raw_html = response.text

                # Extract main content using trafilatura
                extracted_text = await self._extract_content(raw_html, url)

        except httpx.HTTPStatusError as e:
            fetch_error = f"HTTP {e.response.status_code}"
            logger.warning(f"URL fetch failed: {fetch_error}")
        except httpx.RequestError as e:
            fetch_error = f"Request error: {str(e)}"
            logger.warning(f"URL fetch failed: {fetch_error}")
        except Exception as e:
            fetch_error = str(e)
            logger.warning(f"URL fetch failed: {fetch_error}")

        # Create content object
        content_data = {
            "url": url,
            "retrieved_at": now.isoformat(),
            "domain": validation_result.metadata.get("domain"),
            "error": fetch_error,
            "raw_html_length": len(raw_html),
            "extracted_text": extracted_text,
            "extracted_text_length": len(extracted_text),
        }

        # Store in S3
        content = json.dumps(content_data, indent=2).encode("utf-8")
        content_ref, content_hash = await store_evidence_content(
            case_id=str(case_id),
            evidence_id=str(evidence_id),
            content=content,
            content_type="application/json",
            filename="url_content",
            extension="json",
            metadata={"url": url},
        )

        # Also store raw HTML if available
        if raw_html:
            await store_evidence_content(
                case_id=str(case_id),
                evidence_id=str(evidence_id),
                content=raw_html.encode("utf-8"),
                content_type="text/html",
                filename="raw",
                extension="html",
            )

        return Evidence(
            id=evidence_id,
            case_id=case_id,
            evidence_type=EvidenceType.URL_FETCH,
            source_url=url,
            content_ref=content_ref,
            content_hash=content_hash,
            content_type="application/json",
            extractor="url_adapter",
            extractor_version="1.0.0",
            extraction_result={
                "url": url,
                "domain": validation_result.metadata.get("domain"),
                "has_content": bool(extracted_text),
                "error": fetch_error,
            },
            retrieved_at=now,
            created_at=now,
        )

    async def _extract_content(self, html: str, url: str) -> str:
        """Extract main content from HTML using trafilatura."""
        try:
            import trafilatura

            # Extract main content
            text = trafilatura.extract(
                html,
                url=url,
                include_comments=False,
                include_tables=True,
                include_links=True,
                output_format="txt",
            )

            return text or ""

        except ImportError:
            logger.warning("trafilatura not installed, falling back to basic extraction")
            return await self._basic_extract(html)
        except Exception as e:
            logger.warning(f"trafilatura extraction failed: {e}")
            return await self._basic_extract(html)

    async def _basic_extract(self, html: str) -> str:
        """Basic content extraction using BeautifulSoup."""
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")

            # Remove script and style elements
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()

            # Get text
            text = soup.get_text(separator=" ", strip=True)

            # Clean up whitespace
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            return "\n".join(lines)

        except ImportError:
            logger.warning("BeautifulSoup not installed")
            return ""
        except Exception as e:
            logger.warning(f"Basic extraction failed: {e}")
            return ""

    async def extract_leads(self, evidence: Evidence) -> list[ExtractedLead]:
        """Extract leads from URL content.

        Uses the extraction pipeline to find entities in the text.
        """
        # Load evidence content
        from ...storage import retrieve_evidence_content

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to load evidence content: {e}")
            return []

        extracted_text = data.get("extracted_text", "")
        if not extracted_text:
            logger.warning("No extracted text in evidence")
            return []

        # Run extraction pipeline
        return await self.extraction_pipeline.extract(extracted_text, evidence.id)

    async def get_seed_entity(self, evidence: Evidence) -> SeedEntity | None:
        """Get the seed entity from URL evidence.

        URLs typically don't have a single seed entity - the extracted
        leads become the seeds.
        """
        # Load evidence to get domain
        if evidence.extraction_result:
            domain = evidence.extraction_result.get("domain")
            if domain:
                return SeedEntity(
                    entity_type="domain",
                    name=domain,
                    identifiers={"domain": domain},
                    is_new=True,
                )

        return None
