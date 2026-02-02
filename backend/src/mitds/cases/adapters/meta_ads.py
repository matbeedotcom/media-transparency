"""Meta Ad entry point adapter.

Handles Meta Ad sponsor names or page IDs as entry points for case creation.
"""

import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from ...config import get_settings
from ...ingestion.meta_ads import MetaAdIngester
from ...storage import compute_content_hash, store_evidence_content
from ..models import (
    EntryPointType,
    Evidence,
    EvidenceType,
    ExtractedLead,
    ExtractionMethod,
)
from .base import BaseEntryPointAdapter, SeedEntity, ValidationResult

logger = logging.getLogger(__name__)


class MetaAdAdapter(BaseEntryPointAdapter):
    """Adapter for Meta Ad Library sponsor/page entry points.

    Supports:
    - Sponsor names (e.g., "Americans for Prosperity")
    - Page names (e.g., "National Citizens Coalition")
    - Page IDs (numeric Meta page identifiers)
    """

    def __init__(self):
        self._ingester: MetaAdIngester | None = None

    @property
    def entry_point_type(self) -> str:
        return EntryPointType.META_AD.value

    @property
    def ingester(self) -> MetaAdIngester:
        """Get or create the Meta Ad ingester."""
        if self._ingester is None:
            settings = get_settings()
            self._ingester = MetaAdIngester(
                access_token=settings.meta_access_token,
            )
        return self._ingester

    async def validate(self, input_value: str) -> ValidationResult:
        """Validate a Meta Ad sponsor name or page ID.

        Performs quick validation:
        - Non-empty value
        - If numeric, assume it's a page ID
        - Otherwise, assume it's a sponsor/page name
        """
        if not input_value or not input_value.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Sponsor name or page ID is required",
            )

        value = input_value.strip()

        # Check if it looks like a page ID (numeric)
        is_page_id = value.isdigit()

        return ValidationResult(
            is_valid=True,
            normalized_value=value,
            metadata={
                "input_type": "page_id" if is_page_id else "sponsor_name",
                "original_value": input_value,
            },
        )

    async def create_evidence(
        self,
        case_id: UUID,
        input_value: str,
        validation_result: ValidationResult,
    ) -> Evidence:
        """Create evidence by querying the Meta Ad Library API.

        Fetches ads for the sponsor/page and stores the response as evidence.
        """
        evidence_id = uuid4()
        now = datetime.utcnow()

        # Query Meta Ad Library
        input_type = validation_result.metadata.get("input_type", "sponsor_name")

        try:
            if input_type == "page_id":
                # Search by page ID
                ads = await self._search_by_page_id(validation_result.normalized_value)
            else:
                # Search by sponsor name
                ads = await self._search_by_sponsor_name(validation_result.normalized_value)

            # Serialize response
            import json
            content = json.dumps({
                "query": validation_result.normalized_value,
                "query_type": input_type,
                "retrieved_at": now.isoformat(),
                "ad_count": len(ads),
                "ads": ads,
            }, indent=2).encode("utf-8")

        except Exception as e:
            logger.error(f"Failed to query Meta Ad Library: {e}")
            # Store error as evidence
            import json
            content = json.dumps({
                "query": validation_result.normalized_value,
                "query_type": input_type,
                "retrieved_at": now.isoformat(),
                "error": str(e),
            }, indent=2).encode("utf-8")

        # Store in S3
        content_ref, content_hash = await store_evidence_content(
            case_id=str(case_id),
            evidence_id=str(evidence_id),
            content=content,
            content_type="application/json",
            filename="meta_ads_response",
            extension="json",
            metadata={
                "query": validation_result.normalized_value,
                "query_type": input_type,
            },
        )

        return Evidence(
            id=evidence_id,
            case_id=case_id,
            evidence_type=EvidenceType.API_RESPONSE,
            source_url=f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&q={validation_result.normalized_value}",
            content_ref=content_ref,
            content_hash=content_hash,
            content_type="application/json",
            extractor="meta_ads",
            extractor_version="1.0.0",
            extraction_result={
                "query": validation_result.normalized_value,
                "query_type": input_type,
            },
            retrieved_at=now,
            created_at=now,
        )

    async def _search_by_page_id(self, page_id: str) -> list[dict[str, Any]]:
        """Search Meta Ad Library by page ID."""
        try:
            # Use the existing ingester's search capability
            records = await self.ingester.search_by_page_id(page_id, limit=100)
            return [record.model_dump() for record in records]
        except Exception as e:
            logger.warning(f"Meta Ad search by page ID failed: {e}")
            return []

    async def _search_by_sponsor_name(self, sponsor_name: str) -> list[dict[str, Any]]:
        """Search Meta Ad Library by sponsor name."""
        try:
            # Use the existing ingester's search capability
            records = await self.ingester.search_by_sponsor(sponsor_name, limit=100)
            return [record.model_dump() for record in records]
        except Exception as e:
            logger.warning(f"Meta Ad search by sponsor failed: {e}")
            return []

    async def extract_leads(self, evidence: Evidence) -> list[ExtractedLead]:
        """Extract leads from Meta Ad Library response.

        Extracts:
        - Sponsor/page names as organization leads
        - Page IDs as identifiers
        """
        leads: list[ExtractedLead] = []

        if not evidence.extraction_result:
            return leads

        # Load the evidence content
        from ...storage import retrieve_evidence_content
        import json

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to load evidence content: {e}")
            return leads

        # Extract from ads
        ads = data.get("ads", [])
        seen_pages: set[str] = set()

        for ad in ads:
            page_name = ad.get("page_name")
            page_id = ad.get("page_id")
            bylines = ad.get("bylines", [])

            # Extract page as organization
            if page_name and page_name not in seen_pages:
                seen_pages.add(page_name)
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="organization",
                    extracted_value=page_name,
                    identifier_type="name",
                    confidence=0.9,  # High confidence - direct from Meta
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"Meta Ad page: {page_id}",
                ))

            # Extract page ID as identifier
            if page_id and str(page_id) not in seen_pages:
                seen_pages.add(str(page_id))
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="identifier",
                    extracted_value=str(page_id),
                    identifier_type="meta_page_id",
                    confidence=1.0,  # Certain - direct identifier
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"Meta page: {page_name}",
                ))

            # Extract from bylines (disclaimers)
            for byline in bylines:
                if byline and byline not in seen_pages:
                    seen_pages.add(byline)
                    leads.append(ExtractedLead(
                        evidence_id=evidence.id,
                        entity_type="organization",
                        extracted_value=byline,
                        identifier_type="name",
                        confidence=0.85,  # Slightly lower - byline may be abbreviated
                        extraction_method=ExtractionMethod.DETERMINISTIC,
                        context=f"Ad byline for page {page_name}",
                    ))

        return leads

    async def get_seed_entity(self, evidence: Evidence) -> SeedEntity | None:
        """Get the seed entity from Meta Ad evidence.

        Creates a Sponsor entity that can be resolved to an organization.
        """
        if not evidence.extraction_result:
            return None

        query = evidence.extraction_result.get("query")
        query_type = evidence.extraction_result.get("query_type")

        if not query:
            return None

        # Load evidence to get page details
        from ...storage import retrieve_evidence_content
        import json

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
            ads = data.get("ads", [])

            if ads:
                first_ad = ads[0]
                page_name = first_ad.get("page_name", query)
                page_id = first_ad.get("page_id")

                identifiers = {"name": page_name}
                if page_id:
                    identifiers["meta_page_id"] = str(page_id)

                return SeedEntity(
                    entity_type="sponsor",
                    name=page_name,
                    identifiers=identifiers,
                    is_new=True,
                )
        except Exception as e:
            logger.warning(f"Failed to get seed entity from evidence: {e}")

        # Fallback to query value
        return SeedEntity(
            entity_type="sponsor",
            name=query,
            identifiers={"name": query},
            is_new=True,
        )
