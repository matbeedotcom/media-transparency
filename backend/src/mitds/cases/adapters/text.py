"""Text entry point adapter.

Handles pasted text as entry points for case creation,
running entity extraction to create leads.
"""

import json
import logging
from datetime import datetime
from uuid import UUID, uuid4

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


class TextAdapter(BaseEntryPointAdapter):
    """Adapter for pasted text entry points.

    Stores the text as evidence and runs entity extraction
    to create leads. Useful for:
    - LinkedIn posts (can't be fetched automatically)
    - PDF content (after OCR)
    - Email content
    - Any text that can't be fetched via URL
    """

    # Maximum text length
    MAX_TEXT_LENGTH = 100000  # 100KB

    # Minimum text length
    MIN_TEXT_LENGTH = 10

    def __init__(self, enable_llm: bool = False):
        """Initialize the adapter.

        Args:
            enable_llm: Whether to enable LLM extraction
        """
        self.enable_llm = enable_llm
        self._extraction_pipeline: ExtractionPipeline | None = None

    @property
    def entry_point_type(self) -> str:
        return EntryPointType.TEXT.value

    @property
    def extraction_pipeline(self) -> ExtractionPipeline:
        """Get the extraction pipeline."""
        if self._extraction_pipeline is None:
            self._extraction_pipeline = get_extraction_pipeline(
                enable_llm=self.enable_llm,
            )
        return self._extraction_pipeline

    async def validate(self, input_value: str) -> ValidationResult:
        """Validate text input.

        Performs quick validation:
        - Non-empty value
        - Minimum length
        - Maximum length
        """
        if not input_value:
            return ValidationResult(
                is_valid=False,
                error_message="Text is required",
            )

        text = input_value.strip()

        if len(text) < self.MIN_TEXT_LENGTH:
            return ValidationResult(
                is_valid=False,
                error_message=f"Text must be at least {self.MIN_TEXT_LENGTH} characters",
            )

        if len(text) > self.MAX_TEXT_LENGTH:
            return ValidationResult(
                is_valid=False,
                error_message=f"Text must be less than {self.MAX_TEXT_LENGTH} characters",
            )

        # Try to detect source type from content
        source_type = self._detect_source_type(text)

        return ValidationResult(
            is_valid=True,
            normalized_value=text,
            metadata={
                "source_type": source_type,
                "char_count": len(text),
                "word_count": len(text.split()),
            },
        )

    def _detect_source_type(self, text: str) -> str:
        """Detect the likely source type of the text."""
        text_lower = text.lower()

        # LinkedIn patterns
        if "linkedin" in text_lower or "#linkedin" in text_lower:
            return "linkedin"

        # Email patterns
        if text_lower.startswith("from:") or "subject:" in text_lower[:200]:
            return "email"

        # News article patterns
        if any(word in text_lower[:500] for word in ["reuters", "associated press", "ap news"]):
            return "news"

        # Press release patterns
        if "for immediate release" in text_lower[:500]:
            return "press_release"

        return "unknown"

    async def create_evidence(
        self,
        case_id: UUID,
        input_value: str,
        validation_result: ValidationResult,
    ) -> Evidence:
        """Create evidence from the pasted text."""
        evidence_id = uuid4()
        now = datetime.utcnow()
        text = validation_result.normalized_value

        # Create content object
        content_data = {
            "text": text,
            "created_at": now.isoformat(),
            "source_type": validation_result.metadata.get("source_type"),
            "char_count": validation_result.metadata.get("char_count"),
            "word_count": validation_result.metadata.get("word_count"),
        }

        # Store in S3
        content = json.dumps(content_data, indent=2).encode("utf-8")
        content_ref, content_hash = await store_evidence_content(
            case_id=str(case_id),
            evidence_id=str(evidence_id),
            content=content,
            content_type="application/json",
            filename="text_content",
            extension="json",
        )

        # Also store raw text
        await store_evidence_content(
            case_id=str(case_id),
            evidence_id=str(evidence_id),
            content=text.encode("utf-8"),
            content_type="text/plain",
            filename="raw",
            extension="txt",
        )

        return Evidence(
            id=evidence_id,
            case_id=case_id,
            evidence_type=EvidenceType.ENTRY_POINT,
            source_url=None,
            content_ref=content_ref,
            content_hash=content_hash,
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            extraction_result={
                "source_type": validation_result.metadata.get("source_type"),
                "char_count": validation_result.metadata.get("char_count"),
                "word_count": validation_result.metadata.get("word_count"),
            },
            retrieved_at=now,
            created_at=now,
        )

    async def extract_leads(self, evidence: Evidence) -> list[ExtractedLead]:
        """Extract leads from the text.

        Uses the extraction pipeline to find entities.
        """
        # Load evidence content
        from ...storage import retrieve_evidence_content

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to load evidence content: {e}")
            return []

        text = data.get("text", "")
        if not text:
            logger.warning("No text in evidence")
            return []

        # Run extraction pipeline
        return await self.extraction_pipeline.extract(text, evidence.id)

    async def get_seed_entity(self, evidence: Evidence) -> SeedEntity | None:
        """Get the seed entity from text evidence.

        Text typically doesn't have a single seed entity - the extracted
        leads become the seeds. Returns None.
        """
        return None
