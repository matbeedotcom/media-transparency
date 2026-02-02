"""Entity extraction pipeline.

Orchestrates deterministic and LLM-based extraction,
combining results with deduplication and confidence scoring.
"""

import logging
from dataclasses import dataclass
from uuid import UUID

from ..models import ExtractedLead, ExtractionMethod
from .deterministic import DeterministicExtractor, ExtractedEntity, get_deterministic_extractor
from .llm import LLMExtractor, LLMExtractedEntity, get_llm_extractor

logger = logging.getLogger(__name__)


@dataclass
class ExtractionConfig:
    """Configuration for the extraction pipeline."""

    enable_llm: bool = False
    llm_provider: str = "openai"
    min_confidence: float = 0.5
    deduplicate: bool = True


class ExtractionPipeline:
    """Orchestrates entity extraction from text.

    Flow:
    1. Run deterministic extraction (always)
    2. Run LLM extraction (optional, if enabled)
    3. Merge and deduplicate results
    4. Filter by confidence threshold
    """

    def __init__(self, config: ExtractionConfig | None = None):
        """Initialize the pipeline.

        Args:
            config: Pipeline configuration
        """
        self.config = config or ExtractionConfig()
        self._deterministic: DeterministicExtractor | None = None
        self._llm: LLMExtractor | None = None

    @property
    def deterministic(self) -> DeterministicExtractor:
        """Get the deterministic extractor."""
        if self._deterministic is None:
            self._deterministic = get_deterministic_extractor()
        return self._deterministic

    @property
    def llm(self) -> LLMExtractor:
        """Get the LLM extractor."""
        if self._llm is None:
            self._llm = get_llm_extractor(self.config.llm_provider)
        return self._llm

    async def extract(self, text: str, evidence_id: UUID) -> list[ExtractedLead]:
        """Extract entities from text.

        Args:
            text: The text to extract from
            evidence_id: The evidence ID to associate with leads

        Returns:
            List of ExtractedLead objects
        """
        all_leads: list[ExtractedLead] = []

        # Step 1: Deterministic extraction (always)
        deterministic_entities = self.deterministic.extract(text)
        for entity in deterministic_entities:
            lead = self._entity_to_lead(entity, evidence_id, ExtractionMethod.DETERMINISTIC)
            all_leads.append(lead)

        logger.info(f"Deterministic extraction found {len(deterministic_entities)} entities")

        # Step 2: LLM extraction (optional)
        if self.config.enable_llm:
            try:
                llm_entities = await self.llm.extract(text)
                for entity in llm_entities:
                    lead = self._llm_entity_to_lead(entity, evidence_id)
                    all_leads.append(lead)
                logger.info(f"LLM extraction found {len(llm_entities)} entities")
            except Exception as e:
                logger.warning(f"LLM extraction failed: {e}")

        # Step 3: Deduplicate
        if self.config.deduplicate:
            all_leads = self._deduplicate(all_leads)

        # Step 4: Filter by confidence
        all_leads = [
            lead for lead in all_leads
            if lead.confidence >= self.config.min_confidence
        ]

        logger.info(f"Extraction pipeline produced {len(all_leads)} leads")
        return all_leads

    def _entity_to_lead(
        self,
        entity: ExtractedEntity,
        evidence_id: UUID,
        method: ExtractionMethod,
    ) -> ExtractedLead:
        """Convert a deterministic entity to an ExtractedLead."""
        return ExtractedLead(
            evidence_id=evidence_id,
            entity_type=entity.entity_type,
            extracted_value=entity.value,
            identifier_type=entity.identifier_type,
            confidence=entity.confidence,
            extraction_method=method,
            context=entity.context,
        )

    def _llm_entity_to_lead(
        self,
        entity: LLMExtractedEntity,
        evidence_id: UUID,
    ) -> ExtractedLead:
        """Convert an LLM entity to an ExtractedLead."""
        # Determine identifier type from entity type
        if entity.entity_type == "organization":
            identifier_type = "name"
        elif entity.entity_type == "person":
            identifier_type = "name"
        else:
            identifier_type = None

        return ExtractedLead(
            evidence_id=evidence_id,
            entity_type=entity.entity_type,
            extracted_value=entity.value,
            identifier_type=identifier_type,
            confidence=entity.confidence,
            extraction_method=ExtractionMethod.LLM,
            context=entity.context,
        )

    def _deduplicate(self, leads: list[ExtractedLead]) -> list[ExtractedLead]:
        """Deduplicate leads, keeping highest confidence for each value."""
        # Group by normalized value
        by_value: dict[str, list[ExtractedLead]] = {}
        for lead in leads:
            key = lead.extracted_value.lower().strip()
            if key not in by_value:
                by_value[key] = []
            by_value[key].append(lead)

        # Keep the highest confidence lead for each value
        unique: list[ExtractedLead] = []
        for value, group in by_value.items():
            # Sort by confidence (highest first)
            group.sort(key=lambda x: x.confidence, reverse=True)
            best = group[0]

            # If we have both deterministic and LLM extractions,
            # mark as hybrid and boost confidence slightly
            methods = {lead.extraction_method for lead in group}
            if len(methods) > 1:
                # Update to hybrid method with boosted confidence
                best = ExtractedLead(
                    evidence_id=best.evidence_id,
                    entity_type=best.entity_type,
                    extracted_value=best.extracted_value,
                    identifier_type=best.identifier_type,
                    confidence=min(best.confidence + 0.05, 1.0),
                    extraction_method=ExtractionMethod.HYBRID,
                    context=best.context,
                )

            unique.append(best)

        return unique


# Factory function
def get_extraction_pipeline(
    enable_llm: bool = False,
    llm_provider: str = "openai",
    min_confidence: float = 0.5,
) -> ExtractionPipeline:
    """Get an extraction pipeline.

    Args:
        enable_llm: Whether to enable LLM extraction
        llm_provider: LLM provider to use
        min_confidence: Minimum confidence threshold

    Returns:
        ExtractionPipeline instance
    """
    config = ExtractionConfig(
        enable_llm=enable_llm,
        llm_provider=llm_provider,
        min_confidence=min_confidence,
    )
    return ExtractionPipeline(config)
