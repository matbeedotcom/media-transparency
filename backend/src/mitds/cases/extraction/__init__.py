"""Entity extraction pipeline for the Case Intake System.

Provides both deterministic and LLM-based entity extraction from
unstructured text (URLs, pasted content).

Components:
- DeterministicExtractor: Pattern-based extraction (EIN, BN, domains, legal suffixes)
- LLMExtractor: Optional LLM-based extraction for complex entity mentions
- ExtractionPipeline: Orchestrates extractors (deterministic first, LLM optional)
"""

from .deterministic import (
    DeterministicExtractor,
    ExtractedEntity,
    get_deterministic_extractor,
)
from .llm import LLMExtractor, LLMExtractedEntity, get_llm_extractor
from .pipeline import ExtractionConfig, ExtractionPipeline, get_extraction_pipeline

__all__ = [
    "DeterministicExtractor",
    "ExtractedEntity",
    "get_deterministic_extractor",
    "LLMExtractor",
    "LLMExtractedEntity",
    "get_llm_extractor",
    "ExtractionConfig",
    "ExtractionPipeline",
    "get_extraction_pipeline",
]
