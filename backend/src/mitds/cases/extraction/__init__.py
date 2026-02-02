"""Entity extraction pipeline for the Case Intake System.

Provides both deterministic and LLM-based entity extraction from
unstructured text (URLs, pasted content).

Components:
- DeterministicExtractor: Pattern-based extraction (EIN, BN, domains, legal suffixes)
- LLMExtractor: Optional LLM-based extraction for complex entity mentions
- ExtractionPipeline: Orchestrates extractors (deterministic first, LLM optional)
"""

__all__: list[str] = []
