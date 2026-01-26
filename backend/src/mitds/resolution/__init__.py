"""Entity resolution module for MITDS.

Provides matching and resolution strategies for deduplicating
and linking entities across data sources.
"""

from .matcher import (
    DeterministicMatcher,
    FuzzyMatcher,
    MatchCandidate,
    MatchResult,
    MatchStrategy,
)
from .resolver import EntityResolver, ResolutionResult

__all__ = [
    "DeterministicMatcher",
    "FuzzyMatcher",
    "MatchCandidate",
    "MatchResult",
    "MatchStrategy",
    "EntityResolver",
    "ResolutionResult",
]
