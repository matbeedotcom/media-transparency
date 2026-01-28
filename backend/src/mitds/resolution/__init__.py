"""Entity resolution module for MITDS.

Provides matching and resolution strategies for deduplicating
and linking entities across data sources.
"""

from .matcher import (
    DeterministicMatcher,
    FuzzyMatcher,
    HybridMatcher,
    MatchCandidate,
    MatchResult,
    MatchStrategy,
)
from .resolver import EntityResolver, ResolutionResult
from .cross_border import (
    CrossBorderResolver,
    CrossBorderResolutionResult,
    CrossBorderResolutionStats,
    UnresolvedGrant,
    run_cross_border_resolution,
)

__all__ = [
    "DeterministicMatcher",
    "FuzzyMatcher",
    "HybridMatcher",
    "MatchCandidate",
    "MatchResult",
    "MatchStrategy",
    "EntityResolver",
    "ResolutionResult",
    "CrossBorderResolver",
    "CrossBorderResolutionResult",
    "CrossBorderResolutionStats",
    "UnresolvedGrant",
    "run_cross_border_resolution",
]
