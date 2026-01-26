"""Coordination detection modules for MITDS.

Provides detection algorithms for:
- Funding cluster detection
- Temporal coordination detection (US2)
- Infrastructure sharing detection (US4)
"""

from .funding import (
    FundingClusterDetector,
    FundingClusterResult,
    SharedFunderResult,
    detect_funding_clusters,
    find_shared_funders,
)

__all__ = [
    "FundingClusterDetector",
    "FundingClusterResult",
    "SharedFunderResult",
    "detect_funding_clusters",
    "find_shared_funders",
]
