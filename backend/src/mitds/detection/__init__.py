"""Coordination detection modules for MITDS.

Provides detection algorithms for:
- Funding cluster detection
- Temporal coordination detection
- Infrastructure sharing detection
- Composite scoring
- Hard negative filtering
- Political funding obfuscation detection
"""

from .funding import (
    FundingClusterDetector,
    FundingClusterResult,
    SharedFunderResult,
    detect_funding_clusters,
    find_shared_funders,
)
from .temporal import (
    TemporalCoordinationDetector,
    TemporalCoordinationResult,
    TimingEvent,
    BurstDetectionResult,
    LeadLagResult,
    SynchronizationResult,
)
from .infra import (
    InfrastructureDetector,
    InfrastructureProfile,
    SharedInfrastructureMatch,
    InfraSignal,
    InfraSignalType,
    InfrastructureScorer,
)
from .composite import (
    CompositeScoreCalculator,
    CompositeScore,
    DetectedSignal,
    SignalType,
    SignalCategory,
    calculate_composite_score,
    verify_no_single_signal_trigger,
)
from .hardneg import (
    filter_hard_negatives,
    check_hard_negatives,
    HardNegativeFilterChain,
    HardNegativeEvent,
)
from .political_funding import (
    PoliticalFundingObfuscationDetector,
    PoliticalFundingSignal,
    PoliticalFundingSignalType,
    PoliticalFundingCategory,
)

__all__ = [
    # Funding
    "FundingClusterDetector",
    "FundingClusterResult",
    "SharedFunderResult",
    "detect_funding_clusters",
    "find_shared_funders",
    # Temporal
    "TemporalCoordinationDetector",
    "TemporalCoordinationResult",
    "TimingEvent",
    "BurstDetectionResult",
    "LeadLagResult",
    "SynchronizationResult",
    # Infrastructure
    "InfrastructureDetector",
    "InfrastructureProfile",
    "SharedInfrastructureMatch",
    "InfraSignal",
    "InfraSignalType",
    "InfrastructureScorer",
    # Composite
    "CompositeScoreCalculator",
    "CompositeScore",
    "DetectedSignal",
    "SignalType",
    "SignalCategory",
    "calculate_composite_score",
    "verify_no_single_signal_trigger",
    # Hard Negatives
    "filter_hard_negatives",
    "check_hard_negatives",
    "HardNegativeFilterChain",
    "HardNegativeEvent",
    # Political Funding
    "PoliticalFundingObfuscationDetector",
    "PoliticalFundingSignal",
    "PoliticalFundingSignalType",
    "PoliticalFundingCategory",
]
