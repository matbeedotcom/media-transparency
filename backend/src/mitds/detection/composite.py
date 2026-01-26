"""Composite coordination detection for MITDS.

Combines multiple detection signals into a unified coordination score
while ensuring no single signal can trigger a flag alone.

Key design principles:
1. Minimum 2 independent signal types required
2. Signal weights prevent any single signal from exceeding threshold
3. Signal correlation is accounted for to avoid double-counting
4. Confidence bands reflect uncertainty in detection
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from ..reporting.explain import ConfidenceBand, calculate_confidence_band


class SignalType(str, Enum):
    """Types of coordination signals."""

    FUNDING_CONCENTRATION = "funding_concentration"
    SHARED_FUNDER = "shared_funder"
    BOARD_OVERLAP = "board_overlap"
    PERSONNEL_INTERLOCK = "personnel_interlock"
    TEMPORAL_COORDINATION = "temporal_coordination"
    INFRASTRUCTURE_SHARING = "infrastructure_sharing"
    OWNERSHIP_CHAIN = "ownership_chain"
    CONTENT_SIMILARITY = "content_similarity"
    BEHAVIORAL_PATTERN = "behavioral_pattern"


class SignalCategory(str, Enum):
    """Categories for grouping related signals."""

    FINANCIAL = "financial"
    GOVERNANCE = "governance"
    TEMPORAL = "temporal"
    INFRASTRUCTURE = "infrastructure"
    OWNERSHIP = "ownership"
    CONTENT = "content"


# Mapping of signals to categories
SIGNAL_CATEGORIES = {
    SignalType.FUNDING_CONCENTRATION: SignalCategory.FINANCIAL,
    SignalType.SHARED_FUNDER: SignalCategory.FINANCIAL,
    SignalType.BOARD_OVERLAP: SignalCategory.GOVERNANCE,
    SignalType.PERSONNEL_INTERLOCK: SignalCategory.GOVERNANCE,
    SignalType.TEMPORAL_COORDINATION: SignalCategory.TEMPORAL,
    SignalType.INFRASTRUCTURE_SHARING: SignalCategory.INFRASTRUCTURE,
    SignalType.OWNERSHIP_CHAIN: SignalCategory.OWNERSHIP,
    SignalType.CONTENT_SIMILARITY: SignalCategory.CONTENT,
    SignalType.BEHAVIORAL_PATTERN: SignalCategory.TEMPORAL,
}


# =============================================================================
# Signal Weight Configuration
# =============================================================================


@dataclass
class SignalWeight:
    """Weight configuration for a signal type.

    Weights are designed so no single signal can exceed the flagging threshold.
    """

    base_weight: float  # Base contribution to score
    max_contribution: float  # Maximum contribution cap
    correlation_factor: dict[str, float] = field(default_factory=dict)
    requires_corroboration: bool = True  # Must have another signal


# Default weights - designed so no single signal exceeds 0.4 threshold
SIGNAL_WEIGHTS = {
    SignalType.FUNDING_CONCENTRATION: SignalWeight(
        base_weight=0.20,
        max_contribution=0.35,
        correlation_factor={
            SignalType.SHARED_FUNDER.value: 0.5,  # Same category, reduce
        },
    ),
    SignalType.SHARED_FUNDER: SignalWeight(
        base_weight=0.18,
        max_contribution=0.30,
        correlation_factor={
            SignalType.FUNDING_CONCENTRATION.value: 0.5,
        },
    ),
    SignalType.BOARD_OVERLAP: SignalWeight(
        base_weight=0.22,
        max_contribution=0.35,
        correlation_factor={
            SignalType.PERSONNEL_INTERLOCK.value: 0.6,
        },
    ),
    SignalType.PERSONNEL_INTERLOCK: SignalWeight(
        base_weight=0.15,
        max_contribution=0.25,
        correlation_factor={
            SignalType.BOARD_OVERLAP.value: 0.6,
        },
    ),
    SignalType.TEMPORAL_COORDINATION: SignalWeight(
        base_weight=0.18,
        max_contribution=0.30,
        correlation_factor={
            SignalType.BEHAVIORAL_PATTERN.value: 0.7,
        },
    ),
    SignalType.INFRASTRUCTURE_SHARING: SignalWeight(
        base_weight=0.20,
        max_contribution=0.35,
        correlation_factor={},
    ),
    SignalType.OWNERSHIP_CHAIN: SignalWeight(
        base_weight=0.25,
        max_contribution=0.35,
        correlation_factor={},
    ),
    SignalType.CONTENT_SIMILARITY: SignalWeight(
        base_weight=0.12,
        max_contribution=0.20,
        correlation_factor={},
    ),
    SignalType.BEHAVIORAL_PATTERN: SignalWeight(
        base_weight=0.15,
        max_contribution=0.25,
        correlation_factor={
            SignalType.TEMPORAL_COORDINATION.value: 0.7,
        },
    ),
}

# Threshold for flagging - requires multiple signals
FLAG_THRESHOLD = 0.45
MINIMUM_SIGNALS = 2
MINIMUM_CATEGORIES = 2


# =============================================================================
# Signal Data Classes
# =============================================================================


@dataclass
class DetectedSignal:
    """A single detected coordination signal."""

    signal_type: SignalType
    strength: float  # 0-1 signal strength
    confidence: float  # 0-1 confidence in detection
    entity_ids: list[UUID]
    evidence_ids: list[UUID] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def category(self) -> SignalCategory:
        return SIGNAL_CATEGORIES.get(self.signal_type, SignalCategory.CONTENT)

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_type": self.signal_type.value,
            "category": self.category.value,
            "strength": self.strength,
            "confidence": self.confidence,
            "entity_ids": [str(eid) for eid in self.entity_ids],
            "evidence_ids": [str(eid) for eid in self.evidence_ids],
            "metadata": self.metadata,
            "detected_at": self.detected_at.isoformat(),
        }


@dataclass
class CompositeScore:
    """Composite coordination score from multiple signals."""

    raw_score: float
    adjusted_score: float
    confidence_band: ConfidenceBand
    is_flagged: bool
    flag_reason: str | None
    signals: list[DetectedSignal]
    signal_contributions: dict[str, float]
    category_breakdown: dict[str, float]
    validation_passed: bool
    validation_messages: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_score": self.raw_score,
            "adjusted_score": self.adjusted_score,
            "confidence_band": self.confidence_band.to_dict(),
            "is_flagged": self.is_flagged,
            "flag_reason": self.flag_reason,
            "signals": [s.to_dict() for s in self.signals],
            "signal_contributions": self.signal_contributions,
            "category_breakdown": self.category_breakdown,
            "validation": {
                "passed": self.validation_passed,
                "messages": self.validation_messages,
            },
        }


# =============================================================================
# Composite Score Calculator
# =============================================================================


class CompositeScoreCalculator:
    """Calculates composite coordination scores from multiple signals.

    Ensures no single signal can trigger flagging and requires
    corroboration across multiple signal categories.
    """

    def __init__(
        self,
        weights: dict[SignalType, SignalWeight] | None = None,
        flag_threshold: float = FLAG_THRESHOLD,
        min_signals: int = MINIMUM_SIGNALS,
        min_categories: int = MINIMUM_CATEGORIES,
    ):
        self.weights = weights or SIGNAL_WEIGHTS
        self.flag_threshold = flag_threshold
        self.min_signals = min_signals
        self.min_categories = min_categories

    def calculate(self, signals: list[DetectedSignal]) -> CompositeScore:
        """Calculate composite score from detected signals.

        Args:
            signals: List of detected coordination signals

        Returns:
            CompositeScore with flagging determination
        """
        if not signals:
            return self._empty_score()

        # Validate signal requirements
        validation_passed, validation_messages = self._validate_signals(signals)

        # Calculate raw contributions
        contributions = {}
        for signal in signals:
            contribution = self._calculate_contribution(signal, signals)
            contributions[signal.signal_type.value] = contribution

        # Calculate raw score
        raw_score = sum(contributions.values())

        # Apply correlation adjustments
        adjusted_score = self._apply_correlation_adjustment(signals, contributions)

        # Calculate category breakdown
        category_breakdown = self._calculate_category_breakdown(signals, contributions)

        # Calculate confidence band
        confidence_band = self._calculate_confidence_band(signals, adjusted_score)

        # Determine if flagged
        is_flagged, flag_reason = self._determine_flag(
            adjusted_score,
            signals,
            validation_passed,
            validation_messages,
        )

        return CompositeScore(
            raw_score=round(raw_score, 4),
            adjusted_score=round(adjusted_score, 4),
            confidence_band=confidence_band,
            is_flagged=is_flagged,
            flag_reason=flag_reason,
            signals=signals,
            signal_contributions=contributions,
            category_breakdown=category_breakdown,
            validation_passed=validation_passed,
            validation_messages=validation_messages,
        )

    def _empty_score(self) -> CompositeScore:
        """Return an empty score when no signals present."""
        return CompositeScore(
            raw_score=0.0,
            adjusted_score=0.0,
            confidence_band=ConfidenceBand(0.0, 0.0, 0.0),
            is_flagged=False,
            flag_reason=None,
            signals=[],
            signal_contributions={},
            category_breakdown={},
            validation_passed=False,
            validation_messages=["No signals detected"],
        )

    def _validate_signals(
        self, signals: list[DetectedSignal]
    ) -> tuple[bool, list[str]]:
        """Validate that signal requirements are met."""
        messages = []

        # Check minimum signal count
        if len(signals) < self.min_signals:
            messages.append(
                f"Requires {self.min_signals} signals, found {len(signals)}"
            )

        # Check minimum category count
        categories = set(s.category for s in signals)
        if len(categories) < self.min_categories:
            messages.append(
                f"Requires {self.min_categories} categories, found {len(categories)}"
            )

        # Check that no single signal dominates
        for signal in signals:
            weight = self.weights.get(signal.signal_type)
            if weight and weight.max_contribution >= self.flag_threshold:
                messages.append(
                    f"Single signal {signal.signal_type.value} cannot exceed threshold"
                )

        return len(messages) == 0, messages

    def _calculate_contribution(
        self,
        signal: DetectedSignal,
        all_signals: list[DetectedSignal],
    ) -> float:
        """Calculate a signal's contribution to the composite score."""
        weight = self.weights.get(signal.signal_type)
        if not weight:
            return 0.0

        # Base contribution
        contribution = weight.base_weight * signal.strength * signal.confidence

        # Cap at maximum
        contribution = min(contribution, weight.max_contribution)

        return contribution

    def _apply_correlation_adjustment(
        self,
        signals: list[DetectedSignal],
        contributions: dict[str, float],
    ) -> float:
        """Adjust score for correlated signals to avoid double-counting."""
        adjusted_total = 0.0
        processed = set()

        for signal in signals:
            signal_key = signal.signal_type.value
            if signal_key in processed:
                continue

            contribution = contributions.get(signal_key, 0.0)
            weight = self.weights.get(signal.signal_type)

            if weight and weight.correlation_factor:
                # Check for correlated signals
                for other_signal in signals:
                    other_key = other_signal.signal_type.value
                    if other_key in weight.correlation_factor and other_key not in processed:
                        correlation = weight.correlation_factor[other_key]
                        other_contribution = contributions.get(other_key, 0.0)

                        # Reduce correlated contribution
                        reduced = other_contribution * correlation
                        contributions[other_key] = reduced

            adjusted_total += contribution
            processed.add(signal_key)

        # Add remaining (possibly reduced) contributions
        for key, value in contributions.items():
            if key not in processed:
                adjusted_total += value

        return min(1.0, adjusted_total)

    def _calculate_category_breakdown(
        self,
        signals: list[DetectedSignal],
        contributions: dict[str, float],
    ) -> dict[str, float]:
        """Calculate score contribution by category."""
        breakdown: dict[str, float] = {}

        for signal in signals:
            category = signal.category.value
            contribution = contributions.get(signal.signal_type.value, 0.0)

            if category in breakdown:
                breakdown[category] += contribution
            else:
                breakdown[category] = contribution

        return breakdown

    def _calculate_confidence_band(
        self,
        signals: list[DetectedSignal],
        adjusted_score: float,
    ) -> ConfidenceBand:
        """Calculate confidence band for the composite score."""
        if not signals:
            return ConfidenceBand(0.0, 0.0, 0.0)

        # Average confidence across signals
        avg_confidence = sum(s.confidence for s in signals) / len(signals)

        # Average strength
        avg_strength = sum(s.strength for s in signals) / len(signals)

        # Total evidence count
        evidence_count = sum(len(s.evidence_ids) for s in signals)

        return calculate_confidence_band(
            base_confidence=avg_confidence,
            evidence_count=evidence_count,
            signal_strength=avg_strength,
            data_completeness=0.8,  # Assume some data gaps
        )

    def _determine_flag(
        self,
        adjusted_score: float,
        signals: list[DetectedSignal],
        validation_passed: bool,
        validation_messages: list[str],
    ) -> tuple[bool, str | None]:
        """Determine if the composite score should be flagged."""
        # Must pass validation
        if not validation_passed:
            return False, None

        # Must exceed threshold
        if adjusted_score < self.flag_threshold:
            return False, None

        # Build flag reason
        categories = set(s.category.value for s in signals)
        signal_types = [s.signal_type.value for s in signals]

        reason = (
            f"Score {adjusted_score:.2f} exceeds threshold {self.flag_threshold:.2f} "
            f"with {len(signals)} signals across {len(categories)} categories: "
            f"{', '.join(signal_types)}"
        )

        return True, reason


# =============================================================================
# Single Signal Protection
# =============================================================================


def verify_no_single_signal_trigger(
    signals: list[DetectedSignal],
    calculator: CompositeScoreCalculator | None = None,
) -> tuple[bool, str]:
    """Verify that no single signal can trigger flagging.

    This is a safety check to ensure the weighting system is properly
    configured to require corroboration.

    Returns:
        Tuple of (passes_check, message)
    """
    calc = calculator or CompositeScoreCalculator()

    for signal in signals:
        # Test each signal in isolation
        single_result = calc.calculate([signal])

        if single_result.is_flagged:
            return (
                False,
                f"Signal {signal.signal_type.value} can trigger flag alone "
                f"with score {single_result.adjusted_score:.2f}",
            )

    return True, "No single signal can trigger flagging"


def validate_weight_configuration(
    weights: dict[SignalType, SignalWeight] | None = None,
    threshold: float = FLAG_THRESHOLD,
) -> list[str]:
    """Validate that weight configuration prevents single-signal flags.

    Returns list of configuration issues, empty if valid.
    """
    weights = weights or SIGNAL_WEIGHTS
    issues = []

    for signal_type, weight in weights.items():
        # Check that max contribution is below threshold
        if weight.max_contribution >= threshold:
            issues.append(
                f"{signal_type.value}: max_contribution ({weight.max_contribution:.2f}) "
                f">= threshold ({threshold:.2f})"
            )

        # Check that base weight * max possible strength/confidence is below threshold
        max_possible = weight.base_weight * 1.0 * 1.0  # max strength and confidence
        if max_possible >= threshold:
            issues.append(
                f"{signal_type.value}: base_weight ({weight.base_weight:.2f}) "
                f"could exceed threshold with perfect signal"
            )

    return issues


# =============================================================================
# Convenience Functions
# =============================================================================


def calculate_composite_score(
    signals: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate composite score from signal dictionaries.

    Convenience function for API use.

    Args:
        signals: List of signal dictionaries with keys:
            - signal_type: SignalType value
            - strength: 0-1 signal strength
            - confidence: 0-1 confidence
            - entity_ids: List of entity UUIDs
            - evidence_ids: Optional list of evidence UUIDs

    Returns:
        CompositeScore as dictionary
    """
    detected_signals = []

    for s in signals:
        try:
            signal = DetectedSignal(
                signal_type=SignalType(s["signal_type"]),
                strength=s["strength"],
                confidence=s["confidence"],
                entity_ids=[UUID(eid) for eid in s.get("entity_ids", [])],
                evidence_ids=[UUID(eid) for eid in s.get("evidence_ids", [])],
                metadata=s.get("metadata", {}),
            )
            detected_signals.append(signal)
        except (ValueError, KeyError) as e:
            continue

    calculator = CompositeScoreCalculator()
    result = calculator.calculate(detected_signals)

    return result.to_dict()
