"""Explanation generation for MITDS findings.

Provides human-readable explanations for why patterns were flagged,
including confidence band calculations and evidence summaries.
"""

import math
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from .language import (
    LanguageTransformer,
    add_hedging,
    generate_finding_disclaimer,
    get_certainty_qualifier,
)


# =============================================================================
# Confidence Band Calculation
# =============================================================================


@dataclass
class ConfidenceBand:
    """Confidence interval for a finding."""

    point_estimate: float
    lower_bound: float
    upper_bound: float
    sample_size: int = 0
    methodology: str = ""

    @property
    def width(self) -> float:
        """Width of the confidence interval."""
        return self.upper_bound - self.lower_bound

    @property
    def is_high_confidence(self) -> bool:
        """Check if this is a high-confidence finding."""
        return self.lower_bound >= 0.6

    @property
    def is_significant(self) -> bool:
        """Check if the lower bound is above threshold."""
        return self.lower_bound >= 0.3

    def to_dict(self) -> dict[str, Any]:
        return {
            "point_estimate": self.point_estimate,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "width": self.width,
            "sample_size": self.sample_size,
            "methodology": self.methodology,
            "is_high_confidence": self.is_high_confidence,
            "is_significant": self.is_significant,
        }


def calculate_confidence_band(
    base_confidence: float,
    evidence_count: int,
    signal_strength: float,
    data_completeness: float = 1.0,
) -> ConfidenceBand:
    """Calculate confidence band for a finding.

    Args:
        base_confidence: Initial confidence estimate (0-1)
        evidence_count: Number of evidence items supporting finding
        signal_strength: Strength of the detected signal (0-1)
        data_completeness: Estimate of data completeness (0-1)

    Returns:
        ConfidenceBand with point estimate and bounds
    """
    # Adjust base confidence for evidence count
    # More evidence increases confidence but with diminishing returns
    evidence_factor = min(1.0, 0.5 + math.log(1 + evidence_count) * 0.1)

    # Adjust for signal strength
    signal_factor = 0.7 + signal_strength * 0.3

    # Adjust for data completeness
    completeness_factor = 0.5 + data_completeness * 0.5

    # Calculate point estimate
    point_estimate = base_confidence * evidence_factor * signal_factor * completeness_factor
    point_estimate = min(0.95, max(0.05, point_estimate))

    # Calculate margin based on uncertainty factors
    # Less evidence = wider margin
    base_margin = 0.15
    evidence_margin = max(0.02, 0.2 - evidence_count * 0.02)
    completeness_margin = (1 - data_completeness) * 0.1

    total_margin = base_margin + evidence_margin + completeness_margin

    # Calculate bounds
    lower_bound = max(0.0, point_estimate - total_margin)
    upper_bound = min(1.0, point_estimate + total_margin)

    return ConfidenceBand(
        point_estimate=round(point_estimate, 3),
        lower_bound=round(lower_bound, 3),
        upper_bound=round(upper_bound, 3),
        sample_size=evidence_count,
        methodology="Bayesian estimation with evidence weighting",
    )


def calculate_composite_confidence(
    individual_confidences: list[ConfidenceBand],
    correlation_adjustment: float = 0.2,
) -> ConfidenceBand:
    """Calculate composite confidence from multiple findings.

    Uses a conservative approach that accounts for potential
    correlation between signals.

    Args:
        individual_confidences: List of confidence bands
        correlation_adjustment: Factor to account for signal correlation

    Returns:
        Combined ConfidenceBand
    """
    if not individual_confidences:
        return ConfidenceBand(
            point_estimate=0.0,
            lower_bound=0.0,
            upper_bound=0.0,
        )

    n = len(individual_confidences)

    # Calculate weighted average of point estimates
    # Higher confidence estimates get more weight
    total_weight = sum(c.point_estimate for c in individual_confidences)
    if total_weight == 0:
        return ConfidenceBand(
            point_estimate=0.0,
            lower_bound=0.0,
            upper_bound=0.0,
        )

    weighted_estimate = sum(
        c.point_estimate ** 2 for c in individual_confidences
    ) / total_weight

    # Apply correlation adjustment (reduces confidence when signals may be correlated)
    correlation_penalty = 1 - (correlation_adjustment * (1 - 1 / n))
    adjusted_estimate = weighted_estimate * correlation_penalty

    # Calculate combined bounds
    # Use the most conservative approach
    lower = min(c.lower_bound for c in individual_confidences)
    upper = max(c.upper_bound for c in individual_confidences)

    # Narrow bounds based on agreement between signals
    agreement = 1 - (max(c.point_estimate for c in individual_confidences) -
                     min(c.point_estimate for c in individual_confidences))
    narrowing_factor = 0.5 + agreement * 0.5

    adjusted_lower = lower + (adjusted_estimate - lower) * (1 - narrowing_factor)
    adjusted_upper = upper - (upper - adjusted_estimate) * (1 - narrowing_factor)

    return ConfidenceBand(
        point_estimate=round(adjusted_estimate, 3),
        lower_bound=round(adjusted_lower, 3),
        upper_bound=round(adjusted_upper, 3),
        sample_size=sum(c.sample_size for c in individual_confidences),
        methodology="Weighted composite with correlation adjustment",
    )


# =============================================================================
# Signal Explanation Templates
# =============================================================================


SIGNAL_EXPLANATIONS = {
    "funding_concentration": {
        "title": "Funding Concentration",
        "why_matters": (
            "When a single funder provides substantial support to multiple entities, "
            "it may indicate aligned objectives or coordinated activity. However, "
            "this pattern is also common in legitimate philanthropy and advocacy."
        ),
        "template": (
            "{qualifier} that {funder_name} provided funding to {recipient_count} "
            "entities in this analysis. The funding concentration score of {score:.2f} "
            "indicates {interpretation}."
        ),
        "interpretations": {
            "high": "significant funding overlap that warrants attention",
            "medium": "moderate funding relationship worth noting",
            "low": "some funding relationship but within typical patterns",
        },
        "limitations": [
            "Shared funding does not prove coordination",
            "Funders often support entities with aligned missions",
            "Not all funding may be captured in available records",
        ],
    },
    "board_overlap": {
        "title": "Board/Personnel Overlap",
        "why_matters": (
            "Shared board members or key personnel across organizations can indicate "
            "close relationships or coordinated governance. This is common in many "
            "sectors, especially among nonprofits with aligned missions."
        ),
        "template": (
            "{qualifier} that {person_count} individual(s) hold positions at multiple "
            "entities in this analysis. The overlap pattern shows {description}."
        ),
        "interpretations": {
            "high": "significant personnel overlap across the analyzed entities",
            "medium": "some shared personnel that may indicate relationships",
            "low": "limited personnel overlap within typical ranges",
        },
        "limitations": [
            "Board service at multiple nonprofits is common",
            "Shared expertise naturally leads to board overlaps",
            "Advisory roles may have limited influence",
        ],
    },
    "temporal_coordination": {
        "title": "Temporal Pattern",
        "why_matters": (
            "When multiple entities take similar actions within a short timeframe, "
            "it may suggest coordination. However, external events, industry trends, "
            "or coincidence can also explain timing similarities."
        ),
        "template": (
            "{qualifier} temporal proximity in {action_type} across {entity_count} "
            "entities, occurring within {timeframe}. {additional_context}"
        ),
        "interpretations": {
            "high": "Strong temporal clustering that is statistically unusual",
            "medium": "Notable timing similarity worth examining",
            "low": "Some timing overlap but possibly coincidental",
        },
        "limitations": [
            "External events can cause synchronized responses",
            "Industry-wide trends affect many organizations simultaneously",
            "Filing deadlines create natural clustering in some data",
        ],
    },
    "infrastructure_sharing": {
        "title": "Infrastructure Sharing",
        "why_matters": (
            "Shared technical infrastructure like analytics IDs, hosting, or SSL "
            "certificates can indicate common management or ownership. However, "
            "shared vendors and hosting providers are common."
        ),
        "template": (
            "{qualifier} shared infrastructure elements between {domain_count} domains, "
            "including {shared_elements}. This {interpretation}."
        ),
        "interpretations": {
            "high": "strongly suggests common technical management",
            "medium": "may indicate operational relationship",
            "low": "could reflect common vendor choice",
        },
        "limitations": [
            "Common hosting providers serve many unrelated sites",
            "SSL certificate sharing may be coincidental",
            "CMS and technology choices are influenced by many factors",
        ],
    },
    "ownership_chain": {
        "title": "Ownership/Control Relationship",
        "why_matters": (
            "Corporate ownership structures can reveal influence relationships "
            "not apparent from public-facing information. These structures have "
            "many legitimate purposes including liability protection."
        ),
        "template": (
            "{qualifier} {relationship_type} relationship between {entity_a} and "
            "{entity_b}, documented in {source}. {additional_context}"
        ),
        "interpretations": {
            "high": "clear ownership or control relationship",
            "medium": "apparent affiliated relationship",
            "low": "possible connection worth noting",
        },
        "limitations": [
            "Corporate structures serve legitimate business purposes",
            "Historical ownership may not reflect current control",
            "Minority stakes may have limited influence",
        ],
    },
}


# =============================================================================
# Explanation Generator
# =============================================================================


@dataclass
class ExplanationContext:
    """Context for generating an explanation."""

    signal_type: str
    entities: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    confidence: ConfidenceBand
    additional_data: dict[str, Any] = field(default_factory=dict)


class ExplanationGenerator:
    """Generates human-readable explanations for findings."""

    def __init__(self):
        self.language_transformer = LanguageTransformer()

    def generate_why_flagged(
        self,
        context: ExplanationContext,
    ) -> str:
        """Generate a 'why flagged' explanation for a finding.

        Returns a concise explanation suitable for display in reports
        and user interfaces.
        """
        signal_info = SIGNAL_EXPLANATIONS.get(context.signal_type, {})

        if not signal_info:
            return self._generate_generic_explanation(context)

        # Determine interpretation level
        if context.confidence.is_high_confidence:
            level = "high"
        elif context.confidence.is_significant:
            level = "medium"
        else:
            level = "low"

        interpretation = signal_info.get("interpretations", {}).get(
            level, "warrants attention"
        )

        # Get certainty qualifier based on confidence
        qualifier = get_certainty_qualifier(context.confidence.point_estimate)

        # Build explanation parts
        parts = []

        # Why this signal matters
        why_matters = signal_info.get("why_matters", "")
        if why_matters:
            parts.append(why_matters)

        # Specific finding description
        template = signal_info.get("template", "")
        if template:
            try:
                filled = template.format(
                    qualifier=qualifier,
                    interpretation=interpretation,
                    **context.additional_data,
                )
                parts.append(filled)
            except KeyError:
                pass

        # Confidence information
        conf_text = (
            f"This finding has a confidence score of "
            f"{context.confidence.point_estimate:.0%} "
            f"(range: {context.confidence.lower_bound:.0%} - "
            f"{context.confidence.upper_bound:.0%}), "
            f"based on {len(context.evidence)} piece(s) of evidence."
        )
        parts.append(conf_text)

        return " ".join(parts)

    def generate_limitations(
        self,
        context: ExplanationContext,
    ) -> list[str]:
        """Generate list of limitations for a finding."""
        signal_info = SIGNAL_EXPLANATIONS.get(context.signal_type, {})
        limitations = signal_info.get("limitations", []).copy()

        # Add generic limitations based on confidence
        if context.confidence.point_estimate < 0.5:
            limitations.append(
                "Lower confidence findings should be treated with additional caution"
            )

        if len(context.evidence) < 3:
            limitations.append(
                "Limited evidence items support this finding"
            )

        return limitations

    def generate_evidence_summary(
        self,
        evidence: list[dict[str, Any]],
        max_items: int = 3,
    ) -> str:
        """Generate a summary of supporting evidence."""
        if not evidence:
            return "No specific evidence items linked to this finding."

        summary_parts = [f"This finding is supported by {len(evidence)} evidence item(s):"]

        for item in evidence[:max_items]:
            source = item.get("source_name", "Unknown source")
            evidence_type = item.get("evidence_type", "record")
            summary_parts.append(f"- {evidence_type} from {source}")

        if len(evidence) > max_items:
            summary_parts.append(f"- Plus {len(evidence) - max_items} additional item(s)")

        return " ".join(summary_parts)

    def generate_full_explanation(
        self,
        context: ExplanationContext,
    ) -> dict[str, Any]:
        """Generate a complete explanation package for a finding."""
        return {
            "signal_type": context.signal_type,
            "signal_title": SIGNAL_EXPLANATIONS.get(
                context.signal_type, {}
            ).get("title", context.signal_type),
            "why_flagged": self.generate_why_flagged(context),
            "limitations": self.generate_limitations(context),
            "evidence_summary": self.generate_evidence_summary(context.evidence),
            "confidence": context.confidence.to_dict(),
            "disclaimer": generate_finding_disclaimer(),
            "entities_involved": [
                {"id": e.get("entity_id"), "name": e.get("name"), "role": e.get("role")}
                for e in context.entities
            ],
        }

    def _generate_generic_explanation(
        self,
        context: ExplanationContext,
    ) -> str:
        """Generate a generic explanation for unknown signal types."""
        qualifier = get_certainty_qualifier(context.confidence.point_estimate)

        entity_names = [e.get("name", "Unknown") for e in context.entities[:3]]
        entity_text = ", ".join(entity_names)
        if len(context.entities) > 3:
            entity_text += f" and {len(context.entities) - 3} other(s)"

        return (
            f"{qualifier} a {context.signal_type} pattern involving {entity_text}. "
            f"This observation is based on {len(context.evidence)} evidence item(s) "
            f"with a confidence of {context.confidence.point_estimate:.0%}."
        )


# =============================================================================
# Utility Functions
# =============================================================================


def explain_finding(
    finding_id: UUID,
    signal_type: str,
    entities: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    base_confidence: float,
    signal_strength: float,
    **additional_data,
) -> dict[str, Any]:
    """Convenience function to generate explanation for a finding.

    Args:
        finding_id: UUID of the finding
        signal_type: Type of signal detected
        entities: Entities involved in finding
        evidence: Supporting evidence items
        base_confidence: Base confidence estimate
        signal_strength: Strength of detected signal
        **additional_data: Additional context for template filling

    Returns:
        Complete explanation dictionary
    """
    # Calculate confidence band
    confidence = calculate_confidence_band(
        base_confidence=base_confidence,
        evidence_count=len(evidence),
        signal_strength=signal_strength,
    )

    # Create context
    context = ExplanationContext(
        signal_type=signal_type,
        entities=entities,
        evidence=evidence,
        confidence=confidence,
        additional_data=additional_data,
    )

    # Generate explanation
    generator = ExplanationGenerator()
    explanation = generator.generate_full_explanation(context)
    explanation["finding_id"] = str(finding_id)

    return explanation
