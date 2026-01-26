"""Non-accusatory language rules for MITDS reports.

Transforms findings and descriptions into neutral, factual language
suitable for civil society and journalistic use without implying
wrongdoing or malicious intent.
"""

import re
from dataclasses import dataclass
from typing import Callable


@dataclass
class LanguageRule:
    """A language transformation rule."""

    name: str
    description: str
    pattern: str
    replacement: str | Callable[[re.Match], str]
    flags: int = re.IGNORECASE


# =============================================================================
# Accusatory to Neutral Transformations
# =============================================================================


ACCUSATORY_PATTERNS = [
    LanguageRule(
        name="coordinated_to_concurrent",
        description="Replace 'coordinated' with more neutral phrasing",
        pattern=r"\bcoordinat(ed|ing|ion)\b",
        replacement="concurrent",
    ),
    LanguageRule(
        name="collusion_to_overlap",
        description="Replace 'collusion' with factual description",
        pattern=r"\bcollusion\b",
        replacement="overlap in activities",
    ),
    LanguageRule(
        name="network_to_relationships",
        description="Replace 'network' when implying conspiracy",
        pattern=r"\b(dark|shadow|hidden)\s+network\b",
        replacement="relationship pattern",
    ),
    LanguageRule(
        name="scheme_to_arrangement",
        description="Replace 'scheme' with neutral term",
        pattern=r"\bscheme\b",
        replacement="arrangement",
    ),
    LanguageRule(
        name="shell_company_to_entity",
        description="Replace 'shell company' with factual description",
        pattern=r"\bshell\s+compan(y|ies)\b",
        replacement="limited-activity entity",
    ),
    LanguageRule(
        name="front_group_to_affiliated",
        description="Replace 'front group' with neutral term",
        pattern=r"\bfront\s+group\b",
        replacement="affiliated organization",
    ),
    LanguageRule(
        name="puppet_to_associated",
        description="Replace 'puppet' terminology",
        pattern=r"\bpuppet\s+(organization|outlet|entity)\b",
        replacement=r"associated \1",
    ),
    LanguageRule(
        name="laundering_to_flow",
        description="Replace 'laundering' with factual flow description",
        pattern=r"\b(money\s+)?launder(ing|ed)\b",
        replacement="fund flow pattern",
    ),
    LanguageRule(
        name="astroturf_to_organized",
        description="Replace 'astroturf' with factual description",
        pattern=r"\bastroturf(ing)?\b",
        replacement="organized campaign activity",
    ),
    LanguageRule(
        name="propaganda_to_messaging",
        description="Replace 'propaganda' with neutral term",
        pattern=r"\bpropaganda\b",
        replacement="messaging",
    ),
    LanguageRule(
        name="manipulation_to_influence",
        description="Replace 'manipulation' with factual term",
        pattern=r"\bmanipulat(ion|ing|ed)\b",
        replacement="influence",
    ),
    LanguageRule(
        name="deceive_to_present",
        description="Replace 'deceive' and variations",
        pattern=r"\bdeceiv(e|ing|ed)\b",
        replacement="present",
    ),
    LanguageRule(
        name="hide_to_not_disclose",
        description="Replace 'hide/hidden' with factual description",
        pattern=r"\b(hid(e|den|ing)|conceal(ed|ing)?)\b",
        replacement="not publicly disclosed",
    ),
    LanguageRule(
        name="secret_to_undisclosed",
        description="Replace 'secret' with neutral term",
        pattern=r"\bsecret(ly)?\b",
        replacement="undisclosed",
    ),
    LanguageRule(
        name="expose_to_document",
        description="Replace 'expose' with factual term",
        pattern=r"\bexpos(e|ed|ing)\b",
        replacement="document",
    ),
    LanguageRule(
        name="corrupt_to_concerning",
        description="Replace 'corrupt' with factual observation",
        pattern=r"\bcorrupt(ion|ed)?\b",
        replacement="concerning pattern",
    ),
    LanguageRule(
        name="illegal_to_potentially",
        description="Soften legal claims without evidence",
        pattern=r"\billegal(ly)?\b",
        replacement="potentially non-compliant",
    ),
    LanguageRule(
        name="criminal_to_requires_review",
        description="Replace criminal allegations",
        pattern=r"\bcriminal\b",
        replacement="requiring legal review",
    ),
]


# =============================================================================
# Certainty Modifiers
# =============================================================================


CERTAINTY_QUALIFIERS = {
    "high": [
        "The data shows",
        "Records indicate",
        "Analysis reveals",
        "Documentation confirms",
    ],
    "medium": [
        "The data suggests",
        "Records appear to indicate",
        "Analysis suggests",
        "Available information indicates",
    ],
    "low": [
        "The data may suggest",
        "Limited records indicate",
        "Preliminary analysis suggests",
        "Available information may indicate",
    ],
}


def get_certainty_qualifier(confidence: float) -> str:
    """Get an appropriate certainty qualifier based on confidence level."""
    import random

    if confidence >= 0.8:
        qualifiers = CERTAINTY_QUALIFIERS["high"]
    elif confidence >= 0.5:
        qualifiers = CERTAINTY_QUALIFIERS["medium"]
    else:
        qualifiers = CERTAINTY_QUALIFIERS["low"]

    return random.choice(qualifiers)


# =============================================================================
# Non-Accusatory Sentence Templates
# =============================================================================


FINDING_TEMPLATES = {
    "funding_concentration": [
        "{qualifier} that {funder} provided funding to {count} organizations in the analyzed set, "
        "representing {percentage:.0%} of their total disclosed grants.",
        "{qualifier} a pattern of funding from {funder} to {count} related entities. "
        "This concentration may reflect shared mission alignment or other factors.",
    ],
    "board_overlap": [
        "{qualifier} that {person} serves on the boards of {count} organizations in this analysis. "
        "Board service across multiple organizations is common in many sectors.",
        "{qualifier} {person} holds positions at {count} entities included in this review. "
        "The significance of this overlap depends on the nature of these organizations.",
    ],
    "temporal_pattern": [
        "{qualifier} that several organizational actions occurred within a similar timeframe. "
        "This temporal proximity may or may not indicate coordination.",
        "{qualifier} timing patterns across {count} entities that show {description}. "
        "Multiple explanations for this timing are possible.",
    ],
    "infrastructure_sharing": [
        "{qualifier} shared technical infrastructure between {domain_a} and {domain_b}, "
        "including {shared_elements}. This may indicate shared management or common vendors.",
        "{qualifier} that {count} domains share {infrastructure_type}. "
        "Infrastructure sharing is common and may have various explanations.",
    ],
    "ownership_chain": [
        "{qualifier} an ownership relationship between {owner} and {owned}, "
        "as documented in {source}.",
        "{qualifier} that {owned} appears to be affiliated with {owner} "
        "based on available corporate records.",
    ],
}


def generate_finding_text(
    finding_type: str,
    confidence: float,
    **kwargs,
) -> str:
    """Generate non-accusatory finding text from a template."""
    import random

    templates = FINDING_TEMPLATES.get(finding_type, [])
    if not templates:
        return f"Analysis identified a {finding_type} pattern."

    template = random.choice(templates)
    qualifier = get_certainty_qualifier(confidence)

    return template.format(qualifier=qualifier, **kwargs)


# =============================================================================
# Language Transformer
# =============================================================================


class LanguageTransformer:
    """Transforms text to use non-accusatory language."""

    def __init__(self, rules: list[LanguageRule] | None = None):
        self.rules = rules or ACCUSATORY_PATTERNS

    def transform(self, text: str) -> str:
        """Apply all transformation rules to text."""
        result = text

        for rule in self.rules:
            if callable(rule.replacement):
                result = re.sub(rule.pattern, rule.replacement, result, flags=rule.flags)
            else:
                result = re.sub(rule.pattern, rule.replacement, result, flags=rule.flags)

        return result

    def check(self, text: str) -> list[dict]:
        """Check text for accusatory language and return matches."""
        issues = []

        for rule in self.rules:
            matches = re.finditer(rule.pattern, text, flags=rule.flags)
            for match in matches:
                issues.append({
                    "rule": rule.name,
                    "description": rule.description,
                    "matched_text": match.group(),
                    "position": match.span(),
                    "suggestion": rule.replacement if isinstance(rule.replacement, str) else None,
                })

        return issues

    def transform_with_report(self, text: str) -> tuple[str, list[dict]]:
        """Transform text and return both result and change report."""
        issues = self.check(text)
        transformed = self.transform(text)
        return transformed, issues


# =============================================================================
# Disclaimer Generator
# =============================================================================


def generate_methodology_disclaimer() -> str:
    """Generate standard methodology disclaimer for reports."""
    return """
**Methodology and Limitations**

This analysis is based on publicly available data including regulatory filings,
corporate registries, and documented organizational relationships. The findings
represent structural observations and do not constitute claims of wrongdoing,
coordination, or malicious intent.

Key limitations include:
- Data sources may be incomplete or contain errors
- Relationships between entities may have legitimate explanations not captured
- Temporal patterns may result from coincidence or industry-wide trends
- Board and personnel overlaps are common in many sectors

This report is provided for informational purposes. Users should conduct
independent verification before drawing conclusions or taking action.
""".strip()


def generate_finding_disclaimer() -> str:
    """Generate standard disclaimer for individual findings."""
    return (
        "This observation is based on structural analysis of available data. "
        "Alternative explanations may exist, and this finding does not "
        "constitute an allegation of wrongdoing."
    )


# =============================================================================
# Hedging Utilities
# =============================================================================


def add_hedging(text: str, confidence: float) -> str:
    """Add appropriate hedging language based on confidence level."""
    hedges = {
        "high": ["Based on strong evidence, ", "Documentation shows that ", ""],
        "medium": ["Available data suggests that ", "It appears that ", "Evidence indicates that "],
        "low": ["Limited data may suggest that ", "Preliminary analysis indicates that ", "It is possible that "],
    }

    if confidence >= 0.8:
        hedge_list = hedges["high"]
    elif confidence >= 0.5:
        hedge_list = hedges["medium"]
    else:
        hedge_list = hedges["low"]

    import random
    hedge = random.choice(hedge_list)

    # Don't double-hedge if text already starts with hedging
    hedge_patterns = [
        r"^(based on|available|evidence|data|it appears|it is possible)",
        r"^(suggests?|indicates?|shows?|may|might|could)",
    ]
    for pattern in hedge_patterns:
        if re.match(pattern, text, re.IGNORECASE):
            return text

    return hedge + text.lower() if hedge else text


def remove_absolute_claims(text: str) -> str:
    """Remove or soften absolute claims in text."""
    absolute_patterns = [
        (r"\bdefinitely\b", "likely"),
        (r"\bcertainly\b", "appears to"),
        (r"\bproves?\b", "suggests"),
        (r"\bconfirms?\b", "is consistent with"),
        (r"\bdemonstrates?\b", "indicates"),
        (r"\bestablishes?\b", "suggests"),
        (r"\bundeniably\b", ""),
        (r"\bclearly\b", ""),
        (r"\bobviously\b", ""),
        (r"\bwithout\s+doubt\b", ""),
        (r"\bwithout\s+question\b", ""),
    ]

    result = text
    for pattern, replacement in absolute_patterns:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Clean up multiple spaces
    result = re.sub(r"\s+", " ", result).strip()
    return result
