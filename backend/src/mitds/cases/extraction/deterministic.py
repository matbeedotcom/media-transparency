"""Deterministic entity extraction.

Pattern-based extraction for identifiers and organization names
from unstructured text.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExtractedEntity:
    """An entity extracted from text."""

    entity_type: str  # organization, person, identifier
    value: str
    identifier_type: str | None  # ein, bn, domain, name, etc.
    confidence: float
    start: int
    end: int
    context: str | None = None


class DeterministicExtractor:
    """Pattern-based entity extractor.

    Extracts:
    - EIN (XX-XXXXXXX format)
    - BN (9 digits + RR + 4 digits)
    - Domain names
    - Organization names (legal suffixes)
    - Addresses (city, state/province, postal code)
    """

    # EIN pattern: XX-XXXXXXX
    EIN_PATTERN = re.compile(r"\b(\d{2}-\d{7})\b")

    # BN pattern: 9 digits + RR + 4 digits (e.g., 123456789RR0001)
    BN_PATTERN = re.compile(r"\b(\d{9}[A-Z]{2}\d{4})\b", re.IGNORECASE)

    # Domain pattern
    DOMAIN_PATTERN = re.compile(
        r"\b([a-zA-Z0-9][-a-zA-Z0-9]*\.)+[a-zA-Z]{2,}\b"
    )

    # URL pattern
    URL_PATTERN = re.compile(
        r"https?://[^\s<>\"']+",
        re.IGNORECASE,
    )

    # Legal suffixes for organization detection
    LEGAL_SUFFIXES = [
        r"\bInc\.?\b",
        r"\bIncorporated\b",
        r"\bCorp\.?\b",
        r"\bCorporation\b",
        r"\bLtd\.?\b",
        r"\bLimited\b",
        r"\bLLC\b",
        r"\bL\.L\.C\.\b",
        r"\bLLP\b",
        r"\bL\.L\.P\.\b",
        r"\bCo\.?\b",
        r"\bCompany\b",
        r"\bFoundation\b",
        r"\bTrust\b",
        r"\bAssociation\b",
        r"\bSociety\b",
        r"\bInstitute\b",
        r"\bPAC\b",
        r"\bSuper PAC\b",
        r"\bCommittee\b",
        r"\bFund\b",
        r"\bOrganization\b",
        r"\bOrganisation\b",
    ]

    # Pattern for organization names with legal suffixes
    # Use non-greedy match and stop at common boundaries
    ORG_PATTERN = re.compile(
        r"([A-Z][A-Za-z0-9\s\-\&\']+?(?:" +
        "|".join(LEGAL_SUFFIXES) +
        r"))",
        re.IGNORECASE,
    )

    # Canadian postal code pattern
    CA_POSTAL_PATTERN = re.compile(
        r"\b([A-Z]\d[A-Z][\s-]?\d[A-Z]\d)\b",
        re.IGNORECASE,
    )

    # US ZIP code pattern
    US_ZIP_PATTERN = re.compile(
        r"\b(\d{5}(?:-\d{4})?)\b"
    )

    def __init__(self):
        """Initialize the extractor."""
        pass

    def extract(self, text: str) -> list[ExtractedEntity]:
        """Extract entities from text.

        Args:
            text: The text to extract entities from

        Returns:
            List of extracted entities
        """
        entities: list[ExtractedEntity] = []

        # Extract EINs
        entities.extend(self._extract_eins(text))

        # Extract BNs
        entities.extend(self._extract_bns(text))

        # Extract organization names
        entities.extend(self._extract_organizations(text))

        # Extract domains (but not from URLs)
        entities.extend(self._extract_domains(text))

        # Remove duplicates (same value at same position)
        seen: set[tuple[str, int]] = set()
        unique_entities: list[ExtractedEntity] = []
        for entity in entities:
            key = (entity.value.lower(), entity.start)
            if key not in seen:
                seen.add(key)
                unique_entities.append(entity)

        return unique_entities

    def _extract_eins(self, text: str) -> list[ExtractedEntity]:
        """Extract EIN identifiers."""
        entities = []
        for match in self.EIN_PATTERN.finditer(text):
            entities.append(ExtractedEntity(
                entity_type="identifier",
                value=match.group(1),
                identifier_type="ein",
                confidence=1.0,
                start=match.start(),
                end=match.end(),
                context=self._get_context(text, match.start(), match.end()),
            ))
        return entities

    def _extract_bns(self, text: str) -> list[ExtractedEntity]:
        """Extract Canadian Business Numbers."""
        entities = []
        for match in self.BN_PATTERN.finditer(text):
            # Normalize to uppercase
            bn = match.group(1).upper()
            entities.append(ExtractedEntity(
                entity_type="identifier",
                value=bn,
                identifier_type="bn",
                confidence=1.0,
                start=match.start(),
                end=match.end(),
                context=self._get_context(text, match.start(), match.end()),
            ))
        return entities

    def _extract_organizations(self, text: str) -> list[ExtractedEntity]:
        """Extract organization names by legal suffix."""
        entities = []
        for match in self.ORG_PATTERN.finditer(text):
            name = match.group(1).strip()
            # Filter out too short names
            if len(name) < 5:
                continue
            # Filter out names that are just the suffix
            if name.lower() in [s.lower().strip(r"\b") for s in self.LEGAL_SUFFIXES]:
                continue

            entities.append(ExtractedEntity(
                entity_type="organization",
                value=name,
                identifier_type="name",
                confidence=0.85,  # Lower confidence since pattern-based
                start=match.start(),
                end=match.end(),
                context=self._get_context(text, match.start(), match.end()),
            ))
        return entities

    def _extract_domains(self, text: str) -> list[ExtractedEntity]:
        """Extract domain names (excluding those in URLs)."""
        entities = []

        # Find all URLs first to exclude their domains
        url_ranges: set[tuple[int, int]] = set()
        for match in self.URL_PATTERN.finditer(text):
            url_ranges.add((match.start(), match.end()))

        for match in self.DOMAIN_PATTERN.finditer(text):
            # Skip if inside a URL
            in_url = any(
                start <= match.start() < end
                for start, end in url_ranges
            )
            if in_url:
                continue

            domain = match.group(0).lower()

            # Skip common non-organization domains
            skip_domains = [
                "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
                "example.com", "test.com", "localhost.com",
            ]
            if domain in skip_domains:
                continue

            entities.append(ExtractedEntity(
                entity_type="identifier",
                value=domain,
                identifier_type="domain",
                confidence=0.8,
                start=match.start(),
                end=match.end(),
                context=self._get_context(text, match.start(), match.end()),
            ))

        return entities

    def _get_context(self, text: str, start: int, end: int, window: int = 50) -> str:
        """Get surrounding context for an extraction."""
        ctx_start = max(0, start - window)
        ctx_end = min(len(text), end + window)

        prefix = "..." if ctx_start > 0 else ""
        suffix = "..." if ctx_end < len(text) else ""

        return prefix + text[ctx_start:ctx_end].strip() + suffix


# Singleton instance
_extractor: DeterministicExtractor | None = None


def get_deterministic_extractor() -> DeterministicExtractor:
    """Get the deterministic extractor singleton."""
    global _extractor
    if _extractor is None:
        _extractor = DeterministicExtractor()
    return _extractor
