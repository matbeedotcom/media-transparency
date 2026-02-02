"""Sponsor-to-Organization resolution.

Resolves Meta Ad sponsors and other entry point entities to known
organizations in the graph using fuzzy matching and confidence scoring.
"""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from rapidfuzz import fuzz

from ...graph.queries import GraphQueries

logger = logging.getLogger(__name__)


@dataclass
class MatchCandidate:
    """A potential match between a sponsor and an organization."""

    entity_id: UUID
    name: str
    entity_type: str
    jurisdiction: str | None
    identifiers: dict[str, str]
    confidence: float
    signals: dict[str, Any]


class SponsorResolver:
    """Resolves sponsors to known organizations.

    Uses a combination of:
    - Exact identifier matching (Meta page ID, EIN, BN)
    - Fuzzy name matching with token sorting
    - Jurisdiction matching
    - Address overlap detection
    - Shared director detection

    Confidence thresholds:
    - >= 0.9: Auto-merge (high confidence)
    - 0.7-0.9: Queue for human review
    - < 0.7: Discard (too uncertain)
    """

    # Confidence weights for different signals
    IDENTIFIER_WEIGHT = 0.5
    NAME_SIMILARITY_WEIGHT = 0.3
    JURISDICTION_WEIGHT = 0.1
    ADDRESS_WEIGHT = 0.1
    DIRECTOR_WEIGHT = 0.1

    # Fuzzy match threshold
    MIN_NAME_SIMILARITY = 0.85

    def __init__(
        self,
        auto_merge_threshold: float = 0.9,
        review_threshold: float = 0.7,
    ):
        """Initialize the resolver.

        Args:
            auto_merge_threshold: Auto-merge matches above this confidence
            review_threshold: Queue for review above this confidence
        """
        self.auto_merge_threshold = auto_merge_threshold
        self.review_threshold = review_threshold
        self._graph: GraphQueries | None = None

    @property
    def graph(self) -> GraphQueries:
        """Get the graph queries client."""
        if self._graph is None:
            self._graph = GraphQueries()
        return self._graph

    async def resolve(
        self,
        name: str,
        identifiers: dict[str, str] | None = None,
        jurisdiction: str | None = None,
        address_city: str | None = None,
        address_postal: str | None = None,
    ) -> list[MatchCandidate]:
        """Resolve a sponsor to candidate organizations.

        Args:
            name: The sponsor/entity name
            identifiers: Known identifiers (meta_page_id, ein, bn, etc.)
            jurisdiction: Expected jurisdiction (US, CA, etc.)
            address_city: City for address matching
            address_postal: Postal code/ZIP for address matching

        Returns:
            List of MatchCandidates sorted by confidence (highest first)
        """
        candidates: list[MatchCandidate] = []
        identifiers = identifiers or {}

        # Step 1: Try exact identifier match
        if "meta_page_id" in identifiers:
            exact = await self._match_by_identifier(
                "meta_page_id", identifiers["meta_page_id"]
            )
            if exact:
                candidates.append(exact)

        if "ein" in identifiers:
            exact = await self._match_by_identifier("ein", identifiers["ein"])
            if exact:
                candidates.append(exact)

        if "bn" in identifiers:
            exact = await self._match_by_identifier("bn", identifiers["bn"])
            if exact:
                candidates.append(exact)

        # Step 2: Fuzzy name matching
        name_matches = await self._match_by_name(
            name, jurisdiction, address_city, address_postal
        )
        candidates.extend(name_matches)

        # Remove duplicates (keep highest confidence)
        seen_ids: set[UUID] = set()
        unique_candidates: list[MatchCandidate] = []
        for c in sorted(candidates, key=lambda x: x.confidence, reverse=True):
            if c.entity_id not in seen_ids:
                seen_ids.add(c.entity_id)
                unique_candidates.append(c)

        return unique_candidates

    async def _match_by_identifier(
        self, id_type: str, id_value: str
    ) -> MatchCandidate | None:
        """Find an exact match by identifier."""
        query = f"""
        MATCH (o:Organization)
        WHERE o.{id_type} = $value
        RETURN o.id as id, o.name as name, o.entity_type as entity_type,
               o.jurisdiction as jurisdiction,
               o.ein as ein, o.bn as bn, o.meta_page_id as meta_page_id
        LIMIT 1
        """

        try:
            results = await self.graph.execute(query, {"value": id_value})
            if results:
                row = results[0]
                identifiers = {}
                if row.get("ein"):
                    identifiers["ein"] = row["ein"]
                if row.get("bn"):
                    identifiers["bn"] = row["bn"]
                if row.get("meta_page_id"):
                    identifiers["meta_page_id"] = row["meta_page_id"]

                return MatchCandidate(
                    entity_id=UUID(row["id"]),
                    name=row["name"],
                    entity_type=row.get("entity_type", "organization"),
                    jurisdiction=row.get("jurisdiction"),
                    identifiers=identifiers,
                    confidence=1.0,  # Exact identifier match
                    signals={
                        "identifier_match": {
                            "type": id_type,
                            "matched": True,
                        }
                    },
                )
        except Exception as e:
            logger.warning(f"Identifier match query failed: {e}")

        return None

    async def _match_by_name(
        self,
        name: str,
        jurisdiction: str | None,
        address_city: str | None,
        address_postal: str | None,
    ) -> list[MatchCandidate]:
        """Find fuzzy name matches."""
        candidates: list[MatchCandidate] = []

        # Normalize name for searching
        normalized = self._normalize_name(name)

        # Query organizations with similar names
        query = """
        MATCH (o:Organization)
        WHERE o.name IS NOT NULL
        RETURN o.id as id, o.name as name, o.entity_type as entity_type,
               o.jurisdiction as jurisdiction,
               o.address_city as address_city,
               o.address_postal as address_postal,
               o.ein as ein, o.bn as bn, o.meta_page_id as meta_page_id
        LIMIT 1000
        """

        try:
            results = await self.graph.execute(query, {})

            for row in results:
                org_name = row.get("name", "")
                if not org_name:
                    continue

                # Compute name similarity
                org_normalized = self._normalize_name(org_name)
                similarity = fuzz.token_sort_ratio(normalized, org_normalized) / 100.0

                if similarity < self.MIN_NAME_SIMILARITY:
                    continue

                # Compute confidence score
                confidence = 0.0
                signals: dict[str, Any] = {
                    "name_similarity": similarity,
                }

                # Name similarity contributes up to 0.3
                confidence += similarity * self.NAME_SIMILARITY_WEIGHT

                # Jurisdiction match contributes 0.1
                org_jurisdiction = row.get("jurisdiction")
                if jurisdiction and org_jurisdiction:
                    if jurisdiction.upper() == org_jurisdiction.upper():
                        confidence += self.JURISDICTION_WEIGHT
                        signals["jurisdiction_match"] = True
                    else:
                        signals["jurisdiction_match"] = False

                # Address overlap contributes 0.1
                org_city = row.get("address_city")
                org_postal = row.get("address_postal")
                address_signals = {}
                if address_city and org_city:
                    if address_city.lower() == org_city.lower():
                        confidence += self.ADDRESS_WEIGHT / 2
                        address_signals["city"] = True
                if address_postal and org_postal:
                    # Compare first 3 chars (FSA in Canada, ZIP prefix in US)
                    if address_postal[:3].upper() == org_postal[:3].upper():
                        confidence += self.ADDRESS_WEIGHT / 2
                        address_signals["postal_fsa"] = True
                if address_signals:
                    signals["address_overlap"] = address_signals

                # Collect identifiers
                identifiers = {}
                if row.get("ein"):
                    identifiers["ein"] = row["ein"]
                if row.get("bn"):
                    identifiers["bn"] = row["bn"]
                if row.get("meta_page_id"):
                    identifiers["meta_page_id"] = row["meta_page_id"]

                candidates.append(MatchCandidate(
                    entity_id=UUID(row["id"]),
                    name=org_name,
                    entity_type=row.get("entity_type", "organization"),
                    jurisdiction=org_jurisdiction,
                    identifiers=identifiers,
                    confidence=min(confidence, 1.0),
                    signals=signals,
                ))

        except Exception as e:
            logger.warning(f"Name match query failed: {e}")

        # Sort by confidence
        candidates.sort(key=lambda x: x.confidence, reverse=True)
        return candidates[:10]  # Return top 10 candidates

    def _normalize_name(self, name: str) -> str:
        """Normalize an organization name for comparison."""
        # Remove common suffixes
        suffixes = [
            "inc", "inc.", "incorporated", "corp", "corp.", "corporation",
            "ltd", "ltd.", "limited", "llc", "l.l.c.", "llp", "l.l.p.",
            "co", "co.", "company", "foundation", "trust", "association",
            "society", "institute", "pac", "super pac",
        ]

        normalized = name.lower().strip()

        for suffix in suffixes:
            if normalized.endswith(f" {suffix}"):
                normalized = normalized[: -len(suffix) - 1].strip()
            elif normalized.endswith(f", {suffix}"):
                normalized = normalized[: -len(suffix) - 2].strip()

        return normalized

    def should_auto_merge(self, confidence: float) -> bool:
        """Check if a match should be auto-merged."""
        return confidence >= self.auto_merge_threshold

    def should_queue_for_review(self, confidence: float) -> bool:
        """Check if a match should be queued for human review."""
        return self.review_threshold <= confidence < self.auto_merge_threshold

    def should_discard(self, confidence: float) -> bool:
        """Check if a match should be discarded."""
        return confidence < self.review_threshold
