"""Entity matching strategies for MITDS.

Implements multiple matching strategies:
1. Deterministic: Exact ID matching (EIN, BN)
2. Fuzzy: Name normalization with edit distance
3. Embedding: (Future) Semantic similarity matching

See research.md for matching strategy details.
"""

import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from rapidfuzz import fuzz, process

from ..logging import get_context_logger

logger = get_context_logger(__name__)


class MatchStrategy(str, Enum):
    """Available matching strategies."""

    DETERMINISTIC = "deterministic"
    FUZZY = "fuzzy"
    EMBEDDING = "embedding"


class MatchCandidate(BaseModel):
    """A potential match candidate."""

    entity_id: UUID
    entity_type: str
    name: str
    identifiers: dict[str, str] = Field(default_factory=dict)
    attributes: dict[str, Any] = Field(default_factory=dict)


class MatchResult(BaseModel):
    """Result of a matching operation."""

    source: MatchCandidate
    target: MatchCandidate
    strategy: MatchStrategy
    confidence: float = Field(ge=0.0, le=1.0)
    match_details: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_high_confidence(self) -> bool:
        """Check if this is a high confidence match (>0.9)."""
        return self.confidence >= 0.9

    @property
    def is_low_confidence(self) -> bool:
        """Check if this is a low confidence match (<0.7)."""
        return self.confidence < 0.7


class BaseMatcher(ABC):
    """Abstract base class for entity matchers."""

    def __init__(self, strategy: MatchStrategy):
        self.strategy = strategy

    @abstractmethod
    def find_matches(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        threshold: float = 0.7,
    ) -> list[MatchResult]:
        """Find matching candidates for a source entity.

        Args:
            source: The entity to find matches for
            candidates: List of potential match candidates
            threshold: Minimum confidence threshold

        Returns:
            List of match results sorted by confidence (descending)
        """
        ...


class DeterministicMatcher(BaseMatcher):
    """Deterministic matcher using exact identifier matching.

    Matches on stable identifiers:
    - EIN (US Employer Identification Number)
    - BN (Canadian Business Number)
    - OpenCorporates company number + jurisdiction

    Confidence is always 1.0 for exact matches.
    """

    def __init__(self):
        super().__init__(MatchStrategy.DETERMINISTIC)

    def find_matches(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        threshold: float = 0.7,
    ) -> list[MatchResult]:
        """Find exact identifier matches."""
        results = []

        for candidate in candidates:
            # Skip self-matching
            if candidate.entity_id == source.entity_id:
                continue

            match_details = {}
            matched = False

            # Check EIN match
            source_ein = source.identifiers.get("ein")
            candidate_ein = candidate.identifiers.get("ein")
            if source_ein and candidate_ein:
                if self._normalize_ein(source_ein) == self._normalize_ein(candidate_ein):
                    matched = True
                    match_details["matched_identifier"] = "ein"
                    match_details["ein"] = source_ein

            # Check BN match
            source_bn = source.identifiers.get("bn")
            candidate_bn = candidate.identifiers.get("bn")
            if source_bn and candidate_bn:
                if self._normalize_bn(source_bn) == self._normalize_bn(candidate_bn):
                    matched = True
                    match_details["matched_identifier"] = "bn"
                    match_details["bn"] = source_bn

            # Check OpenCorporates match
            source_oc = source.identifiers.get("opencorp_id")
            candidate_oc = candidate.identifiers.get("opencorp_id")
            if source_oc and candidate_oc:
                if source_oc == candidate_oc:
                    matched = True
                    match_details["matched_identifier"] = "opencorp_id"
                    match_details["opencorp_id"] = source_oc

            if matched:
                results.append(
                    MatchResult(
                        source=source,
                        target=candidate,
                        strategy=self.strategy,
                        confidence=1.0,
                        match_details=match_details,
                    )
                )

        return sorted(results, key=lambda r: r.confidence, reverse=True)

    def _normalize_ein(self, ein: str) -> str:
        """Normalize EIN to standard format without hyphens."""
        return re.sub(r"[^0-9]", "", ein)

    def _normalize_bn(self, bn: str) -> str:
        """Normalize BN to standard format."""
        return re.sub(r"[\s-]", "", bn.upper())


class FuzzyMatcher(BaseMatcher):
    """Fuzzy matcher using name normalization and edit distance.

    Uses RapidFuzz for fast fuzzy string matching with:
    - Name normalization (remove Inc., Ltd., etc.)
    - Weighted ratio scoring
    - Address matching for disambiguation

    Confidence based on:
    - Normalized name match: 0.7-0.9
    - Same city/state: +0.05
    - Similar address: +0.05
    """

    # Common suffixes to strip from organization names
    ORG_SUFFIXES = [
        r"\bInc\.?$",
        r"\bIncorporated$",
        r"\bLtd\.?$",
        r"\bLimited$",
        r"\bLLC$",
        r"\bL\.?L\.?C\.?$",
        r"\bLLP$",
        r"\bL\.?L\.?P\.?$",
        r"\bCorp\.?$",
        r"\bCorporation$",
        r"\bFoundation$",
        r"\bAssociation$",
        r"\bAssoc\.?$",
        r"\bCo\.?$",
        r"\bCompany$",
        r"\bPC$",
        r"\bPLC$",
        r"\bPLLC$",
        r"\bPA$",
        r"\bP\.?A\.?$",
        r"\bNA$",
        r"\bN\.?A\.?$",
    ]

    def __init__(self, min_score: int = 85):
        """Initialize fuzzy matcher.

        Args:
            min_score: Minimum fuzzy match score (0-100)
        """
        super().__init__(MatchStrategy.FUZZY)
        self.min_score = min_score

    def find_matches(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        threshold: float = 0.7,
    ) -> list[MatchResult]:
        """Find fuzzy name matches."""
        results = []

        source_name = self._normalize_name(source.name)
        if not source_name:
            return results

        # Build candidate name list
        candidate_names = [
            (self._normalize_name(c.name), c)
            for c in candidates
            if c.entity_id != source.entity_id
        ]

        # Filter out empty names
        candidate_names = [(n, c) for n, c in candidate_names if n]

        if not candidate_names:
            return results

        # Find matches using rapidfuzz
        names = [n for n, _ in candidate_names]
        matches = process.extract(
            source_name,
            names,
            scorer=fuzz.WRatio,
            limit=10,
            score_cutoff=self.min_score,
        )

        for match_name, score, idx in matches:
            _, candidate = candidate_names[idx]

            # Base confidence from fuzzy score (0-100 → 0.5-0.9)
            base_confidence = 0.5 + (score / 100) * 0.4

            # Boost for location match
            location_boost = 0.0
            match_details = {
                "normalized_source": source_name,
                "normalized_target": match_name,
                "fuzzy_score": score,
            }

            # Check city match
            source_city = self._get_city(source)
            target_city = self._get_city(candidate)
            if source_city and target_city:
                if self._cities_match(source_city, target_city):
                    location_boost += 0.05
                    match_details["city_match"] = True

            # Check state/province match
            source_state = self._get_state(source)
            target_state = self._get_state(candidate)
            if source_state and target_state:
                if source_state.upper() == target_state.upper():
                    location_boost += 0.05
                    match_details["state_match"] = True

            # Calculate final confidence
            confidence = min(base_confidence + location_boost, 0.95)

            if confidence >= threshold:
                results.append(
                    MatchResult(
                        source=source,
                        target=candidate,
                        strategy=self.strategy,
                        confidence=confidence,
                        match_details=match_details,
                    )
                )

        return sorted(results, key=lambda r: r.confidence, reverse=True)

    def _normalize_name(self, name: str) -> str:
        """Normalize organization name for matching."""
        if not name:
            return ""

        # Convert to uppercase
        normalized = name.upper()

        # Remove common suffixes
        for suffix in self.ORG_SUFFIXES:
            normalized = re.sub(suffix, "", normalized, flags=re.IGNORECASE)

        # Remove special characters
        normalized = re.sub(r"[^A-Z0-9\s]", "", normalized)

        # Collapse whitespace
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def _get_city(self, candidate: MatchCandidate) -> str | None:
        """Extract city from candidate attributes."""
        address = candidate.attributes.get("address", {})
        if isinstance(address, dict):
            return address.get("city")
        return candidate.attributes.get("city")

    def _get_state(self, candidate: MatchCandidate) -> str | None:
        """Extract state/province from candidate attributes."""
        address = candidate.attributes.get("address", {})
        if isinstance(address, dict):
            return address.get("state")
        return candidate.attributes.get("state")

    def _cities_match(self, city1: str, city2: str) -> bool:
        """Check if two cities match (fuzzy)."""
        c1 = re.sub(r"[^A-Z]", "", city1.upper())
        c2 = re.sub(r"[^A-Z]", "", city2.upper())

        if c1 == c2:
            return True

        # Allow fuzzy match for minor typos
        score = fuzz.ratio(c1, c2)
        return score >= 90


class EmbeddingMatcher(BaseMatcher):
    """Embedding-based matcher using semantic similarity.

    Uses sentence embeddings to find semantically similar entities,
    useful for:
    - Matching names with different word orderings
    - Matching across languages (if using multilingual model)
    - Finding conceptually similar organizations

    Confidence based on cosine similarity score.
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        batch_size: int = 32,
    ):
        """Initialize embedding matcher.

        Args:
            model_name: Sentence transformer model name
            batch_size: Batch size for encoding
        """
        super().__init__(MatchStrategy.EMBEDDING)
        self.model_name = model_name
        self.batch_size = batch_size
        self._model = None
        self._embeddings_cache: dict[str, Any] = {}

    @property
    def model(self):
        """Lazy load the sentence transformer model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name)
                logger.info(f"Loaded embedding model: {self.model_name}")
            except ImportError:
                logger.warning(
                    "sentence-transformers not installed. "
                    "Install with: pip install sentence-transformers"
                )
                raise
        return self._model

    def find_matches(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        threshold: float = 0.7,
    ) -> list[MatchResult]:
        """Find semantically similar matches using embeddings."""
        import numpy as np

        results = []

        # Filter out self-matches
        candidates = [c for c in candidates if c.entity_id != source.entity_id]
        if not candidates:
            return results

        # Build text representations
        source_text = self._build_entity_text(source)
        candidate_texts = [self._build_entity_text(c) for c in candidates]

        try:
            # Get embeddings
            source_embedding = self._get_embedding(source_text)
            candidate_embeddings = self._get_embeddings_batch(candidate_texts)

            # Calculate cosine similarities
            similarities = self._cosine_similarity(
                source_embedding, candidate_embeddings
            )

            # Build results for matches above threshold
            for i, (candidate, similarity) in enumerate(zip(candidates, similarities)):
                if similarity >= threshold:
                    # Adjust confidence to be slightly lower than fuzzy for same scores
                    # This reflects the less deterministic nature of embeddings
                    confidence = float(similarity) * 0.95

                    results.append(
                        MatchResult(
                            source=source,
                            target=candidate,
                            strategy=self.strategy,
                            confidence=confidence,
                            match_details={
                                "similarity_score": float(similarity),
                                "source_text": source_text[:100],
                                "target_text": candidate_texts[i][:100],
                                "model": self.model_name,
                            },
                        )
                    )

        except Exception as e:
            logger.error(f"Embedding matching failed: {e}")
            return results

        return sorted(results, key=lambda r: r.confidence, reverse=True)

    def _build_entity_text(self, candidate: MatchCandidate) -> str:
        """Build text representation for embedding.

        Combines name with relevant attributes to create
        a richer representation for semantic matching.
        """
        parts = [candidate.name]

        # Add entity type for context
        parts.append(candidate.entity_type.lower())

        # Add location info if available
        address = candidate.attributes.get("address", {})
        if isinstance(address, dict):
            if address.get("city"):
                parts.append(address["city"])
            if address.get("state"):
                parts.append(address["state"])
        elif candidate.attributes.get("city"):
            parts.append(candidate.attributes["city"])

        # Add description if available
        if candidate.attributes.get("description"):
            parts.append(candidate.attributes["description"][:200])

        # Add org type if available
        if candidate.attributes.get("org_type"):
            parts.append(candidate.attributes["org_type"])

        return " ".join(parts)

    def _get_embedding(self, text: str) -> Any:
        """Get embedding for a single text, with caching."""
        if text in self._embeddings_cache:
            return self._embeddings_cache[text]

        embedding = self.model.encode(text, convert_to_numpy=True)
        self._embeddings_cache[text] = embedding
        return embedding

    def _get_embeddings_batch(self, texts: list[str]) -> Any:
        """Get embeddings for multiple texts."""
        import numpy as np

        # Check cache for each text
        uncached_indices = []
        uncached_texts = []

        for i, text in enumerate(texts):
            if text not in self._embeddings_cache:
                uncached_indices.append(i)
                uncached_texts.append(text)

        # Encode uncached texts in batch
        if uncached_texts:
            new_embeddings = self.model.encode(
                uncached_texts,
                batch_size=self.batch_size,
                convert_to_numpy=True,
            )

            # Update cache
            for i, text in enumerate(uncached_texts):
                self._embeddings_cache[text] = new_embeddings[i]

        # Build result array in original order
        result = np.array([self._embeddings_cache[text] for text in texts])
        return result

    def _cosine_similarity(
        self,
        source_embedding: Any,
        candidate_embeddings: Any,
    ) -> Any:
        """Calculate cosine similarity between source and candidates."""
        import numpy as np

        # Normalize embeddings
        source_norm = source_embedding / np.linalg.norm(source_embedding)
        candidate_norms = candidate_embeddings / np.linalg.norm(
            candidate_embeddings, axis=1, keepdims=True
        )

        # Compute dot product (cosine similarity for normalized vectors)
        similarities = np.dot(candidate_norms, source_norm)
        return similarities

    def clear_cache(self):
        """Clear the embeddings cache."""
        self._embeddings_cache.clear()


class HybridMatcher:
    """Hybrid matcher combining multiple strategies.

    Uses a cascading approach:
    1. First tries deterministic matching (highest confidence)
    2. Falls back to fuzzy matching
    3. Finally tries embedding matching

    Results are combined and deduplicated.
    """

    def __init__(
        self,
        use_embedding: bool = True,
        fuzzy_min_score: int = 85,
        embedding_model: str = "all-MiniLM-L6-v2",
    ):
        """Initialize hybrid matcher.

        Args:
            use_embedding: Whether to use embedding matching
            fuzzy_min_score: Minimum fuzzy match score
            embedding_model: Sentence transformer model name
        """
        self.deterministic = DeterministicMatcher()
        self.fuzzy = FuzzyMatcher(min_score=fuzzy_min_score)
        self.embedding = EmbeddingMatcher(model_name=embedding_model) if use_embedding else None

    def find_matches(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        threshold: float = 0.7,
    ) -> list[MatchResult]:
        """Find matches using all strategies.

        Args:
            source: Entity to match
            candidates: Potential matches
            threshold: Minimum confidence threshold

        Returns:
            Combined and deduplicated match results
        """
        all_results = []
        matched_ids = set()

        # Strategy 1: Deterministic matching (highest priority)
        deterministic_results = self.deterministic.find_matches(
            source, candidates, threshold=0.0  # Accept any deterministic match
        )
        for result in deterministic_results:
            matched_ids.add(result.target.entity_id)
            all_results.append(result)

        # Strategy 2: Fuzzy matching
        remaining_candidates = [
            c for c in candidates if c.entity_id not in matched_ids
        ]
        fuzzy_results = self.fuzzy.find_matches(
            source, remaining_candidates, threshold=threshold
        )
        for result in fuzzy_results:
            if result.target.entity_id not in matched_ids:
                matched_ids.add(result.target.entity_id)
                all_results.append(result)

        # Strategy 3: Embedding matching (if available)
        if self.embedding:
            remaining_candidates = [
                c for c in candidates if c.entity_id not in matched_ids
            ]
            if remaining_candidates:
                try:
                    embedding_results = self.embedding.find_matches(
                        source, remaining_candidates, threshold=threshold
                    )
                    for result in embedding_results:
                        if result.target.entity_id not in matched_ids:
                            matched_ids.add(result.target.entity_id)
                            all_results.append(result)
                except Exception as e:
                    logger.warning(f"Embedding matching failed, skipping: {e}")

        return sorted(all_results, key=lambda r: r.confidence, reverse=True)


def normalize_organization_name(name: str) -> str:
    """Normalize an organization name for matching.

    Convenience function that uses FuzzyMatcher's normalization.

    Args:
        name: Organization name to normalize

    Returns:
        Normalized name
    """
    matcher = FuzzyMatcher()
    return matcher._normalize_name(name)


def normalize_ein(ein: str) -> str:
    """Normalize an EIN to digits only.

    Args:
        ein: EIN to normalize

    Returns:
        EIN as 9 digits
    """
    return re.sub(r"[^0-9]", "", ein)


def format_ein(ein: str) -> str:
    """Format an EIN as XX-XXXXXXX.

    Args:
        ein: EIN to format (9 digits)

    Returns:
        Formatted EIN
    """
    clean = normalize_ein(ein)
    if len(clean) == 9:
        return f"{clean[:2]}-{clean[2:]}"
    return ein


def normalize_bn(bn: str) -> str:
    """Normalize a Canadian Business Number.

    Args:
        bn: BN to normalize

    Returns:
        Normalized BN (123456789RR0001 format)
    """
    return re.sub(r"[\s-]", "", bn.upper())
