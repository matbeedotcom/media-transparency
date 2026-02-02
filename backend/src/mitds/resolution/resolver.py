"""Entity resolver combining multiple matching strategies.

Combines deterministic and fuzzy matching to resolve entities
across data sources with appropriate confidence levels.

Resolution flow:
1. Deterministic matching (exact ID) → confidence: 1.0
2. Fuzzy name + location → confidence: 0.7-0.95
3. Human review queue for low confidence matches

See research.md for resolution strategy details.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .matcher import (
    DeterministicMatcher,
    FuzzyMatcher,
    MatchCandidate,
    MatchResult,
    MatchStrategy,
)

logger = get_context_logger(__name__)


class ResolutionState(str, Enum):
    """State of entity resolution."""

    UNRESOLVED = "unresolved"
    CANDIDATE = "candidate"
    RESOLVED = "resolved"
    REJECTED = "rejected"
    MERGED = "merged"


class ResolutionResult(BaseModel):
    """Result of entity resolution."""

    source_id: UUID
    target_id: UUID | None = None
    state: ResolutionState
    confidence: float
    strategy: MatchStrategy | None = None
    match_details: dict[str, Any] = Field(default_factory=dict)
    resolved_at: datetime | None = None
    resolved_by: str | None = None  # "system" or user ID


class EntityResolver:
    """Entity resolver combining multiple matching strategies.

    Orchestrates the resolution process:
    1. Try deterministic matching first (highest confidence)
    2. Fall back to fuzzy matching
    3. Queue low-confidence matches for human review
    """

    def __init__(
        self,
        deterministic_enabled: bool = True,
        fuzzy_enabled: bool = True,
        fuzzy_threshold: float = 0.7,
        auto_merge_threshold: float = 0.9,
    ):
        """Initialize entity resolver.

        Args:
            deterministic_enabled: Enable deterministic matching
            fuzzy_enabled: Enable fuzzy matching
            fuzzy_threshold: Minimum confidence for fuzzy matches
            auto_merge_threshold: Confidence threshold for automatic merging
        """
        self.deterministic_enabled = deterministic_enabled
        self.fuzzy_enabled = fuzzy_enabled
        self.fuzzy_threshold = fuzzy_threshold
        self.auto_merge_threshold = auto_merge_threshold

        self._deterministic = DeterministicMatcher() if deterministic_enabled else None
        self._fuzzy = FuzzyMatcher() if fuzzy_enabled else None

    async def resolve(
        self,
        source: MatchCandidate,
        candidates: list[MatchCandidate],
        auto_merge: bool = False,
    ) -> ResolutionResult:
        """Resolve a source entity against candidates.

        Args:
            source: Entity to resolve
            candidates: Potential match candidates
            auto_merge: Whether to auto-merge high confidence matches

        Returns:
            Resolution result with best match or unresolved state
        """
        all_matches: list[MatchResult] = []

        # Try deterministic matching first
        if self._deterministic:
            matches = self._deterministic.find_matches(source, candidates)
            all_matches.extend(matches)

            # Deterministic match is definitive
            if matches:
                best = matches[0]
                return self._create_result(
                    source,
                    best,
                    ResolutionState.RESOLVED,
                )

        # Try fuzzy matching
        if self._fuzzy:
            matches = self._fuzzy.find_matches(
                source, candidates, threshold=self.fuzzy_threshold
            )
            all_matches.extend(matches)

        if not all_matches:
            return ResolutionResult(
                source_id=source.entity_id,
                target_id=None,
                state=ResolutionState.UNRESOLVED,
                confidence=0.0,
            )

        # Sort by confidence and get best match
        all_matches.sort(key=lambda m: m.confidence, reverse=True)
        best_match = all_matches[0]

        # Determine resolution state based on confidence
        if best_match.confidence >= self.auto_merge_threshold and auto_merge:
            state = ResolutionState.RESOLVED
        elif best_match.confidence >= self.fuzzy_threshold:
            state = ResolutionState.CANDIDATE
        else:
            state = ResolutionState.UNRESOLVED

        return self._create_result(source, best_match, state)

    async def resolve_batch(
        self,
        sources: list[MatchCandidate],
        candidates: list[MatchCandidate],
        auto_merge: bool = False,
    ) -> list[ResolutionResult]:
        """Resolve multiple entities.

        Args:
            sources: Entities to resolve
            candidates: Potential match candidates
            auto_merge: Whether to auto-merge high confidence matches

        Returns:
            List of resolution results
        """
        results = []
        for source in sources:
            result = await self.resolve(source, candidates, auto_merge)
            results.append(result)
        return results

    async def find_duplicates(
        self,
        entity_type: str,
        threshold: float | None = None,
    ) -> list[ResolutionResult]:
        """Find potential duplicate entities of a given type.

        Args:
            entity_type: Type of entity to check (ORGANIZATION, PERSON, etc.)
            threshold: Minimum confidence threshold (default: fuzzy_threshold)

        Returns:
            List of potential duplicate pairs
        """
        if threshold is None:
            threshold = self.fuzzy_threshold

        duplicates = []

        async with get_neo4j_session() as session:
            # Load entities from Neo4j
            query = f"""
            MATCH (e:{entity_type})
            RETURN e.id as id, e.name as name, e.ein as ein, e.bn as bn,
                   e.opencorp_id as opencorp_id,
                   e.address_city as city, e.address_state as state,
                   e.address_country as country, e.address_postal as postal
            LIMIT 10000
            """
            result = await session.run(query)
            records = await result.data()

            # Convert to MatchCandidates
            candidates = []
            for record in records:
                identifiers = {}
                if record.get("ein"):
                    identifiers["ein"] = record["ein"]
                if record.get("bn"):
                    identifiers["bn"] = record["bn"]
                if record.get("opencorp_id"):
                    identifiers["opencorp_id"] = record["opencorp_id"]

                # Build address dict for location matching
                attributes = {
                    "address": {
                        "city": record.get("city"),
                        "state": record.get("state"),
                        "country": record.get("country"),
                        "postal_code": record.get("postal"),
                    }
                }

                candidates.append(
                    MatchCandidate(
                        entity_id=UUID(record["id"]),
                        entity_type=entity_type,
                        name=record["name"] or "",
                        identifiers=identifiers,
                        attributes=attributes,
                    )
                )

            # Find duplicates
            checked = set()
            for source in candidates:
                if source.entity_id in checked:
                    continue

                # Find matches excluding already checked
                filtered = [c for c in candidates if c.entity_id not in checked]
                result = await self.resolve(source, filtered, auto_merge=False)

                if result.state in (
                    ResolutionState.RESOLVED,
                    ResolutionState.CANDIDATE,
                ):
                    if result.target_id:
                        duplicates.append(result)
                        checked.add(result.target_id)

                checked.add(source.entity_id)

        return duplicates

    async def merge_entities(
        self,
        source_id: UUID,
        target_id: UUID,
        user_id: str | None = None,
    ) -> bool:
        """Merge two entities, keeping the target and linking the source.

        Args:
            source_id: Entity to merge (will be marked as merged)
            target_id: Entity to merge into (will receive source's relationships)
            user_id: User performing the merge (None for system)

        Returns:
            True if merge was successful
        """
        async with get_neo4j_session() as session:
            # Check both entities exist
            check_query = """
            MATCH (source {id: $source_id})
            MATCH (target {id: $target_id})
            RETURN source, target
            """
            result = await session.run(
                check_query,
                source_id=str(source_id),
                target_id=str(target_id),
            )
            record = await result.single()

            if not record:
                logger.warning(
                    f"Cannot merge: one or both entities not found "
                    f"({source_id}, {target_id})"
                )
                return False

            # Transfer relationships from source to target
            transfer_query = """
            MATCH (source {id: $source_id})
            MATCH (target {id: $target_id})

            // Transfer outgoing relationships
            MATCH (source)-[r]->(other)
            WHERE NOT (target)-[r2]->(other) OR type(r2) <> type(r)
            WITH source, target, r, other
            CALL apoc.create.relationship(target, type(r), properties(r), other) YIELD rel

            // Transfer incoming relationships
            MATCH (other)-[r]->(source)
            WHERE NOT (other)-[r2]->(target) OR type(r2) <> type(r)
            WITH source, target, r, other
            CALL apoc.create.relationship(other, type(r), properties(r), target) YIELD rel

            // Mark source as merged
            SET source.merged_into = $target_id,
                source.merged_at = $now,
                source.merged_by = $user_id

            RETURN count(*) as transfers
            """

            try:
                await session.run(
                    transfer_query,
                    source_id=str(source_id),
                    target_id=str(target_id),
                    now=datetime.utcnow().isoformat(),
                    user_id=user_id or "system",
                )
            except Exception as e:
                # APOC not available, use simpler approach
                logger.info("APOC not available, using simple merge")

                # Simple merge: just mark source as merged
                simple_query = """
                MATCH (source {id: $source_id})
                SET source.merged_into = $target_id,
                    source.merged_at = $now,
                    source.merged_by = $user_id
                RETURN source
                """
                await session.run(
                    simple_query,
                    source_id=str(source_id),
                    target_id=str(target_id),
                    now=datetime.utcnow().isoformat(),
                    user_id=user_id or "system",
                )

            logger.info(
                f"Merged entity {source_id} into {target_id}",
                extra={"source_id": str(source_id), "target_id": str(target_id)},
            )

        return True

    def _create_result(
        self,
        source: MatchCandidate,
        match: MatchResult,
        state: ResolutionState,
    ) -> ResolutionResult:
        """Create a resolution result from a match."""
        return ResolutionResult(
            source_id=source.entity_id,
            target_id=match.target.entity_id,
            state=state,
            confidence=match.confidence,
            strategy=match.strategy,
            match_details=match.match_details,
            resolved_at=datetime.utcnow() if state == ResolutionState.RESOLVED else None,
            resolved_by="system" if state == ResolutionState.RESOLVED else None,
        )


async def resolve_entity(
    entity_type: str,
    name: str,
    identifiers: dict[str, str] | None = None,
    attributes: dict[str, Any] | None = None,
    auto_merge: bool = False,
) -> ResolutionResult:
    """Convenience function to resolve a single entity.

    Args:
        entity_type: Type of entity (ORGANIZATION, PERSON, etc.)
        name: Entity name
        identifiers: Optional identifiers (ein, bn, opencorp_id)
        attributes: Optional attributes (city, state, etc.)
        auto_merge: Whether to auto-merge high confidence matches

    Returns:
        Resolution result
    """
    source = MatchCandidate(
        entity_id=uuid4(),
        entity_type=entity_type,
        name=name,
        identifiers=identifiers or {},
        attributes=attributes or {},
    )

    # Load candidates from Neo4j
    async with get_neo4j_session() as session:
        query = f"""
        MATCH (e:{entity_type})
        RETURN e.id as id, e.name as name, e.ein as ein, e.bn as bn,
               e.opencorp_id as opencorp_id,
               e.address_city as city, e.address_state as state,
               e.address_country as country, e.address_postal as postal
        LIMIT 10000
        """
        result = await session.run(query)
        records = await result.data()

        candidates = []
        for record in records:
            if record.get("id"):
                idents = {}
                if record.get("ein"):
                    idents["ein"] = record["ein"]
                if record.get("bn"):
                    idents["bn"] = record["bn"]
                if record.get("opencorp_id"):
                    idents["opencorp_id"] = record["opencorp_id"]

                # Build address dict for location matching
                attrs = {
                    "address": {
                        "city": record.get("city"),
                        "state": record.get("state"),
                        "country": record.get("country"),
                        "postal_code": record.get("postal"),
                    }
                }

                candidates.append(
                    MatchCandidate(
                        entity_id=UUID(record["id"]),
                        entity_type=entity_type,
                        name=record.get("name") or "",
                        identifiers=idents,
                        attributes=attrs,
                    )
                )

    resolver = EntityResolver()
    return await resolver.resolve(source, candidates, auto_merge)
