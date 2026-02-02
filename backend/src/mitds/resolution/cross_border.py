"""Cross-border entity resolution for MITDS.

Resolves foreign grant recipients (primarily from IRS 990 Schedule I)
to known entities in other jurisdictions (e.g., CRA charities in Canada).

This enables linking US → Canada funding flows that would otherwise
create orphan nodes in the graph.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ..db import get_neo4j_session
from ..logging import get_context_logger
from .matcher import (
    FuzzyMatcher,
    HybridMatcher,
    MatchCandidate,
    MatchResult,
    MatchStrategy,
)
from .reconcile import ReconciliationQueue, ReconciliationPriority
from .resolver import ResolutionResult, ResolutionState

logger = get_context_logger(__name__)


class UnresolvedGrant(BaseModel):
    """An unresolved foreign grant recipient."""

    recipient_id: UUID
    recipient_name: str
    recipient_city: str | None = None
    recipient_state: str | None = None  # Province for CA
    recipient_postal: str | None = None
    recipient_country: str

    # Funder info
    funder_id: UUID | None = None
    funder_name: str | None = None
    funder_ein: str | None = None

    # Grant info
    amount: float | None = None
    fiscal_year: int | None = None


class CrossBorderResolutionResult(BaseModel):
    """Result of cross-border resolution."""

    grant: UnresolvedGrant
    matched_entity_id: UUID | None = None
    matched_entity_name: str | None = None
    matched_entity_bn: str | None = None
    confidence: float = 0.0
    strategy: MatchStrategy | None = None
    match_details: dict[str, Any] = Field(default_factory=dict)
    action: str = "none"  # "auto_merged", "queued_for_review", "no_match"


class CrossBorderResolutionStats(BaseModel):
    """Statistics from a cross-border resolution run."""

    total_unresolved: int = 0
    total_processed: int = 0
    auto_merged: int = 0
    queued_for_review: int = 0
    no_match: int = 0
    errors: int = 0


class CrossBorderResolver:
    """Resolves foreign grant recipients to known entities.

    Primary use case: Linking IRS 990 Schedule I grants to Canadian
    recipients with their corresponding CRA charity entries.

    Resolution strategy:
    1. Find unresolved grants to a target country (e.g., CA)
    2. For each, query known entities in that country by name + location
    3. Use fuzzy matching with location boosting
    4. Auto-merge high confidence matches (>= auto_merge_threshold)
    5. Queue medium confidence matches for human review
    """

    def __init__(
        self,
        auto_merge_threshold: float = 0.9,
        review_threshold: float = 0.7,
        use_postal_boost: bool = True,
    ):
        """Initialize the resolver.

        Args:
            auto_merge_threshold: Confidence above which to auto-merge
            review_threshold: Confidence above which to queue for review
            use_postal_boost: Whether to boost confidence for postal code matches
        """
        self.auto_merge_threshold = auto_merge_threshold
        self.review_threshold = review_threshold
        self.use_postal_boost = use_postal_boost

        self._matcher = HybridMatcher(
            use_embedding=False,  # Faster without embeddings
            fuzzy_min_score=80,
        )
        self._queue = ReconciliationQueue(
            confidence_threshold=auto_merge_threshold,
            auto_approve_threshold=0.98,
        )

    async def find_unresolved_grants(
        self,
        target_country: str = "CA",
        limit: int = 100,
    ) -> list[UnresolvedGrant]:
        """Find grant recipients that need resolution.

        Queries for Organization nodes where:
        - address_country matches target
        - No BN (for CA) or equivalent identifier
        - Has incoming FUNDED_BY relationship

        Args:
            target_country: Country code to find unresolved grants for
            limit: Maximum number of grants to return

        Returns:
            List of unresolved grants
        """
        async with get_neo4j_session() as session:
            # Query for unresolved recipients
            query = """
            MATCH (recipient:Organization)-[r:FUNDED_BY]->(funder:Organization)
            WHERE recipient.address_country = $country
              AND recipient.bn IS NULL
              AND recipient.resolved_to IS NULL
            RETURN DISTINCT
                recipient.id as recipient_id,
                recipient.name as recipient_name,
                recipient.address_city as city,
                recipient.address_state as state,
                recipient.address_postal as postal,
                recipient.address_country as country,
                funder.id as funder_id,
                funder.name as funder_name,
                funder.ein as funder_ein,
                r.amount as amount,
                r.fiscal_year as fiscal_year
            ORDER BY recipient.name
            LIMIT $limit
            """

            result = await session.run(
                query,
                country=target_country,
                limit=limit,
            )
            records = await result.data()

            grants = []
            for record in records:
                if record.get("recipient_id"):
                    grants.append(UnresolvedGrant(
                        recipient_id=UUID(record["recipient_id"]),
                        recipient_name=record.get("recipient_name") or "",
                        recipient_city=record.get("city"),
                        recipient_state=record.get("state"),
                        recipient_postal=record.get("postal"),
                        recipient_country=record.get("country") or target_country,
                        funder_id=UUID(record["funder_id"]) if record.get("funder_id") else None,
                        funder_name=record.get("funder_name"),
                        funder_ein=record.get("funder_ein"),
                        amount=record.get("amount"),
                        fiscal_year=record.get("fiscal_year"),
                    ))

            logger.info(
                f"Found {len(grants)} unresolved grants to {target_country}"
            )
            return grants

    async def find_candidates(
        self,
        grant: UnresolvedGrant,
        limit: int = 50,
    ) -> list[MatchCandidate]:
        """Find potential matches for a grant recipient.

        Queries known entities in the same country with matching
        province/state for more targeted matching.

        Args:
            grant: The unresolved grant to find candidates for
            limit: Maximum candidates to return

        Returns:
            List of match candidates
        """
        async with get_neo4j_session() as session:
            # Build query based on available location info
            filters = ["e.address_country = $country", "e.bn IS NOT NULL"]
            params: dict[str, Any] = {
                "country": grant.recipient_country,
                "limit": limit,
            }

            # Prioritize same province if available
            if grant.recipient_state:
                filters.append("e.address_state = $state")
                params["state"] = grant.recipient_state

            query = f"""
            MATCH (e:Organization)
            WHERE {' AND '.join(filters)}
            RETURN e.id as id, e.name as name, e.bn as bn,
                   e.address_city as city, e.address_state as state,
                   e.address_postal as postal, e.address_country as country
            LIMIT $limit
            """

            result = await session.run(query, params)
            records = await result.data()

            candidates = []
            for record in records:
                if record.get("id") and record.get("name"):
                    identifiers = {}
                    if record.get("bn"):
                        identifiers["bn"] = record["bn"]

                    attributes = {
                        "address": {
                            "city": record.get("city"),
                            "state": record.get("state"),
                            "postal_code": record.get("postal"),
                            "country": record.get("country"),
                        }
                    }

                    candidates.append(MatchCandidate(
                        entity_id=UUID(record["id"]),
                        entity_type="ORGANIZATION",
                        name=record["name"],
                        identifiers=identifiers,
                        attributes=attributes,
                    ))

            return candidates

    def _calculate_postal_boost(
        self,
        source_postal: str | None,
        target_postal: str | None,
    ) -> float:
        """Calculate confidence boost for postal code matching.

        Canadian postal codes: First 3 chars = Forward Sortation Area (FSA)
        Matching FSA is a strong geographic signal.

        Args:
            source_postal: Postal code from grant
            target_postal: Postal code from candidate

        Returns:
            Confidence boost (0.0 to 0.1)
        """
        if not source_postal or not target_postal:
            return 0.0

        # Normalize: uppercase, remove spaces
        source = source_postal.upper().replace(" ", "")
        target = target_postal.upper().replace(" ", "")

        # Full match
        if source == target:
            return 0.1

        # FSA match (first 3 chars)
        if len(source) >= 3 and len(target) >= 3:
            if source[:3] == target[:3]:
                return 0.05

        return 0.0

    async def resolve_grant(
        self,
        grant: UnresolvedGrant,
        auto_merge: bool = True,
    ) -> CrossBorderResolutionResult:
        """Attempt to resolve a grant recipient to a known entity.

        Args:
            grant: The unresolved grant
            auto_merge: Whether to auto-merge high confidence matches

        Returns:
            Resolution result with match details
        """
        result = CrossBorderResolutionResult(grant=grant)

        # Find candidates
        candidates = await self.find_candidates(grant)

        if not candidates:
            result.action = "no_match"
            return result

        # Create source candidate from grant
        source = MatchCandidate(
            entity_id=grant.recipient_id,
            entity_type="ORGANIZATION",
            name=grant.recipient_name,
            identifiers={},
            attributes={
                "address": {
                    "city": grant.recipient_city,
                    "state": grant.recipient_state,
                    "postal_code": grant.recipient_postal,
                    "country": grant.recipient_country,
                }
            },
        )

        # Find matches using hybrid matcher
        matches = self._matcher.find_matches(
            source,
            candidates,
            threshold=self.review_threshold,
        )

        if not matches:
            result.action = "no_match"
            return result

        # Get best match
        best_match = matches[0]
        confidence = best_match.confidence

        # Apply postal code boost if enabled
        if self.use_postal_boost and grant.recipient_postal:
            target_postal = best_match.target.attributes.get("address", {}).get("postal_code")
            boost = self._calculate_postal_boost(grant.recipient_postal, target_postal)
            confidence = min(confidence + boost, 1.0)

            if boost > 0:
                best_match.match_details["postal_boost"] = boost

        result.matched_entity_id = best_match.target.entity_id
        result.matched_entity_name = best_match.target.name
        result.matched_entity_bn = best_match.target.identifiers.get("bn")
        result.confidence = confidence
        result.strategy = best_match.strategy
        result.match_details = best_match.match_details

        # Determine action based on confidence
        if confidence >= self.auto_merge_threshold and auto_merge:
            # Auto-merge
            await self._merge_entities(grant.recipient_id, best_match.target.entity_id)
            result.action = "auto_merged"
            logger.info(
                f"Auto-merged: {grant.recipient_name} -> {best_match.target.name} "
                f"(confidence: {confidence:.2f})"
            )

        elif confidence >= self.review_threshold:
            # Queue for human review
            await self._queue.add_match(
                best_match,
                priority=ReconciliationPriority.MEDIUM,
                context={
                    "grant_funder": grant.funder_name,
                    "grant_amount": grant.amount,
                    "cross_border": True,
                    "source_country": "US",
                    "target_country": grant.recipient_country,
                },
            )
            result.action = "queued_for_review"
            logger.info(
                f"Queued for review: {grant.recipient_name} -> {best_match.target.name} "
                f"(confidence: {confidence:.2f})"
            )

        else:
            result.action = "no_match"

        return result

    async def _merge_entities(
        self,
        source_id: UUID,
        target_id: UUID,
    ) -> None:
        """Merge source entity into target (link, don't delete).

        Sets source.resolved_to = target.id and transfers
        relationships to point to target.

        Args:
            source_id: Unresolved entity ID
            target_id: Known entity ID (with BN)
        """
        async with get_neo4j_session() as session:
            now = datetime.utcnow().isoformat()

            # Mark source as resolved
            await session.run(
                """
                MATCH (source {id: $source_id})
                MATCH (target {id: $target_id})
                SET source.resolved_to = $target_id,
                    source.resolved_at = $now,
                    source.resolution_type = 'cross_border_auto'
                """,
                source_id=str(source_id),
                target_id=str(target_id),
                now=now,
            )

            # Transfer incoming FUNDED_BY relationships to target
            await session.run(
                """
                MATCH (source {id: $source_id})-[r:FUNDED_BY]->(funder)
                MATCH (target {id: $target_id})
                WHERE NOT (target)-[:FUNDED_BY]->(funder)
                CREATE (target)-[r2:FUNDED_BY]->(funder)
                SET r2 = properties(r),
                    r2.transferred_from = $source_id,
                    r2.transferred_at = $now
                DELETE r
                """,
                source_id=str(source_id),
                target_id=str(target_id),
                now=now,
            )

            logger.debug(f"Merged entity {source_id} -> {target_id}")

    async def run(
        self,
        target_country: str = "CA",
        limit: int = 100,
        auto_merge: bool = True,
    ) -> tuple[CrossBorderResolutionStats, list[CrossBorderResolutionResult]]:
        """Run cross-border resolution for a target country.

        Args:
            target_country: Country code (default: CA for Canada)
            limit: Maximum grants to process
            auto_merge: Whether to auto-merge high confidence matches

        Returns:
            Tuple of (stats, results)
        """
        stats = CrossBorderResolutionStats()
        results: list[CrossBorderResolutionResult] = []

        # Find unresolved grants
        grants = await self.find_unresolved_grants(target_country, limit)
        stats.total_unresolved = len(grants)

        # Process each grant
        for grant in grants:
            try:
                result = await self.resolve_grant(grant, auto_merge)
                results.append(result)
                stats.total_processed += 1

                if result.action == "auto_merged":
                    stats.auto_merged += 1
                elif result.action == "queued_for_review":
                    stats.queued_for_review += 1
                else:
                    stats.no_match += 1

            except Exception as e:
                logger.warning(
                    f"Error resolving grant {grant.recipient_name}: {e}"
                )
                stats.errors += 1

        logger.info(
            f"Cross-border resolution complete: "
            f"{stats.total_processed} processed, "
            f"{stats.auto_merged} auto-merged, "
            f"{stats.queued_for_review} queued, "
            f"{stats.no_match} no match, "
            f"{stats.errors} errors"
        )

        return stats, results


async def run_cross_border_resolution(
    target_country: str = "CA",
    limit: int = 100,
    auto_merge: bool = True,
    auto_merge_threshold: float = 0.9,
) -> dict[str, Any]:
    """Convenience function to run cross-border resolution.

    Args:
        target_country: Country code to resolve
        limit: Maximum grants to process
        auto_merge: Whether to auto-merge
        auto_merge_threshold: Confidence threshold for auto-merge

    Returns:
        Dictionary with stats and summary
    """
    resolver = CrossBorderResolver(
        auto_merge_threshold=auto_merge_threshold,
    )

    stats, results = await resolver.run(
        target_country=target_country,
        limit=limit,
        auto_merge=auto_merge,
    )

    return {
        "status": "completed",
        "stats": stats.model_dump(),
        "summary": {
            "total_unresolved": stats.total_unresolved,
            "processed": stats.total_processed,
            "auto_merged": stats.auto_merged,
            "queued_for_review": stats.queued_for_review,
            "no_match": stats.no_match,
            "errors": stats.errors,
        },
    }
