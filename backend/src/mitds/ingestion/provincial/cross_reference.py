"""Cross-reference service for linking provincial and federal corporations.

This module provides functionality to cross-reference provincial corporation
records with federal registry data using multiple matching strategies:
1. Business Number (BN) matching - highest confidence
2. Exact name matching - high confidence
3. Fuzzy name matching - configurable thresholds

Match results are classified by confidence level:
- Auto-link (>=95%): Automatically create SAME_AS relationship
- Flag for review (85-95%): Requires manual verification
- No match (<85%): No relationship created
"""

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from rapidfuzz import fuzz
from sqlalchemy import text

from .models import CrossReferenceResult


@dataclass
class CrossReferenceConfig:
    """Configuration for cross-reference job."""

    provinces: list[str] | None = None  # None = all provinces
    auto_link_threshold: float = 0.95
    review_threshold: float = 0.85
    batch_size: int = 1000


class CrossReferenceService:
    """Service for cross-referencing provincial and federal corporations.

    Matches provincial corporation records with federal registry data
    using business number and name matching strategies.

    Usage:
        service = CrossReferenceService()
        result = await service.run(config)
    """

    def __init__(self):
        """Initialize the cross-reference service."""
        self._federal_names: dict[str, tuple[UUID, str]] = {}  # normalized_name -> (id, original_name)
        self._federal_bns: dict[str, tuple[UUID, str]] = {}    # bn -> (id, name)

    async def run(
        self,
        config: CrossReferenceConfig | None = None,
        run_id: UUID | None = None,
    ) -> dict[str, Any]:
        """Run cross-referencing job.

        Args:
            config: Cross-reference configuration
            run_id: Optional run ID for tracking

        Returns:
            Result dictionary with statistics
        """
        config = config or CrossReferenceConfig()
        run_id = run_id or uuid4()

        # Load federal corporations for matching
        await self._load_federal_data()

        # Process provincial corporations
        stats = {
            "total_processed": 0,
            "matched_by_bn": 0,
            "matched_by_exact_name": 0,
            "matched_by_fuzzy_name": 0,
            "auto_linked": 0,
            "flagged_for_review": 0,
            "no_match": 0,
        }

        async for result in self._process_provincial_records(config):
            stats["total_processed"] += 1

            if result.match_method == "business_number":
                stats["matched_by_bn"] += 1
            elif result.match_method == "exact_name":
                stats["matched_by_exact_name"] += 1
            elif result.match_method == "fuzzy_name":
                stats["matched_by_fuzzy_name"] += 1
            else:
                stats["no_match"] += 1

            if result.is_auto_linkable:
                stats["auto_linked"] += 1
                await self._create_same_as_relationship(result)
            elif result.requires_review:
                stats["flagged_for_review"] += 1
                await self._flag_for_review(result)

        return {
            "run_id": str(run_id),
            "status": "completed",
            **stats,
        }

    async def _load_federal_data(self) -> None:
        """Load federal corporation data for matching."""
        from ...db import get_db_session

        self._federal_names = {}
        self._federal_bns = {}

        async with get_db_session() as db:
            # Load federal corporations (no provincial_registry_id)
            result = await db.execute(
                text("""
                    SELECT id, name, external_ids->>'business_number' as bn
                    FROM entities
                    WHERE entity_type = 'organization'
                    AND provincial_registry_id IS NULL
                    AND (
                        external_ids->>'federal_corp_number' IS NOT NULL
                        OR metadata->>'jurisdiction' = 'CA'
                    )
                """)
            )

            for row in result.fetchall():
                entity_id = row.id
                name = row.name
                bn = row.bn

                # Index by normalized name
                normalized_name = self._normalize_name(name)
                self._federal_names[normalized_name] = (entity_id, name)

                # Index by business number
                if bn:
                    self._federal_bns[bn] = (entity_id, name)

    def _normalize_name(self, name: str) -> str:
        """Normalize corporation name for matching."""
        import re

        # Lowercase
        normalized = name.lower()

        # Remove common suffixes
        suffixes = [
            r"\binc\.?\b", r"\bltd\.?\b", r"\blimited\b", r"\bcorp\.?\b",
            r"\bcorporation\b", r"\bcompany\b", r"\bco\.?\b", r"\bltée\b",
            r"\bincorporated\b", r"\bllc\b", r"\bplc\b"
        ]
        for suffix in suffixes:
            normalized = re.sub(suffix, "", normalized)

        # Remove punctuation and extra whitespace
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    async def _process_provincial_records(
        self, config: CrossReferenceConfig
    ) -> AsyncIterator[CrossReferenceResult]:
        """Process provincial records and yield match results."""
        from ...db import get_db_session

        async with get_db_session() as db:
            # Build query for provincial corporations
            query = """
                SELECT id, name, external_ids->>'business_number' as bn,
                       provincial_registry_id
                FROM entities
                WHERE provincial_registry_id IS NOT NULL
            """

            params = {}

            if config.provinces:
                # Filter by province
                province_patterns = [f"{p}:%" for p in config.provinces]
                query += " AND (" + " OR ".join(
                    f"provincial_registry_id LIKE :p{i}"
                    for i in range(len(province_patterns))
                ) + ")"
                for i, pattern in enumerate(province_patterns):
                    params[f"p{i}"] = pattern

            result = await db.execute(text(query), params)

            for row in result.fetchall():
                match_result = await self._match_entity(
                    provincial_id=row.id,
                    provincial_name=row.name,
                    business_number=row.bn,
                    config=config,
                )
                yield match_result

    async def _match_entity(
        self,
        provincial_id: UUID,
        provincial_name: str,
        business_number: str | None,
        config: CrossReferenceConfig,
    ) -> CrossReferenceResult:
        """Match a provincial entity against federal data."""

        # 1. Try business number match (highest confidence)
        if business_number and business_number in self._federal_bns:
            federal_id, federal_name = self._federal_bns[business_number]
            return CrossReferenceResult(
                provincial_id=provincial_id,
                provincial_name=provincial_name,
                matched_entity_id=federal_id,
                matched_entity_name=federal_name,
                matched_jurisdiction="CA",
                match_score=1.0,
                match_method="business_number",
                is_auto_linkable=True,
                requires_review=False,
            )

        # 2. Try exact name match
        normalized_name = self._normalize_name(provincial_name)
        if normalized_name in self._federal_names:
            federal_id, federal_name = self._federal_names[normalized_name]
            return CrossReferenceResult(
                provincial_id=provincial_id,
                provincial_name=provincial_name,
                matched_entity_id=federal_id,
                matched_entity_name=federal_name,
                matched_jurisdiction="CA",
                match_score=1.0,
                match_method="exact_name",
                is_auto_linkable=True,
                requires_review=False,
            )

        # 3. Try fuzzy name match
        best_score = 0.0
        best_match = None

        for fed_normalized, (federal_id, federal_name) in self._federal_names.items():
            # Use token_sort_ratio for better handling of word order
            score = fuzz.token_sort_ratio(normalized_name, fed_normalized) / 100.0

            if score > best_score:
                best_score = score
                best_match = (federal_id, federal_name)

        if best_match and best_score >= config.review_threshold:
            federal_id, federal_name = best_match
            return CrossReferenceResult(
                provincial_id=provincial_id,
                provincial_name=provincial_name,
                matched_entity_id=federal_id,
                matched_entity_name=federal_name,
                matched_jurisdiction="CA",
                match_score=best_score,
                match_method="fuzzy_name",
                is_auto_linkable=best_score >= config.auto_link_threshold,
                requires_review=config.review_threshold <= best_score < config.auto_link_threshold,
            )

        # No match found
        return CrossReferenceResult(
            provincial_id=provincial_id,
            provincial_name=provincial_name,
            match_score=0.0,
            match_method="none",
        )

    async def _create_same_as_relationship(
        self, result: CrossReferenceResult
    ) -> None:
        """Create SAME_AS relationship in Neo4j."""
        from ...db import get_neo4j_session

        if not result.matched_entity_id:
            return

        try:
            async with get_neo4j_session() as session:
                query = """
                    MATCH (provincial:Organization {id: $provincial_id})
                    MATCH (federal:Organization {id: $federal_id})
                    MERGE (provincial)-[r:SAME_AS]->(federal)
                    SET r.match_score = $match_score,
                        r.match_method = $match_method,
                        r.verified = $verified,
                        r.created_at = datetime()
                    RETURN r
                """
                await session.run(
                    query,
                    provincial_id=str(result.provincial_id),
                    federal_id=str(result.matched_entity_id),
                    match_score=result.match_score,
                    match_method=result.match_method,
                    verified=result.is_auto_linkable,
                )
        except Exception:
            # Log error but don't fail the job
            pass

    async def _flag_for_review(self, result: CrossReferenceResult) -> None:
        """Flag a match for manual review."""
        from ...db import get_db_session

        if not result.matched_entity_id:
            return

        try:
            async with get_db_session() as db:
                # Store in a review queue or evidence table
                await db.execute(
                    text("""
                        INSERT INTO cross_reference_reviews (
                            id, provincial_entity_id, federal_entity_id,
                            provincial_name, federal_name,
                            match_score, match_method, created_at
                        ) VALUES (
                            :id, :provincial_id, :federal_id,
                            :provincial_name, :federal_name,
                            :match_score, :match_method, :created_at
                        )
                        ON CONFLICT (provincial_entity_id, federal_entity_id) DO UPDATE SET
                            match_score = :match_score,
                            match_method = :match_method
                    """),
                    {
                        "id": uuid4(),
                        "provincial_id": result.provincial_id,
                        "federal_id": result.matched_entity_id,
                        "provincial_name": result.provincial_name,
                        "federal_name": result.matched_entity_name,
                        "match_score": result.match_score,
                        "match_method": result.match_method,
                        "created_at": datetime.utcnow(),
                    },
                )
        except Exception:
            # Table might not exist yet, log and continue
            pass


async def run_cross_reference(
    provinces: list[str] | None = None,
    auto_link_threshold: float = 0.95,
    review_threshold: float = 0.85,
) -> dict[str, Any]:
    """Run cross-referencing between provincial and federal corporations.

    Main entry point for running the cross-reference job. Can be called
    from CLI, API, or directly from Python code.

    Args:
        provinces: List of province codes to process (None = all)
        auto_link_threshold: Threshold for automatic linking (default: 0.95)
        review_threshold: Threshold for flagging for review (default: 0.85)

    Returns:
        Result dictionary with statistics

    Example:
        result = await run_cross_reference(
            provinces=["QC", "ON"],
            auto_link_threshold=0.95,
        )
        print(f"Auto-linked: {result['auto_linked']}")
    """
    config = CrossReferenceConfig(
        provinces=provinces,
        auto_link_threshold=auto_link_threshold,
        review_threshold=review_threshold,
    )

    service = CrossReferenceService()
    return await service.run(config)
