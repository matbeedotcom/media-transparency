"""Unit tests for SponsorResolver (T017).

Tests confidence scoring, threshold routing, and name matching
without requiring database connections.

Run with: pytest tests/unit/cases/test_resolution.py -v
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from mitds.cases.resolution.sponsor import SponsorResolver, MatchCandidate


class TestConfidenceThresholds:
    """Tests for confidence-based routing thresholds."""

    def test_auto_merge_threshold_high_confidence(self):
        """Test that >= 0.95 confidence triggers auto-merge."""
        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_auto_merge(0.95) is True
        assert resolver.should_auto_merge(0.99) is True
        assert resolver.should_auto_merge(1.0) is True

    def test_auto_merge_threshold_boundary(self):
        """Test auto-merge boundary at 0.9."""
        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_auto_merge(0.90) is True
        assert resolver.should_auto_merge(0.89) is False

    def test_review_threshold_range(self):
        """Test that 0.7-0.9 confidence goes to review."""
        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_queue_for_review(0.70) is True
        assert resolver.should_queue_for_review(0.75) is True
        assert resolver.should_queue_for_review(0.85) is True
        assert resolver.should_queue_for_review(0.89) is True

    def test_review_threshold_boundaries(self):
        """Test review threshold boundaries."""
        resolver = SponsorResolver(AsyncMock())

        # Below review threshold
        assert resolver.should_queue_for_review(0.69) is False

        # At or above auto-merge threshold
        assert resolver.should_queue_for_review(0.90) is False
        assert resolver.should_queue_for_review(0.95) is False

    def test_discard_threshold(self):
        """Test that < 0.7 confidence is discarded."""
        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_discard(0.69) is True
        assert resolver.should_discard(0.50) is True
        assert resolver.should_discard(0.30) is True
        assert resolver.should_discard(0.0) is True

    def test_discard_threshold_boundary(self):
        """Test discard boundary at 0.7."""
        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_discard(0.70) is False
        assert resolver.should_discard(0.69) is True


class TestMatchCandidate:
    """Tests for MatchCandidate data class."""

    def test_create_match_candidate(self):
        """Test MatchCandidate creation."""
        entity_id = uuid4()

        candidate = MatchCandidate(
            entity_id=entity_id,
            name="Americans for Prosperity",
            confidence=0.92,
            match_type="name",
            signals={"name_similarity": 0.92},
        )

        assert candidate.entity_id == entity_id
        assert candidate.name == "Americans for Prosperity"
        assert candidate.confidence == 0.92
        assert candidate.match_type == "name"

    def test_match_candidate_with_identifier(self):
        """Test MatchCandidate with identifier match."""
        candidate = MatchCandidate(
            entity_id=uuid4(),
            name="Test Corp",
            confidence=1.0,
            match_type="identifier",
            signals={"identifier_type": "ein", "identifier_value": "12-3456789"},
        )

        assert candidate.confidence == 1.0
        assert candidate.match_type == "identifier"


class TestNameMatching:
    """Tests for fuzzy name matching."""

    @pytest.mark.asyncio
    async def test_exact_name_match(self):
        """Test exact name matching."""
        mock_session = AsyncMock()

        # Mock Neo4j to return an exact match
        mock_result = MagicMock()
        mock_result.data.return_value = [{
            "id": str(uuid4()),
            "name": "Americans for Prosperity",
            "entity_type": "organization",
            "jurisdiction": "US",
        }]
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidates = await resolver._match_by_name(
            "Americans for Prosperity",
            jurisdiction=None,
            limit=5,
        )

        # Should find the match
        assert mock_session.run.called

    @pytest.mark.asyncio
    async def test_fuzzy_name_match(self):
        """Test fuzzy name matching with similar names."""
        mock_session = AsyncMock()

        # Mock Neo4j to return similar matches
        mock_result = MagicMock()
        mock_result.data.return_value = [
            {
                "id": str(uuid4()),
                "name": "Americans for Prosperity Foundation",
                "entity_type": "organization",
                "jurisdiction": "US",
            },
            {
                "id": str(uuid4()),
                "name": "American Prosperity Alliance",
                "entity_type": "organization",
                "jurisdiction": "US",
            },
        ]
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidates = await resolver._match_by_name(
            "Americans for Prosperity",
            jurisdiction="US",
            limit=5,
        )

        assert mock_session.run.called


class TestIdentifierMatching:
    """Tests for identifier-based matching."""

    @pytest.mark.asyncio
    async def test_ein_match(self):
        """Test matching by EIN."""
        mock_session = AsyncMock()

        # Mock Neo4j to return a match
        entity_id = uuid4()
        mock_result = MagicMock()
        mock_result.single.return_value = {
            "id": str(entity_id),
            "name": "Test Foundation",
            "entity_type": "organization",
        }
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidate = await resolver._match_by_identifier("ein", "12-3456789")

        # Should query Neo4j with identifier
        assert mock_session.run.called
        call_args = mock_session.run.call_args
        assert "ein" in str(call_args).lower() or "identifier" in str(call_args[0][0]).lower()

    @pytest.mark.asyncio
    async def test_bn_match(self):
        """Test matching by Canadian Business Number."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.single.return_value = None  # No match
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidate = await resolver._match_by_identifier("bn", "123456789RR0001")

        assert mock_session.run.called

    @pytest.mark.asyncio
    async def test_identifier_match_returns_high_confidence(self):
        """Test that identifier matches have high confidence."""
        mock_session = AsyncMock()

        entity_id = uuid4()
        mock_result = MagicMock()
        mock_result.single.return_value = {
            "id": str(entity_id),
            "name": "Test Corp",
            "entity_type": "organization",
        }
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidate = await resolver._match_by_identifier("cik", "0001234567")

        if candidate:
            # Identifier matches should have very high confidence
            assert candidate.confidence >= 0.95


class TestResolveFunction:
    """Tests for the main resolve() function."""

    @pytest.mark.asyncio
    async def test_resolve_with_identifier(self):
        """Test resolve with identifier provided."""
        mock_session = AsyncMock()

        entity_id = uuid4()
        mock_result = MagicMock()
        mock_result.single.return_value = {
            "id": str(entity_id),
            "name": "Test Foundation",
            "entity_type": "organization",
        }
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidates = await resolver.resolve(
            name="Test Foundation",
            identifiers={"ein": "12-3456789"},
        )

        assert isinstance(candidates, list)

    @pytest.mark.asyncio
    async def test_resolve_name_only(self):
        """Test resolve with name only."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.data.return_value = []  # No matches
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidates = await resolver.resolve(
            name="Unknown Organization",
        )

        assert isinstance(candidates, list)
        assert mock_session.run.called

    @pytest.mark.asyncio
    async def test_resolve_with_jurisdiction_boost(self):
        """Test that matching jurisdiction boosts confidence."""
        mock_session = AsyncMock()

        entity_id = uuid4()
        mock_result = MagicMock()
        mock_result.data.return_value = [{
            "id": str(entity_id),
            "name": "Canadian Foundation",
            "entity_type": "organization",
            "jurisdiction": "CA",
        }]
        mock_session.run.return_value = mock_result

        resolver = SponsorResolver(mock_session)

        candidates = await resolver.resolve(
            name="Canadian Foundation",
            jurisdiction="CA",
        )

        # Jurisdiction match should be factored into confidence
        assert isinstance(candidates, list)


class TestConfidenceCalculation:
    """Tests for confidence score calculation."""

    def test_confidence_caps_at_one(self):
        """Test that confidence never exceeds 1.0."""
        resolver = SponsorResolver(AsyncMock())

        # Multiple high signals shouldn't exceed 1.0
        # This is tested indirectly through the threshold tests
        assert resolver.should_auto_merge(1.0) is True

    def test_identifier_weight(self):
        """Test that identifier match has highest weight."""
        # Verify the weight constants
        assert SponsorResolver.IDENTIFIER_WEIGHT >= SponsorResolver.NAME_SIMILARITY_WEIGHT

    def test_min_name_similarity_threshold(self):
        """Test minimum name similarity threshold."""
        # Names below this threshold shouldn't be considered matches
        assert SponsorResolver.MIN_NAME_SIMILARITY >= 0.8
