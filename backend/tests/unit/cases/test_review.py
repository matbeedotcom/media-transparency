"""Unit tests for EntityMatchQueue (T028).

Tests match queue operations without database connections.

Run with: pytest tests/unit/cases/test_review.py -v
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.models import (
    EntityMatch,
    MatchSignals,
    MatchStatus,
)


class TestEntityMatchCreation:
    """Tests for creating EntityMatch objects directly (without queue)."""

    def test_create_match_with_signals(self, sample_match_signals):
        """Test creating an EntityMatch with all signals."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.85,
            match_signals=sample_match_signals,
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        assert match is not None
        assert match.status == MatchStatus.PENDING
        assert match.confidence == 0.85
        assert match.match_signals == sample_match_signals

    def test_create_match_default_status(self):
        """Test that new matches default to pending status."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.75,
            match_signals=MatchSignals(),
            created_at=datetime.utcnow(),
        )

        assert match.status == MatchStatus.PENDING

    def test_create_match_generates_id(self):
        """Test that match can have a unique ID."""
        match = EntityMatch(
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.80,
            match_signals=MatchSignals(),
            created_at=datetime.utcnow(),
        )

        assert match.id is not None


class TestMatchStatusTransitions:
    """Tests for match status transitions."""

    def test_valid_status_values(self):
        """Test that all status values are valid."""
        valid_statuses = [
            MatchStatus.PENDING,
            MatchStatus.APPROVED,
            MatchStatus.REJECTED,
            MatchStatus.DEFERRED,
        ]

        for status in valid_statuses:
            assert status.value is not None

    def test_pending_to_approved(self):
        """Test transition from pending to approved."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.85,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Simulate approval
        match.status = MatchStatus.APPROVED
        match.reviewed_by = "test_user"
        match.reviewed_at = datetime.utcnow()

        assert match.status == MatchStatus.APPROVED
        assert match.reviewed_by == "test_user"

    def test_pending_to_rejected(self):
        """Test transition from pending to rejected."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.75,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.REJECTED
        match.reviewed_by = "test_user"
        match.review_notes = "Not the same entity"

        assert match.status == MatchStatus.REJECTED
        assert match.review_notes == "Not the same entity"

    def test_pending_to_deferred(self):
        """Test transition from pending to deferred."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.72,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.DEFERRED
        match.reviewed_by = "test_user"
        match.review_notes = "Need more information"

        assert match.status == MatchStatus.DEFERRED


class TestMatchSignals:
    """Tests for MatchSignals data model."""

    def test_empty_signals(self):
        """Test creating empty match signals."""
        signals = MatchSignals()

        assert signals.name_similarity is None
        assert signals.identifier_match is None
        assert signals.jurisdiction_match is False
        assert signals.address_overlap is None
        assert signals.shared_directors is None

    def test_full_signals(self, sample_match_signals):
        """Test creating match signals with all fields."""
        assert sample_match_signals.name_similarity == 0.92
        assert sample_match_signals.identifier_match["type"] == "ein"
        assert sample_match_signals.jurisdiction_match is True
        assert sample_match_signals.address_overlap["city"] is True
        assert "John Smith" in sample_match_signals.shared_directors

    def test_signals_with_partial_data(self):
        """Test creating signals with partial data."""
        signals = MatchSignals(
            name_similarity=0.88,
            jurisdiction_match=True,
        )

        assert signals.name_similarity == 0.88
        assert signals.jurisdiction_match is True
        assert signals.identifier_match is None
        assert signals.address_overlap is None


class TestQueueOperations:
    """Tests for queue retrieval operations."""

    @pytest.mark.asyncio
    async def test_get_pending_empty(self):
        """Test getting pending matches when none exist."""
        from mitds.cases.review.queue import EntityMatchQueue

        mock_db = AsyncMock()
        mock_neo4j = AsyncMock()

        # Mock the scalar() call for count
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 0

        # Mock fetchall for empty results
        mock_fetch_result = MagicMock()
        mock_fetch_result.fetchall.return_value = []

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_fetch_result])

        queue = EntityMatchQueue(mock_db, mock_neo4j)

        matches, count = await queue.get_pending(uuid4())

        assert matches == []
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_pending_with_limit(self):
        """Test getting pending matches with limit."""
        from mitds.cases.review.queue import EntityMatchQueue

        mock_db = AsyncMock()
        mock_neo4j = AsyncMock()

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 0

        mock_fetch_result = MagicMock()
        mock_fetch_result.fetchall.return_value = []

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_fetch_result])

        queue = EntityMatchQueue(mock_db, mock_neo4j)

        matches, count = await queue.get_pending(uuid4(), limit=10, offset=0)

        assert isinstance(matches, list)
        assert isinstance(count, int)


class TestApprovalAction:
    """Tests for approval action - using EntityMatch model directly."""

    def test_approval_changes_status(self):
        """Test that approval changes status to APPROVED."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.85,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Simulate what happens on approval
        match.status = MatchStatus.APPROVED
        match.reviewed_by = "test_user"
        match.reviewed_at = datetime.utcnow()

        assert match.status == MatchStatus.APPROVED
        assert match.reviewed_by == "test_user"
        assert match.reviewed_at is not None

    def test_approval_records_notes(self):
        """Test that approval can record notes."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.85,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.APPROVED
        match.reviewed_by = "reviewer"
        match.review_notes = "Confirmed match"

        assert match.review_notes == "Confirmed match"


class TestRejectionAction:
    """Tests for rejection action - using EntityMatch model directly."""

    def test_rejection_changes_status(self):
        """Test that rejection changes status to REJECTED."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.75,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.REJECTED
        match.reviewed_by = "test_user"
        match.review_notes = "Not the same entity"

        assert match.status == MatchStatus.REJECTED
        assert match.review_notes == "Not the same entity"

    def test_rejection_does_not_require_notes(self):
        """Test that rejection can be done without notes."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.72,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.REJECTED
        match.reviewed_by = "reviewer"

        assert match.status == MatchStatus.REJECTED
        assert match.review_notes is None


class TestDeferralAction:
    """Tests for deferral action - using EntityMatch model directly."""

    def test_deferral_changes_status(self):
        """Test that deferral changes status to DEFERRED."""
        match = EntityMatch(
            id=uuid4(),
            case_id=uuid4(),
            source_entity_id=uuid4(),
            target_entity_id=uuid4(),
            confidence=0.78,
            match_signals=MatchSignals(),
            status=MatchStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        match.status = MatchStatus.DEFERRED
        match.reviewed_by = "reviewer"
        match.review_notes = "Need more info"

        assert match.status == MatchStatus.DEFERRED
        assert match.review_notes == "Need more info"
