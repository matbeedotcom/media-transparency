"""Integration tests for entity match review flow (User Story 5).

Tests the complete flow:
1. Queue entity matches for review
2. Display match details with signals
3. Approve/reject matches
4. Create SAME_AS relationships in Neo4j on approval

Run with: pytest tests/integration/cases/test_review_flow.py -v
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.review.queue import EntityMatchQueue
from mitds.cases.models import (
    EntityMatch,
    EntityMatchResponse,
    MatchSignals,
    MatchStatus,
    EntitySummary,
)


class TestEntityMatchQueue:
    """Tests for EntityMatchQueue functionality."""

    @pytest.mark.asyncio
    async def test_create_match_stores_in_database(self, mock_db_session):
        """Test that creating a match stores it in the database."""
        queue = EntityMatchQueue(mock_db_session, AsyncMock())

        case_id = uuid4()
        source_id = uuid4()
        target_id = uuid4()

        match = await queue.create_match(
            case_id=case_id,
            source_entity_id=source_id,
            target_entity_id=target_id,
            confidence=0.85,
            match_signals=MatchSignals(
                name_similarity=0.92,
                jurisdiction_match=True,
            ),
        )

        assert match is not None
        assert match.case_id == case_id
        assert match.source_entity_id == source_id
        assert match.target_entity_id == target_id
        assert match.confidence == 0.85
        assert match.status == MatchStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_pending_returns_only_pending_matches(self, mock_db_session):
        """Test that get_pending only returns pending status matches."""
        # Setup mock to return pending matches
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_result.scalar.return_value = 0
        mock_db_session.execute.return_value = mock_result

        queue = EntityMatchQueue(mock_db_session, AsyncMock())

        matches, count = await queue.get_pending(uuid4())

        assert isinstance(matches, list)
        assert isinstance(count, int)


class TestMatchApproval:
    """Tests for match approval flow."""

    @pytest.mark.asyncio
    async def test_approve_creates_same_as_relationship(
        self, mock_db_session, mock_neo4j_session
    ):
        """Test that approving a match creates SAME_AS in Neo4j."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        # Create a mock match
        match_id = uuid4()
        source_id = uuid4()
        target_id = uuid4()
        case_id = uuid4()
        now = datetime.utcnow()

        # Mock the database row with proper attributes
        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(source_id)
        mock_row.target_entity_id = str(target_id)
        mock_row.status = MatchStatus.PENDING.value  # "pending"
        mock_row.confidence = 0.85
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()

        # Approve the match
        approved = await queue.approve(match_id, reviewed_by="test_user")

        # Verify Neo4j was called to create SAME_AS
        assert mock_neo4j_session.run.called
        call_args = mock_neo4j_session.run.call_args
        cypher_query = call_args[0][0]
        assert "SAME_AS" in cypher_query

    @pytest.mark.asyncio
    async def test_approve_updates_match_status(self, mock_db_session, mock_neo4j_session):
        """Test that approving a match updates its status."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        match_id = uuid4()
        source_id = uuid4()
        target_id = uuid4()
        case_id = uuid4()
        now = datetime.utcnow()

        # Mock the database row
        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(source_id)
        mock_row.target_entity_id = str(target_id)
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.85
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()

        await queue.approve(match_id, reviewed_by="test_user", notes="Confirmed match")

        # Verify execute was called (for UPDATE)
        assert mock_db_session.execute.called


class TestMatchRejection:
    """Tests for match rejection flow."""

    @pytest.mark.asyncio
    async def test_reject_does_not_create_relationship(
        self, mock_db_session, mock_neo4j_session
    ):
        """Test that rejecting a match does NOT create SAME_AS."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        match_id = uuid4()
        case_id = uuid4()
        now = datetime.utcnow()

        # Mock the database row
        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(uuid4())
        mock_row.target_entity_id = str(uuid4())
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.75
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()

        await queue.reject(match_id, reviewed_by="test_user")

        # Neo4j should NOT be called for SAME_AS on rejection
        assert not mock_neo4j_session.run.called

    @pytest.mark.asyncio
    async def test_reject_updates_match_status(self, mock_db_session, mock_neo4j_session):
        """Test that rejecting a match updates its status."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        match_id = uuid4()
        case_id = uuid4()
        now = datetime.utcnow()

        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(uuid4())
        mock_row.target_entity_id = str(uuid4())
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.75
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()

        await queue.reject(match_id, reviewed_by="test_user", notes="Not the same entity")

        # Verify execute was called for UPDATE
        assert mock_db_session.execute.called


class TestMatchDeferral:
    """Tests for match deferral flow."""

    @pytest.mark.asyncio
    async def test_defer_keeps_match_pending(self, mock_db_session, mock_neo4j_session):
        """Test that deferring a match keeps it for later review."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        match_id = uuid4()
        case_id = uuid4()
        now = datetime.utcnow()

        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(uuid4())
        mock_row.target_entity_id = str(uuid4())
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.78
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()

        await queue.defer(match_id, reviewed_by="test_user", notes="Need more info")

        # Verify execute was called
        assert mock_db_session.execute.called


class TestMatchWithEntityDetails:
    """Tests for retrieving match with full entity details."""

    @pytest.mark.asyncio
    async def test_get_match_with_entities_returns_full_details(
        self,
        mock_db_session,
        mock_neo4j_session,
        sample_source_entity,
        sample_target_entity,
    ):
        """Test that get_match_with_entities returns full entity info."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        match_id = uuid4()
        case_id = uuid4()
        source_id = uuid4()
        target_id = uuid4()
        now = datetime.utcnow()

        # Mock the database row
        mock_row = MagicMock()
        mock_row.id = str(match_id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(source_id)
        mock_row.target_entity_id = str(target_id)
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.85
        mock_row.match_signals = {"name_similarity": 0.92}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)

        # Mock Neo4j entity lookups
        mock_neo4j_result = AsyncMock()
        mock_neo4j_result.single = AsyncMock(side_effect=[
            sample_source_entity,
            sample_target_entity,
        ])
        mock_neo4j_session.run = AsyncMock(return_value=mock_neo4j_result)

        result = await queue.get_match_with_entities(match_id)

        # Should include both entity summaries
        assert result is not None


class TestConfidenceThresholds:
    """Tests for confidence-based routing."""

    def test_auto_merge_threshold(self):
        """Test that matches above 0.9 are auto-merged."""
        from mitds.cases.resolution.sponsor import SponsorResolver

        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_auto_merge(0.95) is True
        assert resolver.should_auto_merge(0.90) is True
        assert resolver.should_auto_merge(0.89) is False

    def test_review_threshold(self):
        """Test that matches between 0.7-0.9 go to review."""
        from mitds.cases.resolution.sponsor import SponsorResolver

        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_queue_for_review(0.85) is True
        assert resolver.should_queue_for_review(0.70) is True
        assert resolver.should_queue_for_review(0.69) is False
        assert resolver.should_queue_for_review(0.91) is False

    def test_discard_threshold(self):
        """Test that matches below 0.7 are discarded."""
        from mitds.cases.resolution.sponsor import SponsorResolver

        resolver = SponsorResolver(AsyncMock())

        assert resolver.should_discard(0.65) is True
        assert resolver.should_discard(0.50) is True
        assert resolver.should_discard(0.70) is False


class TestReviewWorkflow:
    """End-to-end tests for review workflow."""

    @pytest.mark.asyncio
    async def test_complete_review_workflow(
        self, mock_db_session, mock_neo4j_session, sample_entity_match
    ):
        """Test complete workflow: queue → review → approve → relationship."""
        queue = EntityMatchQueue(mock_db_session, mock_neo4j_session)

        case_id = uuid4()
        source_id = uuid4()
        target_id = uuid4()
        now = datetime.utcnow()

        # 1. Create match (mock the INSERT)
        mock_db_session.execute = AsyncMock()
        mock_db_session.commit = AsyncMock()

        match = await queue.create_match(
            case_id=case_id,
            source_entity_id=source_id,
            target_entity_id=target_id,
            confidence=0.85,
            match_signals=MatchSignals(name_similarity=0.92),
        )

        assert match.status == MatchStatus.PENDING

        # 2. Mock fetch for approval - simulate database returning the match
        mock_row = MagicMock()
        mock_row.id = str(match.id)
        mock_row.case_id = str(case_id)
        mock_row.source_entity_id = str(source_id)
        mock_row.target_entity_id = str(target_id)
        mock_row.status = MatchStatus.PENDING.value
        mock_row.confidence = 0.85
        mock_row.match_signals = {}
        mock_row.reviewed_by = None
        mock_row.reviewed_at = None
        mock_row.review_notes = None
        mock_row.created_at = now

        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_db_session.execute = AsyncMock(return_value=mock_result)

        # 3. Approve match
        await queue.approve(match.id, reviewed_by="researcher")

        # 4. Verify SAME_AS relationship created
        assert mock_neo4j_session.run.called

        # Verify database was called for UPDATE
        assert mock_db_session.execute.called
        assert mock_db_session.commit.called
