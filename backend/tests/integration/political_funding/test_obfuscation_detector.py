"""Integration tests for political funding obfuscation detector."""

import pytest
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from mitds.detection.political_funding import PoliticalFundingObfuscationDetector


@pytest.mark.integration
@pytest.mark.asyncio
class TestPoliticalFundingObfuscationDetector:
    """Integration tests for PoliticalFundingObfuscationDetector."""

    async def test_analyze_with_election_contributions(self):
        """Test detector with election contribution signals."""
        # This test requires actual database setup with test data
        # For now, we'll test the structure and error handling

        detector = PoliticalFundingObfuscationDetector()

        # Use a known entity ID if available, otherwise use a test UUID
        # In a real test, you would seed test data first
        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=["election_contribution"],
        )

        # Verify result structure
        assert "overall_score" in result
        assert "is_flagged" in result
        assert "signal_count" in result
        assert "category_count" in result
        assert "signals" in result
        assert "suspected_funders" in result

        assert isinstance(result["overall_score"], float)
        assert isinstance(result["is_flagged"], bool)
        assert isinstance(result["signal_count"], int)
        assert isinstance(result["category_count"], int)
        assert isinstance(result["signals"], list)
        assert isinstance(result["suspected_funders"], list)

        assert 0.0 <= result["overall_score"] <= 1.0
        assert result["signal_count"] >= 0
        assert result["category_count"] >= 0

    async def test_analyze_with_multiple_signal_types(self):
        """Test detector with multiple signal types."""
        detector = PoliticalFundingObfuscationDetector()

        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=[
                "election_contribution",
                "shared_beneficial_owner",
                "shared_directors",
            ],
        )

        # Verify result structure
        assert "overall_score" in result
        assert "is_flagged" in result
        assert "signal_count" in result
        assert "category_count" in result

        # Should check up to 3 signal types
        assert result["signal_count"] <= 3

    async def test_analyze_with_specific_funders(self):
        """Test detector with specific funder entity IDs."""
        detector = PoliticalFundingObfuscationDetector()

        advertiser_id = uuid4()
        funder_ids = [uuid4(), uuid4()]

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=funder_ids,
            include_signals=None,  # Check all signals
        )

        # Verify result structure
        assert "overall_score" in result
        assert "is_flagged" in result
        assert "suspected_funders" in result

        # Suspected funders should only include funder_ids if found
        # (or be empty if no signals detected)
        for funder_id in result["suspected_funders"]:
            assert isinstance(funder_id, str)
            # In real scenario, would verify funder_id is in funder_ids

    async def test_analyze_empty_result(self):
        """Test detector with entity that has no signals."""
        detector = PoliticalFundingObfuscationDetector()

        # Use a UUID that likely doesn't exist
        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=None,
        )

        # Should return empty result structure
        assert result["overall_score"] == 0.0
        assert result["is_flagged"] is False
        assert result["signal_count"] == 0
        assert result["category_count"] == 0
        assert result["signals"] == []
        assert result["suspected_funders"] == []

    async def test_analyze_signal_structure(self):
        """Test that detected signals have correct structure."""
        detector = PoliticalFundingObfuscationDetector()

        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=["election_contribution"],
        )

        # Check signal structure if any signals found
        for signal in result["signals"]:
            assert "signal_type" in signal
            assert "category" in signal
            assert "strength" in signal
            assert "confidence" in signal
            assert "entity_ids" in signal
            assert "evidence_ids" in signal
            assert "metadata" in signal
            assert "detected_at" in signal

            assert isinstance(signal["signal_type"], str)
            assert isinstance(signal["category"], str)
            assert isinstance(signal["strength"], float)
            assert isinstance(signal["confidence"], float)
            assert isinstance(signal["entity_ids"], list)
            assert isinstance(signal["evidence_ids"], list)
            assert isinstance(signal["metadata"], dict)

            assert 0.0 <= signal["strength"] <= 1.0
            assert 0.0 <= signal["confidence"] <= 1.0

    async def test_minimum_thresholds(self):
        """Test that minimum thresholds are enforced."""
        detector = PoliticalFundingObfuscationDetector(
            min_signals=2, min_categories=2
        )

        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=None,
        )

        # If flagged, must meet minimums
        if result["is_flagged"]:
            assert result["signal_count"] >= detector.min_signals
            assert result["category_count"] >= detector.min_categories

    async def test_custom_thresholds(self):
        """Test detector with custom thresholds."""
        detector = PoliticalFundingObfuscationDetector(
            min_signals=3, min_categories=2
        )

        advertiser_id = uuid4()

        result = await detector.analyze(
            advertiser_entity_id=advertiser_id,
            funder_entity_ids=None,
            include_signals=None,
        )

        # Verify custom thresholds are used
        if result["is_flagged"]:
            assert result["signal_count"] >= 3
            assert result["category_count"] >= 2
