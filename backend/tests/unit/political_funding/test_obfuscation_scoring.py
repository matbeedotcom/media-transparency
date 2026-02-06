"""Unit tests for political funding obfuscation scoring."""

import pytest
from datetime import datetime
from uuid import UUID, uuid4

from mitds.detection.political_funding import (
    PoliticalFundingObfuscationDetector,
    PoliticalFundingSignal,
    PoliticalFundingSignalType,
    SIGNAL_WEIGHTS,
    MINIMUM_SIGNALS,
    MINIMUM_CATEGORIES,
)


class TestSignalWeights:
    """Test that each signal type contributes correct weight."""

    def test_election_contribution_weight(self):
        """Test election contribution signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.ELECTION_CONTRIBUTION]
        assert score == pytest.approx(expected, abs=0.001)

    def test_shared_beneficial_owner_weight(self):
        """Test shared beneficial owner signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER]
        assert score == pytest.approx(expected, abs=0.001)

    def test_lobbying_client_weight(self):
        """Test lobbying client signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.LOBBYING_CLIENT,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.LOBBYING_CLIENT]
        assert score == pytest.approx(expected, abs=0.001)

    def test_shared_directors_weight(self):
        """Test shared directors signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_DIRECTORS,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.SHARED_DIRECTORS]
        assert score == pytest.approx(expected, abs=0.001)

    def test_shared_address_weight(self):
        """Test shared address signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_ADDRESS,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.SHARED_ADDRESS]
        assert score == pytest.approx(expected, abs=0.001)

    def test_ppsa_weight(self):
        """Test PPSA secured interest signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.PPSA_SECURED_INTEREST,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.PPSA_SECURED_INTEREST]
        assert score == pytest.approx(expected, abs=0.001)

    def test_shared_agent_weight(self):
        """Test shared agent signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_AGENT,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.SHARED_AGENT]
        assert score == pytest.approx(expected, abs=0.001)

    def test_shell_heuristic_weight(self):
        """Test shell heuristic signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHELL_HEURISTIC,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.SHELL_HEURISTIC]
        assert score == pytest.approx(expected, abs=0.001)

    def test_temporal_correlation_weight(self):
        """Test temporal correlation signal weight."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.TEMPORAL_CORRELATION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = SIGNAL_WEIGHTS[PoliticalFundingSignalType.TEMPORAL_CORRELATION]
        assert score == pytest.approx(expected, abs=0.001)

    def test_signal_strength_scaling(self):
        """Test that signal strength scales contribution correctly."""
        signal_full = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        signal_half = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=0.5,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score_full = detector._calculate_score([signal_full])
        score_half = detector._calculate_score([signal_half])

        assert score_half == pytest.approx(score_full / 2.0, abs=0.001)

    def test_signal_confidence_scaling(self):
        """Test that signal confidence scales contribution correctly."""
        signal_full = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        signal_half = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=0.5,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score_full = detector._calculate_score([signal_full])
        score_half = detector._calculate_score([signal_half])

        assert score_half == pytest.approx(score_full / 2.0, abs=0.001)


class TestMinimumThresholds:
    """Test minimum signal and category thresholds."""

    def test_single_signal_not_flagged(self):
        """Test that single signal does not flag."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        result = detector._calculate_score([signal])

        # Score should be calculated but not flagged
        assert result > 0.0
        # But we need to check flagging logic separately
        categories = set(s.category for s in [signal])
        is_flagged = (
            len([signal]) >= MINIMUM_SIGNALS
            and len(categories) >= MINIMUM_CATEGORIES
            and result > 0.0
        )
        assert not is_flagged  # Single signal, single category

    def test_two_signals_same_category_not_flagged(self):
        """Test that two signals from same category do not flag."""
        signal1 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )
        signal2 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=0.8,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        signals = [signal1, signal2]
        detector = PoliticalFundingObfuscationDetector()
        result = detector._calculate_score(signals)

        categories = set(s.category for s in signals)
        is_flagged = (
            len(signals) >= MINIMUM_SIGNALS
            and len(categories) >= MINIMUM_CATEGORIES
            and result > 0.0
        )
        assert not is_flagged  # Two signals but only one category

    def test_two_signals_two_categories_flagged(self):
        """Test that two signals from two categories flag."""
        signal1 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )
        signal2 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        signals = [signal1, signal2]
        detector = PoliticalFundingObfuscationDetector()
        result = detector._calculate_score(signals)

        categories = set(s.category for s in signals)
        is_flagged = (
            len(signals) >= MINIMUM_SIGNALS
            and len(categories) >= MINIMUM_CATEGORIES
            and result > 0.0
        )
        assert is_flagged  # Two signals, two categories

    def test_custom_thresholds(self):
        """Test detector with custom thresholds."""
        signal1 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )
        signal2 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        signals = [signal1, signal2]

        # Custom detector requiring 3 signals
        detector = PoliticalFundingObfuscationDetector(min_signals=3, min_categories=2)
        result = detector._calculate_score(signals)

        categories = set(s.category for s in signals)
        is_flagged = (
            len(signals) >= detector.min_signals
            and len(categories) >= detector.min_categories
            and result > 0.0
        )
        assert not is_flagged  # Only 2 signals, need 3


class TestOverallScoreCalculation:
    """Test overall score calculation."""

    def test_empty_signals_zero_score(self):
        """Test that empty signals return zero score."""
        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([])
        assert score == 0.0

    def test_multiple_signals_additive(self):
        """Test that multiple signals contribute additively."""
        signal1 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )
        signal2 = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.SHARED_BENEFICIAL_OWNER,
            strength=1.0,
            confidence=1.0,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score_both = detector._calculate_score([signal1, signal2])
        score_1 = detector._calculate_score([signal1])
        score_2 = detector._calculate_score([signal2])

        assert score_both == pytest.approx(score_1 + score_2, abs=0.001)

    def test_score_capped_at_one(self):
        """Test that score is capped at 1.0."""
        # Create many high-weight signals
        signals = [
            PoliticalFundingSignal(
                signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
                strength=1.0,
                confidence=1.0,
                entity_ids=[uuid4()],
            )
            for _ in range(10)
        ]

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score(signals)

        assert score <= 1.0

    def test_score_with_partial_strength(self):
        """Test score calculation with partial signal strength."""
        signal = PoliticalFundingSignal(
            signal_type=PoliticalFundingSignalType.ELECTION_CONTRIBUTION,
            strength=0.5,
            confidence=0.8,
            entity_ids=[uuid4()],
        )

        detector = PoliticalFundingObfuscationDetector()
        score = detector._calculate_score([signal])

        expected = (
            SIGNAL_WEIGHTS[PoliticalFundingSignalType.ELECTION_CONTRIBUTION]
            * 0.5
            * 0.8
        )
        assert score == pytest.approx(expected, abs=0.001)
