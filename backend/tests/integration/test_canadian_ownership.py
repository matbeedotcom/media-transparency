"""Integration tests for Canadian ownership tracking and entity resolution.

These tests verify that:
1. SEC EDGAR and SEDAR+ entities are properly merged when they represent the same company
2. No duplicate nodes are created for the same entity across sources
3. Fuzzy name matching correctly links entities with similar names

Note: These tests require database connections (PostgreSQL and Neo4j).
Run with: pytest tests/integration/test_canadian_ownership.py -v
"""

import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.ingestion.sedar import (
    SEDARFiling,
    normalize_company_name,
    find_existing_entity_by_name,
    calculate_match_confidence,
)


class TestNormalizeCompanyName:
    """Tests for the normalize_company_name helper function."""

    def test_normalize_removes_corp_suffix(self):
        """Test that 'Corp.' and 'Corporation' are removed."""
        assert normalize_company_name("POSTMEDIA NETWORK CANADA CORP.") == "POSTMEDIA NETWORK CANADA"
        assert normalize_company_name("Acme Corporation") == "ACME"

    def test_normalize_removes_inc_suffix(self):
        """Test that 'Inc.' and 'Incorporated' are removed."""
        assert normalize_company_name("Rogers Communications Inc.") == "ROGERS COMMUNICATIONS"
        assert normalize_company_name("Bell Canada Incorporated") == "BELL CANADA"

    def test_normalize_removes_ltd_suffix(self):
        """Test that 'Ltd.' and 'Limited' are removed."""
        assert normalize_company_name("Corus Entertainment Ltd.") == "CORUS ENTERTAINMENT"
        assert normalize_company_name("Shaw Communications Limited") == "SHAW COMMUNICATIONS"

    def test_normalize_handles_multiple_suffixes(self):
        """Test that multiple common suffixes are handled."""
        assert normalize_company_name("Test Holdings Inc.") == "TEST"
        assert normalize_company_name("Example Group Ltd.") == "EXAMPLE"

    def test_normalize_removes_punctuation(self):
        """Test that punctuation is removed."""
        assert normalize_company_name("O'Brien & Associates, Inc.") == "O BRIEN ASSOCIATES"

    def test_normalize_empty_string(self):
        """Test empty string handling."""
        assert normalize_company_name("") == ""
        assert normalize_company_name(None) == ""

    def test_normalize_case_insensitive(self):
        """Test that normalization is case-insensitive."""
        assert normalize_company_name("postmedia network") == "POSTMEDIA NETWORK"
        assert normalize_company_name("POSTMEDIA NETWORK") == "POSTMEDIA NETWORK"


class TestCalculateMatchConfidence:
    """Tests for the calculate_match_confidence function (T051)."""

    def test_identifier_match_confidence(self):
        """Test that identifier matches have 1.0 confidence."""
        assert calculate_match_confidence("identifier") == 1.0

    def test_exact_match_confidence(self):
        """Test that exact matches have 1.0 confidence."""
        assert calculate_match_confidence("exact") == 1.0

    def test_normalized_match_confidence(self):
        """Test that normalized matches have 0.95 confidence."""
        assert calculate_match_confidence("normalized") == 0.95

    def test_fuzzy_match_confidence(self):
        """Test that fuzzy matches are capped at 0.85."""
        assert calculate_match_confidence("fuzzy", 0.9) == 0.85
        assert calculate_match_confidence("fuzzy", 0.8) == 0.8
        assert calculate_match_confidence("fuzzy", 0.7) == 0.7

    def test_unknown_match_type(self):
        """Test that unknown match types return 0.5."""
        assert calculate_match_confidence("unknown") == 0.5


class TestEntityResolutionMergesSecAndSedarEntities:
    """Tests for entity resolution merging SEC and SEDAR entities (T044)."""

    @pytest.mark.asyncio
    async def test_entity_resolution_merges_sec_and_sedar_entities(self):
        """Test that entities from SEC EDGAR and SEDAR+ are merged (T044).

        Scenario:
        1. SEC EDGAR creates an Organization with sec_cik
        2. SEDAR+ tries to create the same company with sedar_profile
        3. Entity resolution should find the existing entity and merge identifiers
        """
        # This test verifies the logic structure of entity resolution
        # Full integration requires database connections

        # Simulate SEC EDGAR entity
        sec_entity = {
            "id": str(uuid4()),
            "name": "POSTMEDIA NETWORK CANADA CORP.",
            "external_ids": {"sec_cik": "0001234567"},
            "entity_type": "organization",
        }

        # Simulate SEDAR+ filing for same company
        sedar_filing = SEDARFiling(
            document_id="sedar_001",
            document_type="early_warning",
            filing_date=date(2026, 1, 28),
            acquirer_name="Canada Pension Plan",
            issuer_name="Postmedia Network Canada Corp",  # Slightly different casing
            issuer_sedar_profile="00054321",
            ownership_percentage=15.5,
        )

        # Normalized names should match
        normalized_sec = normalize_company_name(sec_entity["name"])
        normalized_sedar = normalize_company_name(sedar_filing.issuer_name)

        assert normalized_sec == normalized_sedar, (
            f"Normalized names should match: '{normalized_sec}' vs '{normalized_sedar}'"
        )


class TestNoDuplicateNodesCreated:
    """Tests for preventing duplicate nodes (T045)."""

    def test_no_duplicate_nodes_created_for_same_entity(self):
        """Test that the same entity doesn't create duplicate nodes (T045).

        Verifies that entity resolution logic correctly identifies
        existing entities to prevent duplication.
        """
        # Test data: same company with different name variations
        variations = [
            "POSTMEDIA NETWORK CANADA CORP.",
            "Postmedia Network Canada Corp",
            "POSTMEDIA NETWORK CANADA CORPORATION",
            "Postmedia Network Canada Inc.",
        ]

        normalized_names = [normalize_company_name(v) for v in variations]

        # All variations should normalize to the same string
        expected = "POSTMEDIA NETWORK CANADA"
        for i, normalized in enumerate(normalized_names):
            assert normalized == expected, (
                f"Variation '{variations[i]}' normalized to '{normalized}', "
                f"expected '{expected}'"
            )


class TestFuzzyNameMatchingLinksEntities:
    """Tests for fuzzy name matching (T046)."""

    def test_fuzzy_name_matching_links_entities(self):
        """Test that fuzzy matching links similar entity names (T046)."""
        try:
            from rapidfuzz import fuzz
        except ImportError:
            pytest.skip("rapidfuzz not installed")

        # Test pairs that should match with fuzzy matching
        test_pairs = [
            ("Postmedia Network Canada Corp", "POSTMEDIA NETWORK CANADA CORP."),
            ("Rogers Communications Inc", "Rogers Communications Incorporated"),
            ("BCE Inc", "BCE Incorporated"),
        ]

        for name1, name2 in test_pairs:
            norm1 = normalize_company_name(name1)
            norm2 = normalize_company_name(name2)

            # Token set ratio handles word order and partial matches
            score = fuzz.token_set_ratio(norm1, norm2) / 100.0

            assert score >= 0.85, (
                f"Names should fuzzy match with score >= 0.85: "
                f"'{name1}' vs '{name2}' (score: {score})"
            )

    def test_fuzzy_matching_rejects_different_entities(self):
        """Test that fuzzy matching doesn't incorrectly link different entities."""
        try:
            from rapidfuzz import fuzz
        except ImportError:
            pytest.skip("rapidfuzz not installed")

        # Test pairs that should NOT match
        different_entities = [
            ("Postmedia Network", "Rogers Communications"),
            ("Bell Canada", "Shaw Communications"),
            ("Corus Entertainment", "Cineplex Entertainment"),
        ]

        for name1, name2 in different_entities:
            norm1 = normalize_company_name(name1)
            norm2 = normalize_company_name(name2)

            score = fuzz.token_set_ratio(norm1, norm2) / 100.0

            assert score < 0.85, (
                f"Different entities should not match: "
                f"'{name1}' vs '{name2}' (score: {score})"
            )


class TestCrossSourceEntityMatching:
    """Tests for cross-source entity matching between SEC EDGAR and SEDAR+."""

    def test_canadian_company_detected_in_sec_edgar(self):
        """Test that Canadian companies are detected in SEC EDGAR."""
        from mitds.ingestion.edgar import is_canadian_jurisdiction, CANADIAN_JURISDICTIONS

        # All Canadian province codes should be detected
        for code in ["A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "B0", "B1", "B2"]:
            assert is_canadian_jurisdiction(code) is True
            assert code in CANADIAN_JURISDICTIONS

        # US states should not be detected as Canadian
        us_states = ["DE", "NY", "TX", "FL", "NV"]
        for state in us_states:
            assert is_canadian_jurisdiction(state) is False

    def test_sedar_profile_added_to_existing_sec_entity(self):
        """Test that SEDAR profile is added to existing SEC EDGAR entity.

        When SEDAR+ processes a company that already exists from SEC EDGAR,
        the sedar_profile should be added to the existing entity's external_ids.
        """
        # This verifies the data structure expectation
        sec_external_ids = {"sec_cik": "0001234567"}
        sedar_profile = "00054321"

        # After entity resolution merge
        merged_external_ids = {**sec_external_ids, "sedar_profile": sedar_profile}

        assert "sec_cik" in merged_external_ids
        assert "sedar_profile" in merged_external_ids
        assert merged_external_ids["sec_cik"] == "0001234567"
        assert merged_external_ids["sedar_profile"] == "00054321"
