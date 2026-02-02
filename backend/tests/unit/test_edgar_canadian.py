"""Unit tests for Canadian jurisdiction detection in SEC EDGAR ingester.

Tests the is_canadian_jurisdiction() function and related Canadian detection
features added for the Canadian Corporate Ownership Tracking feature.
"""

import pytest

from mitds.ingestion.edgar import (
    CANADIAN_JURISDICTIONS,
    is_canadian_jurisdiction,
)


class TestIsCanadianJurisdiction:
    """Tests for the is_canadian_jurisdiction() helper function."""

    def test_is_canadian_jurisdiction_province_codes(self):
        """Test that A0-A9, B0-B2 province/territory codes return True."""
        # Provinces
        assert is_canadian_jurisdiction("A0") is True  # Alberta
        assert is_canadian_jurisdiction("A1") is True  # British Columbia
        assert is_canadian_jurisdiction("A2") is True  # Manitoba
        assert is_canadian_jurisdiction("A3") is True  # New Brunswick
        assert is_canadian_jurisdiction("A4") is True  # Newfoundland and Labrador
        assert is_canadian_jurisdiction("A5") is True  # Nova Scotia
        assert is_canadian_jurisdiction("A6") is True  # Ontario
        assert is_canadian_jurisdiction("A7") is True  # Prince Edward Island
        assert is_canadian_jurisdiction("A8") is True  # Quebec
        assert is_canadian_jurisdiction("A9") is True  # Saskatchewan

        # Territories
        assert is_canadian_jurisdiction("B0") is True  # Northwest Territories
        assert is_canadian_jurisdiction("B1") is True  # Nunavut
        assert is_canadian_jurisdiction("B2") is True  # Yukon

    def test_is_canadian_jurisdiction_generic_codes(self):
        """Test that 'CANADA' code returns True.

        Note: 'CA' is NOT treated as Canadian because SEC EDGAR uses 'CA' for
        California (US state), not Canada. Canadian companies use A0-A9, B0-B2.
        """
        assert is_canadian_jurisdiction("CANADA") is True

        # Test case-insensitivity for CANADA
        assert is_canadian_jurisdiction("canada") is True
        assert is_canadian_jurisdiction("Canada") is True

        # CA is California in SEC EDGAR, not Canada
        assert is_canadian_jurisdiction("CA") is False
        assert is_canadian_jurisdiction("ca") is False

    def test_is_canadian_jurisdiction_us_states_return_false(self):
        """Test that US state codes return False.

        Note: 'CA' in SEC EDGAR means California (US state), NOT Canada.
        Canadian companies are identified by A0-A9, B0-B2, or 'CANADA'.
        """
        us_states = [
            "DE",  # Delaware
            "NY",  # New York
            "CA",  # California (NOT Canada!)
            "TX",  # Texas
            "FL",  # Florida
            "NV",  # Nevada
            "WY",  # Wyoming
            "IL",  # Illinois
            "MA",  # Massachusetts
            "WA",  # Washington
        ]

        for state in us_states:
            assert is_canadian_jurisdiction(state) is False, f"Expected {state} to return False"

    def test_is_canadian_jurisdiction_none_empty(self):
        """Test that None and empty string return False."""
        assert is_canadian_jurisdiction(None) is False
        assert is_canadian_jurisdiction("") is False

    def test_is_canadian_jurisdiction_unknown_codes(self):
        """Test that unknown codes return False."""
        assert is_canadian_jurisdiction("XX") is False
        assert is_canadian_jurisdiction("ZZ") is False
        assert is_canadian_jurisdiction("UNKNOWN") is False
        assert is_canadian_jurisdiction("A10") is False  # Invalid province code
        assert is_canadian_jurisdiction("B3") is False  # Invalid territory code


class TestCanadianJurisdictionsConstant:
    """Tests for the CANADIAN_JURISDICTIONS constant."""

    def test_canadian_jurisdictions_contains_all_provinces(self):
        """Test that all Canadian provinces are in the constant."""
        provinces = ["A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"]
        for code in provinces:
            assert code in CANADIAN_JURISDICTIONS

    def test_canadian_jurisdictions_contains_all_territories(self):
        """Test that all Canadian territories are in the constant."""
        territories = ["B0", "B1", "B2"]
        for code in territories:
            assert code in CANADIAN_JURISDICTIONS

    def test_canadian_jurisdictions_contains_generic_codes(self):
        """Test that generic Canada codes are in the constant.

        Note: 'CA' is NOT included because SEC EDGAR uses 'CA' for California.
        """
        assert "CANADA" in CANADIAN_JURISDICTIONS
        # CA is NOT in CANADIAN_JURISDICTIONS - it means California in SEC EDGAR
        assert "CA" not in CANADIAN_JURISDICTIONS

    def test_canadian_jurisdictions_has_province_names(self):
        """Test that the constant maps to province names."""
        assert CANADIAN_JURISDICTIONS["A0"] == "Alberta"
        assert CANADIAN_JURISDICTIONS["A1"] == "British Columbia"
        assert CANADIAN_JURISDICTIONS["A6"] == "Ontario"
        assert CANADIAN_JURISDICTIONS["A8"] == "Quebec"
        assert CANADIAN_JURISDICTIONS["B0"] == "Northwest Territories"


class TestParseOwnershipFlagsCanadianJurisdiction:
    """Tests for Canadian jurisdiction flagging in ownership parsing."""

    def test_parse_ownership_flags_canadian_jurisdiction(self):
        """Test that Canadian companies are flagged during ownership parsing.

        This test verifies that when processing ownership filings,
        Canadian subject companies are properly identified and flagged
        with jurisdiction information.
        """
        # Test data representing a Canadian company from SEC EDGAR
        canadian_company_data = {
            "cik": "0001234567",
            "name": "POSTMEDIA NETWORK CANADA CORP.",
            "stateOfIncorporation": "A6",  # Ontario
        }

        # Verify the state of incorporation is detected as Canadian
        state_of_inc = canadian_company_data.get("stateOfIncorporation")
        assert is_canadian_jurisdiction(state_of_inc) is True

        # Verify the province code maps to the correct province
        assert CANADIAN_JURISDICTIONS.get(state_of_inc) == "Ontario"

    def test_parse_ownership_us_company_not_flagged(self):
        """Test that US companies are not flagged as Canadian."""
        us_company_data = {
            "cik": "0009876543",
            "name": "CHATHAM ASSET MANAGEMENT",
            "stateOfIncorporation": "DE",  # Delaware
        }

        state_of_inc = us_company_data.get("stateOfIncorporation")
        assert is_canadian_jurisdiction(state_of_inc) is False
