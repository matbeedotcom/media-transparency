"""Tests for cross-border entity resolution.

Tests cover:
1. Address extraction from IRS 990 Schedule I (foreign addresses)
2. FuzzyMatcher location matching (country, postal code)
3. CrossBorderResolver matching logic
"""

import pytest
from uuid import uuid4
from xml.etree import ElementTree as ET

from mitds.models.base import Address
from mitds.resolution.matcher import (
    FuzzyMatcher,
    MatchCandidate,
    MatchStrategy,
)
from mitds.resolution.cross_border import (
    CrossBorderResolver,
    UnresolvedGrant,
    CrossBorderResolutionResult,
)


# =============================================================================
# IRS 990 Address Extraction Tests
# =============================================================================


class TestIRS990AddressExtraction:
    """Tests for IRS 990 Schedule I recipient address extraction."""

    def test_extract_us_address(self):
        """Test extraction of US address from Schedule I grant."""
        from mitds.ingestion.irs990 import IRS990Ingester

        # Sample XML with US address
        xml_content = """
        <RecipientTable xmlns="http://www.irs.gov/efile">
            <RecipientBusinessName>
                <BusinessNameLine1Txt>AMERICAN CHARITY INC</BusinessNameLine1Txt>
            </RecipientBusinessName>
            <RecipientEIN>123456789</RecipientEIN>
            <USAddress>
                <AddressLine1Txt>123 Main Street</AddressLine1Txt>
                <CityNm>New York</CityNm>
                <StateAbbreviationCd>NY</StateAbbreviationCd>
                <ZIPCd>10001</ZIPCd>
            </USAddress>
            <CashGrantAmt>50000</CashGrantAmt>
        </RecipientTable>
        """

        root = ET.fromstring(xml_content)
        ingester = IRS990Ingester()
        address = ingester._extract_recipient_address(root)

        assert address is not None
        assert address.street == "123 Main Street"
        assert address.city == "New York"
        assert address.state == "NY"
        assert address.postal_code == "10001"
        assert address.country == "US"

    def test_extract_canadian_address(self):
        """Test extraction of Canadian (foreign) address from Schedule I grant."""
        from mitds.ingestion.irs990 import IRS990Ingester

        # Sample XML with Canadian foreign address
        xml_content = """
        <RecipientTable xmlns="http://www.irs.gov/efile">
            <RecipientBusinessName>
                <BusinessNameLine1Txt>CANADIAN HEALTH FOUNDATION</BusinessNameLine1Txt>
            </RecipientBusinessName>
            <ForeignAddress>
                <AddressLine1Txt>456 Queen Street West</AddressLine1Txt>
                <CityNm>Toronto</CityNm>
                <ProvinceOrStateNm>ON</ProvinceOrStateNm>
                <ForeignPostalCd>M5V 2H1</ForeignPostalCd>
                <CountryCd>CA</CountryCd>
            </ForeignAddress>
            <CashGrantAmt>100000</CashGrantAmt>
        </RecipientTable>
        """

        root = ET.fromstring(xml_content)
        ingester = IRS990Ingester()
        address = ingester._extract_recipient_address(root)

        assert address is not None
        assert address.street == "456 Queen Street West"
        assert address.city == "Toronto"
        assert address.state == "ON"
        assert address.postal_code == "M5V 2H1"
        assert address.country == "CA"

    def test_extract_no_address(self):
        """Test handling of grant with no address."""
        from mitds.ingestion.irs990 import IRS990Ingester

        xml_content = """
        <RecipientTable xmlns="http://www.irs.gov/efile">
            <RecipientBusinessName>
                <BusinessNameLine1Txt>SOME CHARITY</BusinessNameLine1Txt>
            </RecipientBusinessName>
            <CashGrantAmt>25000</CashGrantAmt>
        </RecipientTable>
        """

        root = ET.fromstring(xml_content)
        ingester = IRS990Ingester()
        address = ingester._extract_recipient_address(root)

        assert address is None


# =============================================================================
# FuzzyMatcher Location Matching Tests
# =============================================================================


class TestFuzzyMatcherLocation:
    """Tests for FuzzyMatcher location-based confidence boosting."""

    @pytest.fixture
    def matcher(self):
        """Create a FuzzyMatcher instance."""
        return FuzzyMatcher(min_score=70)

    @pytest.fixture
    def source_candidate(self):
        """Create a source candidate (unresolved grant recipient)."""
        return MatchCandidate(
            entity_id=uuid4(),
            entity_type="ORGANIZATION",
            name="CANADIAN HEALTH FOUNDATION",
            identifiers={},
            attributes={
                "address": {
                    "city": "Toronto",
                    "state": "ON",
                    "postal_code": "M5V 2H1",
                    "country": "CA",
                }
            },
        )

    def test_country_match_boost(self, matcher, source_candidate):
        """Test that country match adds confidence boost."""
        target = MatchCandidate(
            entity_id=uuid4(),
            entity_type="ORGANIZATION",
            name="CANADIAN HEALTH FOUNDATION INC",
            identifiers={"bn": "123456789RR0001"},
            attributes={
                "address": {
                    "city": "Vancouver",
                    "state": "BC",
                    "postal_code": "V6B 1A1",
                    "country": "CA",
                }
            },
        )

        matches = matcher.find_matches(source_candidate, [target], threshold=0.5)

        assert len(matches) == 1
        match = matches[0]
        assert match.match_details.get("country_match") is True

    def test_city_and_state_match_boost(self, matcher, source_candidate):
        """Test that city + state match adds significant boost."""
        target = MatchCandidate(
            entity_id=uuid4(),
            entity_type="ORGANIZATION",
            name="CANADIAN HEALTH FOUNDATION INC",
            identifiers={"bn": "123456789RR0001"},
            attributes={
                "address": {
                    "city": "Toronto",
                    "state": "ON",
                    "postal_code": "M5V 3K2",
                    "country": "CA",
                }
            },
        )

        matches = matcher.find_matches(source_candidate, [target], threshold=0.5)

        assert len(matches) == 1
        match = matches[0]
        assert match.match_details.get("city_match") is True
        assert match.match_details.get("state_match") is True

    def test_postal_code_prefix_match_boost(self, matcher, source_candidate):
        """Test that postal code prefix (FSA) match adds boost."""
        target = MatchCandidate(
            entity_id=uuid4(),
            entity_type="ORGANIZATION",
            name="CANADIAN HEALTH FOUNDATION INC",
            identifiers={"bn": "123456789RR0001"},
            attributes={
                "address": {
                    "city": "Toronto",
                    "state": "ON",
                    "postal_code": "M5V 9Z9",  # Same FSA (M5V)
                    "country": "CA",
                }
            },
        )

        matches = matcher.find_matches(source_candidate, [target], threshold=0.5)

        assert len(matches) == 1
        match = matches[0]
        assert match.match_details.get("postal_match") is True
        assert match.match_details.get("postal_boost", 0) > 0

    def test_full_postal_code_match_boost(self, matcher, source_candidate):
        """Test that full postal code match adds maximum boost."""
        target = MatchCandidate(
            entity_id=uuid4(),
            entity_type="ORGANIZATION",
            name="CANADIAN HEALTH FOUNDATION INC",
            identifiers={"bn": "123456789RR0001"},
            attributes={
                "address": {
                    "city": "Toronto",
                    "state": "ON",
                    "postal_code": "M5V 2H1",  # Exact match
                    "country": "CA",
                }
            },
        )

        matches = matcher.find_matches(source_candidate, [target], threshold=0.5)

        assert len(matches) == 1
        match = matches[0]
        assert match.match_details.get("postal_boost") == 0.1


class TestFuzzyMatcherPostalBoost:
    """Test the postal code matching boost calculation."""

    @pytest.fixture
    def matcher(self):
        return FuzzyMatcher()

    def test_full_postal_match(self, matcher):
        """Test full postal code match returns max boost."""
        boost = matcher._postal_match_boost("M5V 2H1", "M5V 2H1")
        assert boost == 0.1

    def test_full_postal_match_normalized(self, matcher):
        """Test full postal match with different formatting."""
        boost = matcher._postal_match_boost("M5V2H1", "m5v 2h1")
        assert boost == 0.1

    def test_fsa_prefix_match(self, matcher):
        """Test FSA (first 3 chars) match returns partial boost."""
        boost = matcher._postal_match_boost("M5V 2H1", "M5V 9Z9")
        assert boost == 0.05

    def test_us_zip_prefix_match(self, matcher):
        """Test US ZIP sectional center match."""
        boost = matcher._postal_match_boost("10001", "10099")
        assert boost == 0.05

    def test_no_match(self, matcher):
        """Test no match returns zero boost."""
        boost = matcher._postal_match_boost("M5V 2H1", "V6B 1A1")
        assert boost == 0.0

    def test_missing_postal(self, matcher):
        """Test missing postal code returns zero boost."""
        assert matcher._postal_match_boost(None, "M5V 2H1") == 0.0
        assert matcher._postal_match_boost("M5V 2H1", None) == 0.0
        assert matcher._postal_match_boost(None, None) == 0.0


# =============================================================================
# CrossBorderResolver Tests
# =============================================================================


class TestCrossBorderResolver:
    """Tests for CrossBorderResolver logic."""

    @pytest.fixture
    def resolver(self):
        """Create a CrossBorderResolver instance."""
        return CrossBorderResolver(
            auto_merge_threshold=0.9,
            review_threshold=0.7,
        )

    @pytest.fixture
    def sample_grant(self):
        """Create a sample unresolved grant."""
        return UnresolvedGrant(
            recipient_id=uuid4(),
            recipient_name="CANADIAN RELIEF ORGANIZATION",
            recipient_city="Toronto",
            recipient_state="ON",
            recipient_postal="M5V 2H1",
            recipient_country="CA",
            funder_id=uuid4(),
            funder_name="US Foundation Inc",
            funder_ein="12-3456789",
            amount=50000,
            fiscal_year=2023,
        )

    def test_postal_boost_calculation(self, resolver):
        """Test postal code boost calculation in resolver."""
        # Full match
        boost = resolver._calculate_postal_boost("M5V 2H1", "M5V 2H1")
        assert boost == 0.1

        # FSA match
        boost = resolver._calculate_postal_boost("M5V 2H1", "M5V 9Z9")
        assert boost == 0.05

        # No match
        boost = resolver._calculate_postal_boost("M5V 2H1", "V6B 1A1")
        assert boost == 0.0

    def test_resolution_result_model(self, sample_grant):
        """Test CrossBorderResolutionResult model."""
        result = CrossBorderResolutionResult(
            grant=sample_grant,
            matched_entity_id=uuid4(),
            matched_entity_name="CANADIAN RELIEF ORGANIZATION INC",
            matched_entity_bn="123456789RR0001",
            confidence=0.92,
            strategy=MatchStrategy.FUZZY,
            action="auto_merged",
        )

        assert result.grant.recipient_name == "CANADIAN RELIEF ORGANIZATION"
        assert result.confidence == 0.92
        assert result.action == "auto_merged"

    def test_unresolved_grant_model(self, sample_grant):
        """Test UnresolvedGrant model."""
        assert sample_grant.recipient_country == "CA"
        assert sample_grant.recipient_city == "Toronto"
        assert sample_grant.recipient_state == "ON"
        assert sample_grant.amount == 50000


# =============================================================================
# Integration-style Tests (require mocking)
# =============================================================================


class TestCrossBorderIntegration:
    """Integration tests for cross-border resolution flow.

    These tests verify the end-to-end logic without hitting Neo4j.
    """

    def test_resolution_action_thresholds(self):
        """Test that resolution actions are determined by confidence thresholds."""
        resolver = CrossBorderResolver(
            auto_merge_threshold=0.9,
            review_threshold=0.7,
        )

        # Confidence >= 0.9 should auto-merge
        assert resolver.auto_merge_threshold == 0.9

        # Confidence >= 0.7 but < 0.9 should queue for review
        assert resolver.review_threshold == 0.7

    def test_matcher_initialization(self):
        """Test that resolver initializes matcher correctly."""
        resolver = CrossBorderResolver()

        assert resolver._matcher is not None
        assert resolver._queue is not None

    def test_address_extraction_in_grant_parsing(self):
        """Test that grant data properly includes address fields."""
        grant = UnresolvedGrant(
            recipient_id=uuid4(),
            recipient_name="Test Org",
            recipient_city="Montreal",
            recipient_state="QC",
            recipient_postal="H2X 1Y4",
            recipient_country="CA",
        )

        assert grant.recipient_city == "Montreal"
        assert grant.recipient_state == "QC"
        assert grant.recipient_postal == "H2X 1Y4"
        assert grant.recipient_country == "CA"


# =============================================================================
# Address Model Tests
# =============================================================================


class TestAddressModel:
    """Tests for the Address model."""

    def test_us_address(self):
        """Test US address creation."""
        address = Address(
            street="123 Main St",
            city="New York",
            state="NY",
            postal_code="10001",
            country="US",
        )

        assert address.country == "US"
        assert str(address) == "123 Main St, New York, NY, 10001"

    def test_canadian_address(self):
        """Test Canadian address creation."""
        address = Address(
            street="456 Queen St W",
            city="Toronto",
            state="ON",
            postal_code="M5V 2H1",
            country="CA",
        )

        assert address.country == "CA"
        assert address.state == "ON"

    def test_address_with_missing_fields(self):
        """Test address with some fields missing."""
        address = Address(
            city="Vancouver",
            state="BC",
            country="CA",
        )

        assert address.street is None
        assert address.postal_code is None
        assert str(address) == "Vancouver, BC"
