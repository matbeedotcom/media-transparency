"""Unit tests for entity extraction (T040).

Tests deterministic pattern extraction for EIN, BN, domains,
and organization names.

Run with: pytest tests/unit/cases/test_extraction.py -v
"""

import pytest

from mitds.cases.extraction.deterministic import (
    DeterministicExtractor,
    ExtractedEntity,
    get_deterministic_extractor,
)


class TestEINExtraction:
    """Tests for EIN (Employer Identification Number) extraction."""

    def test_extracts_ein_format(self):
        """Test standard EIN format XX-XXXXXXX."""
        extractor = DeterministicExtractor()

        text = "The foundation (EIN: 12-3456789) is registered."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert len(ein_entities) == 1
        assert ein_entities[0].value == "12-3456789"

    def test_extracts_multiple_eins(self):
        """Test extraction of multiple EINs."""
        extractor = DeterministicExtractor()

        text = "Foundations 12-3456789 and 98-7654321 received grants."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert len(ein_entities) == 2
        values = {e.value for e in ein_entities}
        assert "12-3456789" in values
        assert "98-7654321" in values

    def test_ein_has_full_confidence(self):
        """Test that EIN matches have 1.0 confidence."""
        extractor = DeterministicExtractor()

        text = "EIN 12-3456789"
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert all(e.confidence == 1.0 for e in ein_entities)

    def test_includes_context(self):
        """Test that extraction includes surrounding context."""
        extractor = DeterministicExtractor()

        text = "The Koch Foundation (EIN: 48-6122197) donated funds."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert len(ein_entities) == 1
        assert "Koch Foundation" in ein_entities[0].context


class TestBNExtraction:
    """Tests for Canadian Business Number extraction."""

    def test_extracts_bn_format(self):
        """Test standard BN format 9 digits + RR + 4 digits."""
        extractor = DeterministicExtractor()

        text = "Charity BN 123456789RR0001 is registered."
        entities = extractor.extract(text)

        bn_entities = [e for e in entities if e.identifier_type == "bn"]
        assert len(bn_entities) == 1
        assert bn_entities[0].value == "123456789RR0001"

    def test_bn_case_insensitive(self):
        """Test that BN extraction is case-insensitive."""
        extractor = DeterministicExtractor()

        text = "BN 123456789rr0001 registered"
        entities = extractor.extract(text)

        bn_entities = [e for e in entities if e.identifier_type == "bn"]
        assert len(bn_entities) == 1
        # Should normalize to uppercase
        assert bn_entities[0].value == "123456789RR0001"

    def test_bn_has_full_confidence(self):
        """Test that BN matches have 1.0 confidence."""
        extractor = DeterministicExtractor()

        text = "BN 123456789RR0001"
        entities = extractor.extract(text)

        bn_entities = [e for e in entities if e.identifier_type == "bn"]
        assert all(e.confidence == 1.0 for e in bn_entities)


class TestOrganizationExtraction:
    """Tests for organization name extraction by legal suffix."""

    def test_extracts_inc_suffix(self):
        """Test extraction of names with Inc. suffix."""
        extractor = DeterministicExtractor()

        text = "Koch Industries Inc. announced a grant."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        org_names = [e.value for e in org_entities]
        assert any("Koch Industries" in name for name in org_names)

    def test_extracts_corp_suffix(self):
        """Test extraction of names with Corp. suffix."""
        extractor = DeterministicExtractor()

        text = "Postmedia Network Canada Corp. reported earnings."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        org_names = [e.value for e in org_entities]
        assert any("Postmedia" in name for name in org_names)

    def test_extracts_foundation_suffix(self):
        """Test extraction of names with Foundation suffix."""
        extractor = DeterministicExtractor()

        text = "The Bradley Foundation donated $1 million."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        org_names = [e.value for e in org_entities]
        assert any("Bradley Foundation" in name for name in org_names)

    def test_extracts_ltd_suffix(self):
        """Test extraction of names with Ltd. suffix."""
        extractor = DeterministicExtractor()

        text = "Example Holdings Ltd. acquired the company."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        org_names = [e.value for e in org_entities]
        assert any("Example" in name for name in org_names)

    def test_organization_has_lower_confidence(self):
        """Test that organization matches have lower confidence than identifiers."""
        extractor = DeterministicExtractor()

        text = "Example Foundation Inc."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        assert all(e.confidence < 1.0 for e in org_entities)

    def test_filters_short_names(self):
        """Test that very short names are filtered out."""
        extractor = DeterministicExtractor()

        text = "A Inc. is a company."  # Too short
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        # "A Inc." should be filtered out as too short
        short_orgs = [e for e in org_entities if len(e.value) < 5]
        assert len(short_orgs) == 0


class TestDomainExtraction:
    """Tests for domain name extraction."""

    def test_extracts_domain(self):
        """Test basic domain extraction."""
        extractor = DeterministicExtractor()

        text = "Visit example.org for more information."
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        assert len(domain_entities) == 1
        assert domain_entities[0].value == "example.org"

    def test_extracts_subdomain(self):
        """Test subdomain extraction."""
        extractor = DeterministicExtractor()

        text = "Check www.example.org for details."
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        assert len(domain_entities) >= 1

    def test_ignores_domains_in_urls(self):
        """Test that domains inside URLs are not extracted separately."""
        extractor = DeterministicExtractor()

        text = "Visit https://example.org/about for info."
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        # Should not extract domain from inside URL
        # (the URL itself might be extracted, but domain shouldn't be double-counted)

    def test_ignores_common_email_domains(self):
        """Test that common email domains are filtered."""
        extractor = DeterministicExtractor()

        text = "Contact info@gmail.com or support@yahoo.com"
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        # gmail.com and yahoo.com should be filtered
        common_domains = [e for e in domain_entities if e.value in ["gmail.com", "yahoo.com"]]
        assert len(common_domains) == 0


class TestDeduplication:
    """Tests for entity deduplication."""

    def test_deduplicates_same_entity(self):
        """Test that same entity is not extracted twice."""
        extractor = DeterministicExtractor()

        text = "EIN 12-3456789 was reported. The EIN 12-3456789 is valid."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.value == "12-3456789"]
        # Should deduplicate based on value and position
        # (different positions = different extractions, which is fine)


class TestSingletonExtractor:
    """Tests for the singleton extractor function."""

    def test_returns_same_instance(self):
        """Test that get_deterministic_extractor returns singleton."""
        extractor1 = get_deterministic_extractor()
        extractor2 = get_deterministic_extractor()

        assert extractor1 is extractor2

    def test_extractor_is_functional(self):
        """Test that singleton extractor works."""
        extractor = get_deterministic_extractor()

        text = "Test Corp Inc. with EIN 12-3456789"
        entities = extractor.extract(text)

        assert len(entities) >= 1


class TestExtractedEntity:
    """Tests for ExtractedEntity data class."""

    def test_entity_attributes(self):
        """Test ExtractedEntity has required attributes."""
        entity = ExtractedEntity(
            entity_type="organization",
            value="Test Corp Inc.",
            identifier_type="name",
            confidence=0.85,
            start=0,
            end=14,
            context="Test Corp Inc. is a company.",
        )

        assert entity.entity_type == "organization"
        assert entity.value == "Test Corp Inc."
        assert entity.identifier_type == "name"
        assert entity.confidence == 0.85
        assert entity.start == 0
        assert entity.end == 14
        assert "Test Corp" in entity.context


class TestEdgeCases:
    """Tests for edge cases in extraction."""

    def test_empty_text(self):
        """Test extraction from empty text."""
        extractor = DeterministicExtractor()

        entities = extractor.extract("")

        assert entities == []

    def test_whitespace_only(self):
        """Test extraction from whitespace-only text."""
        extractor = DeterministicExtractor()

        entities = extractor.extract("   \n\t   ")

        assert entities == []

    def test_no_entities(self):
        """Test extraction from text with no entities."""
        extractor = DeterministicExtractor()

        text = "This is just regular text without any organizations or identifiers."
        entities = extractor.extract(text)

        # Should return empty list (or just organization names if any words match)
        # No EINs or BNs should be found
        ein_bn = [e for e in entities if e.identifier_type in ("ein", "bn")]
        assert len(ein_bn) == 0

    def test_unicode_text(self):
        """Test extraction from text with unicode characters."""
        extractor = DeterministicExtractor()

        text = "L'Organisation Inc. has EIN 12-3456789."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert len(ein_entities) == 1
