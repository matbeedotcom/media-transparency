"""Unit tests for ISC HTML parsing (T024).

Tests the BeneficialOwnershipIngester's HTML parsing methods
for extracting ISC data from ISED web search pages.
"""

import pytest

from mitds.ingestion.beneficial_ownership import BeneficialOwnershipIngester


class TestISCScraper:
    """Test ISC data extraction from ISED HTML."""

    def setup_method(self):
        self.ingester = BeneficialOwnershipIngester()

    def test_extract_corp_name_from_html(self):
        """Test corporation name extraction from page header."""
        html = '<div><h2 class="corp-name">Test Corporation Inc.</h2></div>'
        name = self.ingester._extract_corp_name(html)
        assert name == "Test Corporation Inc."

    def test_extract_corp_name_missing(self):
        """Test that missing corp name returns None."""
        html = "<div><p>No heading here</p></div>"
        name = self.ingester._extract_corp_name(html)
        assert name is None

    def test_parse_isc_section_basic(self):
        """Test parsing of ISC section with basic table structure."""
        html = """
        <div id="isc-section">
            <h3>Individuals with Significant Control</h3>
            <table>
                <tr><td>Name</td><td>John Smith</td></tr>
                <tr><td>Date Became ISC</td><td>2024-03-15</td></tr>
                <tr><td>Control Description</td><td>holds >25% shares</td></tr>
                <tr><td>Service Address</td><td>123 Main St, Toronto, ON</td></tr>
            </table>
        </div>
        """
        records = self.ingester._parse_isc_section(html)
        assert len(records) >= 1
        assert records[0]["full_name"] == "John Smith"

    def test_parse_isc_section_empty(self):
        """Test parsing returns empty list when no ISC section found."""
        html = "<div><p>Nothing relevant here</p></div>"
        records = self.ingester._parse_isc_section(html)
        assert len(records) == 0

    def test_parse_date_formats(self):
        """Test various date format parsing."""
        from datetime import date

        assert self.ingester._parse_date("2024-03-15") == date(2024, 3, 15)
        assert self.ingester._parse_date("March 15, 2024") == date(2024, 3, 15)
        assert self.ingester._parse_date("invalid") is None
        assert self.ingester._parse_date("") is None

    def test_parse_date_with_whitespace(self):
        """Test date parsing with surrounding whitespace."""
        from datetime import date

        assert self.ingester._parse_date("  2024-03-15  ") == date(2024, 3, 15)
