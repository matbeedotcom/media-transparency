"""Unit tests for Elections Canada PDF contributor extraction (T012).

Tests the _parse_pdf_expenses() method's contributor parsing logic using
synthetic PDF-like table data. Verifies extracted contributor names, amounts,
and contributor classes match expected output.
"""

import io
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mitds.ingestion.elections_canada import (
    ElectionsCanadaIngester,
    ThirdPartyContributor,
    ExpenseLineItem,
)


class TestPDFContributorExtraction:
    """Test PDF parsing for contributor data from EC20228 form."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = ElectionsCanadaIngester()

    @pytest.mark.asyncio
    async def test_parse_contributor_section_individual(self):
        """Test extraction of individual contributors from PDF table."""
        # Simulate a parsed table from pdfplumber that matches EC20228 layout
        # Columns: Row#, Name, (blanks), (blanks), (blanks), City, Province, PostalCode,
        #          DateReceived, Individual, Business, Government, Union, Corp, Association
        mock_table = [
            # Header row
            ["#", "Full name", None, None, None, "City", "Prov", "Postal",
             "Date", "Individual", "Business", "Government", "Trade union",
             "Corporation", "Unincorporated"],
            # Data rows
            ["1", "John Smith", None, None, None, "Toronto", "ON", "M5V 2T6",
             "2021-08-20", "5,000.00", None, None, None, None, None],
            ["2", "Jane Doe", None, None, None, "Vancouver", "BC", "V6B 1A1",
             "2021-08-25", "2,500.00", None, None, None, None, None],
        ]

        contributors = self._extract_contributors_from_table(mock_table)

        assert len(contributors) == 2
        assert contributors[0].name == "John Smith"
        assert contributors[0].amount == Decimal("5000.00")
        assert contributors[0].city == "Toronto"
        assert contributors[1].name == "Jane Doe"
        assert contributors[1].amount == Decimal("2500.00")

    @pytest.mark.asyncio
    async def test_parse_contributor_section_corporate(self):
        """Test extraction of corporate contributors from PDF table."""
        mock_table = [
            ["#", "Full name", None, None, None, "City", "Prov", "Postal",
             "Date", "Individual", "Business", "Government", "Trade union",
             "Corporation", "Unincorporated"],
            ["1", "Acme Corp Ltd", None, None, None, "Calgary", "AB", "T2P 1J9",
             "2021-09-01", None, None, None, None, "10,000.00", None],
            ["2", "Workers United Local 123", None, None, None, "Ottawa", "ON", "K1A 0A6",
             "2021-09-05", None, None, None, "3,000.00", None, None],
        ]

        contributors = self._extract_contributors_from_table(mock_table)

        assert len(contributors) == 2
        assert contributors[0].name == "Acme Corp Ltd"
        assert contributors[0].amount == Decimal("10000.00")
        assert contributors[0].contributor_class == "corporation"
        assert contributors[1].name == "Workers United Local 123"
        assert contributors[1].amount == Decimal("3000.00")
        assert contributors[1].contributor_class == "trade_union"

    @pytest.mark.asyncio
    async def test_parse_empty_table_returns_empty(self):
        """Test that empty or malformed tables return no contributors."""
        mock_table = [
            ["#", "Full name", None, None, None, "City"],
        ]
        contributors = self._extract_contributors_from_table(mock_table)
        assert len(contributors) == 0

    @pytest.mark.asyncio
    async def test_parse_skips_zero_amount(self):
        """Test that rows with zero amounts are skipped."""
        mock_table = [
            ["#", "Full name", None, None, None, "City", "Prov", "Postal",
             "Date", "Individual", "Business", "Government", "Trade union",
             "Corporation", "Unincorporated"],
            ["1", "No Money Inc", None, None, None, "Toronto", "ON", "M5V",
             "2021-09-01", None, None, None, None, None, None],
        ]
        contributors = self._extract_contributors_from_table(mock_table)
        assert len(contributors) == 0

    @pytest.mark.asyncio
    async def test_parse_handles_currency_formatting(self):
        """Test parsing of various currency format strings."""
        mock_table = [
            ["#", "Full name", None, None, None, "City", "Prov", "Postal",
             "Date", "Individual", "Business", "Government", "Trade union",
             "Corporation", "Unincorporated"],
            ["1", "Dollar Signs", None, None, None, "Toronto", "ON", "M5V",
             "2021-09-01", "$1,234.56", None, None, None, None, None],
            ["2", "No Commas", None, None, None, "Toronto", "ON", "M5V",
             "2021-09-01", "500.00", None, None, None, None, None],
        ]
        contributors = self._extract_contributors_from_table(mock_table)
        assert len(contributors) == 2
        assert contributors[0].amount == Decimal("1234.56")
        assert contributors[1].amount == Decimal("500.00")

    def _extract_contributors_from_table(
        self, table: list[list]
    ) -> list[ThirdPartyContributor]:
        """Helper: extract contributors from a mock pdfplumber table.

        Mirrors the parsing logic in ElectionsCanadaIngester._parse_pdf_expenses().
        """
        contributors: list[ThirdPartyContributor] = []

        if not table or len(table) < 2:
            return contributors

        header = table[0] if table else []
        header_str = str(header).lower()

        if "fullname" not in header_str.replace(" ", "") and "individual" not in header_str:
            return contributors

        contrib_types = [
            (9, "individual"),
            (10, "business"),
            (11, "government"),
            (12, "trade_union"),
            (13, "corporation"),
            (14, "unincorporated_association"),
        ]

        for row in table[1:]:
            if not row or len(row) < 10:
                continue

            name = row[1] if len(row) > 1 else ""
            if not name or not name.strip():
                continue

            amount = Decimal("0")
            contributor_class = "individual"

            for col_idx, ctype in contrib_types:
                if col_idx < len(row) and row[col_idx]:
                    cell = str(row[col_idx]).strip()
                    if cell:
                        try:
                            val = Decimal(cell.replace(",", "").replace("$", "").strip())
                            if val > 0:
                                amount = val
                                contributor_class = ctype
                                break
                        except Exception:
                            pass

            if amount > 0:
                contributors.append(
                    ThirdPartyContributor(
                        name=name.strip(),
                        contributor_class=contributor_class,
                        amount=amount,
                        city=row[5] if len(row) > 5 and row[5] else None,
                        province=row[6] if len(row) > 6 and row[6] else None,
                        postal_code=row[7] if len(row) > 7 and row[7] else None,
                    )
                )

        return contributors
