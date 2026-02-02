"""Unit tests for Case Intake adapters (T016, T035, T041, T048).

Tests validation logic, input normalization, and adapter configuration
without requiring external services.

Run with: pytest tests/unit/cases/test_adapters.py -v
"""

import pytest
from uuid import uuid4

from mitds.cases.adapters.meta_ads import MetaAdAdapter
from mitds.cases.adapters.corporation import CorporationAdapter
from mitds.cases.adapters.url import URLAdapter
from mitds.cases.adapters.text import TextAdapter
from mitds.cases.adapters import get_adapter
from mitds.cases.models import EntryPointType


class TestMetaAdAdapterUnit:
    """Unit tests for MetaAdAdapter validation (T016)."""

    @pytest.mark.asyncio
    async def test_entry_point_type(self):
        """Test that entry point type is correct."""
        adapter = MetaAdAdapter()
        assert adapter.entry_point_type == EntryPointType.META_AD.value

    @pytest.mark.asyncio
    async def test_validates_sponsor_name(self):
        """Test sponsor name validation."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("Americans for Prosperity")

        assert result.is_valid is True
        assert result.metadata["input_type"] == "sponsor_name"

    @pytest.mark.asyncio
    async def test_validates_numeric_page_id(self):
        """Test numeric page ID validation."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("123456789012345")

        assert result.is_valid is True
        assert result.metadata["input_type"] == "page_id"

    @pytest.mark.asyncio
    async def test_normalizes_whitespace(self):
        """Test whitespace normalization."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("  Americans for Prosperity  ")

        assert result.is_valid is True
        assert result.normalized_value == "Americans for Prosperity"

    @pytest.mark.asyncio
    async def test_rejects_empty_input(self):
        """Test empty input rejection."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("")

        assert result.is_valid is False

    @pytest.mark.asyncio
    async def test_rejects_whitespace_only(self):
        """Test whitespace-only input rejection."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("   \t\n   ")

        assert result.is_valid is False


class TestCorporationAdapterUnit:
    """Unit tests for CorporationAdapter validation (T035)."""

    @pytest.mark.asyncio
    async def test_entry_point_type(self):
        """Test that entry point type is correct."""
        adapter = CorporationAdapter()
        assert adapter.entry_point_type == EntryPointType.CORPORATION.value

    @pytest.mark.asyncio
    async def test_validates_corporation_name(self):
        """Test corporation name validation."""
        adapter = CorporationAdapter()

        result = await adapter.validate("Postmedia Network Canada Corp")

        assert result.is_valid is True
        assert "original_value" in result.metadata

    @pytest.mark.asyncio
    async def test_min_length_validation(self):
        """Test minimum length validation."""
        adapter = CorporationAdapter()

        result = await adapter.validate("A")

        assert result.is_valid is False
        assert "2 characters" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_max_length_validation(self):
        """Test maximum length validation."""
        adapter = CorporationAdapter()

        result = await adapter.validate("X" * 600)

        assert result.is_valid is False
        assert "500" in result.error_message


class TestURLAdapterUnit:
    """Unit tests for URLAdapter validation (T041)."""

    @pytest.mark.asyncio
    async def test_entry_point_type(self):
        """Test that entry point type is correct."""
        adapter = URLAdapter()
        assert adapter.entry_point_type == EntryPointType.URL.value

    @pytest.mark.asyncio
    async def test_validates_https_url(self):
        """Test HTTPS URL validation."""
        adapter = URLAdapter()

        result = await adapter.validate("https://example.org/about")

        assert result.is_valid is True
        assert result.metadata["domain"] == "example.org"
        assert result.metadata["path"] == "/about"

    @pytest.mark.asyncio
    async def test_validates_http_url(self):
        """Test HTTP URL validation."""
        adapter = URLAdapter()

        result = await adapter.validate("http://example.org/page")

        assert result.is_valid is True

    @pytest.mark.asyncio
    async def test_rejects_ftp_scheme(self):
        """Test FTP scheme rejection."""
        adapter = URLAdapter()

        result = await adapter.validate("ftp://files.example.org/doc.pdf")

        assert result.is_valid is False
        assert "HTTP" in result.error_message

    @pytest.mark.asyncio
    async def test_rejects_invalid_url(self):
        """Test invalid URL rejection."""
        adapter = URLAdapter()

        result = await adapter.validate("not-a-url")

        assert result.is_valid is False

    @pytest.mark.asyncio
    async def test_rejects_url_without_host(self):
        """Test URL without hostname rejection."""
        adapter = URLAdapter()

        result = await adapter.validate("https:///path")

        assert result.is_valid is False


class TestTextAdapterUnit:
    """Unit tests for TextAdapter validation (T048)."""

    @pytest.mark.asyncio
    async def test_entry_point_type(self):
        """Test that entry point type is correct."""
        adapter = TextAdapter()
        assert adapter.entry_point_type == EntryPointType.TEXT.value

    @pytest.mark.asyncio
    async def test_validates_text(self):
        """Test text validation."""
        adapter = TextAdapter()

        text = "This is a test text with more than ten characters."
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.metadata["char_count"] == len(text)
        assert result.metadata["word_count"] > 0

    @pytest.mark.asyncio
    async def test_min_length_validation(self):
        """Test minimum length validation."""
        adapter = TextAdapter()

        result = await adapter.validate("Short")

        assert result.is_valid is False
        assert "10 characters" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_max_length_validation(self):
        """Test maximum length validation."""
        adapter = TextAdapter()

        result = await adapter.validate("X" * 150000)

        assert result.is_valid is False
        assert "100000" in result.error_message

    @pytest.mark.asyncio
    async def test_source_type_detection_linkedin(self):
        """Test LinkedIn source detection."""
        adapter = TextAdapter()

        result = await adapter.validate("Excited to share on #LinkedIn that...")

        assert result.is_valid is True
        assert result.metadata["source_type"] == "linkedin"

    @pytest.mark.asyncio
    async def test_source_type_detection_email(self):
        """Test email source detection."""
        adapter = TextAdapter()

        result = await adapter.validate("From: sender@example.org\nSubject: Test\nBody content here")

        assert result.is_valid is True
        assert result.metadata["source_type"] == "email"


class TestGetAdapter:
    """Tests for the adapter factory function."""

    def test_get_meta_ad_adapter(self):
        """Test getting Meta Ad adapter."""
        adapter = get_adapter("meta_ad")
        assert isinstance(adapter, MetaAdAdapter)

    def test_get_corporation_adapter(self):
        """Test getting Corporation adapter."""
        adapter = get_adapter("corporation")
        assert isinstance(adapter, CorporationAdapter)

    def test_get_url_adapter(self):
        """Test getting URL adapter."""
        adapter = get_adapter("url")
        assert isinstance(adapter, URLAdapter)

    def test_get_text_adapter(self):
        """Test getting Text adapter."""
        adapter = get_adapter("text")
        assert isinstance(adapter, TextAdapter)

    def test_raises_for_unknown_type(self):
        """Test error for unknown adapter type."""
        with pytest.raises(ValueError) as exc_info:
            get_adapter("unknown_type")

        assert "Unsupported" in str(exc_info.value)
