"""Integration tests for URL case flow (User Story 3).

Tests the complete flow:
1. Create case from URL
2. Fetch and extract page content
3. Extract entities via deterministic + LLM pipeline
4. Generate leads for discovered entities

Run with: pytest tests/integration/cases/test_url_flow.py -v
"""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.adapters.url import URLAdapter
from mitds.cases.adapters.base import ValidationResult
from mitds.cases.extraction.deterministic import DeterministicExtractor
from mitds.cases.extraction.pipeline import ExtractionPipeline, ExtractionConfig
from mitds.cases.models import (
    Evidence,
    EvidenceType,
    ExtractionMethod,
)


class TestURLAdapterValidation:
    """Tests for URLAdapter.validate() method."""

    @pytest.mark.asyncio
    async def test_validates_https_url(self):
        """Test that HTTPS URLs are validated correctly."""
        adapter = URLAdapter()

        result = await adapter.validate("https://example.org/about")

        assert result.is_valid is True
        assert result.normalized_value == "https://example.org/about"
        assert result.metadata["domain"] == "example.org"

    @pytest.mark.asyncio
    async def test_validates_http_url(self):
        """Test that HTTP URLs are validated correctly."""
        adapter = URLAdapter()

        result = await adapter.validate("http://example.org/page")

        assert result.is_valid is True

    @pytest.mark.asyncio
    async def test_rejects_non_http_scheme(self):
        """Test that non-HTTP schemes are rejected."""
        adapter = URLAdapter()

        result = await adapter.validate("ftp://example.org/file")

        assert result.is_valid is False
        assert "HTTP" in result.error_message

    @pytest.mark.asyncio
    async def test_rejects_empty_url(self):
        """Test that empty URLs are rejected."""
        adapter = URLAdapter()

        result = await adapter.validate("")

        assert result.is_valid is False

    @pytest.mark.asyncio
    async def test_rejects_invalid_url(self):
        """Test that invalid URLs are rejected."""
        adapter = URLAdapter()

        result = await adapter.validate("not a url")

        assert result.is_valid is False


class TestURLContentExtraction:
    """Tests for URL content fetching and extraction."""

    @pytest.mark.asyncio
    async def test_fetches_url_content(
        self, mock_httpx_response, sample_html_content, mock_s3_storage
    ):
        """Test that URL content is fetched and stored."""
        adapter = URLAdapter()

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_httpx_response
            MockClient.return_value = mock_client

            validation = ValidationResult(
                is_valid=True,
                normalized_value="https://example.org/about",
                metadata={"domain": "example.org", "path": "/about"},
            )

            evidence = await adapter.create_evidence(
                uuid4(),
                "https://example.org/about",
                validation,
            )

            assert evidence.evidence_type == EvidenceType.URL_FETCH
            assert evidence.source_url == "https://example.org/about"

    @pytest.mark.asyncio
    async def test_handles_fetch_errors(self, mock_s3_storage):
        """Test that fetch errors are handled gracefully."""
        adapter = URLAdapter()

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.side_effect = Exception("Connection refused")
            MockClient.return_value = mock_client

            validation = ValidationResult(
                is_valid=True,
                normalized_value="https://example.org/about",
                metadata={"domain": "example.org", "path": "/about"},
            )

            # Should create evidence even with error
            evidence = await adapter.create_evidence(
                uuid4(),
                "https://example.org/about",
                validation,
            )

            assert evidence is not None
            # Error should be recorded
            assert evidence.extraction_result.get("error") is not None


class TestDeterministicExtractor:
    """Tests for pattern-based entity extraction."""

    def test_extracts_ein(self):
        """Test EIN extraction (XX-XXXXXXX format)."""
        extractor = DeterministicExtractor()

        text = "The foundation (EIN: 12-3456789) is a nonprofit."
        entities = extractor.extract(text)

        ein_entities = [e for e in entities if e.identifier_type == "ein"]
        assert len(ein_entities) == 1
        assert ein_entities[0].value == "12-3456789"
        assert ein_entities[0].confidence == 1.0

    def test_extracts_bn(self):
        """Test Canadian Business Number extraction."""
        extractor = DeterministicExtractor()

        text = "Registered charity BN 123456789RR0001 in Canada."
        entities = extractor.extract(text)

        bn_entities = [e for e in entities if e.identifier_type == "bn"]
        assert len(bn_entities) == 1
        assert bn_entities[0].value == "123456789RR0001"
        assert bn_entities[0].confidence == 1.0

    def test_extracts_organization_by_legal_suffix(self):
        """Test organization extraction by legal suffix."""
        extractor = DeterministicExtractor()

        text = "Koch Industries Inc. donated to Americans for Prosperity Foundation."
        entities = extractor.extract(text)

        org_entities = [e for e in entities if e.entity_type == "organization"]
        assert len(org_entities) >= 2

        org_names = [e.value for e in org_entities]
        assert any("Koch" in name for name in org_names)
        assert any("Prosperity" in name for name in org_names)

    def test_extracts_domain(self):
        """Test domain extraction."""
        extractor = DeterministicExtractor()

        text = "Visit our website at example.org for more information."
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        assert len(domain_entities) == 1
        assert domain_entities[0].value == "example.org"

    def test_ignores_common_email_domains(self):
        """Test that common email domains are filtered."""
        extractor = DeterministicExtractor()

        text = "Contact us at info@gmail.com for support."
        entities = extractor.extract(text)

        domain_entities = [e for e in entities if e.identifier_type == "domain"]
        gmail_domains = [e for e in domain_entities if "gmail" in e.value]
        assert len(gmail_domains) == 0


class TestExtractionPipeline:
    """Tests for the extraction pipeline orchestrator."""

    @pytest.mark.asyncio
    async def test_deterministic_extraction(self, sample_text_with_entities):
        """Test deterministic-only extraction."""
        config = ExtractionConfig(enable_llm=False, min_confidence=0.5)
        pipeline = ExtractionPipeline(config)

        leads = await pipeline.extract(sample_text_with_entities, uuid4())

        assert len(leads) >= 1
        # All leads should be deterministic
        for lead in leads:
            assert lead.extraction_method in [
                ExtractionMethod.DETERMINISTIC,
                ExtractionMethod.HYBRID,
            ]

    @pytest.mark.asyncio
    async def test_deduplicates_leads(self, sample_text_with_entities):
        """Test that duplicate entities are deduplicated."""
        config = ExtractionConfig(enable_llm=False, deduplicate=True)
        pipeline = ExtractionPipeline(config)

        leads = await pipeline.extract(sample_text_with_entities, uuid4())

        # Check for unique values
        values = [lead.extracted_value.lower() for lead in leads]
        assert len(values) == len(set(values)), "Should have no duplicates"

    @pytest.mark.asyncio
    async def test_filters_by_confidence(self):
        """Test that low-confidence leads are filtered."""
        config = ExtractionConfig(enable_llm=False, min_confidence=0.9)
        pipeline = ExtractionPipeline(config)

        text = "Visit example.org for more info."  # Domain has 0.8 confidence
        leads = await pipeline.extract(text, uuid4())

        for lead in leads:
            assert lead.confidence >= 0.9


class TestURLLeadExtraction:
    """Tests for lead extraction from URL content."""

    @pytest.mark.asyncio
    async def test_extracts_leads_from_content(
        self, mock_s3_storage, sample_html_content
    ):
        """Test that leads are extracted from page content."""
        adapter = URLAdapter(enable_llm=False)

        evidence_id = uuid4()
        content_data = {
            "url": "https://example.org/about",
            "extracted_text": """
            Example Foundation Inc. (EIN: 12-3456789) is dedicated to policy research.
            We receive funding from Koch Industries Ltd. and the Bradley Foundation.
            """,
            "error": None,
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.URL_FETCH,
            source_url="https://example.org/about",
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="url_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        assert len(leads) >= 1
        # Should extract EIN
        ein_leads = [l for l in leads if l.identifier_type == "ein"]
        assert len(ein_leads) >= 1
        # Should extract organizations
        org_leads = [l for l in leads if l.entity_type == "organization"]
        assert len(org_leads) >= 1


class TestURLCaseFlow:
    """End-to-end tests for URL case flow."""

    @pytest.mark.asyncio
    async def test_complete_url_flow(
        self,
        sample_url_case,
        mock_httpx_response,
        mock_s3_storage,
    ):
        """Test complete flow for URL entry point."""
        adapter = URLAdapter(enable_llm=False)

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_httpx_response
            MockClient.return_value = mock_client

            # 1. Validate
            validation = await adapter.validate("https://example.org/about")
            assert validation.is_valid is True

            # 2. Create evidence
            evidence = await adapter.create_evidence(
                sample_url_case.id,
                "https://example.org/about",
                validation,
            )
            assert evidence is not None
            assert evidence.evidence_type == EvidenceType.URL_FETCH

            # 3. Extract leads (mock the content retrieval)
            content_data = {
                "extracted_text": "Example Foundation Inc. (EIN: 12-3456789) is a nonprofit.",
                "error": None,
            }
            mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

            leads = await adapter.extract_leads(evidence)
            assert len(leads) >= 1

            # 4. Get seed entity (domain)
            seed = await adapter.get_seed_entity(evidence)
            # URL adapter returns domain as seed if available
            if seed:
                assert seed.entity_type == "domain"
