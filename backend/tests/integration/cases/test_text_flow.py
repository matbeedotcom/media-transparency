"""Integration tests for Text case flow (User Story 4).

Tests the complete flow:
1. Create case from pasted text
2. Extract entities using extraction pipeline
3. Generate leads for discovered entities

Run with: pytest tests/integration/cases/test_text_flow.py -v
"""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.adapters.text import TextAdapter
from mitds.cases.adapters.base import ValidationResult
from mitds.cases.models import (
    Evidence,
    EvidenceType,
    ExtractionMethod,
)


class TestTextAdapterValidation:
    """Tests for TextAdapter.validate() method."""

    @pytest.mark.asyncio
    async def test_validates_text_content(self):
        """Test that text content is validated correctly."""
        adapter = TextAdapter()

        text = "The Koch Foundation Inc. donated $500,000 to Americans for Prosperity."
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.normalized_value == text
        assert result.metadata["char_count"] == len(text)
        assert result.metadata["word_count"] > 0

    @pytest.mark.asyncio
    async def test_rejects_empty_text(self):
        """Test that empty text is rejected."""
        adapter = TextAdapter()

        result = await adapter.validate("")

        assert result.is_valid is False
        assert "required" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_rejects_too_short_text(self):
        """Test that very short text is rejected."""
        adapter = TextAdapter()

        result = await adapter.validate("Hello")

        assert result.is_valid is False
        assert "10 characters" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_rejects_too_long_text(self):
        """Test that excessively long text is rejected."""
        adapter = TextAdapter()

        result = await adapter.validate("A" * 150000)

        assert result.is_valid is False
        assert "100000" in result.error_message


class TestTextSourceDetection:
    """Tests for source type detection."""

    @pytest.mark.asyncio
    async def test_detects_linkedin_content(self):
        """Test detection of LinkedIn content."""
        adapter = TextAdapter()

        text = "Excited to announce #LinkedIn that we've partnered with Koch Industries."
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.metadata["source_type"] == "linkedin"

    @pytest.mark.asyncio
    async def test_detects_email_content(self):
        """Test detection of email content."""
        adapter = TextAdapter()

        text = """From: john@example.org
Subject: Funding announcement
Dear colleagues, we are pleased to announce..."""
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.metadata["source_type"] == "email"

    @pytest.mark.asyncio
    async def test_detects_press_release(self):
        """Test detection of press release content."""
        adapter = TextAdapter()

        text = """FOR IMMEDIATE RELEASE
Contact: media@example.org
Example Foundation announces new initiative..."""
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.metadata["source_type"] == "press_release"

    @pytest.mark.asyncio
    async def test_unknown_source_type(self):
        """Test that unrecognized content is marked as unknown."""
        adapter = TextAdapter()

        text = "This is just regular text content without any identifying markers."
        result = await adapter.validate(text)

        assert result.is_valid is True
        assert result.metadata["source_type"] == "unknown"


class TestTextEvidenceCreation:
    """Tests for evidence creation from text."""

    @pytest.mark.asyncio
    async def test_creates_evidence_from_text(self, mock_s3_storage):
        """Test that evidence is created from text content."""
        adapter = TextAdapter()

        case_id = uuid4()
        text = "The Koch Foundation Inc. (EIN: 48-6122197) announced a grant."

        validation = ValidationResult(
            is_valid=True,
            normalized_value=text,
            metadata={
                "source_type": "unknown",
                "char_count": len(text),
                "word_count": 10,
            },
        )

        evidence = await adapter.create_evidence(case_id, text, validation)

        assert evidence.case_id == case_id
        assert evidence.evidence_type == EvidenceType.ENTRY_POINT
        assert evidence.extractor == "text_adapter"

    @pytest.mark.asyncio
    async def test_stores_raw_text(self, mock_s3_storage):
        """Test that raw text is stored in S3."""
        adapter = TextAdapter()

        case_id = uuid4()
        text = "Test content for storage."

        validation = ValidationResult(
            is_valid=True,
            normalized_value=text,
            metadata={"source_type": "unknown", "char_count": 25, "word_count": 4},
        )

        evidence = await adapter.create_evidence(case_id, text, validation)

        # S3 store should be called (for both JSON and raw text)
        assert mock_s3_storage["store"].called


class TestTextLeadExtraction:
    """Tests for lead extraction from text."""

    @pytest.mark.asyncio
    async def test_extracts_organizations(self, mock_s3_storage):
        """Test that organization names are extracted."""
        adapter = TextAdapter(enable_llm=False)

        evidence_id = uuid4()
        content_data = {
            "text": "Koch Industries Inc. and the Bradley Foundation are major donors.",
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.ENTRY_POINT,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        org_leads = [l for l in leads if l.entity_type == "organization"]
        assert len(org_leads) >= 1

    @pytest.mark.asyncio
    async def test_extracts_identifiers(self, mock_s3_storage):
        """Test that identifiers (EIN, BN) are extracted."""
        adapter = TextAdapter(enable_llm=False)

        evidence_id = uuid4()
        content_data = {
            "text": """The Koch Foundation (EIN: 48-6122197) and 
            the Fraser Institute (BN: 118743968RR0001) received grants.""",
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.ENTRY_POINT,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        ein_leads = [l for l in leads if l.identifier_type == "ein"]
        bn_leads = [l for l in leads if l.identifier_type == "bn"]

        assert len(ein_leads) >= 1
        assert len(bn_leads) >= 1

    @pytest.mark.asyncio
    async def test_extracts_domains(self, mock_s3_storage):
        """Test that domain names are extracted."""
        adapter = TextAdapter(enable_llm=False)

        evidence_id = uuid4()
        content_data = {
            "text": "Visit kochfoundation.org and fraserinstitute.org for more info.",
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.ENTRY_POINT,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        domain_leads = [l for l in leads if l.identifier_type == "domain"]
        assert len(domain_leads) >= 2


class TestTextSeedEntity:
    """Tests for seed entity from text."""

    @pytest.mark.asyncio
    async def test_text_has_no_seed_entity(self, mock_s3_storage):
        """Test that text adapter returns no seed entity (leads are seeds)."""
        adapter = TextAdapter()

        evidence = Evidence(
            id=uuid4(),
            case_id=uuid4(),
            evidence_type=EvidenceType.ENTRY_POINT,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        seed = await adapter.get_seed_entity(evidence)

        # Text adapter doesn't have a single seed - all leads are seeds
        assert seed is None


class TestTextCaseFlow:
    """End-to-end tests for text case flow."""

    @pytest.mark.asyncio
    async def test_complete_text_flow(
        self,
        sample_text_case,
        sample_text_with_entities,
        mock_s3_storage,
    ):
        """Test complete flow for text entry point."""
        adapter = TextAdapter(enable_llm=False)

        # 1. Validate
        validation = await adapter.validate(sample_text_with_entities)
        assert validation.is_valid is True

        # 2. Create evidence
        evidence = await adapter.create_evidence(
            sample_text_case.id,
            sample_text_with_entities,
            validation,
        )
        assert evidence is not None
        assert evidence.evidence_type == EvidenceType.ENTRY_POINT

        # 3. Extract leads (mock content retrieval)
        content_data = {"text": sample_text_with_entities}
        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        leads = await adapter.extract_leads(evidence)
        assert len(leads) >= 1

        # Should have extracted EIN
        ein_leads = [l for l in leads if l.identifier_type == "ein"]
        assert len(ein_leads) >= 1

        # Should have extracted organizations
        org_leads = [l for l in leads if l.entity_type == "organization"]
        assert len(org_leads) >= 1

    @pytest.mark.asyncio
    async def test_handles_empty_extraction(self, mock_s3_storage):
        """Test handling of text with no extractable entities."""
        adapter = TextAdapter(enable_llm=False)

        evidence_id = uuid4()
        # Text with no recognizable patterns
        content_data = {
            "text": "This is plain text without any organizations or identifiers mentioned.",
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.ENTRY_POINT,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="text_adapter",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        # Should return empty list, not error
        assert isinstance(leads, list)


class TestLinkedInTextExtraction:
    """Tests for LinkedIn post text extraction."""

    @pytest.mark.asyncio
    async def test_extracts_from_linkedin_post(self, mock_s3_storage):
        """Test extraction from LinkedIn post content."""
        adapter = TextAdapter(enable_llm=False)

        linkedin_text = """
        Excited to announce that Koch Industries Inc. has partnered with
        Americans for Prosperity Foundation to launch a new initiative!

        This builds on our previous work with the Fraser Institute and
        the Manning Centre for Democracy.

        #LinkedIn #philanthropy #publicpolicy
        """

        validation = await adapter.validate(linkedin_text)
        assert validation.is_valid is True
        assert validation.metadata["source_type"] == "linkedin"

        # Create and extract
        evidence = await adapter.create_evidence(
            uuid4(),
            linkedin_text,
            validation,
        )

        content_data = {"text": linkedin_text}
        mock_s3_storage["retrieve"].return_value = json.dumps(content_data).encode("utf-8")

        leads = await adapter.extract_leads(evidence)

        org_names = [l.extracted_value for l in leads if l.entity_type == "organization"]

        # Should extract organizations even from social media format
        assert any("Koch" in name for name in org_names)
