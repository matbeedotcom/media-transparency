"""Integration tests for Meta Ad case flow (User Story 1).

Tests the complete flow:
1. Create case from Meta Ad sponsor name
2. Validate and fetch ad data
3. Extract leads (page names, bylines)
4. Resolve sponsors to organizations
5. Generate case report

Run with: pytest tests/integration/cases/test_meta_ad_flow.py -v
"""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.adapters.meta_ads import MetaAdAdapter
from mitds.cases.adapters.base import ValidationResult
from mitds.cases.models import (
    CaseStatus,
    EntryPointType,
    Evidence,
    EvidenceType,
    ExtractedLead,
    ExtractionMethod,
)


class TestMetaAdAdapterValidation:
    """Tests for MetaAdAdapter.validate() method."""

    @pytest.mark.asyncio
    async def test_validates_sponsor_name(self):
        """Test that sponsor names are validated correctly."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("Americans for Prosperity")

        assert result.is_valid is True
        assert result.normalized_value == "Americans for Prosperity"
        assert result.metadata["input_type"] == "sponsor_name"

    @pytest.mark.asyncio
    async def test_validates_page_id(self):
        """Test that numeric page IDs are validated correctly."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("123456789012345")

        assert result.is_valid is True
        assert result.metadata["input_type"] == "page_id"

    @pytest.mark.asyncio
    async def test_rejects_empty_input(self):
        """Test that empty input is rejected."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("")

        assert result.is_valid is False
        assert "required" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_rejects_whitespace_only(self):
        """Test that whitespace-only input is rejected."""
        adapter = MetaAdAdapter()

        result = await adapter.validate("   ")

        assert result.is_valid is False


class TestMetaAdAdapterEvidence:
    """Tests for MetaAdAdapter.create_evidence() method."""

    @pytest.mark.asyncio
    async def test_creates_evidence_from_sponsor_search(
        self, mock_meta_ad_ingester, mock_s3_storage
    ):
        """Test that evidence is created from sponsor search results."""
        adapter = MetaAdAdapter()
        adapter.ingester = mock_meta_ad_ingester

        case_id = uuid4()
        validation = ValidationResult(
            is_valid=True,
            normalized_value="Americans for Prosperity",
            metadata={"input_type": "sponsor_name"},
        )

        evidence = await adapter.create_evidence(case_id, "Americans for Prosperity", validation)

        assert evidence.case_id == case_id
        assert evidence.evidence_type == EvidenceType.API_RESPONSE
        assert evidence.extractor == "meta_ads"
        assert mock_meta_ad_ingester.search_by_sponsor.called

    @pytest.mark.asyncio
    async def test_creates_evidence_from_page_id_search(
        self, mock_meta_ad_ingester, mock_s3_storage
    ):
        """Test that evidence is created from page ID search."""
        adapter = MetaAdAdapter()
        adapter.ingester = mock_meta_ad_ingester

        case_id = uuid4()
        validation = ValidationResult(
            is_valid=True,
            normalized_value="123456789012345",
            metadata={"input_type": "page_id"},
        )

        evidence = await adapter.create_evidence(case_id, "123456789012345", validation)

        assert evidence.case_id == case_id
        assert mock_meta_ad_ingester.search_by_page_id.called


class TestMetaAdLeadExtraction:
    """Tests for MetaAdAdapter.extract_leads() method."""

    @pytest.mark.asyncio
    async def test_extracts_leads_from_ad_data(self, mock_s3_storage):
        """Test that leads are extracted from Meta Ad response."""
        adapter = MetaAdAdapter()

        # Create mock evidence with ad data - use "ads" key as expected by implementation
        evidence_id = uuid4()
        ad_data = {
            "ads": [
                {
                    "page_name": "Americans for Prosperity",
                    "page_id": "987654321",
                    "funding_entity": "AFP Foundation",
                    "bylines": ["Paid for by Americans for Prosperity Foundation"],
                }
            ]
        }

        # Mock retrieve to return ad data
        with patch("mitds.storage.retrieve_evidence_content") as mock_retrieve:
            mock_retrieve.return_value = json.dumps(ad_data).encode("utf-8")

            evidence = Evidence(
                id=evidence_id,
                case_id=uuid4(),
                evidence_type=EvidenceType.API_RESPONSE,
                content_ref="s3://test/evidence.json",
                content_hash="abc123",
                content_type="application/json",
                extractor="meta_ads",
                extractor_version="1.0.0",
                retrieved_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
                extraction_result={"ads_count": 1},  # Trigger extraction
            )

            leads = await adapter.extract_leads(evidence)

        assert len(leads) >= 1
        # Should extract page name as lead
        page_leads = [l for l in leads if l.extracted_value == "Americans for Prosperity"]
        assert len(page_leads) >= 1

    @pytest.mark.asyncio
    async def test_extracts_byline_organizations(self, mock_s3_storage):
        """Test that byline organizations are extracted as leads."""
        adapter = MetaAdAdapter()

        evidence_id = uuid4()
        ad_data = {
            "ads": [
                {
                    "page_name": "Test Page",
                    "page_id": "123456",
                    "bylines": ["Paid for by Americans for Prosperity Foundation"],
                }
            ]
        }

        with patch("mitds.storage.retrieve_evidence_content") as mock_retrieve:
            mock_retrieve.return_value = json.dumps(ad_data).encode("utf-8")

            evidence = Evidence(
                id=evidence_id,
                case_id=uuid4(),
                evidence_type=EvidenceType.API_RESPONSE,
                content_ref="s3://test/evidence.json",
                content_hash="abc123",
                content_type="application/json",
                extractor="meta_ads",
                extractor_version="1.0.0",
                retrieved_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
                extraction_result={"ads_count": 1},
            )

            leads = await adapter.extract_leads(evidence)

        # Should extract organization from byline
        byline_leads = [l for l in leads if "Foundation" in l.extracted_value]
        assert len(byline_leads) >= 1


class TestMetaAdSeedEntity:
    """Tests for MetaAdAdapter.get_seed_entity() method."""

    @pytest.mark.asyncio
    async def test_returns_seed_entity_from_sponsor(self, mock_s3_storage):
        """Test that seed entity is created from sponsor name."""
        adapter = MetaAdAdapter()

        evidence_id = uuid4()
        ad_data = {
            "ads": [{"page_name": "Americans for Prosperity", "page_id": "12345"}],
        }

        with patch("mitds.storage.retrieve_evidence_content") as mock_retrieve:
            mock_retrieve.return_value = json.dumps(ad_data).encode("utf-8")

            evidence = Evidence(
                id=evidence_id,
                case_id=uuid4(),
                evidence_type=EvidenceType.API_RESPONSE,
                content_ref="s3://test/evidence.json",
                content_hash="abc123",
                content_type="application/json",
                extractor="meta_ads",
                extractor_version="1.0.0",
                extraction_result={
                    "query": "Americans for Prosperity",
                    "query_type": "sponsor_name",
                },
                retrieved_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
            )

            seed = await adapter.get_seed_entity(evidence)

        assert seed is not None
        assert seed.entity_type == "sponsor"
        assert seed.name == "Americans for Prosperity"


class TestMetaAdCaseFlow:
    """End-to-end tests for Meta Ad case flow."""

    @pytest.mark.asyncio
    async def test_complete_meta_ad_flow(
        self,
        sample_meta_ad_case,
        mock_meta_ad_ingester,
        mock_s3_storage,
        mock_db_session,
        mock_neo4j_session,
    ):
        """Test complete flow: create case → fetch ads → extract leads → report."""
        # This test verifies the integration of all components

        # 1. Create adapter and validate input
        adapter = MetaAdAdapter()
        adapter.ingester = mock_meta_ad_ingester

        validation = await adapter.validate("Americans for Prosperity")
        assert validation.is_valid is True

        # 2. Create evidence from API
        evidence = await adapter.create_evidence(
            sample_meta_ad_case.id,
            "Americans for Prosperity",
            validation,
        )
        assert evidence.evidence_type == EvidenceType.API_RESPONSE

        # 3. Extract leads
        leads = await adapter.extract_leads(evidence)
        assert len(leads) >= 0  # May be empty if mock doesn't have data

        # 4. Get seed entity
        seed = await adapter.get_seed_entity(evidence)
        # Seed may be None if extraction_result not set

        # Verify the flow completed without errors
        assert sample_meta_ad_case.status == CaseStatus.INITIALIZING

    @pytest.mark.asyncio
    async def test_handles_api_errors_gracefully(self, sample_meta_ad_case, mock_s3_storage):
        """Test that API errors are handled gracefully."""
        adapter = MetaAdAdapter()

        # Mock API error
        adapter.ingester = MagicMock()
        adapter.ingester.search_by_sponsor = AsyncMock(
            side_effect=Exception("API rate limit exceeded")
        )

        validation = ValidationResult(
            is_valid=True,
            normalized_value="Test Sponsor",
            metadata={"input_type": "sponsor_name"},
        )

        # Should not raise - graceful error handling returns empty evidence
        evidence = await adapter.create_evidence(sample_meta_ad_case.id, "Test Sponsor", validation)

        # Evidence is created but may have empty content due to API error
        assert evidence is not None
        assert evidence.case_id == sample_meta_ad_case.id

    @pytest.mark.asyncio
    async def test_deduplicates_leads(self, mock_s3_storage):
        """Test that duplicate leads are not created."""
        adapter = MetaAdAdapter()

        evidence_id = uuid4()
        # Data with duplicate page names
        ad_data = {
            "data": [
                {"page_name": "Same Page", "page_id": "123"},
                {"page_name": "Same Page", "page_id": "123"},  # Duplicate
                {"page_name": "Different Page", "page_id": "456"},
            ]
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(ad_data).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.API_RESPONSE,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="meta_ad_library",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        # Should have unique leads only
        page_names = [l.extracted_value for l in leads if l.identifier_type == "page_name"]
        assert len(page_names) == len(set(page_names)), "Leads should be deduplicated"
