"""Integration tests for Corporation case flow (User Story 2).

Tests the complete flow:
1. Create case from corporation name
2. Search multiple registries (EDGAR, SEDAR, ISED, CRA)
3. Extract leads from search results
4. Generate case report with ownership structure

Run with: pytest tests/integration/cases/test_corporation_flow.py -v
"""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from mitds.cases.adapters.corporation import CorporationAdapter
from mitds.cases.adapters.base import ValidationResult
from mitds.cases.models import (
    EntryPointType,
    Evidence,
    EvidenceType,
    ExtractionMethod,
)


class TestCorporationAdapterValidation:
    """Tests for CorporationAdapter.validate() method."""

    @pytest.mark.asyncio
    async def test_validates_corporation_name(self):
        """Test that corporation names are validated correctly."""
        adapter = CorporationAdapter()

        result = await adapter.validate("Postmedia Network Canada Corp")

        assert result.is_valid is True
        assert result.normalized_value == "Postmedia Network Canada Corp"

    @pytest.mark.asyncio
    async def test_rejects_empty_input(self):
        """Test that empty input is rejected."""
        adapter = CorporationAdapter()

        result = await adapter.validate("")

        assert result.is_valid is False
        assert "required" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_rejects_too_short_name(self):
        """Test that very short names are rejected."""
        adapter = CorporationAdapter()

        result = await adapter.validate("A")

        assert result.is_valid is False
        assert "2 characters" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_rejects_too_long_name(self):
        """Test that excessively long names are rejected."""
        adapter = CorporationAdapter()

        result = await adapter.validate("A" * 600)

        assert result.is_valid is False
        assert "500" in result.error_message


class TestCorporationMultiSourceSearch:
    """Tests for multi-source registry search."""

    @pytest.mark.asyncio
    async def test_searches_all_registries(
        self,
        mock_edgar_results,
        mock_sedar_results,
        mock_ised_results,
        mock_cra_results,
        mock_s3_storage,
    ):
        """Test that all registries are searched."""
        adapter = CorporationAdapter()

        # Mock all registry searches
        with patch.object(adapter, "_search_edgar", return_value=mock_edgar_results):
            with patch.object(adapter, "_search_sedar", return_value=mock_sedar_results):
                with patch.object(adapter, "_search_ised", return_value=mock_ised_results):
                    with patch.object(adapter, "_search_cra", return_value=mock_cra_results):
                        validation = ValidationResult(
                            is_valid=True,
                            normalized_value="Postmedia Network Canada Corp",
                            metadata={},
                        )

                        evidence = await adapter.create_evidence(
                            uuid4(),
                            "Postmedia Network Canada Corp",
                            validation,
                        )

                        assert evidence.evidence_type == EvidenceType.API_RESPONSE
                        assert evidence.extractor == "corporation_search"

    @pytest.mark.asyncio
    async def test_handles_registry_errors_gracefully(self, mock_s3_storage):
        """Test that individual registry errors don't fail the whole search."""
        adapter = CorporationAdapter()

        # Mock EDGAR to fail
        with patch.object(
            adapter, "_search_edgar", side_effect=Exception("EDGAR unavailable")
        ):
            with patch.object(adapter, "_search_sedar", return_value=[]):
                with patch.object(adapter, "_search_ised", return_value=[]):
                    with patch.object(adapter, "_search_cra", return_value=[]):
                        validation = ValidationResult(
                            is_valid=True,
                            normalized_value="Test Corp",
                            metadata={},
                        )

                        # Should not raise, but continue with other sources
                        evidence = await adapter.create_evidence(
                            uuid4(),
                            "Test Corp",
                            validation,
                        )

                        # Evidence should still be created
                        assert evidence is not None


class TestCorporationLeadExtraction:
    """Tests for lead extraction from corporation search results."""

    @pytest.mark.asyncio
    async def test_extracts_leads_from_edgar(self, mock_s3_storage, mock_edgar_results):
        """Test that leads are extracted from EDGAR results."""
        adapter = CorporationAdapter()

        evidence_id = uuid4()
        search_results = {
            "query": "Postmedia",
            "sources": {
                "edgar": {"count": 1, "results": mock_edgar_results},
                "sedar": {"count": 0, "results": []},
                "ised": {"count": 0, "results": []},
                "cra": {"count": 0, "results": []},
            },
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(search_results).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.API_RESPONSE,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="corporation_search",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        assert len(leads) >= 1
        # Should have organization lead with CIK
        org_leads = [l for l in leads if l.entity_type == "organization"]
        assert len(org_leads) >= 1

    @pytest.mark.asyncio
    async def test_extracts_leads_from_multiple_sources(
        self,
        mock_s3_storage,
        mock_edgar_results,
        mock_ised_results,
    ):
        """Test that leads are extracted from multiple sources."""
        adapter = CorporationAdapter()

        evidence_id = uuid4()
        search_results = {
            "query": "Postmedia",
            "sources": {
                "edgar": {"count": 1, "results": mock_edgar_results},
                "sedar": {"count": 0, "results": []},
                "ised": {"count": 1, "results": mock_ised_results},
                "cra": {"count": 0, "results": []},
            },
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(search_results).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.API_RESPONSE,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="corporation_search",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        leads = await adapter.extract_leads(evidence)

        # Should have leads from both sources (deduplicated if same name)
        assert len(leads) >= 1


class TestCorporationSeedEntity:
    """Tests for seed entity extraction from corporation search."""

    @pytest.mark.asyncio
    async def test_prefers_ised_for_seed(self, mock_s3_storage, mock_ised_results):
        """Test that ISED (federal) is preferred for Canadian corps."""
        adapter = CorporationAdapter()

        evidence_id = uuid4()
        search_results = {
            "query": "Postmedia",
            "sources": {
                "edgar": {"count": 0, "results": []},
                "sedar": {"count": 0, "results": []},
                "ised": {"count": 1, "results": mock_ised_results},
                "cra": {"count": 0, "results": []},
            },
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(search_results).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.API_RESPONSE,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="corporation_search",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        seed = await adapter.get_seed_entity(evidence)

        assert seed is not None
        assert seed.entity_type == "organization"
        # Should include BN identifier from ISED
        assert "bn" in seed.identifiers or "name" in seed.identifiers

    @pytest.mark.asyncio
    async def test_fallback_to_edgar_for_us_corps(self, mock_s3_storage, mock_edgar_results):
        """Test fallback to EDGAR when ISED/SEDAR have no results."""
        adapter = CorporationAdapter()

        evidence_id = uuid4()
        search_results = {
            "query": "American Corp",
            "sources": {
                "edgar": {"count": 1, "results": mock_edgar_results},
                "sedar": {"count": 0, "results": []},
                "ised": {"count": 0, "results": []},
                "cra": {"count": 0, "results": []},
            },
        }

        mock_s3_storage["retrieve"].return_value = json.dumps(search_results).encode("utf-8")

        evidence = Evidence(
            id=evidence_id,
            case_id=uuid4(),
            evidence_type=EvidenceType.API_RESPONSE,
            content_ref="s3://test/evidence.json",
            content_hash="abc123",
            content_type="application/json",
            extractor="corporation_search",
            extractor_version="1.0.0",
            retrieved_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
        )

        seed = await adapter.get_seed_entity(evidence)

        assert seed is not None
        assert "cik" in seed.identifiers or "name" in seed.identifiers


class TestCorporationCaseFlow:
    """End-to-end tests for corporation case flow."""

    @pytest.mark.asyncio
    async def test_complete_corporation_flow(
        self,
        sample_corporation_case,
        mock_s3_storage,
    ):
        """Test complete flow for corporation entry point."""
        adapter = CorporationAdapter()

        # Mock all registry searches
        # Mock the evidence content storage
        stored_content = {
            "query": "Postmedia Network Canada Corp",
            "sources": {
                "ised": {
                    "results": [{
                        "corporation_number": "123456-7",
                        "name": "Postmedia Network Canada Corp.",
                        "status": "Active",
                        "bn": "123456789RC0001",
                    }]
                }
            }
        }

        with patch.object(adapter, "_search_edgar", return_value=[]):
            with patch.object(adapter, "_search_sedar", return_value=[]):
                with patch.object(
                    adapter,
                    "_search_ised",
                    return_value=[{
                        "corporation_number": "123456-7",
                        "name": "Postmedia Network Canada Corp.",
                        "status": "Active",
                        "bn": "123456789RC0001",
                    }],
                ):
                    with patch.object(adapter, "_search_cra", return_value=[]):
                        # 1. Validate
                        validation = await adapter.validate("Postmedia Network Canada Corp")
                        assert validation.is_valid is True

                        # 2. Create evidence
                        evidence = await adapter.create_evidence(
                            sample_corporation_case.id,
                            "Postmedia Network Canada Corp",
                            validation,
                        )
                        assert evidence is not None

                        # 3. Extract leads - need to mock storage retrieval
                        with patch(
                            "mitds.storage.retrieve_evidence_content"
                        ) as mock_retrieve:
                            mock_retrieve.return_value = json.dumps(stored_content).encode("utf-8")
                            leads = await adapter.extract_leads(evidence)

                        assert len(leads) >= 1

                        # 4. Get seed entity
                        with patch(
                            "mitds.storage.retrieve_evidence_content"
                        ) as mock_retrieve:
                            mock_retrieve.return_value = json.dumps(stored_content).encode("utf-8")
                            seed = await adapter.get_seed_entity(evidence)

                        assert seed is not None
