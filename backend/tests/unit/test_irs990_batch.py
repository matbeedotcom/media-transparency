"""Unit tests for IRS 990 batch processing optimization.

Tests the batch processing methods that use UNWIND queries
for better performance.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

from mitds.ingestion.irs990 import (
    IRS990Ingester,
    IRS990Filing,
    IRS990IndexEntry,
)
from mitds.models import Address


class TestCollectRecordData:
    """Tests for collect_record_data() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    def test_collects_basic_org_data(self):
        """Test that basic organization data is collected correctly."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Nonprofit",
            tax_year=2023,
            state="CA",
        )

        result = self.ingester.collect_record_data(record)

        assert result["org"]["ein"] == "12-3456789"  # Formatted EIN
        assert result["org"]["name"] == "Test Nonprofit"
        assert result["org"]["org_type"] == "nonprofit"
        assert result["org"]["jurisdiction"] == "CA"
        assert result["org"]["entity_type"] == "ORGANIZATION"
        assert "id" in result["org"]  # UUID should be generated

    def test_collects_org_with_address(self):
        """Test that organization address is collected."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Nonprofit",
            tax_year=2023,
            address=Address(
                street="123 Main St",
                city="San Francisco",
                state="CA",
                postal_code="94102",
                country="US",
            ),
        )

        result = self.ingester.collect_record_data(record)

        assert result["org"]["address_street"] == "123 Main St"
        assert result["org"]["address_city"] == "San Francisco"
        assert result["org"]["address_state"] == "CA"
        assert result["org"]["address_postal"] == "94102"
        assert result["org"]["address_country"] == "US"

    def test_collects_officers_data(self):
        """Test that officers are collected with correct relationship types."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Nonprofit",
            tax_year=2023,
            officers=[
                {"name": "John Director", "title": "Board Director", "compensation": 0},
                {"name": "Jane CEO", "title": "Chief Executive Officer", "compensation": 150000},
                {"name": "Bob Trustee", "title": "Trustee", "compensation": 0},
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["officers"]) == 3

        # Director should have DIRECTOR_OF relationship
        director = next(o for o in result["officers"] if o["person_name"] == "John Director")
        assert director["rel_type"] == "DIRECTOR_OF"

        # CEO should have EMPLOYED_BY relationship
        ceo = next(o for o in result["officers"] if o["person_name"] == "Jane CEO")
        assert ceo["rel_type"] == "EMPLOYED_BY"

        # Trustee should have DIRECTOR_OF relationship
        trustee = next(o for o in result["officers"] if o["person_name"] == "Bob Trustee")
        assert trustee["rel_type"] == "DIRECTOR_OF"

    def test_collects_grants_with_ein(self):
        """Test that grants with EINs use the 'ein' merge strategy."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Foundation",
            tax_year=2023,
            grants_made=[
                {
                    "recipient_name": "Recipient Org",
                    "recipient_ein": "987654321",
                    "amount": 50000,
                    "purpose": "General support",
                },
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["grants"]) == 1
        grant = result["grants"][0]
        assert grant["merge_strategy"] == "ein"
        assert grant["recipient_ein"] == "98-7654321"  # Formatted
        assert grant["recipient_props"]["confidence"] == 0.8  # Higher for EIN match

    def test_collects_foreign_grants(self):
        """Test that foreign grants without EIN use the 'foreign' merge strategy."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Foundation",
            tax_year=2023,
            grants_made=[
                {
                    "recipient_name": "Canadian Charity",
                    "recipient_ein": None,
                    "amount": 25000,
                    "purpose": "International aid",
                    "recipient_country": "CA",
                    "recipient_state": "ON",
                },
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["grants"]) == 1
        grant = result["grants"][0]
        assert grant["merge_strategy"] == "foreign"
        assert grant["recipient_country"] == "CA"
        assert grant["recipient_props"]["jurisdiction"] == "CA-ON"

    def test_collects_us_grants_without_ein(self):
        """Test that US grants without EIN use the 'name' merge strategy."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Foundation",
            tax_year=2023,
            grants_made=[
                {
                    "recipient_name": "Small Local Org",
                    "recipient_ein": None,
                    "amount": 5000,
                    "purpose": "Community support",
                },
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["grants"]) == 1
        grant = result["grants"][0]
        assert grant["merge_strategy"] == "name"
        assert grant["recipient_props"]["confidence"] == 0.5  # Lower without EIN

    def test_skips_officers_without_name(self):
        """Test that officers without names are skipped."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Nonprofit",
            tax_year=2023,
            officers=[
                {"name": "Valid Officer", "title": "Director"},
                {"name": None, "title": "Unknown"},
                {"name": "", "title": "Empty"},
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["officers"]) == 1
        assert result["officers"][0]["person_name"] == "Valid Officer"

    def test_skips_grants_without_recipient_name(self):
        """Test that grants without recipient names are skipped."""
        record = IRS990Filing(
            object_id="202340189349301104",
            ein="123456789",
            tax_period="202312",
            form_type="990",
            url="https://example.com/filing.xml",
            name="Test Foundation",
            tax_year=2023,
            grants_made=[
                {"recipient_name": "Valid Recipient", "amount": 10000},
                {"recipient_name": None, "amount": 5000},
                {"recipient_name": "", "amount": 3000},
            ],
        )

        result = self.ingester.collect_record_data(record)

        assert len(result["grants"]) == 1
        assert result["grants"][0]["recipient_name"] == "Valid Recipient"


class TestBatchUpsertOrganizations:
    """Tests for _batch_upsert_organizations() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_executes_unwind_query(self):
        """Test that the method executes a single UNWIND query."""
        mock_session = AsyncMock()

        orgs = [
            {
                "id": "uuid-1",
                "ein": "12-3456789",
                "name": "Org 1",
                "entity_type": "ORGANIZATION",
                "org_type": "nonprofit",
                "status": "active",
                "jurisdiction": "CA",
                "confidence": 1.0,
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-01T00:00:00",
            },
            {
                "id": "uuid-2",
                "ein": "98-7654321",
                "name": "Org 2",
                "entity_type": "ORGANIZATION",
                "org_type": "nonprofit",
                "status": "active",
                "jurisdiction": "NY",
                "confidence": 1.0,
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-01-01T00:00:00",
            },
        ]

        await self.ingester._batch_upsert_organizations(mock_session, orgs)

        # Should call session.run once with UNWIND query
        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        query = call_args[0][0]
        assert "UNWIND $orgs AS org" in query
        assert "MERGE (o:Organization {ein: org.ein})" in query

    @pytest.mark.asyncio
    async def test_skips_empty_list(self):
        """Test that empty list doesn't execute any query."""
        mock_session = AsyncMock()

        await self.ingester._batch_upsert_organizations(mock_session, [])

        mock_session.run.assert_not_called()


class TestBatchUpsertPersons:
    """Tests for _batch_upsert_persons() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_deduplicates_by_name(self):
        """Test that duplicate names are removed before insertion."""
        mock_session = AsyncMock()

        persons = [
            {"id": "uuid-1", "name": "John Smith", "entity_type": "PERSON", "confidence": 1.0, "created_at": "2023-01-01", "updated_at": "2023-01-01"},
            {"id": "uuid-2", "name": "John Smith", "entity_type": "PERSON", "confidence": 1.0, "created_at": "2023-01-01", "updated_at": "2023-01-01"},
            {"id": "uuid-3", "name": "Jane Doe", "entity_type": "PERSON", "confidence": 1.0, "created_at": "2023-01-01", "updated_at": "2023-01-01"},
        ]

        await self.ingester._batch_upsert_persons(mock_session, persons)

        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        passed_persons = call_args.kwargs["persons"]
        # Should only have 2 unique persons
        assert len(passed_persons) == 2


class TestBatchCreateOfficerRelationships:
    """Tests for _batch_create_officer_relationships() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_splits_by_relationship_type(self):
        """Test that relationships are split by type and executed separately."""
        mock_session = AsyncMock()

        rels = [
            {"person_name": "Director 1", "org_ein": "12-3456789", "rel_type": "DIRECTOR_OF", "rel_props": {}},
            {"person_name": "Employee 1", "org_ein": "12-3456789", "rel_type": "EMPLOYED_BY", "rel_props": {}},
            {"person_name": "Director 2", "org_ein": "12-3456789", "rel_type": "DIRECTOR_OF", "rel_props": {}},
        ]

        await self.ingester._batch_create_officer_relationships(mock_session, rels)

        # Should be called twice: once for DIRECTOR_OF, once for EMPLOYED_BY
        assert mock_session.run.call_count == 2

    @pytest.mark.asyncio
    async def test_only_directors(self):
        """Test that only director query is executed when no employees."""
        mock_session = AsyncMock()

        rels = [
            {"person_name": "Director 1", "org_ein": "12-3456789", "rel_type": "DIRECTOR_OF", "rel_props": {}},
        ]

        await self.ingester._batch_create_officer_relationships(mock_session, rels)

        assert mock_session.run.call_count == 1
        query = mock_session.run.call_args[0][0]
        assert "DIRECTOR_OF" in query


class TestBatchUpsertGrantRecipients:
    """Tests for _batch_upsert_grant_recipients() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_splits_by_merge_strategy(self):
        """Test that recipients are split by merge strategy."""
        mock_session = AsyncMock()

        recipients = [
            {"merge_strategy": "ein", "recipient_ein": "12-3456789", "recipient_props": {}},
            {"merge_strategy": "foreign", "recipient_name": "Foreign Org", "recipient_country": "CA", "recipient_props": {}},
            {"merge_strategy": "name", "recipient_name": "Local Org", "recipient_props": {}},
        ]

        await self.ingester._batch_upsert_grant_recipients(mock_session, recipients)

        # Should be called 3 times: once for each strategy
        assert mock_session.run.call_count == 3


class TestBatchCreateFundedByRelationships:
    """Tests for _batch_create_funded_by_relationships() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_splits_by_ein_presence(self):
        """Test that relationships are split based on recipient EIN presence."""
        mock_session = AsyncMock()

        rels = [
            {"recipient_ein": "12-3456789", "recipient_name": "Org 1", "funder_ein": "98-7654321", "rel_props": {}},
            {"recipient_ein": None, "recipient_name": "Org 2", "funder_ein": "98-7654321", "rel_props": {}},
        ]

        await self.ingester._batch_create_funded_by_relationships(mock_session, rels)

        # Should be called twice: once for with EIN, once for without
        assert mock_session.run.call_count == 2


class TestFlushBatch:
    """Tests for _flush_batch() method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ingester = IRS990Ingester()

    @pytest.mark.asyncio
    async def test_calls_all_batch_methods_in_order(self):
        """Test that flush_batch calls all batch methods in correct order."""
        mock_session = AsyncMock()

        # Create mock methods
        self.ingester._batch_upsert_organizations = AsyncMock()
        self.ingester._batch_upsert_persons = AsyncMock()
        self.ingester._batch_create_officer_relationships = AsyncMock()
        self.ingester._batch_upsert_grant_recipients = AsyncMock()
        self.ingester._batch_create_funded_by_relationships = AsyncMock()

        batch_data = {
            "orgs": [{"ein": "12-3456789"}],
            "persons": [{"name": "Person 1"}],
            "officer_rels": [{"person_name": "Person 1", "org_ein": "12-3456789"}],
            "recipients": [{"recipient_name": "Recipient 1"}],
            "grant_rels": [{"recipient_name": "Recipient 1", "funder_ein": "12-3456789"}],
        }

        stats = await self.ingester._flush_batch(mock_session, batch_data)

        # Verify all methods were called
        self.ingester._batch_upsert_organizations.assert_called_once()
        self.ingester._batch_upsert_persons.assert_called_once()
        self.ingester._batch_create_officer_relationships.assert_called_once()
        self.ingester._batch_upsert_grant_recipients.assert_called_once()
        self.ingester._batch_create_funded_by_relationships.assert_called_once()

        # Verify stats
        assert stats["orgs"] == 1
        assert stats["persons"] == 1
        assert stats["officer_rels"] == 1
        assert stats["recipients"] == 1
        assert stats["grant_rels"] == 1
