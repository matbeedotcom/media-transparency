"""Pytest fixtures for Case Intake integration tests."""

import asyncio
from datetime import datetime
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from mitds.cases.models import (
    Case,
    CaseConfig,
    CaseStats,
    CaseStatus,
    EntryPointType,
    CreateCaseRequest,
)


# =========================
# Async Event Loop Fixture
# =========================


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =========================
# Mock Storage Fixtures
# =========================


@pytest.fixture
def mock_s3_storage():
    """Mock S3/MinIO storage operations."""
    # Patch at both the definition and usage locations
    with patch("mitds.storage.store_evidence_content") as mock_store_main:
        with patch("mitds.cases.adapters.text.store_evidence_content") as mock_store_text:
            # Make both return the same values
            return_value = ("s3://test-bucket/evidence/test.json", "abc123hash")
            mock_store_main.return_value = return_value
            mock_store_text.return_value = return_value
            with patch("mitds.storage.retrieve_evidence_content") as mock_retrieve:
                mock_retrieve.return_value = b'{"test": "data"}'
                yield {
                    "store": mock_store_text,  # Return the one used by text adapter
                    "retrieve": mock_retrieve,
                }


# =========================
# Mock Database Fixtures
# =========================


@pytest.fixture
def mock_db_session():
    """Mock database session for testing without actual DB."""
    session = AsyncMock()
    session.execute = AsyncMock(return_value=MagicMock(scalar=lambda: None))
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_neo4j_session():
    """Mock Neo4j session for testing without actual graph DB."""
    session = AsyncMock()
    session.run = AsyncMock(return_value=MagicMock(
        single=lambda: {"count": 0},
        data=lambda: [],
    ))
    return session


# =========================
# Sample Data Fixtures
# =========================


@pytest.fixture
def sample_case_config() -> CaseConfig:
    """Create a sample case configuration."""
    return CaseConfig(
        max_depth=2,
        max_entities=100,
        max_relationships=500,
        jurisdictions=["US", "CA"],
        min_confidence=0.7,
        auto_merge_threshold=0.9,
        review_threshold=0.7,
        enable_llm_extraction=False,
    )


@pytest.fixture
def sample_meta_ad_case(sample_case_config) -> Case:
    """Create a sample Meta Ad case."""
    return Case(
        id=uuid4(),
        name="Test Meta Ad Case",
        description="Test case for Meta Ad sponsor",
        entry_point_type=EntryPointType.META_AD,
        entry_point_value="Americans for Prosperity",
        status=CaseStatus.INITIALIZING,
        config=sample_case_config,
        stats=CaseStats(),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_corporation_case(sample_case_config) -> Case:
    """Create a sample corporation case."""
    return Case(
        id=uuid4(),
        name="Test Corporation Case",
        description="Test case for corporation lookup",
        entry_point_type=EntryPointType.CORPORATION,
        entry_point_value="Postmedia Network Canada Corp",
        status=CaseStatus.INITIALIZING,
        config=sample_case_config,
        stats=CaseStats(),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_url_case(sample_case_config) -> Case:
    """Create a sample URL case."""
    return Case(
        id=uuid4(),
        name="Test URL Case",
        description="Test case for URL extraction",
        entry_point_type=EntryPointType.URL,
        entry_point_value="https://example.org/about",
        status=CaseStatus.INITIALIZING,
        config=sample_case_config,
        stats=CaseStats(),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_text_case(sample_case_config) -> Case:
    """Create a sample text case."""
    return Case(
        id=uuid4(),
        name="Test Text Case",
        description="Test case for text extraction",
        entry_point_type=EntryPointType.TEXT,
        entry_point_value="The Koch Foundation Inc. donated $500,000 to Americans for Prosperity Foundation (EIN 27-3287075).",
        status=CaseStatus.INITIALIZING,
        config=sample_case_config,
        stats=CaseStats(),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


# =========================
# Meta Ad API Mock Fixtures
# =========================


@pytest.fixture
def mock_meta_ad_response():
    """Sample Meta Ad Library API response."""
    return {
        "data": [
            {
                "id": "123456789",
                "page_id": "987654321",
                "page_name": "Americans for Prosperity",
                "funding_entity": "Americans for Prosperity Foundation",
                "bylines": "Paid for by Americans for Prosperity Foundation",
                "ad_creation_time": "2026-01-15T12:00:00Z",
                "ad_delivery_start_time": "2026-01-16T00:00:00Z",
                "ad_creative_bodies": ["Support free markets and limited government!"],
                "spend": {"lower_bound": "100", "upper_bound": "499"},
                "impressions": {"lower_bound": "1000", "upper_bound": "5000"},
                "demographic_distribution": [
                    {"age": "25-34", "gender": "male", "percentage": "0.35"},
                ],
            }
        ],
        "paging": {"next": None},
    }


@pytest.fixture
def mock_meta_ad_ingester(mock_meta_ad_response):
    """Mock the Meta Ad Library ingester."""
    with patch("mitds.cases.adapters.meta_ads.MetaAdIngester") as MockIngester:
        instance = MockIngester.return_value
        instance.search_by_sponsor = AsyncMock(return_value=mock_meta_ad_response)
        instance.search_by_page_id = AsyncMock(return_value=mock_meta_ad_response)
        yield instance


# =========================
# Corporate Registry Mock Fixtures
# =========================


@pytest.fixture
def mock_edgar_results():
    """Sample SEC EDGAR search results."""
    return [
        {
            "cik": "0001234567",
            "name": "Postmedia Network Canada Corp",
            "ticker": "PNC",
        }
    ]


@pytest.fixture
def mock_sedar_results():
    """Sample SEDAR+ search results."""
    return [
        {
            "sedar_id": "00054321",
            "name": "Postmedia Network Canada Corp.",
            "jurisdiction": "ON",
        }
    ]


@pytest.fixture
def mock_ised_results():
    """Sample ISED Canada Corporations results."""
    return [
        {
            "corporation_number": "123456-7",
            "name": "Postmedia Network Canada Corp.",
            "status": "Active",
            "bn": "123456789RC0001",
        }
    ]


@pytest.fixture
def mock_cra_results():
    """Sample CRA Charities results."""
    return [
        {
            "bn": "123456789RR0001",
            "name": "Test Foundation",
            "city": "Toronto",
            "province": "ON",
        }
    ]


# =========================
# URL Content Mock Fixtures
# =========================


@pytest.fixture
def sample_html_content():
    """Sample HTML content for URL extraction tests."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>About Us - Example Foundation</title></head>
    <body>
        <h1>About Example Foundation Inc.</h1>
        <p>Example Foundation Inc. (EIN: 12-3456789) is a nonprofit organization
        dedicated to public policy research.</p>
        <p>We receive funding from Koch Industries Ltd. and the Bradley Foundation.</p>
        <p>Contact us at info@example.org</p>
        <footer>
            <p>123 Main Street, Arlington, VA 22201</p>
            <p>Business Number: 123456789RR0001</p>
        </footer>
    </body>
    </html>
    """


@pytest.fixture
def mock_httpx_response(sample_html_content):
    """Mock httpx response for URL fetching."""
    response = MagicMock()
    response.status_code = 200
    response.text = sample_html_content
    response.content = sample_html_content.encode("utf-8")
    response.raise_for_status = MagicMock()
    return response


# =========================
# Text Extraction Fixtures
# =========================


@pytest.fixture
def sample_text_with_entities():
    """Sample text containing various entity types."""
    return """
    The Koch Foundation Inc. (EIN: 48-6122197) announced a $1.5 million grant
    to the Fraser Institute. This continues their partnership with Canadian
    think tanks, following previous donations to the Manning Centre for Democracy.

    Bradley Foundation has also funded similar organizations in both the US
    and Canada, including the Institute for Humane Studies.

    For more information, contact media@kochfoundation.org or visit
    www.kochfoundation.org.
    """


# =========================
# Entity Match Fixtures
# =========================


@pytest.fixture
def sample_entity_match():
    """Sample entity match for review testing."""
    return {
        "id": str(uuid4()),
        "case_id": str(uuid4()),
        "source_entity_id": str(uuid4()),
        "target_entity_id": str(uuid4()),
        "confidence": 0.85,
        "match_signals": {
            "name_similarity": 0.92,
            "identifier_match": None,
            "jurisdiction_match": True,
            "address_overlap": {"city": True, "postal_fsa": False},
            "shared_directors": [],
        },
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }


@pytest.fixture
def sample_source_entity():
    """Sample source entity for match review."""
    return {
        "id": str(uuid4()),
        "name": "Americans for Prosperity",
        "entity_type": "organization",
        "jurisdiction": "US",
        "identifiers": {"ein": "27-3287075"},
    }


@pytest.fixture
def sample_target_entity():
    """Sample target entity for match review."""
    return {
        "id": str(uuid4()),
        "name": "Americans for Prosperity Foundation",
        "entity_type": "organization",
        "jurisdiction": "US",
        "identifiers": {"ein": "27-1763901"},
    }
