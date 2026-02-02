"""Entity fixtures for integration tests."""

from datetime import datetime
from uuid import UUID, uuid4

import pytest

# =========================
# Organization Fixtures
# =========================

SAMPLE_ORGANIZATIONS = [
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440001"),
        "name": "Example Foundation",
        "entity_type": "ORGANIZATION",
        "org_type": "NONPROFIT",
        "ein": "12-3456789",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "confidence": 0.95,
        "created_at": datetime(2024, 1, 15, 10, 30),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440002"),
        "name": "Major Donor Foundation",
        "entity_type": "ORGANIZATION",
        "org_type": "PRIVATE_FOUNDATION",
        "ein": "98-7654321",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "confidence": 0.98,
        "created_at": datetime(2024, 1, 10, 8, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440003"),
        "name": "Media Advocacy Group",
        "entity_type": "ORGANIZATION",
        "org_type": "NONPROFIT",
        "ein": "55-1234567",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "confidence": 0.90,
        "created_at": datetime(2024, 2, 1, 12, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440004"),
        "name": "Canadian Charity Inc",
        "entity_type": "ORGANIZATION",
        "org_type": "NONPROFIT",
        "bn": "123456789RR0001",
        "jurisdiction": "CA",
        "status": "ACTIVE",
        "confidence": 0.92,
        "created_at": datetime(2024, 2, 15, 9, 30),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440005"),
        "name": "SEC Corp Inc",
        "entity_type": "ORGANIZATION",
        "org_type": "CORPORATION",
        "sec_cik": "0001234567",
        "jurisdiction": "US",
        "status": "ACTIVE",
        "confidence": 0.97,
        "created_at": datetime(2024, 3, 1, 11, 0),
    },
]


# =========================
# Person Fixtures
# =========================

SAMPLE_PERSONS = [
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440010"),
        "name": "John Smith",
        "entity_type": "PERSON",
        "confidence": 0.90,
        "properties": {
            "title": "CEO",
            "primary_organization_id": "550e8400-e29b-41d4-a716-446655440001",
        },
        "created_at": datetime(2024, 1, 20, 14, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440011"),
        "name": "Jane Doe",
        "entity_type": "PERSON",
        "confidence": 0.88,
        "properties": {
            "title": "Board Member",
            "primary_organization_id": "550e8400-e29b-41d4-a716-446655440002",
        },
        "created_at": datetime(2024, 1, 22, 10, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440012"),
        "name": "Robert Johnson",
        "entity_type": "PERSON",
        "confidence": 0.85,
        "properties": {
            "title": "Editor-in-Chief",
        },
        "created_at": datetime(2024, 2, 5, 16, 0),
    },
]


# =========================
# Outlet Fixtures
# =========================

SAMPLE_OUTLETS = [
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440020"),
        "name": "Example News Network",
        "entity_type": "OUTLET",
        "confidence": 0.95,
        "properties": {
            "domain": "example-news.com",
            "primary_language": "en",
            "country": "US",
        },
        "created_at": datetime(2024, 1, 5, 8, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440021"),
        "name": "Truth Daily",
        "entity_type": "OUTLET",
        "confidence": 0.92,
        "properties": {
            "domain": "truth-daily.com",
            "primary_language": "en",
            "country": "US",
        },
        "created_at": datetime(2024, 1, 8, 9, 0),
    },
    {
        "id": UUID("550e8400-e29b-41d4-a716-446655440022"),
        "name": "Independent Voice",
        "entity_type": "OUTLET",
        "confidence": 0.88,
        "properties": {
            "domain": "independent-voice.org",
            "primary_language": "en",
            "country": "CA",
        },
        "created_at": datetime(2024, 1, 12, 11, 0),
    },
]


# =========================
# Pytest Fixtures
# =========================


@pytest.fixture
def sample_organization():
    """Return a single sample organization."""
    return SAMPLE_ORGANIZATIONS[0].copy()


@pytest.fixture
def sample_organizations():
    """Return all sample organizations."""
    return [org.copy() for org in SAMPLE_ORGANIZATIONS]


@pytest.fixture
def sample_person():
    """Return a single sample person."""
    return SAMPLE_PERSONS[0].copy()


@pytest.fixture
def sample_persons():
    """Return all sample persons."""
    return [person.copy() for person in SAMPLE_PERSONS]


@pytest.fixture
def sample_outlet():
    """Return a single sample outlet."""
    return SAMPLE_OUTLETS[0].copy()


@pytest.fixture
def sample_outlets():
    """Return all sample outlets."""
    return [outlet.copy() for outlet in SAMPLE_OUTLETS]


@pytest.fixture
def sample_all_entities():
    """Return all sample entities combined."""
    return (
        [org.copy() for org in SAMPLE_ORGANIZATIONS]
        + [person.copy() for person in SAMPLE_PERSONS]
        + [outlet.copy() for outlet in SAMPLE_OUTLETS]
    )


def create_organization(
    name: str = "Test Organization",
    org_type: str = "NONPROFIT",
    ein: str | None = None,
    jurisdiction: str = "US",
    **kwargs,
) -> dict:
    """Factory function to create organization fixtures.

    Args:
        name: Organization name
        org_type: Organization type
        ein: EIN number
        jurisdiction: Jurisdiction code
        **kwargs: Additional properties

    Returns:
        Organization dictionary
    """
    return {
        "id": uuid4(),
        "name": name,
        "entity_type": "ORGANIZATION",
        "org_type": org_type,
        "ein": ein or f"{uuid4().int % 100:02d}-{uuid4().int % 10000000:07d}",
        "jurisdiction": jurisdiction,
        "status": "ACTIVE",
        "confidence": 0.95,
        "created_at": datetime.utcnow(),
        **kwargs,
    }


def create_person(
    name: str = "Test Person",
    title: str | None = None,
    organization_id: UUID | None = None,
    **kwargs,
) -> dict:
    """Factory function to create person fixtures.

    Args:
        name: Person name
        title: Job title
        organization_id: Primary organization ID
        **kwargs: Additional properties

    Returns:
        Person dictionary
    """
    properties = kwargs.pop("properties", {})
    if title:
        properties["title"] = title
    if organization_id:
        properties["primary_organization_id"] = str(organization_id)

    return {
        "id": uuid4(),
        "name": name,
        "entity_type": "PERSON",
        "confidence": 0.90,
        "properties": properties,
        "created_at": datetime.utcnow(),
        **kwargs,
    }


def create_outlet(
    name: str = "Test Outlet",
    domain: str | None = None,
    country: str = "US",
    **kwargs,
) -> dict:
    """Factory function to create outlet fixtures.

    Args:
        name: Outlet name
        domain: Website domain
        country: Country code
        **kwargs: Additional properties

    Returns:
        Outlet dictionary
    """
    properties = kwargs.pop("properties", {})
    properties["domain"] = domain or f"{name.lower().replace(' ', '-')}.com"
    properties["primary_language"] = "en"
    properties["country"] = country

    return {
        "id": uuid4(),
        "name": name,
        "entity_type": "OUTLET",
        "confidence": 0.92,
        "properties": properties,
        "created_at": datetime.utcnow(),
        **kwargs,
    }
