"""Relationship fixtures for integration tests."""

from datetime import datetime, date
from uuid import UUID, uuid4

import pytest

# =========================
# Funding Relationship Fixtures
# =========================

SAMPLE_FUNDING_RELATIONSHIPS = [
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440001"),
        "rel_type": "FUNDED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440003"),  # Media Advocacy Group
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440002"),  # Major Donor Foundation
        "properties": {
            "amount": 500000,
            "fiscal_year": 2023,
            "currency": "USD",
            "purpose": "General operating support",
        },
        "valid_from": datetime(2023, 1, 1),
        "valid_to": datetime(2023, 12, 31),
        "confidence": 0.95,
    },
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440002"),
        "rel_type": "FUNDED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440020"),  # Example News Network
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440003"),  # Media Advocacy Group
        "properties": {
            "amount": 250000,
            "fiscal_year": 2023,
            "currency": "USD",
            "purpose": "Media initiative grant",
        },
        "valid_from": datetime(2023, 3, 1),
        "valid_to": datetime(2023, 12, 31),
        "confidence": 0.92,
    },
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440003"),
        "rel_type": "FUNDED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440021"),  # Truth Daily
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440003"),  # Media Advocacy Group
        "properties": {
            "amount": 175000,
            "fiscal_year": 2023,
            "currency": "USD",
        },
        "valid_from": datetime(2023, 4, 1),
        "valid_to": datetime(2023, 12, 31),
        "confidence": 0.88,
    },
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440004"),
        "rel_type": "FUNDED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440001"),  # Example Foundation
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440002"),  # Major Donor Foundation
        "properties": {
            "amount": 100000,
            "fiscal_year": 2022,
            "currency": "USD",
        },
        "valid_from": datetime(2022, 1, 1),
        "valid_to": datetime(2022, 12, 31),
        "confidence": 0.90,
    },
]


# =========================
# Employment Relationship Fixtures
# =========================

SAMPLE_EMPLOYMENT_RELATIONSHIPS = [
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440010"),
        "rel_type": "EMPLOYED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440010"),  # John Smith
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440001"),  # Example Foundation
        "properties": {
            "title": "CEO",
            "start_date": "2020-01-15",
        },
        "valid_from": datetime(2020, 1, 15),
        "valid_to": None,  # Current
        "confidence": 0.95,
    },
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440011"),
        "rel_type": "EMPLOYED_BY",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440012"),  # Robert Johnson
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440020"),  # Example News Network
        "properties": {
            "title": "Editor-in-Chief",
            "start_date": "2019-06-01",
        },
        "valid_from": datetime(2019, 6, 1),
        "valid_to": None,
        "confidence": 0.90,
    },
]


# =========================
# Director Relationship Fixtures
# =========================

SAMPLE_DIRECTOR_RELATIONSHIPS = [
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440020"),
        "rel_type": "DIRECTOR_OF",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440011"),  # Jane Doe
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440002"),  # Major Donor Foundation
        "properties": {
            "role": "Board Member",
            "committee": "Finance",
        },
        "valid_from": datetime(2018, 3, 1),
        "valid_to": None,
        "confidence": 0.92,
    },
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440021"),
        "rel_type": "DIRECTOR_OF",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440011"),  # Jane Doe (also on)
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440003"),  # Media Advocacy Group
        "properties": {
            "role": "Board Member",
        },
        "valid_from": datetime(2020, 1, 1),
        "valid_to": None,
        "confidence": 0.88,
    },
]


# =========================
# Ownership Relationship Fixtures
# =========================

SAMPLE_OWNERSHIP_RELATIONSHIPS = [
    {
        "id": UUID("660e8400-e29b-41d4-a716-446655440030"),
        "rel_type": "OWNS",
        "source_id": UUID("550e8400-e29b-41d4-a716-446655440001"),  # Example Foundation
        "target_id": UUID("550e8400-e29b-41d4-a716-446655440020"),  # Example News Network
        "properties": {
            "ownership_percentage": 51.0,
            "ownership_type": "direct",
        },
        "valid_from": datetime(2015, 6, 1),
        "valid_to": None,
        "confidence": 0.97,
    },
]


# =========================
# Pytest Fixtures
# =========================


@pytest.fixture
def sample_funding_relationships():
    """Return all sample funding relationships."""
    return [rel.copy() for rel in SAMPLE_FUNDING_RELATIONSHIPS]


@pytest.fixture
def sample_employment_relationships():
    """Return all sample employment relationships."""
    return [rel.copy() for rel in SAMPLE_EMPLOYMENT_RELATIONSHIPS]


@pytest.fixture
def sample_director_relationships():
    """Return all sample director relationships."""
    return [rel.copy() for rel in SAMPLE_DIRECTOR_RELATIONSHIPS]


@pytest.fixture
def sample_ownership_relationships():
    """Return all sample ownership relationships."""
    return [rel.copy() for rel in SAMPLE_OWNERSHIP_RELATIONSHIPS]


@pytest.fixture
def sample_all_relationships():
    """Return all sample relationships combined."""
    return (
        [rel.copy() for rel in SAMPLE_FUNDING_RELATIONSHIPS]
        + [rel.copy() for rel in SAMPLE_EMPLOYMENT_RELATIONSHIPS]
        + [rel.copy() for rel in SAMPLE_DIRECTOR_RELATIONSHIPS]
        + [rel.copy() for rel in SAMPLE_OWNERSHIP_RELATIONSHIPS]
    )


def create_funding_relationship(
    source_id: UUID,
    target_id: UUID,
    amount: float = 100000,
    fiscal_year: int = 2023,
    **kwargs,
) -> dict:
    """Factory function to create funding relationship fixtures.

    Args:
        source_id: Recipient entity ID
        target_id: Funder entity ID
        amount: Funding amount
        fiscal_year: Fiscal year
        **kwargs: Additional properties

    Returns:
        Funding relationship dictionary
    """
    properties = kwargs.pop("properties", {})
    properties.update({
        "amount": amount,
        "fiscal_year": fiscal_year,
        "currency": "USD",
    })

    return {
        "id": uuid4(),
        "rel_type": "FUNDED_BY",
        "source_id": source_id,
        "target_id": target_id,
        "properties": properties,
        "valid_from": datetime(fiscal_year, 1, 1),
        "valid_to": datetime(fiscal_year, 12, 31),
        "confidence": kwargs.pop("confidence", 0.90),
        **kwargs,
    }


def create_employment_relationship(
    person_id: UUID,
    organization_id: UUID,
    title: str = "Employee",
    start_date: datetime | None = None,
    **kwargs,
) -> dict:
    """Factory function to create employment relationship fixtures.

    Args:
        person_id: Person entity ID
        organization_id: Organization entity ID
        title: Job title
        start_date: Employment start date
        **kwargs: Additional properties

    Returns:
        Employment relationship dictionary
    """
    start = start_date or datetime.utcnow()
    properties = kwargs.pop("properties", {})
    properties.update({
        "title": title,
        "start_date": start.isoformat(),
    })

    return {
        "id": uuid4(),
        "rel_type": "EMPLOYED_BY",
        "source_id": person_id,
        "target_id": organization_id,
        "properties": properties,
        "valid_from": start,
        "valid_to": kwargs.pop("end_date", None),
        "confidence": kwargs.pop("confidence", 0.90),
        **kwargs,
    }


def create_director_relationship(
    person_id: UUID,
    organization_id: UUID,
    role: str = "Board Member",
    start_date: datetime | None = None,
    **kwargs,
) -> dict:
    """Factory function to create director relationship fixtures.

    Args:
        person_id: Person entity ID
        organization_id: Organization entity ID
        role: Director role
        start_date: Board service start date
        **kwargs: Additional properties

    Returns:
        Director relationship dictionary
    """
    start = start_date or datetime.utcnow()
    properties = kwargs.pop("properties", {})
    properties["role"] = role

    return {
        "id": uuid4(),
        "rel_type": "DIRECTOR_OF",
        "source_id": person_id,
        "target_id": organization_id,
        "properties": properties,
        "valid_from": start,
        "valid_to": kwargs.pop("end_date", None),
        "confidence": kwargs.pop("confidence", 0.90),
        **kwargs,
    }
