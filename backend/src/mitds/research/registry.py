"""Ingester capabilities registry for MITDS research.

Maps ingester names to their capabilities for the research system.
"""

from dataclasses import dataclass
from typing import Any

from .models import IdentifierType, LeadType


@dataclass
class IngesterCapability:
    """Capabilities of an ingester for research purposes."""

    name: str
    supported_identifiers: list[IdentifierType]
    lead_types_generated: list[LeadType]
    jurisdictions: list[str]  # Country codes or "*" for all
    requires_api_key: bool = False
    rate_limit_per_minute: int | None = None
    supports_incremental: bool = True


# Registry of ingester capabilities
INGESTER_CAPABILITIES: dict[str, IngesterCapability] = {
    "irs990": IngesterCapability(
        name="irs990",
        supported_identifiers=[IdentifierType.EIN, IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING, LeadType.BOARD_INTERLOCK],
        jurisdictions=["US"],
        requires_api_key=False,
    ),
    "sec_edgar": IngesterCapability(
        name="sec_edgar",
        supported_identifiers=[IdentifierType.CIK, IdentifierType.NAME],
        lead_types_generated=[LeadType.OWNERSHIP, LeadType.BOARD_INTERLOCK],
        jurisdictions=["US"],
        requires_api_key=False,
        rate_limit_per_minute=10,
    ),
    "sedar": IngesterCapability(
        name="sedar",
        supported_identifiers=[IdentifierType.SEDAR_PROFILE, IdentifierType.NAME],
        lead_types_generated=[LeadType.OWNERSHIP],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "cra": IngesterCapability(
        name="cra",
        supported_identifiers=[IdentifierType.BN, IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "meta_ads": IngesterCapability(
        name="meta_ads",
        supported_identifiers=[IdentifierType.META_PAGE_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.SPONSORSHIP],
        jurisdictions=["US", "CA"],
        requires_api_key=True,
        rate_limit_per_minute=200,  # Meta's limit
    ),
    "opencorporates": IngesterCapability(
        name="opencorporates",
        supported_identifiers=[IdentifierType.OPENCORP_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.OWNERSHIP, LeadType.BOARD_INTERLOCK],
        jurisdictions=["*"],  # Global
        requires_api_key=True,  # For higher rate limits
    ),
    "littlesis": IngesterCapability(
        name="littlesis",
        supported_identifiers=[IdentifierType.LITTLESIS_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING, LeadType.BOARD_INTERLOCK, LeadType.OWNERSHIP],
        jurisdictions=["US"],
        requires_api_key=False,
    ),
    "elections_canada": IngesterCapability(
        name="elections_canada",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "canada_corps": IngesterCapability(
        name="canada_corps",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.OWNERSHIP],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "lobbying": IngesterCapability(
        name="lobbying",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
}


def get_capability(ingester_name: str) -> IngesterCapability | None:
    """Get capabilities for an ingester.

    Args:
        ingester_name: Ingester name

    Returns:
        IngesterCapability or None if not found
    """
    return INGESTER_CAPABILITIES.get(ingester_name)


def get_ingesters_for_identifier(
    identifier_type: IdentifierType,
    jurisdiction: str | None = None,
) -> list[str]:
    """Get ingesters that support a given identifier type.

    Args:
        identifier_type: Type of identifier
        jurisdiction: Optional jurisdiction filter

    Returns:
        List of ingester names
    """
    results = []

    for name, cap in INGESTER_CAPABILITIES.items():
        if identifier_type not in cap.supported_identifiers:
            continue

        if jurisdiction:
            if "*" not in cap.jurisdictions and jurisdiction not in cap.jurisdictions:
                continue

        results.append(name)

    return results


def get_ingesters_for_lead_type(
    lead_type: LeadType,
    jurisdiction: str | None = None,
) -> list[str]:
    """Get ingesters that generate a given lead type.

    Args:
        lead_type: Type of lead
        jurisdiction: Optional jurisdiction filter

    Returns:
        List of ingester names
    """
    results = []

    for name, cap in INGESTER_CAPABILITIES.items():
        if lead_type not in cap.lead_types_generated:
            continue

        if jurisdiction:
            if "*" not in cap.jurisdictions and jurisdiction not in cap.jurisdictions:
                continue

        results.append(name)

    return results


def get_ingesters_for_jurisdiction(jurisdiction: str) -> list[str]:
    """Get ingesters that support a given jurisdiction.

    Args:
        jurisdiction: Country code (e.g., "US", "CA")

    Returns:
        List of ingester names
    """
    results = []

    for name, cap in INGESTER_CAPABILITIES.items():
        if "*" in cap.jurisdictions or jurisdiction in cap.jurisdictions:
            results.append(name)

    return results
