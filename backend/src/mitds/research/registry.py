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
    # Political Ad Funding ingesters (007)
    "elections_third_party": IngesterCapability(
        name="elections_third_party",
        supported_identifiers=[IdentifierType.ELECTIONS_TP_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.POLITICAL_CONTRIBUTION, LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "beneficial_ownership": IngesterCapability(
        name="beneficial_ownership",
        supported_identifiers=[IdentifierType.CORP_NUMBER, IdentifierType.NAME],
        lead_types_generated=[LeadType.BENEFICIAL_OWNERSHIP, LeadType.OWNERSHIP],
        jurisdictions=["CA"],
        requires_api_key=False,
        rate_limit_per_minute=60,  # Polite crawling
    ),
    "bc_lobbying": IngesterCapability(
        name="bc_lobbying",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "google_ads": IngesterCapability(
        name="google_ads",
        supported_identifiers=[IdentifierType.GOOGLE_AD_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.SPONSORSHIP],
        jurisdictions=["CA"],
        requires_api_key=True,  # Requires Google Cloud credentials
    ),
    # Provincial elections ingesters (Phase 9)
    "elections_ontario": IngesterCapability(
        name="elections_ontario",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.POLITICAL_CONTRIBUTION],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "elections_bc": IngesterCapability(
        name="elections_bc",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.POLITICAL_CONTRIBUTION],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    "elections_alberta": IngesterCapability(
        name="elections_alberta",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.POLITICAL_CONTRIBUTION],
        jurisdictions=["CA"],
        requires_api_key=False,
    ),
    # Corroborating evidence ingesters (Phase 10)
    "canlii": IngesterCapability(
        name="canlii",
        supported_identifiers=[IdentifierType.CANLII_ID, IdentifierType.NAME],
        lead_types_generated=[LeadType.INFRASTRUCTURE],
        jurisdictions=["CA"],
        requires_api_key=True,
        rate_limit_per_minute=30,
    ),
    "ppsa": IngesterCapability(
        name="ppsa",
        supported_identifiers=[IdentifierType.NAME],
        lead_types_generated=[LeadType.FUNDING],
        jurisdictions=["CA"],
        requires_api_key=True,
        rate_limit_per_minute=10,  # Cost-gated
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
