"""Provincial corporation and non-profit data ingestion module.

This module provides ingesters for provincial corporation registries,
including non-profit organizations, co-operatives, and all corporation types.

PROVINCES WITH BULK DATA:
- Quebec (QC): All corporation types - daily CSV from Données Québec
- Alberta (AB): Non-profit organizations only - monthly XLSX from Alberta Open Data
- Nova Scotia (NS): Co-operatives only - CSV from NS Open Data Portal

PROVINCES WITHOUT BULK DATA:
- British Columbia (BC): Paid API only ($100+ BC OnLine)
- Ontario (ON): Search-only registry
- Saskatchewan (SK): Search-only registry
- Manitoba (MB): Search-only registry
- New Brunswick (NB): Search-only registry
- Prince Edward Island (PE): Search-only registry
- Newfoundland and Labrador (NL): Search-only registry
- Northwest Territories (NT): Search-only registry
- Yukon (YT): Search-only registry
- Nunavut (NU): Search-only registry

For provinces without bulk data, use the cross-reference service to match
entities discovered through other sources (SEC, SEDAR, Elections Canada, etc.)
with federal corporation records.

Usage:
    from mitds.ingestion.provincial import run_quebec_corps_ingestion

    result = await run_quebec_corps_ingestion(
        incremental=True,
        limit=100,
    )
"""

from .alberta import AlbertaNonProfitIngester, run_alberta_nonprofits_ingestion
from .base import BaseProvincialCorpIngester, BaseProvincialIngester
from .cross_reference import CrossReferenceService, run_cross_reference
from .nova_scotia import NovaScotiaCoopsIngester, run_nova_scotia_coops_ingestion
from .quebec import QuebecCorporationIngester, run_quebec_corps_ingestion
from .targeted import (
    NoBulkDataError,
    check_bulk_data_available,
    get_available_provinces,
    get_unavailable_provinces,
)
from .search import (
    BaseRegistrySearch,
    OntarioRegistrySearch,
    SaskatchewanRegistrySearch,
    ManitobaRegistrySearch,
    BCRegistrySearch,
    NewBrunswickRegistrySearch,
    PEIRegistrySearch,
    NewfoundlandRegistrySearch,
    NWTRegistrySearch,
    YukonRegistrySearch,
    NunavutRegistrySearch,
    SearchResult,
    get_registry_search,
    get_registry_access_info,
    get_public_search_provinces,
    get_account_required_provinces,
    run_targeted_search,
    SEARCH_REGISTRY_CLASSES,
)
from .models import (
    # Corporation enums and models (005)
    Address,
    CrossReferenceResult,
    Director,
    EntityMatchResult,
    ProvincialCorporationRecord,
    ProvincialCorpStatus,
    ProvincialCorpType,
    ProvincialDataSource,
    # Non-profit enums and models (004 legacy)
    ProvincialNonProfitRecord,
    ProvincialOrgStatus,
    ProvincialOrgType,
)

__all__ = [
    # Non-profit models (004 legacy)
    "ProvincialNonProfitRecord",
    "ProvincialOrgType",
    "ProvincialOrgStatus",
    "EntityMatchResult",
    # Corporation models (005)
    "ProvincialCorporationRecord",
    "ProvincialCorpType",
    "ProvincialCorpStatus",
    "Address",
    "Director",
    "ProvincialDataSource",
    "CrossReferenceResult",
    # Base classes
    "BaseProvincialIngester",
    "BaseProvincialCorpIngester",
    # Alberta non-profits (bulk data)
    "AlbertaNonProfitIngester",
    "run_alberta_nonprofits_ingestion",
    # Quebec corporations (bulk data)
    "QuebecCorporationIngester",
    "run_quebec_corps_ingestion",
    # Nova Scotia co-ops (bulk data)
    "NovaScotiaCoopsIngester",
    "run_nova_scotia_coops_ingestion",
    # Cross-reference service
    "CrossReferenceService",
    "run_cross_reference",
    # Data availability helpers
    "NoBulkDataError",
    "check_bulk_data_available",
    "get_available_provinces",
    "get_unavailable_provinces",
    # Search-based registry scrapers (Playwright)
    "BaseRegistrySearch",
    "OntarioRegistrySearch",
    "SaskatchewanRegistrySearch",
    "ManitobaRegistrySearch",
    "BCRegistrySearch",
    "NewBrunswickRegistrySearch",
    "PEIRegistrySearch",
    "NewfoundlandRegistrySearch",
    "NWTRegistrySearch",
    "YukonRegistrySearch",
    "NunavutRegistrySearch",
    "SearchResult",
    "get_registry_search",
    "get_registry_access_info",
    "get_public_search_provinces",
    "get_account_required_provinces",
    "run_targeted_search",
    "SEARCH_REGISTRY_CLASSES",
]
