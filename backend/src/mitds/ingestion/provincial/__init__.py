"""Provincial corporation and non-profit data ingestion module.

This module provides ingesters for provincial corporation registries,
including non-profit organizations and all corporation types.

Each province has its own ingester class that inherits from either:
- BaseProvincialIngester (004 legacy - XLSX non-profits only)
- BaseProvincialCorpIngester (005+ - multi-format, all corp types)

Usage:
    from mitds.ingestion.provincial import run_alberta_nonprofits_ingestion

    result = await run_alberta_nonprofits_ingestion(
        incremental=True,
        limit=100,
    )
"""

from .alberta import AlbertaNonProfitIngester, run_alberta_nonprofits_ingestion
from .base import BaseProvincialCorpIngester, BaseProvincialIngester
from .cross_reference import CrossReferenceService, run_cross_reference
from .ontario import OntarioCorporationIngester, run_ontario_corps_ingestion
from .quebec import QuebecCorporationIngester, run_quebec_corps_ingestion
from .targeted import (
    BaseTargetedIngester,
    OntarioTargetedIngester,
    SaskatchewanTargetedIngester,
    ManitobaTargetedIngester,
    NewBrunswickTargetedIngester,
    PEITargetedIngester,
    NewfoundlandTargetedIngester,
    NWTTargetedIngester,
    YukonTargetedIngester,
    NunavutTargetedIngester,
    get_targeted_ingester,
    run_targeted_ingestion,
    generate_csv_template,
    TARGETED_INGESTERS,
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
    # Alberta non-profits
    "AlbertaNonProfitIngester",
    "run_alberta_nonprofits_ingestion",
    # Quebec corporations
    "QuebecCorporationIngester",
    "run_quebec_corps_ingestion",
    # Ontario corporations
    "OntarioCorporationIngester",
    "run_ontario_corps_ingestion",
    # Cross-reference service
    "CrossReferenceService",
    "run_cross_reference",
    # Targeted ingesters (provinces without bulk data)
    "BaseTargetedIngester",
    "OntarioTargetedIngester",
    "SaskatchewanTargetedIngester",
    "ManitobaTargetedIngester",
    "NewBrunswickTargetedIngester",
    "PEITargetedIngester",
    "NewfoundlandTargetedIngester",
    "NWTTargetedIngester",
    "YukonTargetedIngester",
    "NunavutTargetedIngester",
    "get_targeted_ingester",
    "run_targeted_ingestion",
    "generate_csv_template",
    "TARGETED_INGESTERS",
]
