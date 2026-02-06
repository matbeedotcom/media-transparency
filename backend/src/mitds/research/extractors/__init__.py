"""Lead extractors for the research system.

Extractors discover new leads from entities and relationships.
"""

from .base import BaseLeadExtractor
from .beneficial_ownership import BeneficialOwnershipExtractor
from .funding import FundingLeadExtractor
from .ownership import OwnershipLeadExtractor
from .political_contribution import PoliticalContributionExtractor
from .shared_address import SharedAddressExtractor

__all__ = [
    "BaseLeadExtractor",
    "BeneficialOwnershipExtractor",
    "FundingLeadExtractor",
    "OwnershipLeadExtractor",
    "PoliticalContributionExtractor",
    "SharedAddressExtractor",
]
