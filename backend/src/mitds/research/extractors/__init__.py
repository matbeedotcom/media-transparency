"""Lead extractors for the research system.

Extractors discover new leads from entities and relationships.
"""

from .base import BaseLeadExtractor
from .ownership import OwnershipLeadExtractor
from .funding import FundingLeadExtractor

__all__ = [
    "BaseLeadExtractor",
    "OwnershipLeadExtractor",
    "FundingLeadExtractor",
]
