"""Data ingestion modules for MITDS."""

from .base import BaseIngester, IngestionConfig, IngestionResult, RetryConfig, with_retry
from .cra import CRAIngester, run_cra_ingestion
from .irs990 import IRS990Ingester, run_irs990_ingestion
from .edgar import SECEDGARIngester, run_sec_edgar_ingestion
from .canada_corps import CanadaCorporationsIngester, run_canada_corps_ingestion
from .search import search_all_sources, warmup_search_cache, CompanySearchResult, CompanySearchResponse

__all__ = [
    "BaseIngester",
    "IngestionConfig",
    "IngestionResult",
    "RetryConfig",
    "with_retry",
    "IRS990Ingester",
    "run_irs990_ingestion",
    "CRAIngester",
    "run_cra_ingestion",
    "SECEDGARIngester",
    "run_sec_edgar_ingestion",
    "CanadaCorporationsIngester",
    "run_canada_corps_ingestion",
    "search_all_sources",
    "warmup_search_cache",
    "CompanySearchResult",
    "CompanySearchResponse",
]
