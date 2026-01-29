"""Data ingestion modules for MITDS.

This module provides a unified framework for ingesting data from various sources
into the MITDS dual-database architecture (PostgreSQL + Neo4j).

## Quick Start

To create a new ingester:
1. Copy `_template.py` as your starting point
2. Implement `fetch_records()` and `process_record()` methods
3. Add your ingester to the exports below
4. Register in CLI if needed

## Key Classes

- `BaseIngester`: Abstract base class all ingesters inherit from
- `IngestionConfig`: Configuration for ingestion runs
- `IngestionResult`: Results from an ingestion run
- `Neo4jHelper`: Helper for common Neo4j operations
- `PostgresHelper`: Helper for common PostgreSQL operations

## Database Patterns

All ingesters should follow these patterns:
- Use `get_db_session()` context manager for PostgreSQL
- Use `get_neo4j_session()` context manager for Neo4j
- Do NOT call `db.commit()` - context manager handles it
- Wrap Neo4j operations in try/except for graceful degradation

See `_template.py` for a complete example implementation.
"""

from .base import (
    BaseIngester,
    IngestionConfig,
    IngestionResult,
    RetryConfig,
    with_retry,
    Neo4jHelper,
    PostgresHelper,
    suppress_db_logging,
    create_progress_bar,
    download_with_progress,
)
from .cra import CRAIngester, run_cra_ingestion
from .irs990 import IRS990Ingester, run_irs990_ingestion
from .edgar import SECEDGARIngester, run_sec_edgar_ingestion
from .canada_corps import CanadaCorporationsIngester, run_canada_corps_ingestion
from .lobbying import LobbyingIngester, run_lobbying_ingestion
from .elections_canada import ElectionsCanadaIngester, run_elections_canada_ingestion
from .littlesis import LittleSisIngester, run_littlesis_ingestion, get_littlesis_stats
from .meta_ads import MetaAdIngester, run_meta_ads_ingestion
from .sedar import SEDARIngester, run_sedar_ingestion
from .search import search_all_sources, warmup_search_cache, CompanySearchResult, CompanySearchResponse

__all__ = [
    # Base classes and utilities
    "BaseIngester",
    "IngestionConfig",
    "IngestionResult",
    "RetryConfig",
    "with_retry",
    "Neo4jHelper",
    "PostgresHelper",
    # Progress utilities
    "suppress_db_logging",
    "create_progress_bar",
    "download_with_progress",
    # Ingesters
    "IRS990Ingester",
    "run_irs990_ingestion",
    "CRAIngester",
    "run_cra_ingestion",
    "SECEDGARIngester",
    "run_sec_edgar_ingestion",
    "CanadaCorporationsIngester",
    "run_canada_corps_ingestion",
    "LobbyingIngester",
    "run_lobbying_ingestion",
    "ElectionsCanadaIngester",
    "run_elections_canada_ingestion",
    "LittleSisIngester",
    "run_littlesis_ingestion",
    "get_littlesis_stats",
    "MetaAdIngester",
    "run_meta_ads_ingestion",
    "SEDARIngester",
    "run_sedar_ingestion",
    "search_all_sources",
    "warmup_search_cache",
    "CompanySearchResult",
    "CompanySearchResponse",
]
