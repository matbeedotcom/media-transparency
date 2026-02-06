"""Ingestion API endpoints for MITDS."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from ..db import get_db_session
from . import ValidationError
from .auth import OptionalUser

router = APIRouter(prefix="/ingestion")


# =========================
# Request Models
# =========================


class IngestionTriggerRequest(BaseModel):
    """Request for triggering ingestion."""

    incremental: bool = True
    start_year: int | None = None
    end_year: int | None = None
    limit: int | None = None
    target_entities: list[str] | None = None


class IngestionRunResponse(BaseModel):
    """Response for ingestion run status."""

    run_id: UUID
    source: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    duplicates_found: int = 0
    errors: list[dict[str, Any]] = []


class ProvincialIngestionRequest(BaseModel):
    """Request for triggering provincial corporation ingestion."""

    incremental: bool = True
    limit: int | None = None
    target_entities: list[str] | None = None
    from_csv: str | None = None  # Path to CSV file for targeted ingestion


class CrossReferenceRequest(BaseModel):
    """Request for triggering cross-reference operation."""

    provinces: list[str] | None = None  # List of province codes, None = all
    auto_link_threshold: float = 0.95
    review_threshold: float = 0.85


class LinkedInIngestionRequest(BaseModel):
    """Request for triggering LinkedIn member ingestion."""

    company_name: str | None = None  # Company name to filter/search
    company_url: str | None = None  # LinkedIn company URL
    company_entity_id: str | None = None  # UUID of existing org to link members to
    csv_data: str | None = None  # Base64-encoded CSV data (for file upload)
    scrape: bool = False  # Enable browser scraping
    session_cookie: str | None = None  # LinkedIn li_at cookie
    titles_filter: list[str] | None = None  # Filter by title keywords
    limit: int | None = None


class CrossReferenceResponse(BaseModel):
    """Response for cross-reference operation."""

    total_processed: int = 0
    matched_by_bn: int = 0
    matched_by_exact_name: int = 0
    matched_by_fuzzy_name: int = 0
    auto_linked: int = 0
    flagged_for_review: int = 0
    no_match: int = 0


class ReviewItem(BaseModel):
    """A match flagged for manual review."""

    provincial_record_name: str
    matched_entity_id: UUID | None = None
    matched_entity_name: str | None = None
    match_score: float
    match_method: str
    jurisdiction: str


# =========================
# Search Companies
# =========================


class CompanySearchResultResponse(BaseModel):
    """A single company search result."""

    source: str
    identifier: str
    identifier_type: str
    name: str
    details: dict[str, Any] = {}
    # Optional fields for extended info
    jurisdiction: str | None = None
    status: str | None = None
    address: str | None = None
    match_score: float | None = None


class CompanySearchResponse(BaseModel):
    """Response from company search."""

    query: str
    results: list[CompanySearchResultResponse]
    sources_searched: list[str]
    sources_failed: list[str] = []


class SourceStatusItem(BaseModel):
    """Status of a single data source."""
    source: str
    status: str
    last_run_id: str | None = None
    last_run_at: str | None = None
    records_total: int = 0
    last_records_processed: int = 0
    error: str | None = None


class IngestionStatusResponse(BaseModel):
    """Response for ingestion status endpoint."""
    sources: list[SourceStatusItem]
    total_records: int
    healthy_sources: int
    total_sources: int
    total: int


@router.get("/search", response_model=CompanySearchResponse)
async def search_companies(
    q: str,
    sources: str | None = None,
    limit: int = 10,
    user: OptionalUser = None,
) -> CompanySearchResponse:
    """Search for companies across all data sources.

    Searches by company name, ticker, or identifier across SEC EDGAR,
    IRS 990, CRA, and Canada Corporations data.

    Args:
        q: Search query (company name, ticker, etc.)
        sources: Comma-separated list of sources to search (default: all)
        limit: Maximum results per source (default: 10)
    """
    from ..ingestion.search import search_all_sources

    source_list = None
    if sources:
        source_list = [s.strip() for s in sources.split(",")]

    result = await search_all_sources(
        query=q,
        sources=source_list,
        limit=min(limit, 50),
    )

    return CompanySearchResponse(
        query=result.query,
        results=[
            CompanySearchResultResponse(
                source=r.source,
                identifier=r.identifier,
                identifier_type=r.identifier_type,
                name=r.name,
                details=r.details,
            )
            for r in result.results
        ],
        sources_searched=result.sources_searched,
        sources_failed=result.sources_failed,
        total=len(result.results),
    )


# =========================
# Autocomplete (Fast Suggestions)
# =========================


class AutocompleteSuggestion(BaseModel):
    """Single autocomplete suggestion."""
    
    name: str
    entity_type: str  # "organization", "person", "sponsor"
    source: str  # Where this data came from
    id: str | None = None  # Entity ID if in Neo4j
    jurisdiction: str | None = None


@router.get("/autocomplete")
async def autocomplete_entities(
    q: str,
    limit: int = 10,
    types: str | None = None,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Fast autocomplete for entity names.

    Returns quick suggestions from pre-scraped data as user types.
    Optimized for low latency - returns minimal data.

    Args:
        q: Search query (minimum 2 characters)
        limit: Maximum suggestions (default: 10, max: 25)
        types: Comma-separated entity types to include (organization, person, sponsor)
    """
    from ..db import get_neo4j_session

    if len(q) < 2:
        return {"suggestions": [], "query": q}

    limit = min(limit, 25)
    
    # Parse types filter - use proper Neo4j label case (e.g., Organization, not ORGANIZATION)
    type_filter = ""
    if types:
        # Map incoming types to actual Neo4j labels (case-sensitive)
        type_mapping = {
            "organization": "Organization",
            "person": "Person",
            "sponsor": "Sponsor",
            "outlet": "Outlet",
        }
        type_list = [t.strip().lower() for t in types.split(",")]
        neo4j_labels = [type_mapping[t] for t in type_list if t in type_mapping]
        if neo4j_labels:
            type_labels = " OR ".join([f"e:{label}" for label in neo4j_labels])
            type_filter = f"AND ({type_labels})"

    suggestions: list[AutocompleteSuggestion] = []

    # Search Neo4j for entities
    try:
        async with get_neo4j_session() as session:
            # Fast prefix search with STARTS WITH for speed
            query = f"""
            MATCH (e)
            WHERE (e:Organization OR e:Person OR e:Sponsor OR e:Outlet)
            AND (
                toLower(e.name) STARTS WITH toLower($prefix)
                OR toLower(e.name) CONTAINS toLower($search_term)
                OR any(alias IN coalesce(e.aliases, []) WHERE toLower(alias) STARTS WITH toLower($prefix))
            )
            {type_filter}
            WITH e,
                 CASE WHEN toLower(e.name) STARTS WITH toLower($prefix) THEN 0 ELSE 1 END as match_rank
            RETURN DISTINCT
                e.id as id,
                e.name as name,
                e.entity_type as entity_type,
                e.jurisdiction as jurisdiction,
                coalesce(e.confidence, 0.5) as confidence,
                match_rank,
                'neo4j' as source
            ORDER BY match_rank, confidence DESC, name
            LIMIT $limit
            """

            result = await session.run(
                query,
                prefix=q,
                search_term=q,
                limit=limit,
            )
            records = await result.data()

            for record in records:
                suggestions.append(AutocompleteSuggestion(
                    name=record.get("name", "Unknown"),
                    entity_type=record.get("entity_type", "organization").lower(),
                    source=record.get("source", "neo4j"),
                    id=record.get("id"),
                    jurisdiction=record.get("jurisdiction"),
                ))
    except Exception as e:
        # Log but don't fail - autocomplete should be resilient
        import logging
        logging.getLogger(__name__).warning(f"Neo4j autocomplete failed: {e}")

    # Future: Could also search provincial registry cache if needed
    # For now, Neo4j contains all scraped data

    return {
        "suggestions": [s.model_dump() for s in suggestions[:limit]],
        "query": q,
        "total": len(suggestions),
    }


# =========================
# Get Status
# =========================


@router.get("/status", response_model=IngestionStatusResponse)
async def get_ingestion_status(
    user: OptionalUser = None,
) -> IngestionStatusResponse:
    """Get status of all data ingestion pipelines.

    Returns the health status, last run information, and
    record counts for each configured data source.
    """
    async with get_db_session() as db:
        # Get latest run for each source
        query = text("""
            SELECT DISTINCT ON (source)
                source,
                id as run_id,
                status,
                started_at,
                completed_at,
                records_processed,
                records_created
            FROM ingestion_runs
            ORDER BY source, completed_at DESC NULLS LAST
        """)

        result = await db.execute(query)
        runs = result.fetchall()

        # Build status for each source
        run_by_source = {r.source: r for r in runs}

        sources_status = []

        # Sources that are enabled (free, no API key required)
        enabled_sources = ["irs990", "cra", "sec_edgar", "canada_corps", "sedar", "alberta-nonprofits", "linkedin"]
        # Sources that require API keys
        disabled_sources = ["opencorporates", "meta_ads"]

        for source_name in enabled_sources + disabled_sources:
            run = run_by_source.get(source_name)

            if run:
                # Determine health status
                if run.status == "running":
                    status = "running"
                elif run.status in ("completed", "partial"):
                    status = "healthy"
                else:
                    status = "error"

                sources_status.append({
                    "source": source_name,
                    "status": status,
                    "last_run_id": str(run.run_id) if run.run_id else None,
                    "last_run_status": run.status,
                    "last_successful_run": run.completed_at.isoformat() if run.completed_at else None,
                    "records_processed": run.records_processed or 0,
                    "records_created": run.records_created or 0,
                })
            else:
                # No runs yet
                sources_status.append({
                    "source": source_name,
                    "status": "never_run" if source_name in enabled_sources else "disabled",
                    "last_run_id": None,
                    "last_run_status": None,
                    "last_successful_run": None,
                    "records_processed": 0,
                    "records_created": 0,
                    "records_updated": 0,
                })

        return {
            "sources": sources_status,
            "timestamp": datetime.utcnow().isoformat(),
        }


# =========================
# Get Run History
# =========================


@router.get("/runs")
async def get_ingestion_runs(
    source: str | None = None,
    status: str | None = None,
    limit: int = 20,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get history of ingestion runs.

    Args:
        source: Filter by source name
        status: Filter by status
        limit: Maximum results
    """
    async with get_db_session() as db:
        filters = []
        params = {"limit": limit}

        if source:
            filters.append("source = :source")
            params["source"] = source

        if status:
            filters.append("status = :status")
            params["status"] = status

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = text(f"""
            SELECT
                id as run_id,
                source,
                status,
                started_at,
                completed_at,
                records_processed,
                records_created,
                records_updated,
                duplicates_found,
                errors
            FROM ingestion_runs
            {where_clause}
            ORDER BY started_at DESC
            LIMIT :limit
        """)

        result = await db.execute(query, params)
        runs = result.fetchall()

        return {
            "runs": [
                {
                    "run_id": str(r.run_id),
                    "source": r.source,
                    "status": r.status,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                    "records_processed": r.records_processed or 0,
                    "records_created": r.records_created or 0,
                    "records_updated": r.records_updated or 0,
                    "duplicates_found": r.duplicates_found or 0,
                    "errors": r.errors or [],
                }
                for r in runs
            ],
            "total": len(runs),
        }


# =========================
# Get Single Run
# =========================


@router.get("/runs/{run_id}")
async def get_ingestion_run(
    run_id: UUID,
    user: OptionalUser = None,
) -> IngestionRunResponse:
    """Get details of a specific ingestion run."""
    async with get_db_session() as db:
        query = text("""
            SELECT
                id as run_id,
                source,
                status,
                started_at,
                completed_at,
                records_processed,
                records_created,
                records_updated,
                duplicates_found,
                errors
            FROM ingestion_runs
            WHERE id = :run_id
        """)

        result = await db.execute(query, {"run_id": run_id})
        run = result.fetchone()

        if not run:
            from . import NotFoundError
            raise NotFoundError("Ingestion run", run_id)

        return IngestionRunResponse(
            run_id=run.run_id,
            source=run.source,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            records_processed=run.records_processed or 0,
            records_created=run.records_created or 0,
            records_updated=run.records_updated or 0,
            duplicates_found=run.duplicates_found or 0,
            errors=run.errors or [],
        )


@router.get("/runs/{run_id}/logs")
async def get_ingestion_run_logs(
    run_id: UUID,
    offset: int = 0,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get log output for an ingestion run.

    For active runs, returns logs from the in-memory buffer.
    For completed runs, returns logs from the database.

    Args:
        run_id: The ingestion run ID
        offset: Line offset for incremental polling
    """
    from ..ingestion.run_log import get_live_logs

    # Try in-memory buffer first (active run)
    live_lines = get_live_logs(str(run_id), offset=offset)
    if live_lines is not None:
        return {
            "lines": live_lines,
            "total_lines": offset + len(live_lines),
            "is_live": True,
        }

    # Fall back to database (completed run)
    async with get_db_session() as db:
        query = text("""
            SELECT log_output, status
            FROM ingestion_runs
            WHERE id = :run_id
        """)
        result = await db.execute(query, {"run_id": run_id})
        row = result.fetchone()

        if not row:
            from . import NotFoundError
            raise NotFoundError("Ingestion run", run_id)

        log_text = row.log_output or ""
        all_lines = log_text.split("\n") if log_text else []
        sliced = all_lines[offset:]

        return {
            "lines": sliced,
            "total_lines": len(all_lines),
            "is_live": False,
        }


# =========================
# Trigger Ingestion
# =========================


async def _run_ingestion_task(
    source: str,
    run_id: UUID,
    request: IngestionTriggerRequest,
):
    """Background task to run ingestion."""
    from ..ingestion import run_cra_ingestion, run_irs990_ingestion
    from ..ingestion.canada_corps import run_canada_corps_ingestion
    from ..ingestion.edgar import run_sec_edgar_ingestion
    from ..ingestion.meta_ads import run_meta_ads_ingestion

    try:
        if source == "irs990":
            result = await run_irs990_ingestion(
                start_year=request.start_year,
                end_year=request.end_year,
                incremental=request.incremental,
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        elif source == "cra":
            result = await run_cra_ingestion(
                incremental=request.incremental,
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        elif source == "sec_edgar":
            result = await run_sec_edgar_ingestion(
                limit=request.limit,
                target_entities=request.target_entities,
                flag_canadian=True,  # Always flag Canadian companies
                run_id=run_id,
            )
        elif source == "canada_corps":
            result = await run_canada_corps_ingestion(
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        elif source == "meta_ads":
            result = await run_meta_ads_ingestion(
                countries=["US", "CA"],
                days_back=7,
                incremental=request.incremental,
                limit=request.limit,
            )
        elif source == "sedar":
            from ..ingestion.sedar import run_sedar_ingestion
            result = await run_sedar_ingestion(
                incremental=request.incremental,
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        elif source == "alberta-nonprofits":
            from ..ingestion.provincial import run_alberta_nonprofits_ingestion
            result = await run_alberta_nonprofits_ingestion(
                incremental=request.incremental,
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        else:
            # Not implemented yet
            result = {
                "status": "failed",
                "errors": [{"error": f"Source {source} not implemented"}],
            }

        # Update run in database
        async with get_db_session() as db:
            update_query = text("""
                UPDATE ingestion_runs
                SET status = :status,
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_created,
                    records_updated = :records_updated,
                    duplicates_found = :duplicates_found,
                    errors = CAST(:errors AS jsonb),
                    log_output = :log_output
                WHERE id = :run_id
            """)

            await db.execute(
                update_query,
                {
                    "run_id": run_id,
                    "status": result.get("status", "completed"),
                    "completed_at": datetime.utcnow(),
                    "records_processed": result.get("records_processed", 0),
                    "records_created": result.get("records_created", 0),
                    "records_updated": result.get("records_updated", 0),
                    "duplicates_found": result.get("duplicates_found", 0),
                    "errors": json.dumps(result.get("errors", []), default=str),
                    "log_output": result.get("log_output", ""),
                },
            )
            await db.commit()

    except Exception as e:
        # Flush any captured logs before recording error
        from ..ingestion.run_log import finish_capture
        error_log_output = finish_capture(str(run_id))

        # Update run with error
        async with get_db_session() as db:
            error_query = text("""
                UPDATE ingestion_runs
                SET status = 'failed',
                    completed_at = :completed_at,
                    errors = CAST(:errors AS jsonb),
                    log_output = :log_output
                WHERE id = :run_id
            """)

            await db.execute(
                error_query,
                {
                    "run_id": run_id,
                    "completed_at": datetime.utcnow(),
                    "errors": json.dumps([{"error": str(e), "fatal": True}]),
                    "log_output": error_log_output,
                },
            )
            await db.commit()


@router.post("/{source}/trigger")
async def trigger_ingestion(
    source: str,
    background_tasks: BackgroundTasks,
    request: IngestionTriggerRequest | None = None,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Trigger manual ingestion for a source.

    Starts an asynchronous ingestion job and returns immediately
    with a job ID that can be used to track progress.

    Args:
        source: Data source name (irs990, cra, opencorporates, meta_ads)
        request: Ingestion configuration

    Returns:
        Job ID and status URL for tracking
    """
    valid_sources = ["irs990", "cra", "sec_edgar", "canada_corps", "sedar", "opencorporates", "meta_ads", "alberta-nonprofits"]

    if source not in valid_sources:
        raise ValidationError(
            f"Invalid source: {source}. Must be one of {valid_sources}"
        )

    if source == "opencorporates":
        raise HTTPException(
            status_code=501,
            detail=f"Source '{source}' requires API key configuration"
        )

    # Check Meta ads configuration
    if source == "meta_ads":
        from ..config import get_settings
        settings = get_settings()
        if not settings.meta_access_token:
            raise HTTPException(
                status_code=501,
                detail="Meta Ads requires META_ACCESS_TOKEN to be configured"
            )

    if request is None:
        request = IngestionTriggerRequest()

    # Create ingestion run record
    run_id = uuid4()

    async with get_db_session() as db:
        insert_query = text("""
            INSERT INTO ingestion_runs (id, source, started_at, status)
            VALUES (:id, :source, :started_at, :status)
        """)

        await db.execute(
            insert_query,
            {
                "id": run_id,
                "source": source,
                "started_at": datetime.utcnow(),
                "status": "running",
            },
        )
        await db.commit()

    # Start background task
    background_tasks.add_task(_run_ingestion_task, source, run_id, request)

    return {
        "run_id": str(run_id),
        "source": source,
        "status": "running",
        "status_url": f"/api/v1/ingestion/runs/{run_id}",
        "message": f"Ingestion started for {source}",
    }


# =========================
# Provincial Corporation Ingestion
# =========================


# Valid province codes for bulk data ingestion
BULK_DATA_PROVINCES = ["QC"]  # Only Quebec has bulk data

# Provinces requiring targeted ingestion (CSV upload or web scraping)
TARGETED_PROVINCES = ["ON", "SK", "MB", "NB", "PE", "NL", "NT", "YT", "NU"]

ALL_PROVINCES = BULK_DATA_PROVINCES + TARGETED_PROVINCES


@router.get("/provincial")
async def list_provincial_sources(
    user: OptionalUser = None,
) -> dict[str, Any]:
    """List available provincial corporation data sources.

    Returns information about each province's data availability
    and ingestion method (bulk vs targeted).
    """
    sources = []

    for province in BULK_DATA_PROVINCES:
        sources.append({
            "province": province,
            "name": _get_province_name(province),
            "method": "bulk",
            "description": "Bulk CSV download available",
            "cli_command": f"mitds ingest {province.lower()}-corps",
        })

    for province in TARGETED_PROVINCES:
        sources.append({
            "province": province,
            "name": _get_province_name(province),
            "method": "targeted",
            "description": "CSV upload or web scraping (no bulk data)",
            "cli_command": f"mitds ingest targeted-corps --province {province}",
        })

    return {
        "sources": sources,
        "total": len(sources),
        "bulk_data_provinces": BULK_DATA_PROVINCES,
        "targeted_provinces": TARGETED_PROVINCES,
    }


def _get_province_name(code: str) -> str:
    """Get full province name from code."""
    names = {
        "QC": "Quebec",
        "ON": "Ontario",
        "AB": "Alberta",
        "BC": "British Columbia",
        "SK": "Saskatchewan",
        "MB": "Manitoba",
        "NB": "New Brunswick",
        "NS": "Nova Scotia",
        "PE": "Prince Edward Island",
        "NL": "Newfoundland and Labrador",
        "NT": "Northwest Territories",
        "YT": "Yukon",
        "NU": "Nunavut",
    }
    return names.get(code, code)


async def _run_provincial_ingestion_task(
    province: str,
    run_id: UUID,
    request: ProvincialIngestionRequest,
):
    """Background task to run provincial ingestion."""
    try:
        if province == "QC":
            from ..ingestion.provincial import run_quebec_corps_ingestion
            result = await run_quebec_corps_ingestion(
                incremental=request.incremental,
                limit=request.limit,
                target_entities=request.target_entities,
                run_id=run_id,
            )
        elif province in TARGETED_PROVINCES:
            from ..ingestion.provincial import run_targeted_ingestion
            result = await run_targeted_ingestion(
                province=province,
                target_entities=request.target_entities,
                from_csv=request.from_csv,
                limit=request.limit,
                run_id=run_id,
            )
        else:
            result = {
                "status": "failed",
                "errors": [{"error": f"Province {province} not implemented"}],
            }

        # Update run in database
        async with get_db_session() as db:
            update_query = text("""
                UPDATE ingestion_runs
                SET status = :status,
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_created,
                    records_updated = :records_updated,
                    duplicates_found = :duplicates_found,
                    errors = CAST(:errors AS jsonb)
                WHERE id = :run_id
            """)

            await db.execute(
                update_query,
                {
                    "run_id": run_id,
                    "status": result.get("status", "completed"),
                    "completed_at": datetime.utcnow(),
                    "records_processed": result.get("records_processed", 0),
                    "records_created": result.get("records_created", 0),
                    "records_updated": result.get("records_updated", 0),
                    "duplicates_found": result.get("duplicates_found", 0),
                    "errors": json.dumps(result.get("errors", []), default=str),
                },
            )
            await db.commit()

    except Exception as e:
        async with get_db_session() as db:
            error_query = text("""
                UPDATE ingestion_runs
                SET status = 'failed',
                    completed_at = :completed_at,
                    errors = CAST(:errors AS jsonb)
                WHERE id = :run_id
            """)

            await db.execute(
                error_query,
                {
                    "run_id": run_id,
                    "completed_at": datetime.utcnow(),
                    "errors": json.dumps([{"error": str(e), "fatal": True}]),
                },
            )
            await db.commit()


@router.post("/provincial/{province}")
async def trigger_provincial_ingestion(
    province: str,
    background_tasks: BackgroundTasks,
    request: ProvincialIngestionRequest | None = None,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Trigger provincial corporation ingestion.

    For bulk data provinces (QC), downloads and processes the full dataset.
    For targeted provinces (ON, SK, MB, etc.), requires either:
    - target_entities: List of entity names to search
    - from_csv: Path to CSV file with corporation data

    Args:
        province: Province code (QC, ON, SK, MB, NB, PE, NL, NT, YT, NU)
        request: Ingestion configuration
    """
    province = province.upper()

    if province not in ALL_PROVINCES:
        raise ValidationError(
            f"Invalid province: {province}. Must be one of {ALL_PROVINCES}"
        )

    if request is None:
        request = ProvincialIngestionRequest()

    # Validate targeted ingestion requirements
    if province in TARGETED_PROVINCES:
        if not request.target_entities and not request.from_csv:
            raise ValidationError(
                f"Province {province} requires either target_entities or from_csv parameter. "
                f"No bulk data is available for this province."
            )

    # Create ingestion run record
    run_id = uuid4()
    source_name = f"{province.lower()}-corps"

    async with get_db_session() as db:
        insert_query = text("""
            INSERT INTO ingestion_runs (id, source, started_at, status)
            VALUES (:id, :source, :started_at, :status)
        """)

        await db.execute(
            insert_query,
            {
                "id": run_id,
                "source": source_name,
                "started_at": datetime.utcnow(),
                "status": "running",
            },
        )
        await db.commit()

    # Start background task
    background_tasks.add_task(
        _run_provincial_ingestion_task, province, run_id, request
    )

    return {
        "run_id": str(run_id),
        "province": province,
        "source": source_name,
        "status": "running",
        "status_url": f"/api/v1/ingestion/runs/{run_id}",
        "message": f"Provincial ingestion started for {_get_province_name(province)}",
    }


@router.get("/provincial/{province}/status")
async def get_provincial_ingestion_status(
    province: str,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get status of provincial corporation ingestion.

    Returns the latest run information for the specified province.
    """
    province = province.upper()
    source_name = f"{province.lower()}-corps"

    if province in TARGETED_PROVINCES:
        source_name = f"{province.lower()}-targeted"

    async with get_db_session() as db:
        query = text("""
            SELECT
                id as run_id,
                source,
                status,
                started_at,
                completed_at,
                records_processed,
                records_created,
                records_updated,
                duplicates_found,
                errors
            FROM ingestion_runs
            WHERE source LIKE :source_pattern
            ORDER BY started_at DESC
            LIMIT 1
        """)

        result = await db.execute(query, {"source_pattern": f"{province.lower()}%"})
        run = result.fetchone()

        if not run:
            return {
                "province": province,
                "name": _get_province_name(province),
                "status": "never_run",
                "method": "bulk" if province in BULK_DATA_PROVINCES else "targeted",
                "last_run": None,
            }

        return {
            "province": province,
            "name": _get_province_name(province),
            "status": run.status,
            "method": "bulk" if province in BULK_DATA_PROVINCES else "targeted",
            "last_run": {
                "run_id": str(run.run_id),
                "status": run.status,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "records_processed": run.records_processed or 0,
                "records_created": run.records_created or 0,
                "records_updated": run.records_updated or 0,
                "duplicates_found": run.duplicates_found or 0,
            },
        }


# =========================
# Cross-Reference Endpoints
# =========================


async def _run_cross_reference_task(
    run_id: UUID,
    request: CrossReferenceRequest,
):
    """Background task to run cross-referencing."""
    try:
        from ..ingestion.provincial import run_cross_reference

        result = await run_cross_reference(
            provinces=request.provinces,
            auto_link_threshold=request.auto_link_threshold,
            review_threshold=request.review_threshold,
        )

        # Update run in database
        async with get_db_session() as db:
            update_query = text("""
                UPDATE ingestion_runs
                SET status = :status,
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_created,
                    records_updated = :records_updated
                WHERE id = :run_id
            """)

            await db.execute(
                update_query,
                {
                    "run_id": run_id,
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "records_processed": result.get("total_processed", 0),
                    "records_created": result.get("auto_linked", 0),
                    "records_updated": result.get("flagged_for_review", 0),
                },
            )
            await db.commit()

    except Exception as e:
        async with get_db_session() as db:
            error_query = text("""
                UPDATE ingestion_runs
                SET status = 'failed',
                    completed_at = :completed_at,
                    errors = CAST(:errors AS jsonb)
                WHERE id = :run_id
            """)

            await db.execute(
                error_query,
                {
                    "run_id": run_id,
                    "completed_at": datetime.utcnow(),
                    "errors": json.dumps([{"error": str(e), "fatal": True}]),
                },
            )
            await db.commit()


@router.post("/provincial/cross-reference")
async def trigger_cross_reference(
    background_tasks: BackgroundTasks,
    request: CrossReferenceRequest | None = None,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Trigger cross-referencing of provincial corporations with federal registry.

    Matches provincial corporation records with federal registry data
    using business number and name matching strategies.

    Match results are classified by confidence:
    - Auto-link (>=95%): Automatically create SAME_AS relationship
    - Flag for review (85-95%): Requires manual verification
    - No match (<85%): No relationship created

    Args:
        request: Cross-reference configuration (provinces, thresholds)
    """
    if request is None:
        request = CrossReferenceRequest()

    # Create ingestion run record
    run_id = uuid4()

    async with get_db_session() as db:
        insert_query = text("""
            INSERT INTO ingestion_runs (id, source, started_at, status)
            VALUES (:id, :source, :started_at, :status)
        """)

        await db.execute(
            insert_query,
            {
                "id": run_id,
                "source": "cross-reference",
                "started_at": datetime.utcnow(),
                "status": "running",
            },
        )
        await db.commit()

    # Start background task
    background_tasks.add_task(_run_cross_reference_task, run_id, request)

    return {
        "run_id": str(run_id),
        "source": "cross-reference",
        "status": "running",
        "status_url": f"/api/v1/ingestion/runs/{run_id}",
        "message": "Cross-referencing started",
        "provinces": request.provinces or "all",
        "thresholds": {
            "auto_link": request.auto_link_threshold,
            "review": request.review_threshold,
        },
    }


# =========================
# LinkedIn Member Ingestion
# =========================


async def _run_linkedin_ingestion_task(
    run_id: UUID,
    request: LinkedInIngestionRequest,
    csv_path: str | None = None,
):
    """Background task to run LinkedIn ingestion."""
    from ..ingestion.linkedin import run_linkedin_ingestion

    try:
        result = await run_linkedin_ingestion(
            csv_path=csv_path,
            company_name=request.company_name,
            company_url=request.company_url,
            company_entity_id=request.company_entity_id,
            scrape=request.scrape,
            session_cookie=request.session_cookie,
            titles_filter=request.titles_filter,
            limit=request.limit,
            run_id=run_id,
        )

        # Update run in database
        async with get_db_session() as db:
            update_query = text("""
                UPDATE ingestion_runs
                SET status = :status,
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_created,
                    records_updated = :records_updated,
                    errors = CAST(:errors AS jsonb)
                WHERE id = :run_id
            """)

            await db.execute(
                update_query,
                {
                    "run_id": run_id,
                    "status": result.get("status", "completed"),
                    "completed_at": datetime.utcnow(),
                    "records_processed": result.get("records_processed", 0),
                    "records_created": result.get("records_created", 0),
                    "records_updated": result.get("records_updated", 0),
                    "errors": json.dumps(result.get("errors", []), default=str),
                },
            )
            await db.commit()

    except Exception as e:
        async with get_db_session() as db:
            error_query = text("""
                UPDATE ingestion_runs
                SET status = 'failed',
                    completed_at = :completed_at,
                    errors = CAST(:errors AS jsonb)
                WHERE id = :run_id
            """)

            await db.execute(
                error_query,
                {
                    "run_id": run_id,
                    "completed_at": datetime.utcnow(),
                    "errors": json.dumps([{"error": str(e), "fatal": True}]),
                },
            )
            await db.commit()


@router.get("/linkedin/status")
async def get_linkedin_status(
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Check LinkedIn ingestion configuration status.
    
    Returns whether a session cookie is configured (either via env or will need manual input).
    """
    import os
    
    has_cookie = bool(os.environ.get("LINKEDIN_SESSION_COOKIE"))
    
    return {
        "configured": has_cookie,
        "message": "LinkedIn session cookie is configured" if has_cookie else "No session cookie configured - you'll need to provide one",
        "methods_available": ["csv_import", "browser_scrape"] if has_cookie else ["csv_import"],
    }


@router.post("/linkedin")
async def trigger_linkedin_ingestion(
    background_tasks: BackgroundTasks,
    request: LinkedInIngestionRequest,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Trigger LinkedIn member ingestion for network research.

    Ingests LinkedIn member data to map out organizational networks.
    Creates Person nodes with EMPLOYED_BY relationships.

    Supports two modes:
    1. CSV Import: Upload base64-encoded CSV data (recommended)
    2. Browser Scraping: Scrape company pages (requires session cookie)

    Args:
        request: Ingestion configuration including:
            - company_name: Company name to filter/search
            - company_url: LinkedIn company URL (for scraping)
            - company_entity_id: UUID of existing org to link members to
            - csv_data: Base64-encoded CSV (for CSV import)
            - scrape: Enable browser scraping mode
            - session_cookie: LinkedIn li_at cookie (for scraping)
            - titles_filter: Filter by title keywords (e.g., ["CEO", "Director"])
            - limit: Maximum profiles to process

    Returns:
        Job ID and status URL for tracking
    """
    import base64
    import os
    import tempfile

    # Validate request
    if not request.csv_data and not request.scrape:
        raise ValidationError(
            "Must provide either csv_data for import or enable scrape mode"
        )

    # Use environment variable as fallback for session cookie
    session_cookie = request.session_cookie or os.environ.get("LINKEDIN_SESSION_COOKIE")
    
    if request.scrape and not session_cookie:
        raise ValidationError(
            "Browser scraping requires session_cookie for LinkedIn authentication. "
            "Either provide it in the request or set LINKEDIN_SESSION_COOKIE in .env"
        )

    if request.scrape and not request.company_name and not request.company_url:
        raise ValidationError(
            "Scraping requires company_name or company_url"
        )
    
    # Update request with resolved cookie
    if request.scrape:
        request.session_cookie = session_cookie

    # Handle CSV data
    csv_path = None
    if request.csv_data:
        try:
            csv_bytes = base64.b64decode(request.csv_data)
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".csv", delete=False
            ) as f:
                f.write(csv_bytes)
                csv_path = f.name
        except Exception as e:
            raise ValidationError(f"Invalid CSV data: {e}")

    # Create ingestion run record
    run_id = uuid4()

    async with get_db_session() as db:
        insert_query = text("""
            INSERT INTO ingestion_runs (id, source, started_at, status)
            VALUES (:id, :source, :started_at, :status)
        """)

        await db.execute(
            insert_query,
            {
                "id": run_id,
                "source": "linkedin",
                "started_at": datetime.utcnow(),
                "status": "running",
            },
        )
        await db.commit()

    # Start background task
    background_tasks.add_task(
        _run_linkedin_ingestion_task, run_id, request, csv_path
    )

    return {
        "run_id": str(run_id),
        "source": "linkedin",
        "status": "running",
        "status_url": f"/api/v1/ingestion/runs/{run_id}",
        "message": f"LinkedIn ingestion started"
        + (f" for {request.company_name}" if request.company_name else ""),
    }


@router.get("/linkedin/status")
async def get_linkedin_ingestion_status(
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get status of LinkedIn ingestion.

    Returns the latest run information for LinkedIn member ingestion.
    """
    async with get_db_session() as db:
        query = text("""
            SELECT
                id as run_id,
                source,
                status,
                started_at,
                completed_at,
                records_processed,
                records_created,
                records_updated,
                errors
            FROM ingestion_runs
            WHERE source = 'linkedin'
            ORDER BY started_at DESC
            LIMIT 1
        """)

        result = await db.execute(query)
        run = result.fetchone()

        if not run:
            return {
                "source": "linkedin",
                "status": "never_run",
                "last_run": None,
            }

        return {
            "source": "linkedin",
            "status": run.status,
            "last_run": {
                "run_id": str(run.run_id),
                "status": run.status,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "records_processed": run.records_processed or 0,
                "records_created": run.records_created or 0,
                "records_updated": run.records_updated or 0,
            },
        }


@router.get("/linkedin/members")
async def get_linkedin_members(
    company: str | None = None,
    title: str | None = None,
    is_executive: bool | None = None,
    limit: int = 50,
    offset: int = 0,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get ingested LinkedIn members.

    Args:
        company: Filter by company name
        title: Filter by title (partial match)
        is_executive: Filter for executives only
        limit: Maximum results (default: 50)
        offset: Pagination offset
    """
    async with get_db_session() as db:
        filters = ["entity_type = 'person'", "metadata->>'source' = 'linkedin'"]
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if company:
            filters.append("metadata->>'linkedin_company' ILIKE :company")
            params["company"] = f"%{company}%"

        if title:
            filters.append("metadata->>'linkedin_title' ILIKE :title")
            params["title"] = f"%{title}%"

        if is_executive is not None:
            filters.append("(metadata->>'is_executive')::boolean = :is_exec")
            params["is_exec"] = is_executive

        where_clause = " AND ".join(filters)

        query = text(f"""
            SELECT
                id,
                name,
                external_ids->>'linkedin_id' as linkedin_id,
                external_ids->>'linkedin_url' as linkedin_url,
                metadata->>'linkedin_title' as title,
                metadata->>'linkedin_company' as company,
                metadata->>'linkedin_location' as location,
                (metadata->>'is_executive')::boolean as is_executive,
                (metadata->>'is_board_member')::boolean as is_board_member,
                (metadata->>'linkedin_connections')::int as connections,
                created_at,
                updated_at
            FROM entities
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """)

        result = await db.execute(query, params)
        members = result.fetchall()

        # Get total count
        count_query = text(f"""
            SELECT COUNT(*) as total FROM entities WHERE {where_clause}
        """)
        count_result = await db.execute(count_query, params)
        total = count_result.fetchone().total

        return {
            "members": [
                {
                    "id": str(m.id),
                    "name": m.name,
                    "linkedin_id": m.linkedin_id,
                    "linkedin_url": m.linkedin_url,
                    "title": m.title,
                    "company": m.company,
                    "location": m.location,
                    "is_executive": m.is_executive,
                    "is_board_member": m.is_board_member,
                    "connections": m.connections,
                    "created_at": m.created_at.isoformat() if m.created_at else None,
                    "updated_at": m.updated_at.isoformat() if m.updated_at else None,
                }
                for m in members
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@router.get("/provincial/cross-reference/review")
async def get_cross_reference_review_items(
    province: str | None = None,
    limit: int = 50,
    offset: int = 0,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get matches flagged for manual review.

    Returns provincial corporation records that matched with
    85-95% confidence and require human verification.

    Args:
        province: Filter by province code (optional)
        limit: Maximum results (default: 50)
        offset: Pagination offset
    """
    async with get_db_session() as db:
        # Query entities that have review flags
        filters = ["metadata->>'requires_review' = 'true'"]
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if province:
            filters.append("metadata->>'jurisdiction' = :province")
            params["province"] = province.upper()

        where_clause = " AND ".join(filters)

        query = text(f"""
            SELECT
                id,
                name,
                metadata->>'provincial_record_name' as provincial_name,
                metadata->>'matched_entity_id' as matched_id,
                metadata->>'matched_entity_name' as matched_name,
                CAST(metadata->>'match_score' AS float) as match_score,
                metadata->>'match_method' as match_method,
                metadata->>'jurisdiction' as jurisdiction
            FROM entities
            WHERE {where_clause}
            ORDER BY CAST(metadata->>'match_score' AS float) DESC
            LIMIT :limit OFFSET :offset
        """)

        result = await db.execute(query, params)
        items = result.fetchall()

        # Get total count
        count_query = text(f"""
            SELECT COUNT(*) as total
            FROM entities
            WHERE {where_clause}
        """)
        count_result = await db.execute(count_query, params)
        total = count_result.fetchone().total

        return {
            "items": [
                {
                    "entity_id": str(item.id),
                    "provincial_record_name": item.provincial_name or item.name,
                    "matched_entity_id": item.matched_id,
                    "matched_entity_name": item.matched_name,
                    "match_score": item.match_score or 0.0,
                    "match_method": item.match_method or "unknown",
                    "jurisdiction": item.jurisdiction,
                }
                for item in items
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@router.post("/provincial/batch")
async def trigger_batch_provincial_ingestion(
    background_tasks: BackgroundTasks,
    provinces: list[str] | None = None,
    request: ProvincialIngestionRequest | None = None,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Trigger batch ingestion for multiple provinces.

    Starts ingestion for all specified provinces (or all bulk-data
    provinces if not specified).

    Note: Only provinces with bulk data (QC) are included by default.
    Targeted provinces require CSV files and cannot be batched.

    Args:
        provinces: List of province codes (default: all bulk-data provinces)
        request: Shared ingestion configuration
    """
    if provinces is None:
        provinces = BULK_DATA_PROVINCES

    if request is None:
        request = ProvincialIngestionRequest()

    # Validate provinces
    invalid = [p for p in provinces if p.upper() not in BULK_DATA_PROVINCES]
    if invalid:
        raise ValidationError(
            f"Batch ingestion only supports bulk-data provinces. "
            f"Invalid: {invalid}. Valid: {BULK_DATA_PROVINCES}"
        )

    # Create run records and start tasks
    runs = []
    for province in provinces:
        province = province.upper()
        run_id = uuid4()
        source_name = f"{province.lower()}-corps"

        async with get_db_session() as db:
            insert_query = text("""
                INSERT INTO ingestion_runs (id, source, started_at, status)
                VALUES (:id, :source, :started_at, :status)
            """)

            await db.execute(
                insert_query,
                {
                    "id": run_id,
                    "source": source_name,
                    "started_at": datetime.utcnow(),
                    "status": "running",
                },
            )
            await db.commit()

        background_tasks.add_task(
            _run_provincial_ingestion_task, province, run_id, request
        )

        runs.append({
            "run_id": str(run_id),
            "province": province,
            "source": source_name,
            "status": "running",
            "status_url": f"/api/v1/ingestion/runs/{run_id}",
        })

    return {
        "message": f"Batch ingestion started for {len(runs)} provinces",
        "runs": runs,
    }


# =========================
# Quick Corporation Lookup & Ingest
# =========================


class QuickIngestRequest(BaseModel):
    """Request for quick corporation ingestion."""

    name: str
    jurisdiction: str | None = None  # CA, US, or province code like ON, BC
    identifiers: dict[str, str] | None = None  # bn, ein, corp_number, etc.
    discover_executives: bool = False  # Also search LinkedIn for executives/board


class QuickIngestResponse(BaseModel):
    """Response for quick corporation ingestion."""

    found: bool
    entity_id: str | None = None
    entity_name: str | None = None
    source: str | None = None
    sources_searched: list[str] = []
    external_matches: list[dict[str, Any]] = []
    message: str
    # LinkedIn executive discovery
    linkedin_available: bool = False
    linkedin_company_url: str | None = None
    executives_hint: str | None = None


@router.post("/quick-ingest")
async def quick_ingest_corporation(
    request: QuickIngestRequest,
    background_tasks: BackgroundTasks,
    user: OptionalUser = None,
) -> QuickIngestResponse:
    """Quickly lookup and ingest a single corporation.

    This endpoint:
    1. Searches the local graph for existing entity
    2. If not found, searches external sources (ISED, OpenCorporates, provincial registries)
    3. If found externally, creates the entity in the graph
    4. Returns the entity details or external matches for user selection

    Args:
        request: Corporation name and optional jurisdiction/identifiers

    Returns:
        QuickIngestResponse with entity details or external matches
    """
    from ..db import get_neo4j_driver
    from ..ingestion.search import search_all_sources

    sources_searched = []
    external_matches = []

    # 1. Check if entity already exists in graph
    driver = await get_neo4j_driver()
    async with driver.session() as session:
        # Search by name (fuzzy)
        result = await session.run(
            """
            MATCH (o:Organization)
            WHERE toLower(o.name) CONTAINS toLower($name)
               OR any(alias IN coalesce(o.aliases, []) WHERE toLower(alias) CONTAINS toLower($name))
            RETURN o.id as id, o.name as name, o.jurisdiction as jurisdiction,
                   o.bn as bn, o.ein as ein, labels(o) as labels
            ORDER BY 
                CASE WHEN toLower(o.name) = toLower($name) THEN 0 ELSE 1 END,
                size(o.name)
            LIMIT 5
            """,
            {"name": request.name}
        )
        records = [r async for r in result]

        if records:
            # Found existing entity
            best_match = records[0]
            return QuickIngestResponse(
                found=True,
                entity_id=best_match["id"],
                entity_name=best_match["name"],
                source="graph",
                sources_searched=["graph"],
                external_matches=[],
                message=f"Found existing entity: {best_match['name']}",
            )

    sources_searched.append("graph")

    # 2. Search external sources
    # Determine which sources to search based on jurisdiction
    source_list = None
    if request.jurisdiction:
        jurisdiction = request.jurisdiction.upper()
        if jurisdiction == "US":
            source_list = ["sec_edgar", "irs990", "opencorporates"]
        elif jurisdiction == "CA":
            source_list = ["canada_corps", "cra", "opencorporates"]
        elif len(jurisdiction) == 2:
            # Province code - use provincial search
            source_list = ["canada_corps", "opencorporates"]

    try:
        search_result = await search_all_sources(
            query=request.name,
            sources=source_list,
            limit=10,
        )
        sources_searched.extend(search_result.sources_searched)

        if search_result.results:
            for r in search_result.results[:5]:
                external_matches.append({
                    "name": r.name,
                    "source": r.source,
                    "identifiers": r.identifiers,
                    "jurisdiction": r.jurisdiction,
                    "status": r.status,
                    "address": r.address,
                })
    except Exception as e:
        # Log but continue
        pass

    # 3. If we have external matches, offer to ingest the best one
    if external_matches:
        best = external_matches[0]

        # Create entity in graph from best match
        async with driver.session() as session:
            entity_id = str(uuid4())
            props = {
                "id": entity_id,
                "name": best["name"],
                "entity_type": "Organization",
                "jurisdiction": best.get("jurisdiction"),
                "source": best["source"],
                "ingested_at": datetime.utcnow().isoformat(),
            }

            # Add identifiers
            if best.get("identifiers"):
                for id_type, id_value in best["identifiers"].items():
                    if id_value:
                        props[id_type] = id_value

            # Create node
            await session.run(
                """
                CREATE (o:Organization $props)
                RETURN o.id as id
                """,
                {"props": props}
            )

            # Generate LinkedIn hint for executive discovery
            linkedin_hint = None
            linkedin_url = None
            if request.discover_executives:
                # Create a LinkedIn company search URL hint
                company_slug = best["name"].lower().replace(" ", "-").replace(",", "").replace(".", "")[:50]
                linkedin_url = f"https://www.linkedin.com/company/{company_slug}"
                linkedin_hint = (
                    f"To discover executives, use: "
                    f"POST /api/v1/ingestion/linkedin with company_name='{best['name']}'"
                )

            return QuickIngestResponse(
                found=True,
                entity_id=entity_id,
                entity_name=best["name"],
                source=best["source"],
                sources_searched=sources_searched,
                external_matches=external_matches,
                message=f"Ingested from {best['source']}: {best['name']}",
                linkedin_available=True,
                linkedin_company_url=linkedin_url,
                executives_hint=linkedin_hint,
            )

    # 4. Not found anywhere
    return QuickIngestResponse(
        found=False,
        entity_id=None,
        entity_name=None,
        source=None,
        sources_searched=sources_searched,
        external_matches=external_matches,
        message=f"No matches found for '{request.name}'. Try different search terms or check spelling.",
    )


# =============================================================================
# Political Ad Funding Endpoints (007)
# =============================================================================


class ContributorItem(BaseModel):
    """A contributor to a third-party advertiser."""

    name: str
    contributor_class: str | None = None
    amount: float | None = None
    city: str | None = None
    province: str | None = None
    entity_id: str | None = None


class ContributorListResponse(BaseModel):
    """Response for third-party contributor list."""

    advertiser_name: str | None = None
    jurisdiction: str
    election_id: str | None = None
    total_contributors: int = 0
    contributors: list[ContributorItem] = []


@router.get(
    "/elections-third-party/{jurisdiction}/contributors",
    response_model=ContributorListResponse,
    tags=["Political Funding"],
    summary="Get Third-Party Contributors",
)
async def get_third_party_contributors(
    jurisdiction: str,
    advertiser_name: str | None = None,
    advertiser_id: str | None = None,
    election_id: str | None = None,
    min_amount: float | None = None,
    contributor_class: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """Get contributor list for a specific third-party advertiser.

    Returns contributors extracted from Elections Canada/Provincial
    financial returns (EC20228 form).
    """
    from ..db import get_neo4j_session

    try:
        async with get_neo4j_session() as session:
            # Build query
            where_clauses = ["tp.is_election_third_party = true"]
            params: dict[str, Any] = {"limit": limit, "offset": offset}

            if advertiser_name:
                where_clauses.append("toLower(tp.name) CONTAINS toLower($advertiser_name)")
                params["advertiser_name"] = advertiser_name

            if election_id:
                where_clauses.append("r.election_id = $election_id")
                params["election_id"] = election_id

            if min_amount:
                where_clauses.append("r.amount >= $min_amount")
                params["min_amount"] = min_amount

            if contributor_class:
                where_clauses.append("r.contributor_class = $contributor_class")
                params["contributor_class"] = contributor_class

            where_str = " AND ".join(where_clauses)

            query = f"""
                MATCH (c)-[r:CONTRIBUTED_TO]->(tp:Organization)
                WHERE {where_str}
                RETURN c.name AS name, c.id AS entity_id,
                       r.amount AS amount, r.contributor_class AS contributor_class,
                       r.election_id AS election_id,
                       c.city AS city, c.province AS province,
                       tp.name AS advertiser_name
                ORDER BY r.amount DESC
                SKIP $offset LIMIT $limit
            """

            result = await session.run(query, **params)
            records = await result.data()

            contributors = [
                ContributorItem(
                    name=r["name"],
                    contributor_class=r.get("contributor_class"),
                    amount=r.get("amount"),
                    city=r.get("city"),
                    province=r.get("province"),
                    entity_id=r.get("entity_id"),
                )
                for r in records
            ]

            tp_name = records[0]["advertiser_name"] if records else advertiser_name

            return ContributorListResponse(
                advertiser_name=tp_name,
                jurisdiction=jurisdiction,
                election_id=election_id,
                total_contributors=len(contributors),
                contributors=contributors,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query contributors: {e}")


class BeneficialOwnerItem(BaseModel):
    """A beneficial owner (ISC) record."""

    full_name: str
    date_became_isc: str | None = None
    date_ceased_isc: str | None = None
    control_description: str | None = None
    service_address: str | None = None
    entity_id: str | None = None


class BeneficialOwnershipResponse(BaseModel):
    """Response for beneficial ownership query."""

    corporation_number: str
    corporation_name: str | None = None
    individuals_with_significant_control: list[BeneficialOwnerItem] = []


@router.get(
    "/beneficial-ownership/{corporation_number}",
    response_model=BeneficialOwnershipResponse,
    tags=["Political Funding"],
    summary="Get Beneficial Owners",
)
async def get_beneficial_owners(corporation_number: str):
    """Query beneficial ownership data for a corporation.

    Returns ISC (Individuals with Significant Control) data
    from the federal beneficial ownership registry.
    """
    from ..db import get_neo4j_session

    try:
        async with get_neo4j_session() as session:
            result = await session.run(
                """
                MATCH (p:Person)-[r:BENEFICIAL_OWNER_OF]->(o:Organization {canada_corp_num: $corp_num})
                RETURN p.name AS full_name, p.id AS entity_id,
                       r.control_description AS control_description,
                       r.date_from AS date_from, r.date_to AS date_to,
                       o.name AS corp_name
                ORDER BY p.name
                """,
                corp_num=corporation_number,
            )
            records = await result.data()

            corp_name = records[0]["corp_name"] if records else None

            owners = [
                BeneficialOwnerItem(
                    full_name=r["full_name"],
                    date_became_isc=r.get("date_from"),
                    date_ceased_isc=r.get("date_to"),
                    control_description=r.get("control_description"),
                    entity_id=r.get("entity_id"),
                )
                for r in records
            ]

            return BeneficialOwnershipResponse(
                corporation_number=corporation_number,
                corporation_name=corp_name,
                individuals_with_significant_control=owners,
            )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to query beneficial ownership: {e}"
        )
