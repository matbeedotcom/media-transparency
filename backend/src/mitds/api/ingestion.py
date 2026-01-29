"""Ingestion API endpoints for MITDS."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from . import ValidationError
from .auth import CurrentUser, OptionalUser
from ..db import get_db_session

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


# =========================
# Search Companies
# =========================


@router.get("/search")
async def search_companies(
    q: str,
    sources: str | None = None,
    limit: int = 10,
    user: OptionalUser = None,
) -> dict[str, Any]:
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

    return {
        "query": result.query,
        "results": [r.model_dump() for r in result.results],
        "sources_searched": result.sources_searched,
        "sources_failed": result.sources_failed,
        "total": len(result.results),
    }


# =========================
# Get Status
# =========================


@router.get("/status")
async def get_ingestion_status(
    user: OptionalUser = None,
) -> dict[str, Any]:
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
        enabled_sources = ["irs990", "cra", "sec_edgar", "canada_corps", "sedar"]
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
    from ..ingestion import run_irs990_ingestion, run_cra_ingestion
    from ..ingestion.edgar import run_sec_edgar_ingestion
    from ..ingestion.canada_corps import run_canada_corps_ingestion
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
    valid_sources = ["irs990", "cra", "sec_edgar", "canada_corps", "sedar", "opencorporates", "meta_ads"]

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
