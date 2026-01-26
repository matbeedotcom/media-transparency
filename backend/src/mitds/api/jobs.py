"""Job status API endpoints for MITDS.

Provides endpoints for tracking async job progress and retrieving results.
"""

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from . import NotFoundError
from .auth import OptionalUser, CurrentUser
from ..db import get_db_session
from ..logging import get_context_logger

logger = get_context_logger(__name__)

router = APIRouter(prefix="/jobs")


# =========================
# Response Models
# =========================


class JobStatus(BaseModel):
    """Job status response."""

    job_id: str
    job_type: str
    status: str  # pending, running, completed, failed
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    progress: int | None = Field(None, ge=0, le=100)
    metadata: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class JobResult(BaseModel):
    """Job result response."""

    job_id: str
    job_type: str
    status: str
    created_at: datetime
    completed_at: datetime | None
    result: dict[str, Any] | None = None
    error: str | None = None


class JobList(BaseModel):
    """List of jobs."""

    jobs: list[JobStatus]
    total: int
    limit: int
    offset: int


# =========================
# Get Job Status
# =========================


@router.get("/{job_id}")
async def get_job_status(
    job_id: UUID,
    user: OptionalUser = None,
) -> JobStatus:
    """Get status of an async job.

    Args:
        job_id: The job ID to check

    Returns:
        Current job status and metadata
    """
    async with get_db_session() as db:
        query = text("""
            SELECT
                id,
                job_type,
                status,
                created_at,
                started_at,
                completed_at,
                progress,
                metadata,
                error
            FROM jobs
            WHERE id = :job_id
        """)

        result = await db.execute(query, {"job_id": str(job_id)})
        job = result.fetchone()

        if not job:
            raise NotFoundError("Job", job_id)

        return JobStatus(
            job_id=str(job.id),
            job_type=job.job_type,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            progress=job.progress,
            metadata=job.metadata or {},
            error=job.error,
        )


# =========================
# Get Job Result
# =========================


@router.get("/{job_id}/result")
async def get_job_result(
    job_id: UUID,
    user: OptionalUser = None,
) -> JobResult:
    """Get result of a completed job.

    Args:
        job_id: The job ID to get results for

    Returns:
        Job result data if completed
    """
    async with get_db_session() as db:
        query = text("""
            SELECT
                id,
                job_type,
                status,
                created_at,
                completed_at,
                result,
                error
            FROM jobs
            WHERE id = :job_id
        """)

        result = await db.execute(query, {"job_id": str(job_id)})
        job = result.fetchone()

        if not job:
            raise NotFoundError("Job", job_id)

        # Parse result if it's a JSON string
        result_data = None
        if job.result:
            if isinstance(job.result, str):
                try:
                    result_data = json.loads(job.result)
                except json.JSONDecodeError:
                    result_data = {"raw": job.result}
            else:
                result_data = job.result

        return JobResult(
            job_id=str(job.id),
            job_type=job.job_type,
            status=job.status,
            created_at=job.created_at,
            completed_at=job.completed_at,
            result=result_data,
            error=job.error,
        )


# =========================
# List Jobs
# =========================


@router.get("/")
async def list_jobs(
    job_type: str | None = Query(None, description="Filter by job type"),
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: CurrentUser = None,
) -> JobList:
    """List jobs with optional filtering.

    Args:
        job_type: Filter by job type
        status: Filter by status (pending, running, completed, failed)
        limit: Maximum results
        offset: Pagination offset

    Returns:
        List of jobs matching filters
    """
    async with get_db_session() as db:
        # Build filters
        filters = []
        params = {"limit": limit, "offset": offset}

        if job_type:
            filters.append("job_type = :job_type")
            params["job_type"] = job_type

        if status:
            filters.append("status = :status")
            params["status"] = status

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        # Get jobs
        query = text(f"""
            SELECT
                id,
                job_type,
                status,
                created_at,
                started_at,
                completed_at,
                progress,
                metadata,
                error
            FROM jobs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """)

        result = await db.execute(query, params)
        jobs = result.fetchall()

        # Get total count
        count_query = text(f"""
            SELECT COUNT(*) as total FROM jobs {where_clause}
        """)
        count_result = await db.execute(count_query, params)
        total = count_result.scalar() or 0

        return JobList(
            jobs=[
                JobStatus(
                    job_id=str(job.id),
                    job_type=job.job_type,
                    status=job.status,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    progress=job.progress,
                    metadata=job.metadata or {},
                    error=job.error,
                )
                for job in jobs
            ],
            total=total,
            limit=limit,
            offset=offset,
        )


# =========================
# Cancel Job
# =========================


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: UUID,
    user: CurrentUser = None,
) -> JobStatus:
    """Cancel a pending or running job.

    Args:
        job_id: The job ID to cancel

    Returns:
        Updated job status
    """
    async with get_db_session() as db:
        # Check job exists and is cancellable
        check_query = text("""
            SELECT id, status FROM jobs WHERE id = :job_id
        """)
        check_result = await db.execute(check_query, {"job_id": str(job_id)})
        job = check_result.fetchone()

        if not job:
            raise NotFoundError("Job", job_id)

        if job.status not in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel job with status '{job.status}'"
            )

        # Update status to cancelled
        update_query = text("""
            UPDATE jobs
            SET status = 'cancelled',
                completed_at = :completed_at,
                error = 'Cancelled by user'
            WHERE id = :job_id
            RETURNING id, job_type, status, created_at, started_at, completed_at, progress, metadata, error
        """)

        result = await db.execute(
            update_query,
            {
                "job_id": str(job_id),
                "completed_at": datetime.utcnow(),
            },
        )
        await db.commit()

        updated = result.fetchone()

        return JobStatus(
            job_id=str(updated.id),
            job_type=updated.job_type,
            status=updated.status,
            created_at=updated.created_at,
            started_at=updated.started_at,
            completed_at=updated.completed_at,
            progress=updated.progress,
            metadata=updated.metadata or {},
            error=updated.error,
        )
