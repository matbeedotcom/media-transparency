"""Audit logging for analyst queries (FR-030).

Provides comprehensive audit trail for all analyst interactions
with the system for accountability and reproducibility.
"""

import json
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from ..db import get_db_session
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class AuditAction(str, Enum):
    """Types of auditable actions."""

    # Query actions
    ENTITY_SEARCH = "entity_search"
    ENTITY_VIEW = "entity_view"
    RELATIONSHIP_QUERY = "relationship_query"
    FUNDING_CLUSTER_QUERY = "funding_cluster_query"
    PATH_QUERY = "path_query"

    # Detection actions
    TEMPORAL_DETECTION = "temporal_detection"
    INFRASTRUCTURE_DETECTION = "infrastructure_detection"
    COMPOSITE_SCORE = "composite_score"

    # Report actions
    REPORT_GENERATE = "report_generate"
    REPORT_EXPORT = "report_export"

    # Ingestion actions
    INGESTION_TRIGGER = "ingestion_trigger"

    # Validation actions
    VALIDATION_RUN = "validation_run"


class AuditEntry:
    """Represents an audit log entry."""

    def __init__(
        self,
        action: AuditAction,
        user_id: str | None = None,
        request_id: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        parameters: dict[str, Any] | None = None,
        result_summary: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ):
        self.id = uuid4()
        self.timestamp = datetime.utcnow()
        self.action = action
        self.user_id = user_id
        self.request_id = request_id
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.parameters = parameters or {}
        self.result_summary = result_summary or {}
        self.ip_address = ip_address
        self.user_agent = user_agent

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "id": str(self.id),
            "timestamp": self.timestamp.isoformat(),
            "action": self.action.value,
            "user_id": self.user_id,
            "request_id": self.request_id,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "parameters": self.parameters,
            "result_summary": self.result_summary,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }


async def log_audit_entry(entry: AuditEntry) -> None:
    """Store an audit entry in the database.

    Args:
        entry: Audit entry to store
    """
    from sqlalchemy import text

    try:
        async with get_db_session() as db:
            await db.execute(
                text("""
                    INSERT INTO audit_log (
                        id, timestamp, action, user_id, request_id,
                        resource_type, resource_id, parameters, result_summary,
                        ip_address, user_agent
                    ) VALUES (
                        :id, :timestamp, :action, :user_id, :request_id,
                        :resource_type, :resource_id, :parameters, :result_summary,
                        :ip_address, :user_agent
                    )
                """),
                {
                    "id": entry.id,
                    "timestamp": entry.timestamp,
                    "action": entry.action.value,
                    "user_id": entry.user_id,
                    "request_id": entry.request_id,
                    "resource_type": entry.resource_type,
                    "resource_id": entry.resource_id,
                    "parameters": json.dumps(entry.parameters),
                    "result_summary": json.dumps(entry.result_summary),
                    "ip_address": entry.ip_address,
                    "user_agent": entry.user_agent,
                },
            )
            await db.commit()
    except Exception as e:
        # Log but don't fail the request if audit logging fails
        logger.error(
            "audit_log_failed",
            error=str(e),
            action=entry.action.value,
        )

    # Also log to structured log for real-time monitoring
    logger.info(
        "audit_event",
        action=entry.action.value,
        user_id=entry.user_id,
        resource_type=entry.resource_type,
        resource_id=entry.resource_id,
        ip_address=entry.ip_address,
    )


def get_client_ip(request: Request) -> str:
    """Extract client IP from request headers.

    Args:
        request: FastAPI request

    Returns:
        Client IP address
    """
    # Check for forwarded headers (behind proxy)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip

    # Fall back to direct connection
    if request.client:
        return request.client.host

    return "unknown"


# Route to action mapping
ROUTE_ACTIONS: dict[tuple[str, str], AuditAction] = {
    ("GET", "/api/v1/entities"): AuditAction.ENTITY_SEARCH,
    ("GET", "/api/v1/entities/{id}"): AuditAction.ENTITY_VIEW,
    ("GET", "/api/v1/entities/{id}/relationships"): AuditAction.RELATIONSHIP_QUERY,
    ("GET", "/api/v1/relationships/funding-clusters"): AuditAction.FUNDING_CLUSTER_QUERY,
    ("GET", "/api/v1/relationships/path"): AuditAction.PATH_QUERY,
    ("POST", "/api/v1/detection/temporal-coordination"): AuditAction.TEMPORAL_DETECTION,
    ("GET", "/api/v1/relationships/shared-infrastructure"): AuditAction.INFRASTRUCTURE_DETECTION,
    ("POST", "/api/v1/detection/composite-score"): AuditAction.COMPOSITE_SCORE,
    ("POST", "/api/v1/reports"): AuditAction.REPORT_GENERATE,
    ("GET", "/api/v1/reports/{id}"): AuditAction.REPORT_EXPORT,
    ("POST", "/api/v1/ingestion/{source}/trigger"): AuditAction.INGESTION_TRIGGER,
    ("POST", "/api/v1/validation/run"): AuditAction.VALIDATION_RUN,
}


def match_route_action(method: str, path: str) -> AuditAction | None:
    """Match a request to an audit action.

    Args:
        method: HTTP method
        path: Request path

    Returns:
        Matched audit action or None
    """
    # Direct match
    if (method, path) in ROUTE_ACTIONS:
        return ROUTE_ACTIONS[(method, path)]

    # Pattern matching for parameterized routes
    for (route_method, route_pattern), action in ROUTE_ACTIONS.items():
        if method != route_method:
            continue

        # Convert route pattern to regex-like matching
        pattern_parts = route_pattern.split("/")
        path_parts = path.split("/")

        if len(pattern_parts) != len(path_parts):
            continue

        match = True
        for pattern_part, path_part in zip(pattern_parts, path_parts):
            if pattern_part.startswith("{") and pattern_part.endswith("}"):
                continue  # Parameter placeholder matches any value
            if pattern_part != path_part:
                match = False
                break

        if match:
            return action

    return None


class AuditMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic audit logging of API requests."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Process request and create audit entry.

        Args:
            request: Incoming request
            call_next: Next middleware/handler

        Returns:
            Response from handler
        """
        # Skip non-API paths and health checks
        if not request.url.path.startswith("/api/") or request.url.path.endswith("/health"):
            return await call_next(request)

        # Determine audit action
        action = match_route_action(request.method, request.url.path)
        if action is None:
            return await call_next(request)

        # Generate request ID for tracing
        request_id = str(uuid4())

        # Extract user info (from auth context if available)
        user_id = getattr(request.state, "user_id", None)

        # Call the actual handler
        response = await call_next(request)

        # Extract resource info from path
        path_parts = request.url.path.split("/")
        resource_id = None
        resource_type = None

        if "entities" in path_parts:
            resource_type = "entity"
            entities_idx = path_parts.index("entities")
            if entities_idx + 1 < len(path_parts) and not path_parts[entities_idx + 1].startswith("?"):
                resource_id = path_parts[entities_idx + 1]
        elif "reports" in path_parts:
            resource_type = "report"
            reports_idx = path_parts.index("reports")
            if reports_idx + 1 < len(path_parts):
                resource_id = path_parts[reports_idx + 1]

        # Create and store audit entry
        entry = AuditEntry(
            action=action,
            user_id=user_id,
            request_id=request_id,
            resource_type=resource_type,
            resource_id=resource_id,
            parameters=dict(request.query_params),
            result_summary={"status_code": response.status_code},
            ip_address=get_client_ip(request),
            user_agent=request.headers.get("User-Agent"),
        )

        # Log asynchronously (don't block response)
        await log_audit_entry(entry)

        return response


async def get_audit_log(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    action: AuditAction | None = None,
    user_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Query audit log entries.

    Args:
        start_date: Filter entries after this date
        end_date: Filter entries before this date
        action: Filter by action type
        user_id: Filter by user
        limit: Maximum entries to return
        offset: Number of entries to skip

    Returns:
        List of audit entries
    """
    from sqlalchemy import text

    async with get_db_session() as db:
        conditions = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if start_date:
            conditions.append("timestamp >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("timestamp <= :end_date")
            params["end_date"] = end_date

        if action:
            conditions.append("action = :action")
            params["action"] = action.value

        if user_id:
            conditions.append("user_id = :user_id")
            params["user_id"] = user_id

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT id, timestamp, action, user_id, request_id,
                   resource_type, resource_id, parameters, result_summary,
                   ip_address, user_agent
            FROM audit_log
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT :limit OFFSET :offset
        """

        result = await db.execute(text(query), params)
        rows = result.fetchall()

        return [
            {
                "id": str(row.id),
                "timestamp": row.timestamp.isoformat(),
                "action": row.action,
                "user_id": row.user_id,
                "request_id": row.request_id,
                "resource_type": row.resource_type,
                "resource_id": row.resource_id,
                "parameters": json.loads(row.parameters) if row.parameters else {},
                "result_summary": json.loads(row.result_summary) if row.result_summary else {},
                "ip_address": row.ip_address,
                "user_agent": row.user_agent,
            }
            for row in rows
        ]
