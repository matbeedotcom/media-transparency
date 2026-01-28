"""Settings API endpoints for MITDS.

Provides read-only access to system configuration and data source status.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from mitds.config import get_settings
from mitds.db import get_db_session, get_neo4j_session, get_redis
from mitds.logging import get_logger

router = APIRouter(prefix="/settings")
logger = get_logger(__name__)


# =========================
# Response Models
# =========================


class ConnectionStatus(str, Enum):
    """Connection health status."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ConnectionInfo(BaseModel):
    """Information about a service connection."""

    name: str
    status: ConnectionStatus
    host: str
    port: int | None = None
    latency_ms: float | None = None
    error: str | None = None


class DataSourceInfo(BaseModel):
    """Information about a data source."""

    id: str
    name: str
    description: str
    enabled: bool
    requires_api_key: bool
    has_api_key: bool
    api_key_env_var: str | None = None
    feature_flag: str | None = None
    last_successful_run: datetime | None = None
    records_total: int = 0


class APIConfigInfo(BaseModel):
    """API configuration information (safe to display)."""

    environment: str
    api_host: str
    api_port: int
    debug_mode: bool
    cors_origins: list[str]
    log_level: str


class SettingsResponse(BaseModel):
    """Complete settings response."""

    api: APIConfigInfo
    connections: list[ConnectionInfo]
    data_sources: list[DataSourceInfo]


class ConnectionsResponse(BaseModel):
    """Connections status response."""

    connections: list[ConnectionInfo]
    all_healthy: bool


class DataSourcesResponse(BaseModel):
    """Data sources configuration response."""

    sources: list[DataSourceInfo]
    total_enabled: int
    total_disabled: int


# =========================
# Data Source Definitions
# =========================


DATA_SOURCES = [
    {
        "id": "irs990",
        "name": "IRS 990",
        "description": "US nonprofit 990 filings from the IRS",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "cra",
        "name": "CRA Charities",
        "description": "Canadian Registered Charities data from CRA",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "sec_edgar",
        "name": "SEC EDGAR",
        "description": "US public company filings from SEC",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "canada_corps",
        "name": "Canada Corporations",
        "description": "Canadian federal corporation registry",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "elections_canada",
        "name": "Elections Canada",
        "description": "Canadian federal election data including contributions",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "littlesis",
        "name": "LittleSis",
        "description": "US political and corporate network data",
        "requires_api_key": False,
        "api_key_env_var": None,
        "feature_flag": None,
    },
    {
        "id": "meta_ads",
        "name": "Meta Ad Library",
        "description": "Political advertising data from Meta platforms",
        "requires_api_key": True,
        "api_key_env_var": "META_ACCESS_TOKEN",
        "feature_flag": "enable_meta_ads_ingestion",
    },
    {
        "id": "opencorporates",
        "name": "OpenCorporates",
        "description": "Global corporation registry data",
        "requires_api_key": True,
        "api_key_env_var": "OPENCORPORATES_API_KEY",
        "feature_flag": "enable_opencorporates_ingestion",
    },
]


# =========================
# Endpoints
# =========================


@router.get("", response_model=SettingsResponse)
async def get_all_settings() -> SettingsResponse:
    """Get all system settings and status.

    Returns API configuration, connection status, and data source information.
    Sensitive values like passwords and API keys are not exposed.
    """
    settings = get_settings()

    # Get API config
    api_config = APIConfigInfo(
        environment=settings.environment,
        api_host=settings.api_host,
        api_port=settings.api_port,
        debug_mode=settings.api_debug,
        cors_origins=settings.cors_origins_list,
        log_level=settings.log_level,
    )

    # Get connections status
    connections = await _check_connections()

    # Get data sources
    data_sources = await _get_data_sources_info()

    return SettingsResponse(
        api=api_config,
        connections=connections,
        data_sources=data_sources,
    )


@router.get("/connections", response_model=ConnectionsResponse)
async def get_connections_status() -> ConnectionsResponse:
    """Get status of all service connections.

    Checks connectivity to PostgreSQL, Neo4j, Redis, and S3.
    """
    connections = await _check_connections()
    all_healthy = all(c.status == ConnectionStatus.HEALTHY for c in connections)

    return ConnectionsResponse(
        connections=connections,
        all_healthy=all_healthy,
    )


@router.get("/sources", response_model=DataSourcesResponse)
async def get_data_sources() -> DataSourcesResponse:
    """Get data source configuration and status.

    Returns information about all available data sources including
    whether they are enabled and if required API keys are configured.
    """
    sources = await _get_data_sources_info()
    total_enabled = sum(1 for s in sources if s.enabled)
    total_disabled = len(sources) - total_enabled

    return DataSourcesResponse(
        sources=sources,
        total_enabled=total_enabled,
        total_disabled=total_disabled,
    )


# =========================
# Helper Functions
# =========================


async def _check_connections() -> list[ConnectionInfo]:
    """Check all service connections."""
    settings = get_settings()
    connections = []

    # Check PostgreSQL
    pg_conn = await _check_postgres(settings)
    connections.append(pg_conn)

    # Check Neo4j
    neo4j_conn = await _check_neo4j(settings)
    connections.append(neo4j_conn)

    # Check Redis
    redis_conn = await _check_redis(settings)
    connections.append(redis_conn)

    # Check S3/MinIO
    s3_conn = await _check_s3(settings)
    connections.append(s3_conn)

    return connections


async def _check_postgres(settings: Any) -> ConnectionInfo:
    """Check PostgreSQL connection."""
    start = datetime.utcnow()
    try:
        async with get_db_session() as session:
            await session.execute("SELECT 1")
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return ConnectionInfo(
            name="PostgreSQL",
            status=ConnectionStatus.HEALTHY,
            host=settings.postgres_host,
            port=settings.postgres_port,
            latency_ms=round(latency, 2),
        )
    except Exception as e:
        return ConnectionInfo(
            name="PostgreSQL",
            status=ConnectionStatus.UNHEALTHY,
            host=settings.postgres_host,
            port=settings.postgres_port,
            error=str(e),
        )


async def _check_neo4j(settings: Any) -> ConnectionInfo:
    """Check Neo4j connection."""
    # Parse host from URI
    uri = settings.neo4j_uri
    host = uri.replace("bolt://", "").replace("neo4j://", "").split(":")[0]
    port = 7687
    if ":" in uri.replace("bolt://", "").replace("neo4j://", ""):
        try:
            port = int(uri.split(":")[-1])
        except ValueError:
            pass

    start = datetime.utcnow()
    try:
        async with get_neo4j_session() as session:
            await session.run("RETURN 1")
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return ConnectionInfo(
            name="Neo4j",
            status=ConnectionStatus.HEALTHY,
            host=host,
            port=port,
            latency_ms=round(latency, 2),
        )
    except Exception as e:
        return ConnectionInfo(
            name="Neo4j",
            status=ConnectionStatus.UNHEALTHY,
            host=host,
            port=port,
            error=str(e),
        )


async def _check_redis(settings: Any) -> ConnectionInfo:
    """Check Redis connection."""
    # Parse host from URL
    url = settings.redis_url
    host = url.replace("redis://", "").split(":")[0]
    port = 6379
    if ":" in url.replace("redis://", ""):
        try:
            port_str = url.replace("redis://", "").split(":")[1].split("/")[0]
            port = int(port_str)
        except (ValueError, IndexError):
            pass

    start = datetime.utcnow()
    try:
        redis = await get_redis()
        await redis.ping()
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return ConnectionInfo(
            name="Redis",
            status=ConnectionStatus.HEALTHY,
            host=host,
            port=port,
            latency_ms=round(latency, 2),
        )
    except Exception as e:
        return ConnectionInfo(
            name="Redis",
            status=ConnectionStatus.UNHEALTHY,
            host=host,
            port=port,
            error=str(e),
        )


async def _check_s3(settings: Any) -> ConnectionInfo:
    """Check S3/MinIO connection."""
    # Parse host from endpoint
    endpoint = settings.s3_endpoint
    host = endpoint.replace("http://", "").replace("https://", "").split(":")[0]
    port = None
    if ":" in endpoint.replace("http://", "").replace("https://", ""):
        try:
            port = int(endpoint.split(":")[-1])
        except ValueError:
            pass

    try:
        # Try to check S3 connection via boto3
        import aioboto3

        session = aioboto3.Session()
        start = datetime.utcnow()
        async with session.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
        ) as s3:
            await s3.head_bucket(Bucket=settings.s3_bucket)
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return ConnectionInfo(
            name="S3/MinIO",
            status=ConnectionStatus.HEALTHY,
            host=host,
            port=port,
            latency_ms=round(latency, 2),
        )
    except Exception as e:
        return ConnectionInfo(
            name="S3/MinIO",
            status=ConnectionStatus.UNHEALTHY,
            host=host,
            port=port,
            error=str(e),
        )


async def _get_data_sources_info() -> list[DataSourceInfo]:
    """Get information about all data sources."""
    settings = get_settings()
    sources = []

    for source_def in DATA_SOURCES:
        source_id = source_def["id"]

        # Check if enabled
        enabled = True
        if source_def.get("feature_flag"):
            enabled = getattr(settings, source_def["feature_flag"], False)

        # Check if API key is configured
        has_api_key = False
        if source_def.get("api_key_env_var"):
            api_key_attr = source_def["api_key_env_var"].lower()
            api_key_value = getattr(settings, api_key_attr, "")
            has_api_key = bool(api_key_value)
            # If requires API key but doesn't have one, disable
            if source_def["requires_api_key"] and not has_api_key:
                enabled = False

        # Get last successful run and record count from database
        last_run = None
        records_total = 0
        try:
            async with get_db_session() as db:
                from sqlalchemy import text

                result = await db.execute(
                    text("""
                        SELECT completed_at, records_created + records_updated as total
                        FROM ingestion_runs
                        WHERE source = :source AND status = 'completed'
                        ORDER BY completed_at DESC
                        LIMIT 1
                    """),
                    {"source": source_id},
                )
                row = result.fetchone()
                if row:
                    last_run = row[0]
                    records_total = row[1] or 0
        except Exception as e:
            logger.warning(f"Failed to get ingestion stats for {source_id}: {e}")

        sources.append(
            DataSourceInfo(
                id=source_id,
                name=source_def["name"],
                description=source_def["description"],
                enabled=enabled,
                requires_api_key=source_def["requires_api_key"],
                has_api_key=has_api_key,
                api_key_env_var=source_def.get("api_key_env_var"),
                feature_flag=source_def.get("feature_flag"),
                last_successful_run=last_run,
                records_total=records_total,
            )
        )

    return sources
