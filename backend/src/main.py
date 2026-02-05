"""FastAPI application entry point for MITDS.

Media Influence Topology & Detection System REST API.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from mitds.config import get_settings
from mitds.db import close_all_connections
from mitds.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Path to external OpenAPI spec
OPENAPI_SPEC_PATH = Path(__file__).parent.parent.parent / "openapi" / "openapi.yaml"


def load_external_openapi_spec() -> dict[str, Any] | None:
    """Load OpenAPI spec from external YAML file if it exists.
    
    Returns:
        Parsed OpenAPI spec dict, or None if file doesn't exist or fails to load
    """
    if not OPENAPI_SPEC_PATH.exists():
        logger.debug(f"External OpenAPI spec not found at {OPENAPI_SPEC_PATH}")
        return None
    
    try:
        import yaml
        with open(OPENAPI_SPEC_PATH, "r", encoding="utf-8") as f:
            spec = yaml.safe_load(f)
        logger.info(f"Loaded external OpenAPI spec from {OPENAPI_SPEC_PATH}")
        return spec
    except ImportError:
        logger.warning("PyYAML not installed, cannot load external OpenAPI spec")
        return None
    except Exception as e:
        logger.warning(f"Failed to load external OpenAPI spec: {e}")
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown events."""
    import asyncio

    # Startup
    settings = get_settings()
    logger.info(
        "Starting MITDS API",
        extra={
            "environment": settings.environment,
            "debug": settings.api_debug,
        },
    )

    # Cancel orphaned ingestion runs from previous server lifetime
    try:
        from mitds.db import get_db_session
        from sqlalchemy import text
        from datetime import datetime

        async with get_db_session() as db:
            result = await db.execute(
                text("""
                    UPDATE ingestion_runs
                    SET status = 'cancelled',
                        completed_at = :now,
                        errors = COALESCE(errors, CAST('[]' AS jsonb)) || CAST(:err AS jsonb)
                    WHERE status IN ('running', 'pending')
                """),
                {
                    "now": datetime.utcnow(),
                    "err": '[{"error": "Server restarted - run cancelled"}]',
                },
            )
            if result.rowcount > 0:
                logger.info(
                    f"Cancelled {result.rowcount} orphaned ingestion run(s) from previous session"
                )
    except Exception as e:
        logger.warning(f"Failed to clean up orphaned ingestion runs: {e}")

    # Warm up search cache in background (non-blocking)
    from mitds.ingestion.search import warmup_search_cache
    asyncio.create_task(warmup_search_cache())

    yield

    # Shutdown
    logger.info("Shutting down MITDS API")
    await close_all_connections()


# Create FastAPI application
settings = get_settings()

app = FastAPI(
    title="MITDS API",
    description="Media Influence Topology & Detection System REST API",
    version="1.0.0",
    docs_url="/docs" if settings.is_development else None,
    redoc_url="/redoc" if settings.is_development else None,
    openapi_url="/openapi.json" if settings.is_development else None,
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# Custom OpenAPI Schema
# =========================


def custom_openapi() -> dict[str, Any]:
    """Generate or load OpenAPI schema.
    
    If USE_EXTERNAL_OPENAPI env var is set to 'true' and the external spec exists,
    loads from the modular YAML files. Otherwise, uses FastAPI's auto-generation
    with customizations from mitds.api.docs.
    """
    if app.openapi_schema:
        return app.openapi_schema
    
    # Check if we should use external spec
    use_external = os.environ.get("USE_EXTERNAL_OPENAPI", "").lower() == "true"
    
    if use_external:
        external_spec = load_external_openapi_spec()
        if external_spec:
            app.openapi_schema = external_spec
            return app.openapi_schema
    
    # Fall back to auto-generated spec with customizations
    from mitds.api.docs import customize_openapi
    return customize_openapi(app)


# Set custom openapi function
app.openapi = custom_openapi


# =========================
# Health Check Endpoints
# =========================


@app.get("/health", tags=["Health"])
async def health_check():
    """Basic health check endpoint."""
    return {"status": "healthy", "service": "mitds-api"}


@app.get("/health/ready", tags=["Health"])
async def readiness_check():
    """Readiness check that verifies database connectivity."""
    from mitds.db import get_db_session, get_neo4j_session, get_redis

    checks = {
        "postgres": "unknown",
        "neo4j": "unknown",
        "redis": "unknown",
    }

    try:
        # Check PostgreSQL
        async with get_db_session() as session:
            await session.execute("SELECT 1")
            checks["postgres"] = "healthy"
    except Exception as e:
        checks["postgres"] = f"unhealthy: {str(e)}"

    try:
        # Check Neo4j
        async with get_neo4j_session() as session:
            await session.run("RETURN 1")
            checks["neo4j"] = "healthy"
    except Exception as e:
        checks["neo4j"] = f"unhealthy: {str(e)}"

    try:
        # Check Redis
        redis = await get_redis()
        await redis.ping()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = f"unhealthy: {str(e)}"

    # Determine overall status
    all_healthy = all(v == "healthy" for v in checks.values())
    status_code = 200 if all_healthy else 503

    return JSONResponse(
        status_code=status_code,
        content={
            "status": "ready" if all_healthy else "not_ready",
            "checks": checks,
        },
    )


@app.get("/health/live", tags=["Health"])
async def liveness_check():
    """Liveness check - just confirms the service is running."""
    return {"status": "alive"}


# =========================
# API Routers
# =========================

# Import and include routers
from mitds.api.entities import router as entities_router
from mitds.api.relationships import router as relationships_router
from mitds.api.detection import router as detection_router
from mitds.api.reports import router as reports_router
from mitds.api.ingestion import router as ingestion_router
from mitds.api.jobs import router as jobs_router
from mitds.api.validation import router as validation_router
from mitds.api.resolution import router as resolution_router
from mitds.api.settings import router as settings_router
from mitds.api.research import router as research_router
from mitds.api.cases import router as cases_router
from mitds.api.meta_oauth import router as meta_oauth_router
from mitds.api.tool_gateway import router as tool_gateway_router

app.include_router(entities_router, prefix="/api/v1", tags=["Entities"])
app.include_router(relationships_router, prefix="/api/v1", tags=["Relationships"])
app.include_router(detection_router, prefix="/api/v1", tags=["Detection"])
app.include_router(reports_router, prefix="/api/v1", tags=["Reports"])
app.include_router(ingestion_router, prefix="/api/v1", tags=["Ingestion"])
app.include_router(jobs_router, prefix="/api/v1", tags=["Jobs"])
app.include_router(validation_router, prefix="/api/v1", tags=["Validation"])
app.include_router(resolution_router, prefix="/api/v1", tags=["Resolution"])
app.include_router(settings_router, prefix="/api/v1", tags=["Settings"])
app.include_router(research_router, prefix="/api/v1", tags=["Research"])
app.include_router(cases_router, prefix="/api/v1", tags=["Cases"])
app.include_router(meta_oauth_router, prefix="/api/v1/meta", tags=["Meta OAuth"])
app.include_router(tool_gateway_router, prefix="/api/v1", tags=["Tools"])


# =========================
# Root Endpoint
# =========================


@app.get("/", tags=["Root"])
async def root():
    """API root endpoint."""
    return {
        "name": "MITDS API",
        "version": "1.0.0",
        "description": "Media Influence Topology & Detection System",
        "docs": "/docs" if settings.is_development else None,
    }
