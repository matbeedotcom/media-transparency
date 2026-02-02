"""FastAPI application entry point for MITDS.

Media Influence Topology & Detection System REST API.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from mitds.config import get_settings
from mitds.db import close_all_connections
from mitds.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger(__name__)


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
