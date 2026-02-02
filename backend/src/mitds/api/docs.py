"""API documentation configuration with OpenAPI examples.

Provides comprehensive API documentation including:
- OpenAPI schema customization
- Example requests and responses
- Authentication documentation
"""

from typing import Any

# API Documentation metadata
API_TITLE = "MITDS API"
API_VERSION = "1.0.0"
API_DESCRIPTION = """
# Media Influence Topology & Detection System (MITDS)

MITDS exposes a REST API for investigating connections between media outlets,
organizations, and their funding sources. The system detects potential coordinated
influence campaigns through analysis of funding patterns, shared infrastructure,
and temporal coordination.

## Core Capabilities

### Entity Management
- Search and retrieve organizations, persons, outlets, and sponsors
- View entity relationships and evidence chains
- Track changes over time with temporal queries

### Funding Analysis
- Discover funding paths between entities
- Detect clusters of entities sharing common funders
- Analyze funding concentration and patterns

### Detection Services
- Temporal coordination detection
- Infrastructure sharing analysis
- Composite influence scoring

### Data Ingestion
- IRS 990 filings (US nonprofits)
- CRA T3010 filings (Canadian charities)
- SEC EDGAR company filings (with Canadian jurisdiction detection)
- SEDAR+ Canadian securities filings (Early Warning Reports for 10%+ ownership)
- Canada Corporations registry

## Authentication

Most endpoints require JWT authentication. Include the token in the Authorization header:

```
Authorization: Bearer <your-token>
```

Public endpoints (health, documentation) don't require authentication.

## Rate Limits

- Default: 100 requests/minute
- Search endpoints: 30 requests/minute
- Detection endpoints: 10 requests/minute
- Authentication: 20 requests/minute

Rate limit headers are included in all responses:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Requests remaining
- `X-RateLimit-Reset`: Seconds until limit resets

## Error Handling

Errors follow a standard format:

```json
{
  "success": false,
  "error": "Human-readable error message",
  "error_code": "ERROR_CODE",
  "details": [{"code": "...", "message": "...", "field": "..."}]
}
```

Common error codes:
- `NOT_FOUND` (404): Resource not found
- `VALIDATION_ERROR` (422): Invalid request parameters
- `AUTHENTICATION_REQUIRED` (401): Missing or invalid token
- `ACCESS_DENIED` (403): Insufficient permissions
- `RATE_LIMIT_EXCEEDED` (429): Too many requests
"""

# OpenAPI tags with descriptions
API_TAGS = [
    {
        "name": "Entities",
        "description": "Entity search, retrieval, and management operations",
    },
    {
        "name": "Relationships",
        "description": "Relationship queries including funding paths and shared funders",
    },
    {
        "name": "Detection",
        "description": "Detection algorithms for coordination and influence patterns",
    },
    {
        "name": "Ingestion",
        "description": "Data ingestion from external sources",
    },
    {
        "name": "Reports",
        "description": "Generate and export investigation reports",
    },
    {
        "name": "Jobs",
        "description": "Background job management",
    },
    {
        "name": "Validation",
        "description": "Data validation framework endpoints",
    },
    {
        "name": "Auth",
        "description": "Authentication and authorization",
    },
]

# Example request/response payloads
EXAMPLES = {
    "entity_search": {
        "summary": "Search for organizations",
        "description": "Search for entities by name, alias, or identifier",
        "value": {
            "results": [
                {
                    "id": "550e8400-e29b-41d4-a716-446655440000",
                    "entity_type": "ORGANIZATION",
                    "name": "Example Foundation",
                    "confidence": 0.95,
                    "created_at": "2024-01-15T10:30:00Z",
                }
            ],
            "total": 42,
            "limit": 20,
            "offset": 0,
        },
    },
    "entity_detail": {
        "summary": "Full entity details",
        "description": "Complete entity with properties and aliases",
        "value": {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "entity_type": "ORGANIZATION",
            "name": "Example Foundation",
            "confidence": 0.95,
            "created_at": "2024-01-15T10:30:00Z",
            "updated_at": "2024-06-20T14:22:00Z",
            "aliases": ["Example Fund", "The Example Foundation"],
            "properties": {
                "ein": "12-3456789",
                "jurisdiction": "US",
                "org_type": "NONPROFIT",
                "website": "https://example.org",
            },
        },
    },
    "funding_path": {
        "summary": "Funding path result",
        "description": "Path showing funding flow from funder to recipient",
        "value": {
            "path_found": True,
            "from_entity": {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "entity_type": "ORGANIZATION",
                "name": "Major Donor Foundation",
            },
            "to_entity": {
                "id": "550e8400-e29b-41d4-a716-446655440002",
                "entity_type": "OUTLET",
                "name": "Example News Network",
            },
            "hops": 2,
            "intermediaries": [
                {
                    "id": "550e8400-e29b-41d4-a716-446655440003",
                    "entity_type": "ORGANIZATION",
                    "name": "Media Advocacy Group",
                }
            ],
            "relationships": [
                {
                    "rel_type": "FUNDED_BY",
                    "source_id": "550e8400-e29b-41d4-a716-446655440003",
                    "target_id": "550e8400-e29b-41d4-a716-446655440001",
                    "properties": {"amount": 500000, "fiscal_year": 2023},
                },
                {
                    "rel_type": "FUNDED_BY",
                    "source_id": "550e8400-e29b-41d4-a716-446655440002",
                    "target_id": "550e8400-e29b-41d4-a716-446655440003",
                    "properties": {"amount": 250000, "fiscal_year": 2023},
                },
            ],
        },
    },
    "funding_cluster": {
        "summary": "Funding cluster detection result",
        "description": "Group of entities sharing common funders",
        "value": {
            "clusters": [
                {
                    "cluster_id": "cluster-001",
                    "shared_funder": {
                        "id": "550e8400-e29b-41d4-a716-446655440010",
                        "entity_type": "ORGANIZATION",
                        "name": "Coordinating Foundation",
                    },
                    "members": [
                        {"id": "...", "entity_type": "OUTLET", "name": "News Site A"},
                        {"id": "...", "entity_type": "OUTLET", "name": "News Site B"},
                        {"id": "...", "entity_type": "OUTLET", "name": "News Site C"},
                    ],
                    "total_funding": 1500000,
                    "score": 8.5,
                    "confidence": 0.92,
                    "evidence_summary": "3 outlets funded by same funder in 2023",
                }
            ],
            "total_clusters": 1,
        },
    },
    "temporal_detection": {
        "summary": "Temporal coordination detection result",
        "description": "Detection of coordinated publishing patterns",
        "value": {
            "detection_id": "detection-temporal-001",
            "detection_type": "temporal_coordination",
            "entities_analyzed": 5,
            "coordination_score": 7.8,
            "confidence": 0.85,
            "findings": [
                {
                    "pattern": "synchronized_publishing",
                    "description": "3 outlets published similar content within 2 hours",
                    "score_contribution": 4.2,
                    "entities_involved": ["outlet-a", "outlet-b", "outlet-c"],
                    "timeframe": {
                        "start": "2024-01-15T08:00:00Z",
                        "end": "2024-01-15T10:00:00Z",
                    },
                }
            ],
        },
    },
    "infrastructure_match": {
        "summary": "Shared infrastructure detection result",
        "description": "Detection of shared technical infrastructure between domains",
        "value": {
            "matches": [
                {
                    "domain_a": "news-site-a.com",
                    "domain_b": "news-site-b.com",
                    "total_score": 6.5,
                    "confidence": 0.88,
                    "signals": [
                        {
                            "type": "same_google_analytics",
                            "value": "UA-12345678-1",
                            "weight": 3.0,
                            "description": "Both domains use the same Google Analytics ID",
                        },
                        {
                            "type": "same_hosting",
                            "value": "AS12345 (ExampleHost)",
                            "weight": 2.0,
                            "description": "Both domains hosted on same ASN",
                        },
                    ],
                    "sharing_category": "google_analytics",
                }
            ],
            "total_matches": 1,
        },
    },
    "error_not_found": {
        "summary": "Resource not found error",
        "description": "Error response when entity doesn't exist",
        "value": {
            "success": False,
            "error": "Entity not found: 550e8400-e29b-41d4-a716-446655440099",
            "error_code": "NOT_FOUND",
        },
    },
    "error_validation": {
        "summary": "Validation error",
        "description": "Error response for invalid request parameters",
        "value": {
            "success": False,
            "error": "Validation failed",
            "error_code": "VALIDATION_ERROR",
            "details": [
                {
                    "code": "INVALID_FORMAT",
                    "message": "Invalid UUID format",
                    "field": "entity_id",
                }
            ],
        },
    },
    "error_rate_limit": {
        "summary": "Rate limit exceeded",
        "description": "Error when too many requests are made",
        "value": {
            "success": False,
            "error": "Rate limit exceeded. Retry after 60 seconds.",
            "error_code": "RATE_LIMIT_EXCEEDED",
        },
    },
}


def get_openapi_schema_customizer() -> dict[str, Any]:
    """Get OpenAPI schema customizations.

    Returns:
        Dictionary with schema customizations
    """
    return {
        "info": {
            "title": API_TITLE,
            "version": API_VERSION,
            "description": API_DESCRIPTION,
            "contact": {
                "name": "MITDS Team",
                "url": "https://github.com/mitds/mitds",
            },
            "license": {
                "name": "MIT",
                "url": "https://opensource.org/licenses/MIT",
            },
        },
        "tags": API_TAGS,
        "servers": [
            {
                "url": "/",
                "description": "Current server",
            },
            {
                "url": "http://localhost:8000",
                "description": "Local development",
            },
        ],
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                    "description": "JWT token obtained from /api/v1/auth/token",
                }
            },
        },
        "security": [{"bearerAuth": []}],
    }


def customize_openapi(app) -> dict[str, Any]:
    """Customize OpenAPI schema for the application.

    Args:
        app: FastAPI application instance

    Returns:
        Customized OpenAPI schema
    """
    if app.openapi_schema:
        return app.openapi_schema

    from fastapi.openapi.utils import get_openapi

    openapi_schema = get_openapi(
        title=API_TITLE,
        version=API_VERSION,
        description=API_DESCRIPTION,
        routes=app.routes,
    )

    # Add custom info
    customizations = get_openapi_schema_customizer()
    openapi_schema.update(customizations)

    # Add security schemes
    if "components" not in openapi_schema:
        openapi_schema["components"] = {}
    openapi_schema["components"]["securitySchemes"] = customizations["components"]["securitySchemes"]

    # Add tags
    openapi_schema["tags"] = customizations["tags"]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


# =========================
# Route Documentation Decorators
# =========================


def api_docs(
    summary: str,
    description: str | None = None,
    response_description: str = "Successful Response",
    responses: dict[int, dict[str, Any]] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Create documentation for an API endpoint.

    Usage:
        @router.get("/items", **api_docs(
            summary="List all items",
            description="Returns paginated list of items",
            tags=["Items"],
        ))
        async def list_items():
            ...

    Args:
        summary: Brief endpoint description
        description: Detailed description (markdown supported)
        response_description: Description for 200 response
        responses: Additional response definitions
        tags: Tag names for grouping

    Returns:
        Dictionary of kwargs for route decorator
    """
    docs: dict[str, Any] = {
        "summary": summary,
        "response_description": response_description,
    }

    if description:
        docs["description"] = description

    if tags:
        docs["tags"] = tags

    if responses:
        docs["responses"] = responses

    return docs


# Common response definitions for reuse
COMMON_RESPONSES = {
    400: {
        "description": "Bad Request",
        "content": {
            "application/json": {
                "example": EXAMPLES["error_validation"]["value"],
            }
        },
    },
    401: {
        "description": "Unauthorized",
        "content": {
            "application/json": {
                "example": {
                    "success": False,
                    "error": "Authentication required",
                    "error_code": "AUTHENTICATION_REQUIRED",
                }
            }
        },
    },
    404: {
        "description": "Not Found",
        "content": {
            "application/json": {
                "example": EXAMPLES["error_not_found"]["value"],
            }
        },
    },
    429: {
        "description": "Rate Limit Exceeded",
        "content": {
            "application/json": {
                "example": EXAMPLES["error_rate_limit"]["value"],
            }
        },
    },
    500: {
        "description": "Internal Server Error",
        "content": {
            "application/json": {
                "example": {
                    "success": False,
                    "error": "An unexpected error occurred",
                    "error_code": "INTERNAL_ERROR",
                }
            }
        },
    },
}
