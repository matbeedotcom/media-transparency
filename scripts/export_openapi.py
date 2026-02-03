#!/usr/bin/env python3
"""Export and split FastAPI's auto-generated OpenAPI spec into modular YAML files.

This script:
1. Imports the FastAPI app and extracts the OpenAPI schema
2. Splits paths by tag into separate files
3. Splits schemas by category into separate files
4. Creates a master openapi.yaml with $ref references
5. Generates reusable parameters, responses, and security components

Usage:
    python scripts/export_openapi.py [--output-dir backend/openapi]
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

# Add backend/src to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_SRC = PROJECT_ROOT / "backend" / "src"
sys.path.insert(0, str(BACKEND_SRC))


def get_openapi_schema() -> dict[str, Any]:
    """Get OpenAPI schema from FastAPI app."""
    # Import here to avoid import errors if dependencies missing
    from main import app
    
    # Force generation of OpenAPI schema
    return app.openapi()


def categorize_schema(name: str) -> str:
    """Categorize a schema by its name prefix/pattern."""
    # Entity-related
    if any(name.startswith(p) for p in [
        "Entity", "Organization", "Person", "Outlet", "Sponsor", 
        "Domain", "Vendor", "PlatformAccount", "OrgType", "OrgStatus",
        "MediaType", "VendorType", "PlatformType"
    ]):
        return "entities"
    
    # Relationship-related
    if any(name.startswith(p) for p in [
        "Relationship", "RelationType", "FundedBy", "DirectorOf", 
        "EmployedBy", "SharedInfra", "Owns", "Role", "Path"
    ]):
        return "relationships"
    
    # Detection-related
    if any(name.startswith(p) for p in [
        "Detection", "Temporal", "Funding", "Infrastructure", 
        "Composite", "Finding", "Signal", "Burst", "LeadLag",
        "Synchronization", "Coordination"
    ]):
        return "detection"
    
    # Case-related
    if any(name.startswith(p) for p in [
        "Case", "Evidence", "ExtractedLead", "Match", "EntryPoint",
        "Processing", "Report", "Ranked", "AdMetadata", "CrossBorder",
        "Unknown", "Citation", "Similarity"
    ]):
        return "cases"
    
    # Research-related
    if any(name.startswith(p) for p in [
        "Research", "Session", "Lead", "Queue", "Graph"
    ]):
        return "research"
    
    # Ingestion-related
    if any(name.startswith(p) for p in [
        "Ingestion", "IRS990", "CRA", "EDGAR", "SEDAR", "Canada",
        "Provincial", "LinkedIn", "LittleSis", "Meta", "OpenCorp",
        "Elections", "Lobbying", "Search", "Autocomplete", "QuickIngest"
    ]):
        return "ingestion"
    
    # Validation-related
    if any(name.startswith(p) for p in [
        "Validation", "Metrics", "Dashboard", "Golden", "Synthetic",
        "Pattern", "Quality"
    ]):
        return "validation"
    
    # Resolution-related
    if any(name.startswith(p) for p in [
        "Resolution", "Reconciliation", "Candidate", "Matcher"
    ]):
        return "resolution"
    
    # Jobs-related
    if any(name.startswith(p) for p in ["Job"]):
        return "jobs"
    
    # Settings-related
    if any(name.startswith(p) for p in [
        "Settings", "Connection", "DataSource", "APIConfig"
    ]):
        return "settings"
    
    # Meta OAuth-related
    if any(name.startswith(p) for p in ["MetaAuth", "MetaDisconnect"]):
        return "meta-oauth"
    
    # Common/shared schemas (pagination, errors, etc.)
    if any(name.startswith(p) for p in [
        "Paginated", "Cursor", "API", "Error", "HTTP", "Validation"
    ]):
        return "common"
    
    # Enums (catch remaining enums)
    if name.endswith("Type") or name.endswith("Status") or name.endswith("State"):
        return "enums"
    
    # Default to common
    return "common"


def tag_to_filename(tag: str) -> str:
    """Convert a tag name to a filename."""
    # Handle special cases
    mapping = {
        "Meta OAuth": "meta-oauth",
        "Health": "health",
        "Root": "health",  # Merge root into health
    }
    if tag in mapping:
        return mapping[tag]
    return tag.lower().replace(" ", "-")


def extract_common_parameters(openapi: dict[str, Any]) -> dict[str, Any]:
    """Extract common query/path parameters from paths."""
    parameters = {}
    
    # Common pagination parameters
    parameters["LimitParam"] = {
        "name": "limit",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        "description": "Maximum number of results to return"
    }
    
    parameters["OffsetParam"] = {
        "name": "offset",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "default": 0, "minimum": 0},
        "description": "Number of results to skip"
    }
    
    parameters["QueryParam"] = {
        "name": "q",
        "in": "query",
        "required": False,
        "schema": {"type": "string"},
        "description": "Search query string"
    }
    
    parameters["EntityIdParam"] = {
        "name": "entity_id",
        "in": "path",
        "required": True,
        "schema": {"type": "string", "format": "uuid"},
        "description": "Unique entity identifier"
    }
    
    parameters["JobIdParam"] = {
        "name": "job_id",
        "in": "path",
        "required": True,
        "schema": {"type": "string", "format": "uuid"},
        "description": "Background job identifier"
    }
    
    parameters["CaseIdParam"] = {
        "name": "case_id",
        "in": "path",
        "required": True,
        "schema": {"type": "string", "format": "uuid"},
        "description": "Case identifier"
    }
    
    parameters["SessionIdParam"] = {
        "name": "session_id",
        "in": "path",
        "required": True,
        "schema": {"type": "string", "format": "uuid"},
        "description": "Research session identifier"
    }
    
    parameters["EntityTypeParam"] = {
        "name": "type",
        "in": "query",
        "required": False,
        "schema": {
            "type": "string",
            "enum": ["PERSON", "ORGANIZATION", "OUTLET", "DOMAIN", "PLATFORM_ACCOUNT", "SPONSOR", "VENDOR"]
        },
        "description": "Filter by entity type"
    }
    
    parameters["JurisdictionParam"] = {
        "name": "jurisdiction",
        "in": "query",
        "required": False,
        "schema": {"type": "string"},
        "description": "Filter by jurisdiction (e.g., 'US', 'CA', 'CA-ON')"
    }
    
    parameters["StatusParam"] = {
        "name": "status",
        "in": "query",
        "required": False,
        "schema": {"type": "string"},
        "description": "Filter by status"
    }
    
    return parameters


def extract_common_responses() -> dict[str, Any]:
    """Create common response definitions."""
    return {
        "BadRequest": {
            "description": "Bad Request - Invalid parameters",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "Validation failed",
                        "error_code": "VALIDATION_ERROR",
                        "details": [{"code": "INVALID_FORMAT", "message": "Invalid UUID format", "field": "entity_id"}]
                    }
                }
            }
        },
        "Unauthorized": {
            "description": "Unauthorized - Missing or invalid token",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "Authentication required",
                        "error_code": "AUTHENTICATION_REQUIRED"
                    }
                }
            }
        },
        "Forbidden": {
            "description": "Forbidden - Insufficient permissions",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "Access denied",
                        "error_code": "ACCESS_DENIED"
                    }
                }
            }
        },
        "NotFound": {
            "description": "Not Found - Resource does not exist",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "Entity not found",
                        "error_code": "NOT_FOUND"
                    }
                }
            }
        },
        "RateLimited": {
            "description": "Too Many Requests - Rate limit exceeded",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "Rate limit exceeded. Retry after 60 seconds.",
                        "error_code": "RATE_LIMIT_EXCEEDED"
                    }
                }
            }
        },
        "InternalError": {
            "description": "Internal Server Error",
            "content": {
                "application/json": {
                    "schema": {"$ref": "./schemas/common.yaml#/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": "An unexpected error occurred",
                        "error_code": "INTERNAL_ERROR"
                    }
                }
            }
        }
    }


def create_security_schemes() -> dict[str, Any]:
    """Create security scheme definitions."""
    return {
        "bearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "JWT token for authentication. Obtain from /api/v1/auth/token"
        },
        "apiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API key for service-to-service authentication"
        }
    }


def create_master_spec(openapi: dict[str, Any], tags: list[str]) -> dict[str, Any]:
    """Create the master openapi.yaml content."""
    # Tag descriptions
    tag_descriptions = {
        "Entities": "Entity search, retrieval, and management operations. Access organizations, persons, outlets, sponsors, and other entities in the knowledge graph.",
        "Relationships": "Relationship queries including funding paths, board interlocks, shared funders, and infrastructure connections between entities.",
        "Detection": "Detection algorithms for identifying coordination patterns, funding clusters, and shared infrastructure indicative of influence campaigns.",
        "Ingestion": "Data ingestion from external sources including IRS 990, CRA, SEC EDGAR, SEDAR, provincial registries, Meta Ads, LinkedIn, and more.",
        "Reports": "Generate and export investigation reports with customizable templates and multiple output formats.",
        "Jobs": "Background job management for long-running operations like detection analysis and report generation.",
        "Validation": "Data validation framework for testing detection algorithms against golden datasets and synthetic test cases.",
        "Resolution": "Entity resolution and reconciliation for merging duplicate entities and resolving cross-border matches.",
        "Settings": "System settings, connection status, and data source configuration.",
        "Research": "Research session management for automated entity discovery and relationship exploration.",
        "Cases": "Case management for autonomous investigations starting from entry points like Meta Ads, corporations, or URLs.",
        "Meta OAuth": "Meta (Facebook) OAuth authentication for accessing the Meta Ad Library API.",
        "Health": "Health check endpoints for monitoring service status and readiness."
    }
    
    master = {
        "openapi": "3.1.0",
        "info": {
            "title": "MITDS API",
            "version": "1.0.0",
            "description": """# Media Influence Topology & Detection System (MITDS)

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
- SEC EDGAR company filings
- SEDAR+ Canadian securities filings
- Provincial corporate registries
- Meta Ad Library

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

Rate limit headers are included in all responses:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Requests remaining
- `X-RateLimit-Reset`: Seconds until limit resets
""",
            "contact": {
                "name": "MITDS Team",
                "url": "https://github.com/mitds/mitds"
            },
            "license": {
                "name": "MIT",
                "url": "https://opensource.org/licenses/MIT"
            }
        },
        "servers": [
            {
                "url": "http://localhost:8000",
                "description": "Local development server"
            },
            {
                "url": "/",
                "description": "Current server"
            }
        ],
        "tags": [
            {"name": tag, "description": tag_descriptions.get(tag, f"{tag} operations")}
            for tag in tags
        ],
        "paths": {},  # Will be populated with $ref
        "components": {
            "schemas": {},  # Will be populated with $ref
            "parameters": {"$ref": "./components/parameters.yaml"},
            "responses": {"$ref": "./components/responses.yaml"},
            "securitySchemes": {"$ref": "./components/security.yaml"}
        },
        "security": [{"bearerAuth": []}]
    }
    
    return master


def split_paths_by_tag(openapi: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Split paths into separate dictionaries by primary tag."""
    paths_by_tag = defaultdict(dict)
    
    for path, methods in openapi.get("paths", {}).items():
        # Get the primary tag from the first method
        primary_tag = None
        for method, spec in methods.items():
            if method.startswith("x-"):  # Skip extensions
                continue
            tags = spec.get("tags", [])
            if tags:
                primary_tag = tags[0]
                break
        
        if primary_tag:
            filename = tag_to_filename(primary_tag)
            paths_by_tag[filename][path] = methods
        else:
            # Default to health for untagged paths
            paths_by_tag["health"][path] = methods
    
    return dict(paths_by_tag)


def split_schemas_by_category(openapi: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Split schemas into separate dictionaries by category."""
    schemas_by_category = defaultdict(dict)
    
    schemas = openapi.get("components", {}).get("schemas", {})
    for name, schema in schemas.items():
        category = categorize_schema(name)
        schemas_by_category[category][name] = schema
    
    return dict(schemas_by_category)


def yaml_str(data: Any) -> str:
    """Convert data to YAML string with nice formatting."""
    return yaml.dump(
        data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120
    )


def write_yaml_file(path: Path, data: Any, header: str = "") -> None:
    """Write data to a YAML file with optional header comment."""
    path.parent.mkdir(parents=True, exist_ok=True)
    
    content = ""
    if header:
        content = f"# {header}\n# Auto-generated by export_openapi.py\n\n"
    
    content += yaml_str(data)
    
    path.write_text(content, encoding="utf-8")
    print(f"  Created: {path}")


def create_paths_index(paths_by_tag: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Create an index file that references all path files."""
    index = {}
    for filename in sorted(paths_by_tag.keys()):
        # Add a comment-like entry for documentation
        for path in paths_by_tag[filename].keys():
            index[path] = {"$ref": f"./{filename}.yaml#/{path.replace('/', '~1')}"}
    return index


def create_schemas_index(schemas_by_category: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Create an index file that references all schema files."""
    index = {}
    for category, schemas in schemas_by_category.items():
        for name in schemas.keys():
            index[name] = {"$ref": f"./{category}.yaml#/{name}"}
    return index


def export_openapi(output_dir: Path) -> None:
    """Main export function."""
    print("=" * 60)
    print("MITDS OpenAPI Spec Export Tool")
    print("=" * 60)
    
    # Get the OpenAPI schema from FastAPI
    print("\n1. Fetching OpenAPI schema from FastAPI app...")
    try:
        openapi = get_openapi_schema()
        print(f"   Found {len(openapi.get('paths', {}))} paths")
        print(f"   Found {len(openapi.get('components', {}).get('schemas', {}))} schemas")
    except Exception as e:
        print(f"   ERROR: Failed to get OpenAPI schema: {e}")
        print("   Make sure the backend dependencies are installed.")
        sys.exit(1)
    
    # Create output directories
    print(f"\n2. Creating output directory structure at {output_dir}...")
    paths_dir = output_dir / "paths"
    schemas_dir = output_dir / "components" / "schemas"
    components_dir = output_dir / "components"
    
    for d in [paths_dir, schemas_dir, components_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    # Split paths by tag
    print("\n3. Splitting paths by tag...")
    paths_by_tag = split_paths_by_tag(openapi)
    for filename, paths in paths_by_tag.items():
        write_yaml_file(
            paths_dir / f"{filename}.yaml",
            paths,
            f"{filename.title()} API paths"
        )
    
    # Create paths index
    print("\n4. Creating paths index...")
    # Instead of an index file, we'll inline the paths in the master spec
    
    # Split schemas by category
    print("\n5. Splitting schemas by category...")
    schemas_by_category = split_schemas_by_category(openapi)
    for category, schemas in schemas_by_category.items():
        write_yaml_file(
            schemas_dir / f"{category}.yaml",
            schemas,
            f"{category.title()} schemas"
        )
    
    # Create common components
    print("\n6. Creating common components...")
    
    # Parameters
    parameters = extract_common_parameters(openapi)
    write_yaml_file(
        components_dir / "parameters.yaml",
        parameters,
        "Reusable API parameters"
    )
    
    # Responses
    responses = extract_common_responses()
    write_yaml_file(
        components_dir / "responses.yaml",
        responses,
        "Common API responses"
    )
    
    # Security
    security = create_security_schemes()
    write_yaml_file(
        components_dir / "security.yaml",
        security,
        "Security schemes"
    )
    
    # Get unique tags
    tags = set()
    for path, methods in openapi.get("paths", {}).items():
        for method, spec in methods.items():
            if not method.startswith("x-"):
                tags.update(spec.get("tags", []))
    
    # Add Health tag if we have health endpoints
    if "health" in paths_by_tag or any("/health" in p for p in openapi.get("paths", {})):
        tags.add("Health")
    
    # Sort tags in a logical order
    tag_order = [
        "Entities", "Relationships", "Detection", "Ingestion", "Reports",
        "Jobs", "Validation", "Resolution", "Settings", "Research", 
        "Cases", "Meta OAuth", "Health", "Root"
    ]
    sorted_tags = [t for t in tag_order if t in tags]
    sorted_tags.extend([t for t in sorted(tags) if t not in tag_order])
    
    # Create master spec
    print("\n7. Creating master OpenAPI spec...")
    master = create_master_spec(openapi, sorted_tags)
    
    # Add paths with references to separate files
    for filename, paths in paths_by_tag.items():
        for path, methods in paths.items():
            # Reference the path file directly
            master["paths"][path] = {"$ref": f"./paths/{filename}.yaml#/{path.replace('/', '~1')}"}
    
    # For the master file, we'll inline the paths for better compatibility
    # Some tools don't support $ref in paths, so we'll write them directly
    master["paths"] = {}
    for filename, paths in paths_by_tag.items():
        master["paths"].update(paths)
    
    # Add schema references
    for category, schemas in schemas_by_category.items():
        for name in schemas.keys():
            master["components"]["schemas"][name] = {
                "$ref": f"./components/schemas/{category}.yaml#/{name}"
            }
    
    # For better compatibility, inline schemas too
    master["components"]["schemas"] = {}
    for category, schemas in schemas_by_category.items():
        master["components"]["schemas"].update(schemas)
    
    # Fix component references to be absolute in the master file
    master["components"]["parameters"] = parameters
    master["components"]["responses"] = responses
    master["components"]["securitySchemes"] = security
    
    write_yaml_file(
        output_dir / "openapi.yaml",
        master,
        "MITDS API - OpenAPI 3.1.0 Specification"
    )
    
    # Also write a bundled JSON version for direct FastAPI use
    print("\n8. Creating bundled JSON version...")
    json_path = output_dir / "openapi.json"
    json_path.write_text(json.dumps(master, indent=2), encoding="utf-8")
    print(f"  Created: {json_path}")
    
    print("\n" + "=" * 60)
    print("Export complete!")
    print("=" * 60)
    print(f"\nOutput directory: {output_dir}")
    print(f"Master spec: {output_dir / 'openapi.yaml'}")
    print(f"Bundled JSON: {output_dir / 'openapi.json'}")
    print(f"\nPath modules: {len(paths_by_tag)} files in {paths_dir}")
    print(f"Schema modules: {len(schemas_by_category)} files in {schemas_dir}")
    print(f"\nValidate with:")
    print(f"  swagger-cli validate {output_dir / 'openapi.yaml'}")
    print(f"  redocly lint {output_dir / 'openapi.yaml'}")


def main():
    parser = argparse.ArgumentParser(
        description="Export FastAPI OpenAPI spec to modular YAML files"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "backend" / "openapi",
        help="Output directory for OpenAPI files (default: backend/openapi)"
    )
    
    args = parser.parse_args()
    export_openapi(args.output_dir)


if __name__ == "__main__":
    main()
