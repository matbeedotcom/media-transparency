# MITDS OpenAPI Documentation

This directory contains the modular OpenAPI 3.1.0 specification for the MITDS (Media Influence Topology & Detection System) REST API.

## Directory Structure

```
openapi/
├── openapi.yaml              # Master specification file
├── README.md                 # This file
├── paths/                    # API endpoint definitions by domain
│   ├── health.yaml           # Health check endpoints
│   ├── entities.yaml         # Entity management
│   ├── relationships.yaml    # Relationship queries
│   ├── detection.yaml        # Detection algorithms
│   ├── ingestion.yaml        # Data ingestion
│   ├── reports.yaml          # Report generation
│   ├── jobs.yaml             # Background jobs
│   ├── validation.yaml       # Validation framework
│   ├── resolution.yaml       # Entity resolution
│   ├── settings.yaml         # System settings
│   ├── research.yaml         # Research sessions
│   ├── cases.yaml            # Case management
│   └── meta-oauth.yaml       # Meta OAuth
└── components/               # Reusable components
    ├── schemas/              # Data models
    │   ├── common.yaml       # Shared schemas (pagination, errors)
    │   ├── entities.yaml     # Entity models
    │   ├── relationships.yaml # Relationship models
    │   ├── detection.yaml    # Detection schemas
    │   ├── cases.yaml        # Case management schemas
    │   ├── research.yaml     # Research session schemas
    │   ├── ingestion.yaml    # Ingestion schemas
    │   ├── jobs.yaml         # Job schemas
    │   ├── validation.yaml   # Validation schemas
    │   ├── resolution.yaml   # Resolution schemas
    │   ├── settings.yaml     # Settings schemas
    │   └── reports.yaml      # Report schemas
    ├── parameters.yaml       # Reusable query/path parameters
    ├── responses.yaml        # Common error responses
    └── security.yaml         # Authentication schemes
```

## Usage

### Viewing Documentation

The API documentation is available at:

- **Swagger UI**: `http://localhost:8000/docs` (development only)
- **ReDoc**: `http://localhost:8000/redoc` (development only)
- **OpenAPI JSON**: `http://localhost:8000/openapi.json` (development only)

### Validating the Specification

The main `openapi.yaml` file is a bundled, self-contained specification that can be validated directly:

```bash
# Using swagger-cli
npm install -g @apidevtools/swagger-cli
swagger-cli validate backend/openapi/openapi.yaml

# Using redocly
npm install -g @redocly/cli
redocly lint backend/openapi/openapi.yaml

# Using spectral
npm install -g @stoplight/spectral-cli
spectral lint backend/openapi/openapi.yaml
```

**Note**: The modular files in `paths/` and `components/schemas/` are partial YAML files designed for organization and maintainability. They cannot be validated independently - they require bundling into a complete OpenAPI spec first.

### Generating Documentation

```bash
# Generate static HTML documentation
redocly build-docs backend/openapi/openapi.yaml -o docs/api.html

# Generate Postman collection
npx openapi-to-postmanv2 -s backend/openapi/openapi.yaml -o postman_collection.json
```

### Bundling the Specification

To create a single bundled file (resolving all `$ref`s):

```bash
# Bundle to YAML
redocly bundle backend/openapi/openapi.yaml -o backend/openapi/bundled.yaml

# Bundle to JSON
redocly bundle backend/openapi/openapi.yaml -o backend/openapi/bundled.json
```

## Maintenance

### Updating the Specification

1. **Adding a new endpoint**:
   - Add the path definition to the appropriate file in `paths/`
   - Add any new schemas to the appropriate file in `components/schemas/`
   - Reference shared parameters and responses where applicable

2. **Adding a new schema**:
   - Determine the appropriate category file in `components/schemas/`
   - Add the schema with proper documentation
   - Use `$ref` to reference other schemas

3. **Regenerating from FastAPI**:
   ```bash
   python scripts/export_openapi.py --output-dir backend/openapi
   ```
   This will regenerate the specification from the FastAPI app's auto-generated schema.

### Naming Conventions

- **Operation IDs**: camelCase (e.g., `getEntity`, `createCase`)
- **Schema names**: PascalCase (e.g., `EntityResponse`, `CaseConfig`)
- **Parameter names**: snake_case (e.g., `entity_id`, `fiscal_year`)
- **Path files**: kebab-case (e.g., `meta-oauth.yaml`)

### Schema Guidelines

1. **Required properties**: Always specify `required` array for objects
2. **Descriptions**: Provide meaningful descriptions for all schemas and properties
3. **Examples**: Include examples for complex schemas
4. **Enums**: Document all enum values with descriptions
5. **Formats**: Use appropriate formats (`uuid`, `date-time`, `uri`, etc.)

### Reference Syntax

Use relative paths for `$ref` references:

```yaml
# From paths/entities.yaml to components/schemas/entities.yaml
$ref: '../components/schemas/entities.yaml#/EntityResponse'

# From components/schemas/cases.yaml to entities.yaml
$ref: 'entities.yaml#/EntitySummary'

# From paths to parameters.yaml
$ref: '../components/parameters.yaml#/LimitParam'
```

## API Overview

### Core APIs

| API | Description | Endpoints |
|-----|-------------|-----------|
| Entities | Entity CRUD and search | 6 |
| Relationships | Graph queries and paths | 13 |
| Detection | Coordination detection | 5 |
| Cases | Investigation management | 13 |
| Research | Automated discovery | 13 |

### Data APIs

| API | Description | Endpoints |
|-----|-------------|-----------|
| Ingestion | Data source management | 16+ |
| Resolution | Entity reconciliation | 7 |
| Validation | Testing framework | 7 |

### System APIs

| API | Description | Endpoints |
|-----|-------------|-----------|
| Jobs | Background task management | 4 |
| Settings | Configuration | 3 |
| Reports | Report generation | 5 |
| Meta OAuth | Facebook authentication | 5 |
| Health | Service health checks | 4 |

## Authentication

Most endpoints require JWT authentication:

```http
Authorization: Bearer <token>
```

Public endpoints (no auth required):
- `GET /health`
- `GET /health/ready`
- `GET /health/live`
- `GET /` (root)

## Rate Limits

| Endpoint Category | Limit |
|-------------------|-------|
| Default | 100 req/min |
| Search | 30 req/min |
| Detection | 10 req/min |

Rate limit headers:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Requests remaining
- `X-RateLimit-Reset`: Seconds until reset

## Error Handling

All errors follow this format:

```json
{
  "success": false,
  "error": "Human-readable message",
  "error_code": "MACHINE_READABLE_CODE",
  "details": [
    {
      "code": "SPECIFIC_ERROR",
      "message": "Details",
      "field": "affected_field"
    }
  ]
}
```

Common error codes:
- `NOT_FOUND` (404)
- `VALIDATION_ERROR` (400/422)
- `AUTHENTICATION_REQUIRED` (401)
- `ACCESS_DENIED` (403)
- `RATE_LIMIT_EXCEEDED` (429)
- `INTERNAL_ERROR` (500)
