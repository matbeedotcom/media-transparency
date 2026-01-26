# Implementation Plan: Media Influence Topology & Detection System (MITDS)

**Branch**: `001-media-influence-detection` | **Date**: 2026-01-26 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-media-influence-detection/spec.md`

## Summary

Build a civic-grade detection system that identifies and explains structural media manipulation for political gain using reproducible technical methods. The system ingests public data from IRS 990s, CRA charities, OpenCorporates, and Meta Ad Library; resolves entities across sources; constructs a temporal knowledge graph; detects coordination patterns (funding clusters, timing synchronization, infrastructure sharing); and generates evidence-linked, non-accusatory reports. MVP targets ~100K entities and ~1M relationships for US + Canada regional analysis.

## Technical Context

**Language/Version**: Python 3.11+ (data pipelines, analysis, API), TypeScript 5.x (frontend)
**Primary Dependencies**:
- Backend: FastAPI (API), Celery (task scheduling), httpx (async HTTP), pandas (data processing), networkx (graph algorithms)
- Graph: Neo4j 5.x (primary graph store)
- Storage: PostgreSQL 16 (event store, metadata, provenance)
- Frontend: React 18, D3.js/Cytoscape.js (graph visualization), TanStack Query
- Infrastructure: Docker, Redis (task queue, caching)

**Storage**:
- Neo4j: Entity graph with temporal relationships
- PostgreSQL: Immutable event store, source snapshots, provenance tracking, audit logs
- S3-compatible: Raw source file archives (IRS 990 XMLs, etc.)

**Testing**: pytest (unit, integration), pytest-asyncio, Playwright (E2E), contract tests for APIs

**Target Platform**: Linux server (Docker), web browser (analyst UI)

**Project Type**: Web application (backend API + frontend visualization)

**Performance Goals**:
- Query response <10s for funding relationship queries (SC-006)
- Ingestion: Process full IRS 990 dataset (500K+ filings) within 24h
- Graph traversal: 3-hop queries complete in <5s

**Constraints**:
- All data from public sources only
- No credentials in code (environment + secrets manager)
- Immutable audit trail for all data
- Non-accusatory language in all outputs

**Scale/Scope**:
- MVP: 100K entities, 1M relationships
- 4 data sources: IRS 990, CRA, OpenCorporates, Meta Ad Library
- Refresh: Daily (ads), Weekly (registries)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Status**: No project constitution defined yet (template placeholders only). Proceeding with standard software engineering best practices:

- [x] **Library-First**: Core detection algorithms will be standalone, testable modules
- [x] **CLI Interface**: All ingestion pipelines will have CLI entry points for debugging
- [x] **Test-First**: Integration tests for data source contracts; unit tests for detection algorithms
- [x] **Observability**: Structured logging for all ingestion and detection operations
- [x] **Simplicity**: Start with PostgreSQL + Neo4j; avoid premature optimization

No gate violations identified. Proceeding to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-media-influence-detection/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0 output - technology decisions
├── data-model.md        # Phase 1 output - entity schemas
├── quickstart.md        # Phase 1 output - developer setup
├── contracts/           # Phase 1 output - API specifications
│   ├── api.yaml         # OpenAPI spec for REST endpoints
│   └── events.md        # Event schemas for ingestion
├── checklists/          # Quality checklists
│   └── requirements.md  # Spec validation checklist
└── tasks.md             # Phase 2 output (from /speckit.tasks)
```

### Source Code (repository root)

```text
backend/
├── src/
│   ├── mitds/
│   │   ├── __init__.py
│   │   ├── models/           # Domain models (Entity, Relationship, Evidence)
│   │   │   ├── __init__.py
│   │   │   ├── entities.py
│   │   │   ├── relationships.py
│   │   │   └── evidence.py
│   │   ├── ingestion/        # Data source connectors
│   │   │   ├── __init__.py
│   │   │   ├── base.py       # Abstract ingestion interface
│   │   │   ├── irs990.py     # IRS 990 connector
│   │   │   ├── cra.py        # CRA charities connector
│   │   │   ├── opencorp.py   # OpenCorporates connector
│   │   │   └── meta_ads.py   # Meta Ad Library connector
│   │   ├── resolution/       # Entity resolution engine
│   │   │   ├── __init__.py
│   │   │   ├── matcher.py    # Fuzzy matching
│   │   │   ├── resolver.py   # Cross-source resolution
│   │   │   └── reconcile.py  # Human-in-the-loop queue
│   │   ├── graph/            # Graph operations
│   │   │   ├── __init__.py
│   │   │   ├── builder.py    # Graph construction
│   │   │   ├── queries.py    # Common graph queries
│   │   │   └── temporal.py   # Time-sliced views
│   │   ├── detection/        # Coordination detection
│   │   │   ├── __init__.py
│   │   │   ├── temporal.py   # Timing coordination
│   │   │   ├── funding.py    # Funding cluster detection
│   │   │   ├── infra.py      # Infrastructure sharing
│   │   │   ├── composite.py  # Multi-signal scoring
│   │   │   └── hardneg.py    # Hard negative filtering
│   │   ├── reporting/        # Report generation
│   │   │   ├── __init__.py
│   │   │   ├── explain.py    # "Why flagged" explanations
│   │   │   ├── templates.py  # Report templates
│   │   │   └── language.py   # Non-accusatory phrasing
│   │   ├── api/              # FastAPI routes
│   │   │   ├── __init__.py
│   │   │   ├── entities.py
│   │   │   ├── relationships.py
│   │   │   ├── detection.py
│   │   │   └── reports.py
│   │   └── cli/              # CLI entry points
│   │       ├── __init__.py
│   │       ├── ingest.py
│   │       └── analyze.py
│   └── main.py               # FastAPI app entry
├── tests/
│   ├── contract/             # API contract tests
│   ├── integration/          # Cross-module tests
│   │   ├── test_ingestion.py
│   │   ├── test_resolution.py
│   │   └── test_detection.py
│   └── unit/                 # Unit tests per module
│       ├── test_models.py
│       ├── test_detection.py
│       └── test_reporting.py
├── migrations/               # Database migrations
├── pyproject.toml
├── Dockerfile
└── docker-compose.yml

frontend/
├── src/
│   ├── components/
│   │   ├── graph/            # Graph visualization
│   │   │   ├── EntityGraph.tsx
│   │   │   ├── FundingCluster.tsx
│   │   │   └── Timeline.tsx
│   │   ├── evidence/         # Evidence panels
│   │   │   ├── EvidencePanel.tsx
│   │   │   └── SourceLink.tsx
│   │   └── reports/          # Report views
│   │       ├── StructuralRisk.tsx
│   │       └── TopologySummary.tsx
│   ├── pages/
│   │   ├── Dashboard.tsx
│   │   ├── EntityExplorer.tsx
│   │   ├── DetectionResults.tsx
│   │   └── ReportGenerator.tsx
│   ├── services/
│   │   └── api.ts            # Backend API client
│   ├── App.tsx
│   └── main.tsx
├── tests/
│   └── e2e/
├── package.json
├── vite.config.ts
└── Dockerfile

infrastructure/
├── docker-compose.yml        # Local dev stack
├── docker-compose.prod.yml   # Production stack
└── scripts/
    ├── init-db.sh
    └── seed-test-data.sh
```

**Structure Decision**: Web application architecture with separate backend (Python/FastAPI) and frontend (React) to enable:
1. Independent scaling of API and UI
2. CLI access for pipeline debugging (no-stub requirement)
3. Clear separation between data processing and visualization
4. Testable modules for each data source connector

## Complexity Tracking

No constitution violations requiring justification. Architecture follows minimal complexity:
- Two-tier (API + UI) rather than microservices
- Single graph database rather than polyglot persistence
- Celery for scheduling rather than dedicated orchestration platform
