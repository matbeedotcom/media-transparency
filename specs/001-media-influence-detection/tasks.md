# Tasks: Media Influence Topology & Detection System (MITDS)

**Input**: Design documents from `/specs/001-media-influence-detection/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4, US5, US6)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `backend/src/mitds/`, `backend/tests/`
- **Frontend**: `frontend/src/`
- **Infrastructure**: `infrastructure/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create backend project structure with pyproject.toml in backend/
- [X] T002 [P] Create frontend project structure with package.json in frontend/
- [X] T003 [P] Create infrastructure directory with docker-compose.yml in infrastructure/
- [X] T004 Initialize Python project with FastAPI, Celery, httpx, pandas dependencies in backend/pyproject.toml
- [X] T005 [P] Initialize React 18 project with TypeScript, TanStack Query, Cytoscape.js in frontend/package.json
- [X] T006 [P] Configure Python linting (ruff) and formatting (black) in backend/pyproject.toml
- [X] T007 [P] Configure TypeScript/ESLint settings in frontend/tsconfig.json and frontend/.eslintrc.cjs
- [X] T008 Create .env.example with all required environment variables at repository root
- [X] T009 [P] Create .gitignore with Python, Node, Docker patterns at repository root

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Database & Storage Setup

- [X] T010 Create docker-compose.yml with PostgreSQL, Neo4j, Redis, MinIO services in infrastructure/docker-compose.yml
- [X] T011 Create database initialization script in infrastructure/scripts/init-db.sh
- [X] T012 Setup Alembic migrations framework in backend/migrations/
- [X] T013 Create initial PostgreSQL migration with events, evidence, source_snapshots, audit_log, ingestion_runs tables in backend/migrations/versions/001_initial.py
- [X] T014 Create Neo4j constraint setup script in infrastructure/scripts/init-neo4j.cypher

### Core Domain Models

- [X] T015 Create EntityBase, EntityType enum in backend/src/mitds/models/__init__.py
- [X] T016 [P] Create Evidence model with EvidenceType enum in backend/src/mitds/models/evidence.py
- [X] T017 [P] Create Relationship model with RelationType enum in backend/src/mitds/models/relationships.py
- [X] T018 [P] Create Event model with EventType enum in backend/src/mitds/models/events.py

### Configuration & Infrastructure

- [X] T019 Create Settings class with pydantic-settings in backend/src/mitds/config.py
- [X] T020 [P] Create database connection managers (PostgreSQL, Neo4j, Redis) in backend/src/mitds/db.py
- [X] T021 [P] Create S3 client wrapper for raw file storage in backend/src/mitds/storage.py
- [X] T022 Create structured logging configuration in backend/src/mitds/logging.py
- [X] T023 Create Celery app configuration in backend/src/mitds/worker.py

### Base Ingestion Infrastructure

- [X] T024 Create abstract BaseIngester class with retry logic in backend/src/mitds/ingestion/base.py
- [X] T025 [P] Create ingestion event schemas in backend/src/mitds/ingestion/events.py
- [X] T026 [P] Create ingestion run tracking service in backend/src/mitds/ingestion/tracking.py

### API Foundation

- [X] T027 Create FastAPI app with CORS, health check in backend/src/main.py
- [X] T028 [P] Create API error handlers and response models in backend/src/mitds/api/__init__.py
- [X] T029 [P] Create authentication/authorization middleware in backend/src/mitds/api/auth.py

### Frontend Foundation

- [X] T030 Create React app entry with routing in frontend/src/App.tsx and frontend/src/main.tsx
- [X] T031 [P] Create API client service with TanStack Query setup in frontend/src/services/api.ts
- [X] T032 [P] Create base layout component in frontend/src/components/Layout.tsx
- [X] T033 [P] Create Dashboard page shell in frontend/src/pages/Dashboard.tsx

**Checkpoint**: Foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - Explore Funding Relationships (Priority: P1) 🎯 MVP

**Goal**: Analysts can explore funding relationships between organizations and outlets, identify shared funders, and trace funding flows with evidence links.

**Independent Test**: Load sample IRS 990 + CRA data, query "Show funding clusters", verify evidence-linked results in <10s.

### Entity Models for US1

- [X] T034 [P] [US1] Create Organization model with OrgType, OrgStatus enums in backend/src/mitds/models/entities.py
- [X] T035 [P] [US1] Create Person model with role assignments in backend/src/mitds/models/entities.py
- [X] T036 [P] [US1] Create Outlet model with MediaType enum in backend/src/mitds/models/entities.py
- [X] T037 [P] [US1] Create Sponsor model in backend/src/mitds/models/entities.py

### Data Ingestion for US1

- [X] T038 [US1] Implement IRS 990 bulk data downloader from AWS S3 in backend/src/mitds/ingestion/irs990.py
- [X] T039 [US1] Implement IRS 990 XML parser for Schedule I (grants) and Part VII (officers) in backend/src/mitds/ingestion/irs990.py
- [X] T040 [US1] Create Celery task for IRS 990 weekly ingestion in backend/src/mitds/ingestion/irs990.py
- [X] T041 [P] [US1] Implement CRA charities CSV downloader in backend/src/mitds/ingestion/cra.py
- [X] T042 [P] [US1] Implement CRA charities parser for gifts to qualified donees in backend/src/mitds/ingestion/cra.py
- [X] T043 [US1] Create Celery task for CRA weekly ingestion in backend/src/mitds/ingestion/cra.py

### Entity Resolution for US1

- [X] T044 [US1] Implement deterministic matcher (EIN, BN exact match) in backend/src/mitds/resolution/matcher.py
- [X] T045 [US1] Implement fuzzy name matcher with normalization in backend/src/mitds/resolution/matcher.py
- [X] T046 [US1] Create entity resolver that combines matching strategies in backend/src/mitds/resolution/resolver.py

### Graph Operations for US1

- [X] T047 [US1] Implement Neo4j node creation for entities in backend/src/mitds/graph/builder.py
- [X] T048 [US1] Implement FUNDED_BY relationship creation with properties in backend/src/mitds/graph/builder.py
- [X] T049 [US1] Implement funding path query (funder → outlets) in backend/src/mitds/graph/queries.py

### Detection for US1

- [X] T050 [US1] Implement funding cluster detection algorithm in backend/src/mitds/detection/funding.py
- [X] T051 [US1] Implement shared funder identification in backend/src/mitds/detection/funding.py

### API Endpoints for US1

- [X] T052 [US1] Implement GET /entities search endpoint in backend/src/mitds/api/entities.py
- [X] T053 [US1] Implement GET /entities/{id} endpoint in backend/src/mitds/api/entities.py
- [X] T054 [US1] Implement GET /entities/{id}/relationships endpoint in backend/src/mitds/api/entities.py
- [X] T055 [US1] Implement GET /entities/{id}/evidence endpoint in backend/src/mitds/api/entities.py
- [X] T056 [US1] Implement GET /relationships/funding-clusters endpoint in backend/src/mitds/api/relationships.py
- [X] T057 [US1] Implement GET /ingestion/status endpoint in backend/src/mitds/api/ingestion.py
- [X] T058 [US1] Implement POST /ingestion/{source}/trigger endpoint in backend/src/mitds/api/ingestion.py

### CLI for US1

- [X] T059 [US1] Implement ingest CLI command for IRS 990 in backend/src/mitds/cli/ingest.py
- [X] T060 [P] [US1] Implement ingest CLI command for CRA in backend/src/mitds/cli/ingest.py

### Frontend for US1

- [X] T061 [US1] Create EntityExplorer page with search in frontend/src/pages/EntityExplorer.tsx
- [X] T062 [US1] Create EntityGraph component with Cytoscape.js in frontend/src/components/graph/EntityGraph.tsx
- [X] T063 [US1] Create FundingCluster visualization component in frontend/src/components/graph/FundingCluster.tsx
- [X] T064 [US1] Create EvidencePanel component showing source links in frontend/src/components/evidence/EvidencePanel.tsx
- [X] T065 [US1] Create SourceLink component with archive fallback in frontend/src/components/evidence/SourceLink.tsx

**Checkpoint**: User Story 1 complete - funding relationship exploration fully functional

---

## Phase 4: User Story 2 - Detect Temporal Coordination (Priority: P1)

**Goal**: Researchers can detect temporal coordination patterns in publication timing, distinguish from legitimate news cycles, and see evidence-backed explanations.

**Independent Test**: Inject synthetic coordination pattern alongside organic coverage, verify detection with hard negative filtering.

### Data Ingestion for US2

- [X] T066 [US2] Implement Meta Ad Library API client in backend/src/mitds/ingestion/meta_ads.py
- [X] T067 [US2] Implement ad data parser extracting timing, sponsor, spend in backend/src/mitds/ingestion/meta_ads.py
- [X] T068 [US2] Create Celery task for Meta Ad Library daily ingestion in backend/src/mitds/ingestion/meta_ads.py
- [X] T069 [US2] Implement credential refresh for Meta access token in backend/src/mitds/ingestion/meta_ads.py

### Detection for US2

- [X] T070 [US2] Implement burst detection algorithm (Kleinberg) in backend/src/mitds/detection/temporal.py
- [X] T071 [US2] Implement lead-lag correlation analysis in backend/src/mitds/detection/temporal.py
- [X] T072 [US2] Implement synchronization score (Jensen-Shannon divergence) in backend/src/mitds/detection/temporal.py
- [X] T073 [US2] Implement hard negative filter for breaking news in backend/src/mitds/detection/hardneg.py
- [X] T074 [US2] Implement hard negative filter for scheduled events in backend/src/mitds/detection/hardneg.py

### API Endpoints for US2

- [X] T075 [US2] Implement POST /detection/temporal-coordination endpoint in backend/src/mitds/api/detection.py
- [X] T076 [US2] Implement async job handling for long-running analysis in backend/src/mitds/api/detection.py
- [X] T077 [US2] Implement GET /jobs/{id} endpoint for job status in backend/src/mitds/api/jobs.py

### Frontend for US2

- [X] T078 [US2] Create Timeline visualization component in frontend/src/components/graph/Timeline.tsx
- [X] T079 [US2] Create DetectionResults page in frontend/src/pages/DetectionResults.tsx
- [X] T080 [US2] Add temporal analysis form to DetectionResults page in frontend/src/pages/DetectionResults.tsx

**Checkpoint**: User Story 2 complete - temporal coordination detection functional

---

## Phase 5: User Story 3 - Map Entity Relationships (Priority: P2)

**Goal**: Analysts can explore full organizational structure, find paths between entities, and view historical relationship changes.

**Independent Test**: Load organizational data, verify path queries return correct relationships with temporal validity.

### Data Ingestion for US3

- [X] T081 [US3] Implement OpenCorporates API client in backend/src/mitds/ingestion/opencorp.py
- [X] T082 [US3] Implement company and officer data parser in backend/src/mitds/ingestion/opencorp.py
- [X] T083 [US3] Create Celery task for OpenCorporates weekly ingestion in backend/src/mitds/ingestion/opencorp.py
- [X] T084 [US3] Implement API rate limiting with caching in backend/src/mitds/ingestion/opencorp.py

### Graph Operations for US3

- [X] T085 [US3] Implement DIRECTOR_OF, EMPLOYED_BY, OWNS relationship creation in backend/src/mitds/graph/builder.py
- [X] T086 [US3] Implement multi-hop path finding algorithm in backend/src/mitds/graph/queries.py
- [X] T087 [US3] Implement time-sliced graph views in backend/src/mitds/graph/temporal.py
- [X] T088 [US3] Implement historical change detection in backend/src/mitds/graph/temporal.py

### Entity Resolution for US3

- [X] T089 [US3] Implement embedding-based similarity matching in backend/src/mitds/resolution/matcher.py
- [X] T090 [US3] Create human-in-the-loop reconciliation queue in backend/src/mitds/resolution/reconcile.py

### API Endpoints for US3

- [X] T091 [US3] Implement GET /relationships/path endpoint in backend/src/mitds/api/relationships.py
- [X] T092 [US3] Add as_of parameter support for temporal queries in backend/src/mitds/api/entities.py

### CLI for US3

- [X] T093 [US3] Implement ingest CLI command for OpenCorporates in backend/src/mitds/cli/ingest.py
- [X] T094 [US3] Implement resolve CLI command for entity reconciliation in backend/src/mitds/cli/resolve.py

### Frontend for US3

- [X] T095 [US3] Add path finding to EntityGraph component in frontend/src/components/graph/EntityGraph.tsx
- [X] T096 [US3] Add historical timeline slider to EntityExplorer in frontend/src/pages/EntityExplorer.tsx

**Checkpoint**: User Story 3 complete - full entity relationship mapping functional

---

## Phase 6: User Story 4 - Detect Infrastructure Sharing (Priority: P2)

**Goal**: Researchers can identify shared technical infrastructure across outlets even when organizational links are hidden.

**Independent Test**: Load domain metadata, verify shared hosting/analytics detection across outlet set.

### Entity Models for US4

- [X] T097 [P] [US4] Create Domain model with infrastructure fields in backend/src/mitds/models/entities.py
- [X] T098 [P] [US4] Create Vendor model with VendorType enum in backend/src/mitds/models/entities.py
- [X] T099 [P] [US4] Create PlatformAccount model in backend/src/mitds/models/entities.py

### Detection for US4

- [X] T100 [US4] Implement WHOIS/DNS lookup service in backend/src/mitds/detection/infra.py
- [X] T101 [US4] Implement hosting provider detection via IP/ASN in backend/src/mitds/detection/infra.py
- [X] T102 [US4] Implement analytics tag detection in backend/src/mitds/detection/infra.py
- [X] T103 [US4] Implement shared infrastructure scoring in backend/src/mitds/detection/infra.py
- [X] T104 [US4] Create SHARED_INFRA relationships from detected overlaps in backend/src/mitds/detection/infra.py

### API Endpoints for US4

- [X] T105 [US4] Implement GET /relationships/shared-infrastructure endpoint in backend/src/mitds/api/relationships.py

### Frontend for US4

- [X] T106 [US4] Create InfrastructureOverlap visualization component in frontend/src/components/graph/InfrastructureOverlap.tsx
- [X] T107 [US4] Add infrastructure analysis to EntityExplorer in frontend/src/pages/EntityExplorer.tsx

**Checkpoint**: User Story 4 complete - infrastructure sharing detection functional

---

## Phase 7: User Story 5 - Generate Structural Risk Report (Priority: P3)

**Goal**: Civil society organizations can generate comprehensive, defensible reports with non-accusatory language and evidence links.

**Independent Test**: Generate report from sample analysis, verify language passes legal review criteria.

### Reporting Implementation

- [X] T108 [US5] Create report template data structures in backend/src/mitds/reporting/templates.py
- [X] T109 [US5] Implement structural risk report template in backend/src/mitds/reporting/templates.py
- [X] T110 [US5] Implement influence topology summary template in backend/src/mitds/reporting/templates.py
- [X] T111 [US5] Implement timeline narrative template in backend/src/mitds/reporting/templates.py
- [X] T112 [US5] Create non-accusatory language rules and transforms in backend/src/mitds/reporting/language.py
- [X] T113 [US5] Implement "why flagged" explanation generator in backend/src/mitds/reporting/explain.py
- [X] T114 [US5] Implement confidence band calculation for findings in backend/src/mitds/reporting/explain.py

### Composite Detection for US5

- [X] T115 [US5] Implement composite coordination score combining all signals in backend/src/mitds/detection/composite.py
- [X] T116 [US5] Ensure no single signal can trigger flag alone in backend/src/mitds/detection/composite.py

### API Endpoints for US5

- [X] T117 [US5] Implement GET /reports/templates endpoint in backend/src/mitds/api/reports.py
- [X] T118 [US5] Implement POST /reports endpoint with async generation in backend/src/mitds/api/reports.py
- [X] T119 [US5] Implement GET /reports/{id} endpoint with multiple formats in backend/src/mitds/api/reports.py
- [X] T120 [US5] Implement POST /detection/composite-score endpoint in backend/src/mitds/api/detection.py
- [X] T121 [US5] Implement GET /detection/explain/{finding_id} endpoint in backend/src/mitds/api/detection.py

### Frontend for US5

- [X] T122 [US5] Create ReportGenerator page in frontend/src/pages/ReportGenerator.tsx
- [X] T123 [US5] Create StructuralRisk report view component in frontend/src/components/reports/StructuralRisk.tsx
- [X] T124 [US5] Create TopologySummary report view component in frontend/src/components/reports/TopologySummary.tsx
- [X] T125 [US5] Implement report export (PDF, HTML) in frontend/src/pages/ReportGenerator.tsx

**Checkpoint**: User Story 5 complete - structural risk reporting functional

---

## Phase 8: User Story 6 - Validate Against Known Cases (Priority: P3)

**Goal**: System administrators can validate detection accuracy against documented influence operations and track metrics.

**Independent Test**: Load golden dataset, verify 85% recall on known operations, <5% false positives on hard negatives.

### Validation Framework

- [X] T126 [US6] Create golden dataset schema and loader in backend/src/mitds/validation/golden.py
- [X] T127 [US6] Create synthetic coordination pattern generator in backend/src/mitds/validation/synthetic.py
- [X] T128 [US6] Implement recall and precision calculation in backend/src/mitds/validation/metrics.py
- [X] T129 [US6] Implement false positive rate tracking in backend/src/mitds/validation/metrics.py
- [X] T130 [US6] Create metrics dashboard data aggregation in backend/src/mitds/validation/dashboard.py

### API Endpoints for US6

- [X] T131 [US6] Implement GET /validation/metrics endpoint in backend/src/mitds/api/validation.py
- [X] T132 [US6] Implement POST /validation/run endpoint in backend/src/mitds/api/validation.py

### CLI for US6

- [X] T133 [US6] Implement analyze CLI command for batch validation in backend/src/mitds/cli/analyze.py

### Frontend for US6

- [X] T134 [US6] Create ValidationDashboard page in frontend/src/pages/ValidationDashboard.tsx
- [X] T135 [US6] Add metrics charts to Dashboard in frontend/src/pages/Dashboard.tsx

**Checkpoint**: User Story 6 complete - validation and metrics framework functional

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

### Data Quality & Observability

- [ ] T136 [P] Implement data quality metrics tracking (FR-028) in backend/src/mitds/quality/metrics.py
- [ ] T137 [P] Add structured logging to all ingestion pipelines in backend/src/mitds/ingestion/*.py
- [ ] T138 [P] Add audit logging for analyst queries (FR-030) in backend/src/mitds/api/audit.py

### Security & Reliability

- [ ] T139 [P] Implement API rate limiting in backend/src/mitds/api/middleware.py
- [ ] T140 [P] Add request validation and sanitization in backend/src/mitds/api/validation.py
- [ ] T141 Configure HTTPS and security headers in infrastructure/docker-compose.prod.yml

### Performance Optimization

- [ ] T142 Add Redis caching for frequently accessed entities in backend/src/mitds/cache.py
- [ ] T143 Optimize Neo4j queries with proper indexing in backend/src/mitds/graph/queries.py
- [ ] T144 Implement query result pagination across all list endpoints in backend/src/mitds/api/*.py

### Documentation & Testing

- [ ] T145 [P] Create API documentation with OpenAPI examples in backend/src/mitds/api/docs.py
- [ ] T146 [P] Create integration test fixtures in backend/tests/fixtures/
- [ ] T147 Run quickstart.md validation end-to-end

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-8)**: All depend on Foundational phase completion
  - US1 & US2 (P1) can proceed in parallel after Phase 2
  - US3 & US4 (P2) can proceed in parallel after Phase 2
  - US5 & US6 (P3) can proceed in parallel after Phase 2
- **Polish (Phase 9)**: Can start after US1 is complete, continues in parallel with other stories

### User Story Dependencies

| Story | Priority | Dependencies | Can Start After |
|-------|----------|--------------|-----------------|
| US1 - Funding | P1 | Phase 2 only | Foundational complete |
| US2 - Temporal | P1 | Phase 2 only | Foundational complete |
| US3 - Relationships | P2 | Phase 2 only | Foundational complete |
| US4 - Infrastructure | P2 | Phase 2 only | Foundational complete |
| US5 - Reports | P3 | Detection results (soft) | US1+US2 complete (recommended) |
| US6 - Validation | P3 | Detection algorithms | US1+US2 complete (recommended) |

### Within Each User Story

1. Entity models before ingestion
2. Ingestion before graph operations
3. Graph operations before detection
4. Detection before API endpoints
5. API endpoints before frontend

### Parallel Opportunities

**Within Phase 2 (Foundational)**:
- T015, T016, T017, T018 (all models in parallel)
- T019, T020, T021, T022 (all config in parallel)
- T030, T031, T032, T033 (all frontend foundation in parallel)

**Within US1**:
- T034, T035, T036, T037 (all entity models in parallel)
- T041, T042 (CRA tasks can run parallel to IRS 990)
- T059, T060 (CLI commands in parallel)

**Across User Stories**:
- US1 and US2 can be worked on simultaneously by different developers
- US3 and US4 can be worked on simultaneously by different developers

---

## Parallel Example: User Story 1

```bash
# Launch all entity models together:
Task: "Create Organization model in backend/src/mitds/models/entities.py"
Task: "Create Person model in backend/src/mitds/models/entities.py"
Task: "Create Outlet model in backend/src/mitds/models/entities.py"
Task: "Create Sponsor model in backend/src/mitds/models/entities.py"

# Launch CRA ingestion in parallel with IRS 990 (different files):
Task: "Implement CRA charities CSV downloader in backend/src/mitds/ingestion/cra.py"
Task: "Implement CRA charities parser in backend/src/mitds/ingestion/cra.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (~9 tasks)
2. Complete Phase 2: Foundational (~24 tasks)
3. Complete Phase 3: User Story 1 (~32 tasks)
4. **STOP and VALIDATE**: Test funding exploration independently
5. Deploy/demo if ready - analysts can explore funding relationships

**MVP Deliverables**:
- IRS 990 + CRA data ingestion (automated, no stubs)
- Funding relationship queries
- Funding cluster detection
- Evidence-linked results
- Basic visualization

### Incremental Delivery

| Increment | Stories | Capability |
|-----------|---------|------------|
| MVP | US1 | Funding exploration |
| +Temporal | US1+US2 | + Coordination detection |
| +Full Graph | US1-US4 | + Entity mapping + Infrastructure |
| Complete | US1-US6 | + Reports + Validation |

### Parallel Team Strategy

With 3 developers after Foundational:
- **Dev A**: US1 (Funding) → US5 (Reports)
- **Dev B**: US2 (Temporal) → US6 (Validation)
- **Dev C**: US3 (Relationships) → US4 (Infrastructure)

---

## Task Summary

| Phase | Tasks | Parallel | Description |
|-------|-------|----------|-------------|
| Phase 1: Setup | 9 | 6 | Project initialization |
| Phase 2: Foundational | 24 | 16 | Core infrastructure |
| Phase 3: US1 Funding | 32 | 12 | MVP - Funding relationships |
| Phase 4: US2 Temporal | 15 | 3 | Temporal coordination |
| Phase 5: US3 Relationships | 14 | 2 | Entity relationship mapping |
| Phase 6: US4 Infrastructure | 11 | 3 | Infrastructure sharing |
| Phase 7: US5 Reports | 18 | 0 | Structural risk reports |
| Phase 8: US6 Validation | 10 | 0 | Validation framework |
| Phase 9: Polish | 12 | 7 | Cross-cutting concerns |
| **Total** | **145** | **49** | |

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- All data ingestion is automated (no manual collection, no stubs)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
