# Feature Specification: Media Influence Topology & Detection System (MITDS)

**Feature Branch**: `001-media-influence-detection`
**Created**: 2026-01-26
**Status**: Draft
**Input**: User description: "Media Influence Topology and Detection System - Detect, characterize, and explain structural media manipulation for political gain using reproducible technical methods"

## Clarifications

### Session 2026-01-26

- Q: Which primary data sources should be implemented for MVP? → A: US + Canada focus: IRS 990s (nonprofit filings), CRA charities, OpenCorporates API, Meta Ad Library
- Q: How often should the system refresh data from sources? → A: Daily for ad libraries, weekly for corporate/charity registries
- Q: How should the system handle ingestion failures (API down, rate-limited)? → A: Retry with exponential backoff, alert after 3 failures, continue operating with stale data
- Q: What data volume should the MVP support? → A: Medium scale: ~100K entities, ~1M relationships (regional US + Canada analysis)
- Q: How should API credentials for data sources be managed? → A: Environment-based credentials with secrets manager support

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Explore Funding Relationships (Priority: P1)

An investigative journalist wants to understand the funding relationships behind a network of media outlets covering a specific political topic. They need to see which organizations fund which outlets, identify shared funders across seemingly independent outlets, and understand the depth of financial dependencies.

**Why this priority**: Understanding funding is the foundation of detecting structural manipulation. Without funding transparency, other coordination signals lack context. This is the most requested capability by journalists and researchers.

**Independent Test**: Can be fully tested by loading a sample dataset of known organizations and funding relationships, then querying "Show funding clusters for outlets covering [topic]" and verifying the system returns accurate, evidence-linked funding networks.

**Acceptance Scenarios**:

1. **Given** a media outlet in the system, **When** the analyst requests funding information, **Then** the system displays all known funders with evidence links, confidence scores, and timestamps for each relationship
2. **Given** multiple outlets, **When** the analyst requests shared funders, **Then** the system identifies and visualizes organizations that fund multiple outlets with the funding amounts and time periods
3. **Given** a funder organization, **When** the analyst traces downstream influence, **Then** the system shows all funded outlets, intermediaries, and the total funding flowing through each path

---

### User Story 2 - Detect Temporal Coordination (Priority: P1)

A researcher suspects that multiple media outlets are coordinating their coverage timing around specific events. They want to detect patterns where outlets publish similar narratives within suspiciously short time windows, distinguish this from normal news cycle behavior, and see the evidence supporting any coordination findings.

**Why this priority**: Temporal coordination is the strongest behavioral signal of structural manipulation and directly addresses the core problem of detecting coordinated influence that appears organic.

**Independent Test**: Can be fully tested by injecting a synthetic coordination pattern into a dataset alongside legitimate news coverage, then verifying the system detects the coordinated cluster while correctly ignoring the organic coverage.

**Acceptance Scenarios**:

1. **Given** a time period and topic, **When** the analyst requests coordination analysis, **Then** the system identifies publication clusters with abnormal timing synchronization and assigns a coordination confidence score
2. **Given** a detected coordination cluster, **When** the analyst requests explanation, **Then** the system shows a timeline visualization with publication timestamps, statistical analysis of timing patterns, and comparison to baseline news cycle behavior
3. **Given** a major breaking news event, **When** many outlets publish simultaneously, **Then** the system correctly identifies this as normal news behavior (hard negative) and does not flag it as suspicious coordination

---

### User Story 3 - Map Entity Relationships (Priority: P2)

An analyst needs to understand the organizational structure behind a media ecosystem - who owns what, who sits on which boards, which consultants or vendors serve multiple outlets, and how these relationships change over time.

**Why this priority**: Entity relationships reveal hidden connections that funding alone cannot show. Board memberships, shared vendors, and employment relationships often indicate coordination pathways.

**Independent Test**: Can be fully tested by loading a dataset with known organizational relationships and verifying that queries return accurate entity graphs with correct relationship types and temporal validity.

**Acceptance Scenarios**:

1. **Given** a person or organization, **When** the analyst requests relationship mapping, **Then** the system displays all known relationships (ownership, directorship, employment, vendor relationships) with evidence sources
2. **Given** two entities, **When** the analyst requests connection paths, **Then** the system finds and displays all direct and indirect connections between them, ranked by evidence strength
3. **Given** a time range, **When** the analyst requests historical view, **Then** the system shows how entity relationships changed over that period, highlighting significant events (board changes, acquisitions, new funding)

---

### User Story 4 - Detect Infrastructure Sharing (Priority: P2)

A researcher suspects multiple outlets share technical infrastructure (hosting, ad networks, analytics, content management). They want to identify shared infrastructure that suggests coordination even when formal organizational links are hidden.

**Why this priority**: Infrastructure sharing is a strong signal of coordination that persists even when ownership and funding are carefully obscured. It provides independent corroboration of other signals.

**Independent Test**: Can be fully tested by loading domain metadata for a set of outlets and verifying the system correctly identifies those sharing hosting providers, analytics tools, or ad networks.

**Acceptance Scenarios**:

1. **Given** a set of media domains, **When** the analyst requests infrastructure analysis, **Then** the system identifies shared hosting, DNS, ad networks, analytics platforms, and content delivery networks
2. **Given** detected infrastructure overlap, **When** the analyst requests details, **Then** the system shows which specific services are shared, when the sharing was detected, and historical changes
3. **Given** a single vendor/provider, **When** the analyst requests clients, **Then** the system shows all outlets using that provider with timeline of adoption

---

### User Story 5 - Generate Structural Risk Report (Priority: P3)

A civil society organization or policymaker needs a comprehensive, defensible report on the structural risks in a media ecosystem - concentration of ownership, funding dependencies, coordination patterns - written in neutral language suitable for publication.

**Why this priority**: Reports are the primary output for external stakeholders. Without defensible, explainable reports, detection findings cannot inform public debate or policy.

**Independent Test**: Can be fully tested by running a full analysis on a sample dataset and generating a report, then having a legal reviewer confirm the language is non-accusatory and evidence-linked.

**Acceptance Scenarios**:

1. **Given** a completed analysis, **When** the analyst requests a structural risk report, **Then** the system generates a report with executive summary, methodology explanation, key findings with evidence, confidence intervals, and explicit limitations
2. **Given** a generated report, **When** reviewed by external parties, **Then** the report uses risk-based phrasing, makes no attribution of intent, and includes explicit uncertainty statements for all findings
3. **Given** any finding in the report, **When** the reader follows evidence links, **Then** they can access the original public sources supporting that finding

---

### User Story 6 - Validate Against Known Cases (Priority: P3)

A system administrator or auditor needs to verify the detection system works correctly by testing it against documented historical influence operations and confirming it would have detected them.

**Why this priority**: Validation against known cases is essential for establishing credibility and understanding the system's detection envelope.

**Independent Test**: Can be fully tested by loading golden datasets of known influence operations and verifying the system flags them with appropriate confidence levels.

**Acceptance Scenarios**:

1. **Given** a golden dataset of a documented influence operation, **When** the system analyzes it, **Then** the coordination patterns are detected and flagged with high confidence
2. **Given** synthetic test data with known coordination injected, **When** the system analyzes it, **Then** detection rates match expected performance thresholds
3. **Given** analysis results, **When** compared to ground truth, **Then** false positive and false negative rates are calculated and displayed on a metrics dashboard

---

### Edge Cases

- What happens when an entity changes names or merges with another organization? The system must maintain entity continuity and show the transformation history
- How does the system handle when evidence sources become unavailable (dead links, deleted filings)? Archived snapshots must preserve evidentiary chain
- What happens when the same person has multiple roles across organizations? The system must correctly represent multiple concurrent relationships
- How does the system handle partial data (known funder but unknown amount)? Confidence scores must reflect data completeness
- What happens during major news events when many outlets legitimately coordinate coverage timing? Hard negative detection must prevent false positives
- What happens when a data source API is unavailable or rate-limited? System retries with exponential backoff, alerts after 3 failures, and continues serving existing data with staleness indicators

## Requirements *(mandatory)*

### Functional Requirements

**Data Ingestion & Storage**

- **FR-001**: System MUST ingest data only from public sources. MVP sources: IRS 990 nonprofit filings (US), CRA registered charities (Canada), OpenCorporates API (corporate registries), Meta Ad Library (political advertising)
- **FR-002**: System MUST store immutable snapshots of all source data with timestamps, preventing retroactive modification of evidence
- **FR-003**: System MUST track provenance for all data, recording the source URL, retrieval timestamp, and parser/extractor that processed it
- **FR-004**: System MUST support incremental updates, adding new data without full reprocessing of existing data
- **FR-004a**: System MUST automatically refresh Meta Ad Library data daily; corporate registries (OpenCorporates) and charity filings (IRS 990, CRA) weekly
- **FR-004b**: System MUST handle source unavailability by retrying with exponential backoff, generating alerts after 3 consecutive failures, and continuing to serve existing data until source recovers
- **FR-004c**: System MUST manage API credentials via environment variables with secrets manager support; no credentials stored in code or configuration files

**Entity Management**

- **FR-005**: System MUST support entity types: Person, Organization, Outlet, Domain, Platform Account, Sponsor, Vendor
- **FR-006**: System MUST support relationship types: FUNDED_BY, DIRECTOR_OF, EMPLOYED_BY, SPONSORED_BY, CITED, AMPLIFIED, SHARED_INFRA
- **FR-007**: System MUST resolve entities across data sources, matching the same real-world entity appearing in different datasets
- **FR-008**: System MUST maintain stable entity identifiers across system updates, ensuring external references remain valid
- **FR-009**: System MUST support human-in-the-loop entity reconciliation when automated matching confidence is below threshold

**Evidence & Confidence**

- **FR-010**: System MUST require evidence for every relationship edge, with no relationship stored without a source reference
- **FR-011**: System MUST assign confidence scores to all relationships based on evidence quality and quantity
- **FR-012**: System MUST track temporal validity for all relationships, recording when relationships began and ended
- **FR-013**: System MUST distinguish between direct evidence and inferred relationships, displaying inference chains when applicable

**Detection & Analysis**

- **FR-014**: System MUST detect temporal coordination patterns in publication timing across outlets
- **FR-015**: System MUST detect funding clusters showing shared financial dependencies among outlets
- **FR-016**: System MUST detect infrastructure sharing including hosting, analytics, ad networks, and content delivery
- **FR-017**: System MUST calculate composite coordination scores combining multiple signal types, with no single signal capable of triggering a flag alone
- **FR-018**: System MUST support hard negative detection, correctly identifying legitimate synchronization (breaking news, major events) and not flagging it
- **FR-019**: System MUST support time-sliced analysis, showing how coordination patterns change over specified periods

**Querying & Visualization**

- **FR-020**: System MUST support analyst queries for funding clusters, shared vendors, and narrative synchronization timelines
- **FR-021**: System MUST provide graph visualization showing entities and relationships with configurable filtering
- **FR-022**: System MUST provide timeline visualization showing events and publication patterns over time
- **FR-023**: System MUST provide evidence panels showing source documentation for any displayed relationship or finding

**Reporting & Explanation**

- **FR-024**: System MUST generate "why flagged" explanations for any coordination detection, showing contributing factors and evidence
- **FR-025**: System MUST generate reports using risk-based phrasing without attribution of intent
- **FR-026**: System MUST include explicit uncertainty statements and confidence bands in all outputs
- **FR-027**: System MUST support multiple report templates: structural risk reports, influence topology summaries, timeline narratives

**Data Quality & Auditability**

- **FR-028**: System MUST track and report data quality metrics: duplicate rate, resolution confidence, missing-field rates
- **FR-029**: System MUST support reproducible analysis, allowing any finding to be regenerated from stored data
- **FR-030**: System MUST maintain audit logs of all analyst queries and report generations

### Key Entities

- **Person**: An individual with potential influence (journalist, executive, board member, donor). Key attributes: name, known aliases, roles over time
- **Organization**: A legal entity (company, nonprofit, foundation, political organization). Key attributes: name, type, jurisdiction, registration identifiers
- **Outlet**: A media publication or broadcast operation. Key attributes: name, domains, platform presence, editorial focus
- **Domain**: A web domain associated with an outlet or organization. Key attributes: domain name, hosting infrastructure, registration details
- **Platform Account**: A social media or platform presence. Key attributes: platform, handle, follower metrics, verification status
- **Sponsor**: An entity providing financial support (advertiser, underwriter, donor). Key attributes: name, type, funding amounts over time
- **Vendor**: A service provider shared across entities (hosting, analytics, legal, PR). Key attributes: service type, client relationships
- **Event**: A time-stamped occurrence capturing changes (funding received, article published, board appointment). Key attributes: timestamp, event type, involved entities, evidence source

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Known documented influence operations from golden datasets are detected with at least 85% recall
- **SC-002**: False positive rate on hard negative test suite (major news events, legitimate campaigns) remains below 5%
- **SC-003**: Any finding can be traced to source evidence within 3 clicks/interactions
- **SC-004**: Generated reports pass legal review for non-accusatory language in 95% of cases
- **SC-005**: System detects coordination patterns at least 48 hours before patterns are publicly reported by journalists in test scenarios
- **SC-006**: Analyst can query funding relationships for any outlet and receive results within 10 seconds
- **SC-007**: System maintains stable entity identifiers with less than 1% identifier changes between releases
- **SC-008**: At least one external organization (journalist, academic, civil society) validates the methodology and adopts the system for pilot use
- **SC-009**: System explains findings at a level understandable by non-technical stakeholders (validated by external review)
- **SC-010**: Documented performance envelope clearly states what the system can and cannot detect, with known failure modes explicitly listed

## Assumptions

- MVP scale target: approximately 100,000 entities and 1,000,000 relationships (sufficient for regional US + Canada analysis)
- Public data sources (corporate registries, charity filings, ad libraries) remain accessible and provide sufficient information for meaningful analysis
- Coordination patterns in structural media manipulation produce detectable temporal and organizational signals distinguishable from organic behavior
- External reviewers (journalists, academics, legal experts) will be available for methodology validation
- Historical documented influence operations provide sufficient ground truth data for system validation

## Constraints

- System uses only publicly available data; no privileged access, private data collection, or platform cooperation required
- System does not evaluate, judge, or classify political viewpoints or ideological positions
- System does not moderate content or recommend takedowns
- System does not attribute intent or motives to detected patterns
- All language in outputs uses structural and risk-based framing, never accusatory language

## Out of Scope

- Viewpoint classification or ideological labeling
- Content moderation or truthfulness assessment
- Real-time monitoring or alerting (initial release focuses on analytical use)
- Automated takedown recommendations or enforcement actions
- Attribution of specific malicious intent to actors
- Collection of non-public or covertly obtained data
