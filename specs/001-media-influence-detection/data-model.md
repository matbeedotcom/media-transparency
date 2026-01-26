# Data Model: Media Influence Topology & Detection System

**Branch**: `001-media-influence-detection` | **Date**: 2026-01-26
**Derived From**: [spec.md](spec.md) Key Entities, [research.md](research.md) Data Sources

## Entity Schemas

### Core Entities

#### Entity (Abstract Base)

All entities share common provenance fields:

```python
class EntityBase:
    id: UUID                      # Internal stable identifier
    entity_type: EntityType       # Discriminator
    created_at: datetime          # First ingestion timestamp
    updated_at: datetime          # Last update timestamp
    confidence: float             # Resolution confidence (0.0-1.0)
    source_ids: list[SourceRef]   # Evidence references
```

---

#### Person

Individuals with potential influence (journalists, executives, board members, donors).

```python
class Person(EntityBase):
    entity_type = "PERSON"

    # Identifying attributes
    name: str                          # Canonical name
    aliases: list[str]                 # Known alternate names

    # Source identifiers (when available)
    irs_990_name: str | None           # Name as appears in IRS 990
    opencorp_officer_id: str | None    # OpenCorporates officer ID

    # Demographics (when disclosed)
    location: str | None               # City, State/Province

    # Derived
    roles: list[RoleAssignment]        # Time-bounded role assignments
```

**Validation Rules**:
- `name` required, min 2 characters
- `aliases` may be empty
- At least one `source_id` required

---

#### Organization

Legal entities (companies, nonprofits, foundations, political organizations).

```python
class Organization(EntityBase):
    entity_type = "ORGANIZATION"

    # Identifying attributes
    name: str                          # Canonical legal name
    aliases: list[str]                 # DBA names, former names
    org_type: OrgType                  # NONPROFIT, CORPORATION, FOUNDATION, POLITICAL_ORG

    # Jurisdiction
    jurisdiction: str                  # Country code (US, CA) or state/province
    registration_id: str | None        # EIN (US), BN (CA), company number

    # Source identifiers
    ein: str | None                    # US Employer Identification Number
    bn: str | None                     # Canadian Business Number
    opencorp_id: str | None            # OpenCorporates company URL

    # Metadata
    incorporation_date: date | None
    status: OrgStatus                  # ACTIVE, INACTIVE, REVOKED, UNKNOWN
    address: Address | None

class OrgType(Enum):
    NONPROFIT = "nonprofit"
    CORPORATION = "corporation"
    FOUNDATION = "foundation"
    POLITICAL_ORG = "political_org"
    UNKNOWN = "unknown"

class OrgStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    REVOKED = "revoked"
    UNKNOWN = "unknown"
```

**Validation Rules**:
- `name` required, min 2 characters
- `jurisdiction` required, ISO 3166-1 alpha-2 or subdivision
- For US nonprofits: `ein` should match `^\d{2}-\d{7}$`
- For Canadian charities: `bn` should match `^\d{9}RR\d{4}$`

---

#### Outlet

Media publications or broadcast operations.

```python
class Outlet(EntityBase):
    entity_type = "OUTLET"

    # Identifying attributes
    name: str                          # Publication name
    aliases: list[str]                 # Former names, short names

    # Digital presence
    domains: list[str]                 # Associated domains (e.g., ["example.com", "news.example.com"])
    platform_accounts: list[UUID]      # References to PlatformAccount entities

    # Classification
    media_type: MediaType              # DIGITAL, PRINT, BROADCAST, PODCAST
    editorial_focus: list[str]         # Topic tags

    # Ownership (via relationships)
    owner_org_id: UUID | None          # Reference to Organization if known

class MediaType(Enum):
    DIGITAL = "digital"
    PRINT = "print"
    BROADCAST = "broadcast"
    PODCAST = "podcast"
    HYBRID = "hybrid"
```

**Validation Rules**:
- `name` required
- `domains` should be valid domain names (no protocol)
- At least one of `domains` or `platform_accounts` required

---

#### Domain

Web domains associated with outlets or organizations.

```python
class Domain(EntityBase):
    entity_type = "DOMAIN"

    # Identifying attributes
    domain_name: str                   # e.g., "example.com"

    # Infrastructure (from DNS/WHOIS)
    registrar: str | None
    registration_date: date | None
    expiration_date: date | None
    nameservers: list[str]

    # Hosting
    hosting_provider: str | None       # Detected via IP/ASN
    cdn_provider: str | None           # CloudFlare, Akamai, etc.

    # Analytics/Tracking (from page analysis)
    analytics_ids: list[AnalyticsTag]  # GA, GTM, FB Pixel, etc.
    ad_networks: list[str]             # AdSense, etc.

class AnalyticsTag:
    platform: str                      # "google_analytics", "facebook_pixel", etc.
    tag_id: str                        # The tracking ID
    detected_at: datetime
```

**Validation Rules**:
- `domain_name` must be valid domain (no subdomain by default)
- Subdomains tracked separately if editorially distinct

---

#### PlatformAccount

Social media or platform presence.

```python
class PlatformAccount(EntityBase):
    entity_type = "PLATFORM_ACCOUNT"

    # Identifying attributes
    platform: Platform                 # FACEBOOK, TWITTER, YOUTUBE, etc.
    handle: str                        # Username/handle
    platform_id: str | None            # Platform's internal ID if available

    # Metadata
    display_name: str | None
    verified: bool | None
    created_at_platform: datetime | None

    # Metrics (time-series, stored separately)
    follower_count: int | None         # Latest known
    follower_count_updated: datetime | None

class Platform(Enum):
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    TWITTER = "twitter"
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    LINKEDIN = "linkedin"
    OTHER = "other"
```

**Validation Rules**:
- `platform` required
- `handle` required, format validated per platform

---

#### Sponsor

Entities providing financial support.

```python
class Sponsor(EntityBase):
    entity_type = "SPONSOR"

    # This is typically an alias for Organization
    # Created when funding source identified before full org resolution

    name: str                          # Funding entity name (from ad disclaimers, etc.)
    resolved_org_id: UUID | None       # Link to Organization after resolution

    # Meta Ad Library specific
    meta_page_id: str | None
    meta_disclaimer_text: str | None

```

**Validation Rules**:
- `name` required
- Should be resolved to Organization when possible

---

#### Vendor

Service providers shared across entities.

```python
class Vendor(EntityBase):
    entity_type = "VENDOR"

    name: str
    service_type: VendorType

    # For infrastructure vendors
    domain: str | None

    # Resolved organization
    resolved_org_id: UUID | None

class VendorType(Enum):
    HOSTING = "hosting"
    ANALYTICS = "analytics"
    AD_NETWORK = "ad_network"
    CDN = "cdn"
    DNS = "dns"
    LEGAL = "legal"
    PR = "pr"
    CONSULTING = "consulting"
    OTHER = "other"
```

---

### Relationship Types

All relationships are temporal and evidence-linked:

```python
class Relationship:
    id: UUID
    rel_type: RelationType
    source_entity_id: UUID
    target_entity_id: UUID

    # Temporal bounds
    valid_from: datetime | None        # When relationship started
    valid_to: datetime | None          # When relationship ended (None = current)

    # Evidence
    confidence: float                  # 0.0-1.0
    evidence_refs: list[EvidenceRef]

    # Metadata (varies by type)
    properties: dict[str, Any]

class RelationType(Enum):
    FUNDED_BY = "FUNDED_BY"            # org/outlet ← sponsor/org
    DIRECTOR_OF = "DIRECTOR_OF"        # person → org
    EMPLOYED_BY = "EMPLOYED_BY"        # person → org/outlet
    SPONSORED_BY = "SPONSORED_BY"      # ad/content ← sponsor
    OWNS = "OWNS"                      # org → org/outlet
    CITED = "CITED"                    # content → content
    AMPLIFIED = "AMPLIFIED"            # account → content
    SHARED_INFRA = "SHARED_INFRA"      # outlet ↔ outlet (via vendor)
```

#### Relationship Property Schemas

**FUNDED_BY**:
```python
properties = {
    "amount": float | None,            # USD (normalized)
    "amount_currency": str,            # Original currency
    "fiscal_year": int | None,
    "grant_purpose": str | None,       # From Schedule I
}
```

**DIRECTOR_OF / EMPLOYED_BY**:
```python
properties = {
    "title": str | None,
    "compensation": float | None,      # Annual, USD
    "hours_per_week": float | None,
}
```

**SHARED_INFRA**:
```python
properties = {
    "shared_vendor_id": UUID,
    "service_type": VendorType,
}
```

---

### Evidence Model

Every relationship and entity must have evidence:

```python
class Evidence:
    id: UUID
    evidence_type: EvidenceType

    # Source reference
    source_url: str                    # Original URL
    source_archive_url: str | None     # Archive.org or local archive
    retrieved_at: datetime

    # Extraction
    extractor: str                     # Module that extracted (e.g., "irs990.schedule_i")
    extractor_version: str
    raw_data_ref: str                  # S3 path to raw source file

    # Confidence
    extraction_confidence: float       # How confident in extraction

    # Content hash for integrity
    content_hash: str                  # SHA-256 of source document

class EvidenceType(Enum):
    IRS_990_FILING = "irs_990_filing"
    CRA_T3010 = "cra_t3010"
    OPENCORP_RECORD = "opencorp_record"
    META_AD = "meta_ad"
    WHOIS_RECORD = "whois_record"
    DNS_LOOKUP = "dns_lookup"
    PAGE_ANALYSIS = "page_analysis"
    MANUAL_RESEARCH = "manual_research"  # For human-in-loop reconciliation
```

---

### Event Model

Time-stamped occurrences for audit and temporal analysis:

```python
class Event:
    id: UUID
    event_type: EventType
    occurred_at: datetime

    # Involved entities
    entity_ids: list[UUID]
    relationship_id: UUID | None

    # Details
    description: str
    properties: dict[str, Any]

    # Provenance
    evidence_ref: UUID
    detected_at: datetime              # When system detected this

class EventType(Enum):
    # Funding events
    GRANT_RECEIVED = "grant_received"
    GRANT_MADE = "grant_made"
    SPONSORSHIP_STARTED = "sponsorship_started"
    SPONSORSHIP_ENDED = "sponsorship_ended"

    # Organizational events
    BOARD_APPOINTMENT = "board_appointment"
    BOARD_DEPARTURE = "board_departure"
    INCORPORATION = "incorporation"
    DISSOLUTION = "dissolution"
    MERGER = "merger"
    NAME_CHANGE = "name_change"

    # Publication events
    ARTICLE_PUBLISHED = "article_published"
    AD_LAUNCHED = "ad_launched"
    AD_ENDED = "ad_ended"

    # Infrastructure events
    HOSTING_CHANGED = "hosting_changed"
    DOMAIN_REGISTERED = "domain_registered"
```

---

## Database Schemas

### PostgreSQL (Event Store & Metadata)

```sql
-- Immutable event store
CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    entity_ids UUID[] NOT NULL,
    relationship_id UUID,
    description TEXT,
    properties JSONB,
    evidence_ref UUID NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Immutability: no UPDATE/DELETE allowed via triggers
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_occurred ON events(occurred_at);
CREATE INDEX idx_events_entities ON events USING GIN(entity_ids);

-- Evidence archive
CREATE TABLE evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evidence_type VARCHAR(50) NOT NULL,
    source_url TEXT NOT NULL,
    source_archive_url TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL,
    extractor VARCHAR(100) NOT NULL,
    extractor_version VARCHAR(20) NOT NULL,
    raw_data_ref TEXT NOT NULL,
    extraction_confidence FLOAT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_evidence_type ON evidence(evidence_type);
CREATE INDEX idx_evidence_hash ON evidence(content_hash);

-- Source snapshots (for dead link protection)
CREATE TABLE source_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evidence_id UUID REFERENCES evidence(id),
    snapshot_url TEXT NOT NULL,  -- Local/S3 archive path
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_type VARCHAR(100),
    size_bytes BIGINT
);

-- Audit log
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action VARCHAR(50) NOT NULL,
    user_id VARCHAR(100),  -- Analyst identifier
    entity_type VARCHAR(50),
    entity_id UUID,
    query_text TEXT,
    report_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Data quality metrics
CREATE TABLE ingestion_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL,  -- running, completed, failed
    records_processed INT,
    records_created INT,
    records_updated INT,
    duplicates_found INT,
    errors JSONB
);
```

### Neo4j (Graph Store)

```cypher
// Node labels match EntityType enum
// All nodes have common properties:
// - id (UUID)
// - created_at, updated_at (datetime)
// - confidence (float)
// - source_ids (list of evidence UUIDs)

// Person
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.id IS UNIQUE;

// Organization
CREATE CONSTRAINT org_id IF NOT EXISTS
FOR (o:Organization) REQUIRE o.id IS UNIQUE;

CREATE INDEX org_ein IF NOT EXISTS
FOR (o:Organization) ON (o.ein);

CREATE INDEX org_bn IF NOT EXISTS
FOR (o:Organization) ON (o.bn);

// Outlet
CREATE CONSTRAINT outlet_id IF NOT EXISTS
FOR (o:Outlet) REQUIRE o.id IS UNIQUE;

// Relationship indexes
CREATE INDEX rel_funded_by IF NOT EXISTS
FOR ()-[r:FUNDED_BY]-() ON (r.valid_from, r.valid_to);

CREATE INDEX rel_director_of IF NOT EXISTS
FOR ()-[r:DIRECTOR_OF]-() ON (r.valid_from, r.valid_to);
```

---

## State Transitions

### Organization Status

```
UNKNOWN → ACTIVE      (incorporation detected)
ACTIVE → INACTIVE     (no recent filings, >2 years)
ACTIVE → REVOKED      (IRS/CRA revocation detected)
INACTIVE → ACTIVE     (new filing detected)
REVOKED → ACTIVE      (reinstatement detected)
```

### Relationship Lifecycle

```
Created: valid_from set, valid_to = NULL
Ended: valid_to set to end date
Superseded: new relationship created, old one ended
```

### Entity Resolution States

```
UNRESOLVED → CANDIDATE    (potential match found)
CANDIDATE → RESOLVED      (confidence > 0.7 or human confirmed)
CANDIDATE → REJECTED      (human rejected or low confidence)
RESOLVED → MERGED         (entities combined)
```

---

## Data Volume Estimates (MVP)

| Entity Type | Estimated Count | Source |
|-------------|-----------------|--------|
| Organization | 50,000 | IRS 990 + CRA + OpenCorp |
| Person | 30,000 | Directors from filings |
| Outlet | 5,000 | Media organizations |
| Domain | 10,000 | Outlet domains |
| PlatformAccount | 5,000 | Social presence |
| Sponsor | 2,000 | Ad library disclaimers |
| Vendor | 500 | Infrastructure providers |
| **Total Entities** | ~100,000 | |

| Relationship Type | Estimated Count |
|-------------------|-----------------|
| FUNDED_BY | 500,000 |
| DIRECTOR_OF | 200,000 |
| EMPLOYED_BY | 100,000 |
| SHARED_INFRA | 50,000 |
| OWNS | 10,000 |
| Other | 140,000 |
| **Total Relationships** | ~1,000,000 |
