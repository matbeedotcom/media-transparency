# Research: Media Influence Topology & Detection System

**Branch**: `001-media-influence-detection` | **Date**: 2026-01-26
**Purpose**: Resolve all technical unknowns before implementation

## Data Source Research

### 1. IRS 990 Nonprofit Filings (US)

**Decision**: Use IRS AWS bulk data + incremental API

**Access Method**:
- **Bulk Data**: IRS provides machine-readable 990 data on AWS S3 (`s3://irs-form-990/`)
  - Format: XML files organized by year
  - Coverage: 2011-present, ~500K filings/year
  - Free access, no authentication required
- **Incremental Updates**: IRS Exempt Organizations Select Check (EOSC)
  - Monthly update files
  - Revocation lists for status changes

**Key Data Points**:
- Organization name, EIN, address
- Officers/directors with compensation
- Grants made (Schedule I) - includes recipient names and amounts
- Grants received (Part VIII)
- Related organizations (Schedule R)
- Mission statement, program descriptions

**Implementation Notes**:
```
- Parse XML using lxml or defusedxml
- Extract Schedule I for grant-making relationships (FUNDED_BY edges)
- Extract Part VII for officer relationships (DIRECTOR_OF, EMPLOYED_BY edges)
- EIN serves as stable identifier
- Handle multiple filing years for temporal tracking
```

**Rate Limits**: None for bulk S3; EOSC has no documented limits but recommend 1 req/sec

**Alternatives Considered**:
- ProPublica Nonprofit Explorer API: Easier but limited to 1000 requests/day, missing raw filings
- GuideStar/Candid: Requires paid subscription, not public-source compliant

---

### 2. CRA Registered Charities (Canada)

**Decision**: Use CRA Open Data bulk files

**Access Method**:
- **Bulk Data**: CRA Charities Listings dataset on Canada Open Data Portal
  - URL: `https://open.canada.ca/data/en/dataset/registered-charities`
  - Format: CSV files (organization info) + T3010 annual returns
  - Updated monthly
  - Free, no authentication

**Key Data Points**:
- Business Number (BN) - stable identifier
- Legal name, operating name
- Category codes (charitable activities)
- Directors and trustees
- Gifts to qualified donees (funding relationships)
- Revenue sources

**Implementation Notes**:
```
- CSV parsing with pandas
- BN format: 9 digits + 2 letters + 4 digits (e.g., 123456789RR0001)
- T3010 Schedule 2 contains gifts to qualified donees
- Cross-reference with IRS 990 for US-Canada funding flows
```

**Rate Limits**: Bulk download, no rate limits

**Alternatives Considered**:
- Manual T3010 searches: Not automatable, violates no-manual-collection requirement
- Third-party aggregators: No comprehensive free option exists

---

### 3. OpenCorporates API

**Decision**: Use OpenCorporates REST API with free tier + caching

**Access Method**:
- **API**: `https://api.opencorporates.com/v0.4/`
- **Authentication**: API key (free tier available)
- **Coverage**: 200M+ companies across 140+ jurisdictions

**Key Endpoints**:
- `GET /companies/search` - Search by name
- `GET /companies/{jurisdiction_code}/{company_number}` - Company details
- `GET /officers/search` - Search officers
- `GET /companies/{id}/filings` - Company filings

**Key Data Points**:
- Company name, number, jurisdiction
- Incorporation date, status
- Registered address
- Officers and directors with appointment dates
- Industry classifications
- Related companies (subsidiaries, branches)

**Implementation Notes**:
```python
# Rate limit: 500 req/month (free), 10K/month (starter)
# Strategy:
# 1. Bulk search quarterly for new entities
# 2. Cache responses in PostgreSQL (30-day TTL)
# 3. Prioritize by funding relationship relevance
```

**Rate Limits**:
- Free: 500 API calls/month
- Starter ($49/mo): 10,000 calls/month
- **Recommendation**: Start with Starter tier for MVP, upgrade as needed

**Alternatives Considered**:
- Direct jurisdiction APIs: Fragmented, inconsistent, high maintenance
- Dun & Bradstreet: Expensive, not public-source aligned
- Scraped data: Legal risks, maintenance burden

---

### 4. Meta Ad Library API

**Decision**: Use Meta Ad Library API with daily polling

**Access Method**:
- **API**: `https://graph.facebook.com/v18.0/ads_archive`
- **Authentication**: Facebook App access token (user or system user)
- **Coverage**: Ads about social issues, elections, or politics

**Key Endpoints**:
- `GET /ads_archive` - Search political/social ads
- Parameters: `ad_reached_countries`, `search_terms`, `ad_active_status`, `bylines`

**Key Data Points**:
- Ad ID, creation time, delivery start/end
- Page name and ID (advertiser)
- Funding entity (disclaimer)
- Spend range (lower/upper bounds)
- Impressions range
- Demographic breakdown
- Creative content (text, images, video links)

**Implementation Notes**:
```python
# Access requirements:
# 1. Create Facebook App
# 2. Submit for Ads Transparency access (typically 24-48h approval)
# 3. Generate long-lived system user token
#
# Query strategy:
# - Daily incremental: ads created/modified in last 24h
# - Weekly full scan: catch any missed updates
# - Store ad_archive_id as stable identifier
```

**Rate Limits**:
- 200 calls/hour per app
- Pagination: 25 ads/response (max 100)
- **Recommendation**: Batch queries by date range, cache results

**Alternatives Considered**:
- Ad Library web scraping: Against ToS, fragile
- Third-party ad archives: Incomplete coverage
- Google Ads Transparency: Different API, add in Phase 2

---

## Technology Decisions

### Graph Database: Neo4j

**Decision**: Neo4j Community Edition 5.x

**Rationale**:
1. Native graph storage optimized for relationship traversal
2. Cypher query language intuitive for funding path queries
3. Built-in algorithms (PageRank, community detection, shortest path)
4. Python driver with async support
5. Community edition sufficient for MVP scale

**Alternatives Considered**:
- PostgreSQL + recursive CTEs: Adequate for simple queries but poor for multi-hop traversals at scale
- Amazon Neptune: Managed but expensive, vendor lock-in
- ArangoDB: Multi-model complexity not needed
- NetworkX (in-memory): Won't scale to 1M relationships

**Configuration**:
```yaml
# neo4j.conf key settings
dbms.memory.heap.initial_size=1G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G
```

---

### Entity Resolution Strategy

**Decision**: Hybrid deterministic + embedding-based matching

**Approach**:
1. **Deterministic (high confidence)**:
   - EIN match (US nonprofits)
   - BN match (Canadian charities)
   - OpenCorporates company_number + jurisdiction

2. **Fuzzy (medium confidence)**:
   - Name normalization (remove Inc., Ltd., etc.)
   - Address standardization
   - Levenshtein distance < 0.15

3. **Embedding (low confidence, human review)**:
   - Sentence-transformers for name embeddings
   - Cosine similarity > 0.85 triggers review queue

**Implementation**:
```python
# Priority order for matching
# 1. Exact ID match → confidence: 1.0
# 2. Normalized name + jurisdiction → confidence: 0.9
# 3. Fuzzy name (Levenshtein < 0.1) + same city → confidence: 0.7
# 4. Embedding similarity > 0.9 → confidence: 0.5, queue for review
```

**Alternatives Considered**:
- Pure ML matching: Black box, hard to explain/audit
- Only deterministic: Misses legitimate matches
- Commercial entity resolution (e.g., Senzing): Cost, vendor dependency

---

### Temporal Coordination Detection

**Decision**: Statistical burst detection + lead-lag correlation

**Approach**:
1. **Burst Detection**: Kleinberg's automaton model
   - Identifies periods of unusually high publication frequency
   - Parameters: gap threshold, burst state transitions

2. **Lead-Lag Analysis**: Granger causality tests
   - Identifies outlets that consistently publish before others
   - Window: 24-hour rolling

3. **Synchronization Score**: Jensen-Shannon divergence
   - Compares publication timing distributions between outlet pairs
   - Lower divergence = higher sync

**Hard Negative Filtering**:
```python
# Exclude from coordination scoring:
# 1. Breaking news events (AP/Reuters wire within 1h)
# 2. Scheduled events (earnings, elections)
# 3. High-volume news days (>2σ above baseline)
```

**Alternatives Considered**:
- Simple time window matching: Too many false positives
- LLM-based detection: Expensive, opaque
- Social network cascade detection: Requires platform data we don't have

---

### Credential Management

**Decision**: Environment variables + AWS Secrets Manager compatible

**Implementation**:
```python
# Local development: .env file (gitignored)
# Production: Secrets Manager (AWS, GCP, or Azure)

from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    opencorporates_api_key: str
    meta_app_id: str
    meta_app_secret: str
    meta_access_token: str
    neo4j_password: str
    postgres_password: str

    class Config:
        env_file = ".env"
```

**Rotation Strategy**:
- Meta tokens: Refresh every 60 days (before expiry)
- API keys: Rotate quarterly
- Database passwords: Rotate on security events

---

## Unresolved Items

None. All technical decisions resolved for MVP scope.

## References

1. IRS 990 Data: https://www.irs.gov/charities-non-profits/form-990-series-downloads
2. CRA Open Data: https://open.canada.ca/data/en/dataset/registered-charities
3. OpenCorporates API: https://api.opencorporates.com/documentation/API-Reference
4. Meta Ad Library API: https://www.facebook.com/ads/library/api/
5. Neo4j Python Driver: https://neo4j.com/docs/python-manual/current/
6. Kleinberg Burst Detection: https://www.cs.cornell.edu/home/kleinber/bhs.pdf
