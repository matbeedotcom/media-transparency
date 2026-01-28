// MITDS Neo4j Initialization Script
// Creates constraints and indexes for the graph database

// =========================
// Node Constraints (Unique IDs)
// =========================

// Person nodes
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.id IS UNIQUE;

// Organization nodes
CREATE CONSTRAINT org_id IF NOT EXISTS
FOR (o:Organization) REQUIRE o.id IS UNIQUE;

// Outlet nodes
CREATE CONSTRAINT outlet_id IF NOT EXISTS
FOR (o:Outlet) REQUIRE o.id IS UNIQUE;

// Domain nodes
CREATE CONSTRAINT domain_id IF NOT EXISTS
FOR (d:Domain) REQUIRE d.id IS UNIQUE;

// PlatformAccount nodes
CREATE CONSTRAINT platform_account_id IF NOT EXISTS
FOR (p:PlatformAccount) REQUIRE p.id IS UNIQUE;

// Sponsor nodes
CREATE CONSTRAINT sponsor_id IF NOT EXISTS
FOR (s:Sponsor) REQUIRE s.id IS UNIQUE;

// Vendor nodes
CREATE CONSTRAINT vendor_id IF NOT EXISTS
FOR (v:Vendor) REQUIRE v.id IS UNIQUE;

// Election nodes
CREATE CONSTRAINT election_id IF NOT EXISTS
FOR (e:Election) REQUIRE e.election_id IS UNIQUE;

// MediaType nodes
CREATE CONSTRAINT media_type_name IF NOT EXISTS
FOR (m:MediaType) REQUIRE m.name IS UNIQUE;

// =========================
// Business Key Indexes
// =========================

// Organization EIN (US Tax ID)
CREATE INDEX org_ein IF NOT EXISTS
FOR (o:Organization) ON (o.ein);

// Organization BN (Canadian Business Number)
CREATE INDEX org_bn IF NOT EXISTS
FOR (o:Organization) ON (o.bn);

// Organization OpenCorporates ID
CREATE INDEX org_opencorp_id IF NOT EXISTS
FOR (o:Organization) ON (o.opencorp_id);

// Domain name
CREATE INDEX domain_name IF NOT EXISTS
FOR (d:Domain) ON (d.domain_name);

// Outlet name (for search)
CREATE INDEX outlet_name IF NOT EXISTS
FOR (o:Outlet) ON (o.name);

// Organization name (for search)
CREATE INDEX org_name IF NOT EXISTS
FOR (o:Organization) ON (o.name);

// Person name (for search)
CREATE INDEX person_name IF NOT EXISTS
FOR (p:Person) ON (p.name);

// =========================
// Relationship Indexes
// =========================

// FUNDED_BY temporal index
CREATE INDEX rel_funded_by_temporal IF NOT EXISTS
FOR ()-[r:FUNDED_BY]-() ON (r.valid_from, r.valid_to);

// FUNDED_BY amount index for filtering by funding amount
CREATE INDEX rel_funded_by_amount IF NOT EXISTS
FOR ()-[r:FUNDED_BY]-() ON (r.amount);

// FUNDED_BY fiscal year for year-based queries
CREATE INDEX rel_funded_by_fiscal_year IF NOT EXISTS
FOR ()-[r:FUNDED_BY]-() ON (r.fiscal_year);

// DIRECTOR_OF temporal index
CREATE INDEX rel_director_of_temporal IF NOT EXISTS
FOR ()-[r:DIRECTOR_OF]-() ON (r.valid_from, r.valid_to);

// EMPLOYED_BY temporal index
CREATE INDEX rel_employed_by_temporal IF NOT EXISTS
FOR ()-[r:EMPLOYED_BY]-() ON (r.valid_from, r.valid_to);

// OWNS temporal index
CREATE INDEX rel_owns_temporal IF NOT EXISTS
FOR ()-[r:OWNS]-() ON (r.valid_from, r.valid_to);

// OWNS percentage for ownership filtering
CREATE INDEX rel_owns_percentage IF NOT EXISTS
FOR ()-[r:OWNS]-() ON (r.ownership_percentage);

// =========================
// Composite Indexes for Common Queries
// =========================

// Organization by type and status
CREATE INDEX org_type_status IF NOT EXISTS
FOR (o:Organization) ON (o.org_type, o.status);

// Organization by jurisdiction
CREATE INDEX org_jurisdiction IF NOT EXISTS
FOR (o:Organization) ON (o.jurisdiction);

// Person by organization (for board/employee lookups)
CREATE INDEX person_primary_org IF NOT EXISTS
FOR (p:Person) ON (p.primary_organization_id);

// Entity type lookup (generic node label + type)
CREATE INDEX org_entity_type IF NOT EXISTS
FOR (o:Organization) ON (o.entity_type);

// SEC CIK lookup (for SEC EDGAR data)
CREATE INDEX org_sec_cik IF NOT EXISTS
FOR (o:Organization) ON (o.sec_cik);

// Person SEC CIK lookup (for Form 4 insider data)
CREATE INDEX person_sec_cik IF NOT EXISTS
FOR (p:Person) ON (p.sec_cik);

// Canada Corporation number lookup
CREATE INDEX org_canada_corp IF NOT EXISTS
FOR (o:Organization) ON (o.canada_corp_num);

// Lobbying registration lookup
CREATE INDEX org_lobbying_reg IF NOT EXISTS
FOR (o:Organization) ON (o.lobbying_registration);

// Person lobbyist type lookup
CREATE INDEX person_lobbyist_type IF NOT EXISTS
FOR (p:Person) ON (p.lobbyist_type);

// Government organization flag
CREATE INDEX org_is_government IF NOT EXISTS
FOR (o:Organization) ON (o.is_government);

// LOBBIES_FOR relationship index
CREATE INDEX rel_lobbies_for_temporal IF NOT EXISTS
FOR ()-[r:LOBBIES_FOR]-() ON (r.valid_from, r.valid_to);

// LOBBIED relationship index
CREATE INDEX rel_lobbied_registration IF NOT EXISTS
FOR ()-[r:LOBBIED]-() ON (r.registration_id);

// =========================
// Elections Canada Indexes
// =========================

// Election third party flag
CREATE INDEX org_is_election_third_party IF NOT EXISTS
FOR (o:Organization) ON (o.is_election_third_party);

// Election date index
CREATE INDEX election_date IF NOT EXISTS
FOR (e:Election) ON (e.election_date);

// Election jurisdiction
CREATE INDEX election_jurisdiction IF NOT EXISTS
FOR (e:Election) ON (e.jurisdiction);

// REGISTERED_FOR relationship (org -> election)
CREATE INDEX rel_registered_for_election IF NOT EXISTS
FOR ()-[r:REGISTERED_FOR]-() ON (r.registered_date);

// ADVERTISED_ON relationship (org -> media type)
CREATE INDEX rel_advertised_on_amount IF NOT EXISTS
FOR ()-[r:ADVERTISED_ON]-() ON (r.amount);

CREATE INDEX rel_advertised_on_election IF NOT EXISTS
FOR ()-[r:ADVERTISED_ON]-() ON (r.election_id);

// FINANCIAL_AGENT_FOR relationship
CREATE INDEX rel_financial_agent_election IF NOT EXISTS
FOR ()-[r:FINANCIAL_AGENT_FOR]-() ON (r.election_id);

// AUDITED_BY relationship
CREATE INDEX rel_audited_by_election IF NOT EXISTS
FOR ()-[r:AUDITED_BY]-() ON (r.election_id);

// =========================
// Full-text search indexes
// =========================

// Full-text index for entity name search
CREATE FULLTEXT INDEX entity_name_search IF NOT EXISTS
FOR (n:Person|Organization|Outlet|Sponsor|Election)
ON EACH [n.name];

// =========================
// Verification
// =========================
// List all constraints
SHOW CONSTRAINTS;

// List all indexes
SHOW INDEXES;
