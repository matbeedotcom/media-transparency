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

// DIRECTOR_OF temporal index
CREATE INDEX rel_director_of_temporal IF NOT EXISTS
FOR ()-[r:DIRECTOR_OF]-() ON (r.valid_from, r.valid_to);

// EMPLOYED_BY temporal index
CREATE INDEX rel_employed_by_temporal IF NOT EXISTS
FOR ()-[r:EMPLOYED_BY]-() ON (r.valid_from, r.valid_to);

// OWNS temporal index
CREATE INDEX rel_owns_temporal IF NOT EXISTS
FOR ()-[r:OWNS]-() ON (r.valid_from, r.valid_to);

// =========================
// Full-text search indexes
// =========================

// Full-text index for entity name search
CREATE FULLTEXT INDEX entity_name_search IF NOT EXISTS
FOR (n:Person|Organization|Outlet|Sponsor)
ON EACH [n.name];

// =========================
// Verification
// =========================
// List all constraints
SHOW CONSTRAINTS;

// List all indexes
SHOW INDEXES;
