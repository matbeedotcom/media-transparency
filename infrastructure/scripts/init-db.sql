-- MITDS Database Initialization Script
-- Run automatically by PostgreSQL container on first startup

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schema for MITDS
CREATE SCHEMA IF NOT EXISTS mitds;

-- Set search path
SET search_path TO mitds, public;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA mitds TO mitds;
