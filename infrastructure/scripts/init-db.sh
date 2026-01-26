#!/bin/bash
# MITDS Database Initialization Script
# Run this after docker-compose up to initialize databases

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== MITDS Database Initialization ==="

# Load environment variables
if [ -f "$SCRIPT_DIR/../../.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/../../.env" | xargs)
fi

# Default values
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-mitds}"
POSTGRES_USER="${POSTGRES_USER:-mitds}"
NEO4J_HOST="${NEO4J_HOST:-localhost}"
NEO4J_BOLT_PORT="${NEO4J_BOLT_PORT:-7687}"

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q' 2>/dev/null; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done
echo "PostgreSQL is ready!"

echo "Waiting for Neo4j to be ready..."
until curl -s "http://${NEO4J_HOST}:7474" > /dev/null 2>&1; do
    echo "Neo4j is unavailable - sleeping"
    sleep 2
done
echo "Neo4j is ready!"

# Run Neo4j initialization
echo "Initializing Neo4j constraints..."
if [ -f "$SCRIPT_DIR/init-neo4j.cypher" ]; then
    # Use cypher-shell if available, otherwise skip
    if command -v cypher-shell &> /dev/null; then
        cypher-shell -a "bolt://${NEO4J_HOST}:${NEO4J_BOLT_PORT}" \
            -u neo4j -p "$NEO4J_PASSWORD" \
            -f "$SCRIPT_DIR/init-neo4j.cypher"
    else
        echo "cypher-shell not found. Run Neo4j initialization manually or via Neo4j browser."
    fi
fi

echo "=== Database Initialization Complete ==="
