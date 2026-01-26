"""Initial database schema

Revision ID: 001_initial
Revises:
Create Date: 2026-01-26

Creates the foundational PostgreSQL tables:
- events: Immutable event store
- evidence: Source evidence tracking
- source_snapshots: Dead link protection
- audit_log: Analyst query logging
- ingestion_runs: Data quality metrics
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create UUID extension if not exists
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    op.execute('CREATE EXTENSION IF NOT EXISTS "pg_trgm"')

    # =========================
    # Evidence Table
    # =========================
    op.create_table(
        "evidence",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("evidence_type", sa.String(50), nullable=False),
        sa.Column("source_url", sa.Text, nullable=False),
        sa.Column("source_archive_url", sa.Text, nullable=True),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("extractor", sa.String(100), nullable=False),
        sa.Column("extractor_version", sa.String(20), nullable=False),
        sa.Column("raw_data_ref", sa.Text, nullable=False),
        sa.Column("extraction_confidence", sa.Float, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    op.create_index("idx_evidence_type", "evidence", ["evidence_type"])
    op.create_index("idx_evidence_hash", "evidence", ["content_hash"])
    op.create_index("idx_evidence_retrieved", "evidence", ["retrieved_at"])

    # =========================
    # Events Table (Immutable Event Store)
    # =========================
    op.create_table(
        "events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entity_ids", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=False),
        sa.Column("relationship_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("properties", postgresql.JSONB, nullable=True),
        sa.Column(
            "evidence_ref",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evidence.id"),
            nullable=False,
        ),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    op.create_index("idx_events_type", "events", ["event_type"])
    op.create_index("idx_events_occurred", "events", ["occurred_at"])
    op.create_index(
        "idx_events_entities", "events", ["entity_ids"], postgresql_using="gin"
    )

    # =========================
    # Source Snapshots Table (Dead Link Protection)
    # =========================
    op.create_table(
        "source_snapshots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "evidence_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evidence.id"),
            nullable=False,
        ),
        sa.Column("snapshot_url", sa.Text, nullable=False),
        sa.Column(
            "snapshot_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("content_type", sa.String(100), nullable=True),
        sa.Column("size_bytes", sa.BigInteger, nullable=True),
    )

    op.create_index("idx_snapshots_evidence", "source_snapshots", ["evidence_id"])

    # =========================
    # Audit Log Table
    # =========================
    op.create_table(
        "audit_log",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("action", sa.String(50), nullable=False),
        sa.Column("user_id", sa.String(100), nullable=True),
        sa.Column("entity_type", sa.String(50), nullable=True),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("query_text", sa.Text, nullable=True),
        sa.Column("report_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("request_metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    op.create_index("idx_audit_action", "audit_log", ["action"])
    op.create_index("idx_audit_user", "audit_log", ["user_id"])
    op.create_index("idx_audit_created", "audit_log", ["created_at"])

    # =========================
    # Ingestion Runs Table (Data Quality Tracking)
    # =========================
    op.create_table(
        "ingestion_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("records_processed", sa.Integer, nullable=True),
        sa.Column("records_created", sa.Integer, nullable=True),
        sa.Column("records_updated", sa.Integer, nullable=True),
        sa.Column("duplicates_found", sa.Integer, nullable=True),
        sa.Column("errors", postgresql.JSONB, nullable=True),
    )

    op.create_index("idx_ingestion_source", "ingestion_runs", ["source"])
    op.create_index("idx_ingestion_status", "ingestion_runs", ["status"])
    op.create_index("idx_ingestion_started", "ingestion_runs", ["started_at"])

    # =========================
    # Immutability Trigger for Events Table
    # =========================
    op.execute("""
        CREATE OR REPLACE FUNCTION prevent_event_modification()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION 'Events table is immutable. Updates and deletes are not allowed.';
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE TRIGGER events_immutable
        BEFORE UPDATE OR DELETE ON events
        FOR EACH ROW
        EXECUTE FUNCTION prevent_event_modification();
    """)


def downgrade() -> None:
    # Drop trigger first
    op.execute("DROP TRIGGER IF EXISTS events_immutable ON events")
    op.execute("DROP FUNCTION IF EXISTS prevent_event_modification()")

    # Drop tables in reverse order
    op.drop_table("ingestion_runs")
    op.drop_table("audit_log")
    op.drop_table("source_snapshots")
    op.drop_table("events")
    op.drop_table("evidence")
