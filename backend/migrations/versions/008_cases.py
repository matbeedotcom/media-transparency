"""Create tables for Case Intake System.

Revision ID: 008_cases
Revises: 007_provincial_sources
Create Date: 2026-02-01
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

# revision identifiers
revision = "008_cases"
down_revision = "007_provincial_sources"
branch_labels = None
depends_on = None


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    # Cases table
    if not table_exists("cases"):
        op.create_table(
            "cases",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("name", sa.String(200), nullable=False),
            sa.Column("description", sa.Text, nullable=True),
            sa.Column(
                "entry_point_type",
                sa.String(50),
                nullable=False,
                comment="meta_ad, corporation, url, text",
            ),
            sa.Column("entry_point_value", sa.Text, nullable=False),
            sa.Column(
                "status",
                sa.String(50),
                nullable=False,
                default="initializing",
                comment="initializing, processing, paused, completed, failed",
            ),
            sa.Column("config", postgresql.JSONB, nullable=False, default={}),
            sa.Column("stats", postgresql.JSONB, nullable=False, default={}),
            sa.Column("research_session_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("completed_at", sa.DateTime, nullable=True),
            sa.Column("created_by", sa.String(255), nullable=True),
        )

    # Evidence table - may already exist from detection migrations
    if not table_exists("evidence"):
        op.create_table(
            "evidence",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "case_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("cases.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "evidence_type",
                sa.String(50),
                nullable=False,
                comment="entry_point, url_fetch, api_response, uploaded",
            ),
            sa.Column("source_url", sa.Text, nullable=True),
            sa.Column("source_archive_url", sa.Text, nullable=True),
            sa.Column("content_ref", sa.String(500), nullable=False, comment="S3 path"),
            sa.Column("content_hash", sa.String(64), nullable=False, comment="SHA-256"),
            sa.Column("content_type", sa.String(100), nullable=False, default="application/octet-stream"),
            sa.Column("extractor", sa.String(100), nullable=True),
            sa.Column("extractor_version", sa.String(50), nullable=True),
            sa.Column("extraction_result", postgresql.JSONB, nullable=True),
            sa.Column("retrieved_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    else:
        # Add case_id column to existing evidence table if it doesn't exist
        bind = op.get_bind()
        inspector = inspect(bind)
        columns = [c["name"] for c in inspector.get_columns("evidence")]
        if "case_id" not in columns:
            op.add_column(
                "evidence",
                sa.Column(
                    "case_id",
                    postgresql.UUID(as_uuid=True),
                    nullable=True,  # Nullable initially for existing rows
                ),
            )

    # Extracted leads table
    if not table_exists("extracted_leads"):
        op.create_table(
            "extracted_leads",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "case_id",
                postgresql.UUID(as_uuid=True),
                nullable=True,  # Nullable to support creation without case
            ),
            sa.Column(
                "evidence_id",
                postgresql.UUID(as_uuid=True),
                nullable=True,  # Nullable to support creation without evidence
            ),
            sa.Column(
                "entity_type",
                sa.String(50),
                nullable=False,
                comment="organization, person, identifier",
            ),
            sa.Column("extracted_value", sa.Text, nullable=False),
            sa.Column("identifier_type", sa.String(50), nullable=True),
            sa.Column("confidence", sa.Float, nullable=False),
            sa.Column(
                "extraction_method",
                sa.String(50),
                nullable=False,
                comment="deterministic, llm, hybrid",
            ),
            sa.Column("context", sa.Text, nullable=True),
            sa.Column("converted_to_lead_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )

    # Entity matches table
    if not table_exists("entity_matches"):
        op.create_table(
            "entity_matches",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "case_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("cases.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("source_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("target_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("confidence", sa.Float, nullable=False),
            sa.Column("match_signals", postgresql.JSONB, nullable=False, default={}),
            sa.Column(
                "status",
                sa.String(50),
                nullable=False,
                default="pending",
                comment="pending, approved, rejected, deferred",
            ),
            sa.Column("reviewed_by", sa.String(255), nullable=True),
            sa.Column("reviewed_at", sa.DateTime, nullable=True),
            sa.Column("review_notes", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )

    # Case reports table
    if not table_exists("case_reports"):
        op.create_table(
            "case_reports",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "case_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("cases.id", ondelete="CASCADE"),
                nullable=False,
                unique=True,
            ),
            sa.Column("generated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("report_version", sa.Integer, nullable=False, default=1),
            sa.Column("summary", postgresql.JSONB, nullable=False),
            sa.Column("top_entities", postgresql.JSONB, nullable=False, default=[]),
            sa.Column("top_relationships", postgresql.JSONB, nullable=False, default=[]),
            sa.Column("cross_border_flags", postgresql.JSONB, nullable=False, default=[]),
            sa.Column("unknowns", postgresql.JSONB, nullable=False, default=[]),
            sa.Column("evidence_index", postgresql.JSONB, nullable=False, default=[]),
        )

    # Create indexes (use if_not_exists where supported, or try/except)
    def create_index_safe(name, table, columns, **kwargs):
        try:
            op.create_index(name, table, columns, **kwargs)
        except Exception:
            pass  # Index already exists

    create_index_safe("idx_cases_status", "cases", ["status"])
    create_index_safe("idx_cases_created_by", "cases", ["created_by"])
    create_index_safe("idx_cases_created_at", "cases", ["created_at"])
    create_index_safe("idx_evidence_case_id", "evidence", ["case_id"])
    create_index_safe("idx_evidence_type", "evidence", ["evidence_type"])
    create_index_safe("idx_extracted_leads_evidence_id", "extracted_leads", ["evidence_id"])
    create_index_safe("idx_extracted_leads_case_id", "extracted_leads", ["case_id"])
    create_index_safe("idx_entity_matches_case_status", "entity_matches", ["case_id", "status"])
    create_index_safe(
        "idx_entity_matches_pending",
        "entity_matches",
        ["status"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    def drop_index_safe(name, table_name=None):
        try:
            op.drop_index(name, table_name=table_name)
        except Exception:
            pass

    def drop_table_safe(name):
        if table_exists(name):
            op.drop_table(name)

    drop_index_safe("idx_entity_matches_pending", "entity_matches")
    drop_index_safe("idx_entity_matches_case_status", "entity_matches")
    drop_index_safe("idx_extracted_leads_case_id", "extracted_leads")
    drop_index_safe("idx_extracted_leads_evidence_id", "extracted_leads")
    drop_index_safe("idx_evidence_type", "evidence")
    drop_index_safe("idx_evidence_case_id", "evidence")
    drop_index_safe("idx_cases_created_at", "cases")
    drop_index_safe("idx_cases_created_by", "cases")
    drop_index_safe("idx_cases_status", "cases")
    drop_table_safe("case_reports")
    drop_table_safe("entity_matches")
    drop_table_safe("extracted_leads")
    # Don't drop evidence if it existed before this migration
    # drop_table_safe("evidence")
    drop_table_safe("cases")
