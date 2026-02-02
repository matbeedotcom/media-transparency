"""Create tables for Case Intake System.

Revision ID: 008_cases
Revises: 007_provincial_sources
Create Date: 2026-02-01
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "008_cases"
down_revision = "007_provincial_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Cases table
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

    # Evidence table
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

    # Extracted leads table
    op.create_table(
        "extracted_leads",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "evidence_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evidence.id", ondelete="CASCADE"),
            nullable=False,
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

    # Create indexes
    op.create_index("idx_cases_status", "cases", ["status"])
    op.create_index("idx_cases_created_by", "cases", ["created_by"])
    op.create_index("idx_cases_created_at", "cases", ["created_at"])
    op.create_index("idx_evidence_case_id", "evidence", ["case_id"])
    op.create_index("idx_evidence_type", "evidence", ["evidence_type"])
    op.create_index("idx_extracted_leads_evidence_id", "extracted_leads", ["evidence_id"])
    op.create_index("idx_entity_matches_case_status", "entity_matches", ["case_id", "status"])
    op.create_index(
        "idx_entity_matches_pending",
        "entity_matches",
        ["status"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("idx_entity_matches_pending")
    op.drop_index("idx_entity_matches_case_status")
    op.drop_index("idx_extracted_leads_evidence_id")
    op.drop_index("idx_evidence_type")
    op.drop_index("idx_evidence_case_id")
    op.drop_index("idx_cases_created_at")
    op.drop_index("idx_cases_created_by")
    op.drop_index("idx_cases_status")
    op.drop_table("case_reports")
    op.drop_table("entity_matches")
    op.drop_table("extracted_leads")
    op.drop_table("evidence")
    op.drop_table("cases")
