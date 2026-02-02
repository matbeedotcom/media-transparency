"""Add entity resolution tables

Revision ID: 003_resolution_tables
Revises: 002_detection_tables
Create Date: 2026-01-28

Creates:
- reconciliation_tasks: Queue for human review of potential duplicates
- entity_merges: Audit trail for merged/confirmed matches
- entity_non_matches: Record of confirmed non-matches
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "003_resolution_tables"
down_revision: Union[str, None] = "002_detection_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # =========================
    # Reconciliation Tasks Table
    # =========================
    op.create_table(
        "reconciliation_tasks",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("priority", sa.String(20), nullable=False),
        sa.Column("source_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_entity_name", sa.String(500), nullable=False),
        sa.Column("source_entity_type", sa.String(50), nullable=False),
        sa.Column("candidate_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_entity_name", sa.String(500), nullable=False),
        sa.Column("candidate_entity_type", sa.String(50), nullable=False),
        sa.Column("match_strategy", sa.String(50), nullable=False),
        sa.Column("match_confidence", sa.Float, nullable=False),
        sa.Column("match_details", postgresql.JSONB, nullable=True),
        sa.Column("context", postgresql.JSONB, nullable=True),
        sa.Column("assigned_to", sa.String(255), nullable=True),
        sa.Column("reviewed_by", sa.String(255), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution", sa.String(50), nullable=True),
        sa.Column("review_notes", sa.Text, nullable=True),
    )

    op.create_index("idx_recon_status", "reconciliation_tasks", ["status"])
    op.create_index("idx_recon_priority", "reconciliation_tasks", ["priority"])
    op.create_index("idx_recon_source", "reconciliation_tasks", ["source_entity_id"])
    op.create_index("idx_recon_candidate", "reconciliation_tasks", ["candidate_entity_id"])
    op.create_index("idx_recon_assigned", "reconciliation_tasks", ["assigned_to"])
    op.create_index("idx_recon_created", "reconciliation_tasks", ["created_at"])

    # =========================
    # Entity Merges Table (Audit Trail)
    # =========================
    op.create_table(
        "entity_merges",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("source_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("merge_type", sa.String(50), nullable=False),
        sa.Column("confidence", sa.Float, nullable=True),
        sa.Column("approved_by", sa.String(255), nullable=False),
        sa.Column(
            "approved_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("match_strategy", sa.String(50), nullable=True),
        sa.Column("match_details", postgresql.JSONB, nullable=True),
    )

    op.create_index("idx_merges_source", "entity_merges", ["source_entity_id"])
    op.create_index("idx_merges_target", "entity_merges", ["target_entity_id"])
    op.create_index("idx_merges_type", "entity_merges", ["merge_type"])
    op.create_index("idx_merges_approved", "entity_merges", ["approved_at"])

    # =========================
    # Entity Non-Matches Table
    # =========================
    op.create_table(
        "entity_non_matches",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("entity_id_1", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id_2", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("confirmed_by", sa.String(255), nullable=False),
        sa.Column(
            "confirmed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_index("idx_nonmatch_entity1", "entity_non_matches", ["entity_id_1"])
    op.create_index("idx_nonmatch_entity2", "entity_non_matches", ["entity_id_2"])

    # Create unique constraint to prevent duplicate non-match records
    op.create_unique_constraint(
        "uq_non_match_pair",
        "entity_non_matches",
        ["entity_id_1", "entity_id_2"],
    )


def downgrade() -> None:
    op.drop_table("entity_non_matches")
    op.drop_table("entity_merges")
    op.drop_table("reconciliation_tasks")
