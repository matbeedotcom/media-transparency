"""Add detection findings and jobs tables

Revision ID: 002_detection_tables
Revises: 001_initial
Create Date: 2026-01-27

Creates:
- detection_findings: Persists detection analysis results
- jobs: Tracks async background job execution
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "002_detection_tables"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # =========================
    # Detection Findings Table
    # =========================
    op.create_table(
        "detection_findings",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("finding_type", sa.String(50), nullable=False),
        sa.Column(
            "entity_ids",
            postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
            nullable=False,
        ),
        sa.Column("score", sa.Float, nullable=False),
        sa.Column("confidence", sa.Float, nullable=False),
        sa.Column("flagged", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("created_by", sa.String(255), nullable=True),
    )

    op.create_index("idx_findings_type", "detection_findings", ["finding_type"])
    op.create_index("idx_findings_flagged", "detection_findings", ["flagged"])
    op.create_index("idx_findings_created", "detection_findings", ["created_at"])
    op.create_index(
        "idx_findings_entities",
        "detection_findings",
        ["entity_ids"],
        postgresql_using="gin",
    )

    # =========================
    # Jobs Table
    # =========================
    op.create_table(
        "jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("job_type", sa.String(50), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("progress", sa.Integer, nullable=True),
        sa.Column("result", postgresql.JSONB, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index("idx_jobs_status", "jobs", ["status"])
    op.create_index("idx_jobs_type", "jobs", ["job_type"])
    op.create_index("idx_jobs_created", "jobs", ["created_at"])


def downgrade() -> None:
    op.drop_table("jobs")
    op.drop_table("detection_findings")
