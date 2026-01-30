"""Add research tables for follow-the-leads feature

Revision ID: 005_research_tables
Revises: 003_resolution_tables
Create Date: 2026-01-29

Creates:
- research_sessions: Investigation session tracking
- lead_queue: Priority queue of leads to investigate
- session_entities: Junction table for discovered entities
- session_relationships: Junction table for discovered relationships
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision: str = "005_research_tables"
down_revision: Union[str, None] = "004_ingestion_run_logs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # =========================
    # Research Sessions Table
    # =========================
    op.create_table(
        "research_sessions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        # Entry point
        sa.Column(
            "entry_point_type",
            sa.String(50),
            nullable=False,
        ),  # meta_ads, company, ein, bn, nonprofit, entity_id
        sa.Column("entry_point_value", sa.Text, nullable=False),
        sa.Column(
            "entry_point_entity_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        # Status
        sa.Column(
            "status",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'initializing'"),
        ),  # initializing, running, paused, completed, failed
        # Configuration and stats (JSONB)
        sa.Column(
            "config",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "stats",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        # Timestamps
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
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        # Ownership
        sa.Column("created_by", sa.String(255), nullable=True),
    )

    # Foreign key for entry_point_entity_id
    op.create_foreign_key(
        "fk_research_sessions_entry_entity",
        "research_sessions",
        "entities",
        ["entry_point_entity_id"],
        ["id"],
    )

    op.create_index("idx_research_sessions_status", "research_sessions", ["status"])
    op.create_index(
        "idx_research_sessions_created_by", "research_sessions", ["created_by"]
    )
    op.create_index(
        "idx_research_sessions_created_at",
        "research_sessions",
        [sa.text("created_at DESC")],
    )

    # =========================
    # Lead Queue Table
    # =========================
    op.create_table(
        "lead_queue",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "session_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "source_entity_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        # Lead details
        sa.Column(
            "lead_type", sa.String(50), nullable=False
        ),  # ownership, funding, sponsorship, board_interlock, cross_border, infrastructure
        sa.Column("target_identifier", sa.Text, nullable=False),
        sa.Column(
            "target_identifier_type", sa.String(50), nullable=False
        ),  # name, ein, bn, cik, sedar_profile, meta_page_id, etc.
        # Priority and confidence
        sa.Column("priority", sa.Integer, nullable=False, server_default=sa.text("3")),
        sa.Column(
            "confidence", sa.Float, nullable=False, server_default=sa.text("0.8")
        ),
        sa.Column("depth", sa.Integer, nullable=False, server_default=sa.text("0")),
        # Status
        sa.Column(
            "status",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),  # pending, in_progress, completed, skipped, failed
        # Context and results
        sa.Column(
            "context",
            postgresql.JSONB,
            nullable=True,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("result", postgresql.JSONB, nullable=True),
        sa.Column("skip_reason", sa.Text, nullable=True),
        # Timestamps
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        # Check constraints
        sa.CheckConstraint("priority >= 1 AND priority <= 5", name="ck_lead_priority"),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1", name="ck_lead_confidence"
        ),
        sa.CheckConstraint("depth >= 0", name="ck_lead_depth"),
    )

    # Foreign keys for lead_queue
    op.create_foreign_key(
        "fk_lead_queue_session",
        "lead_queue",
        "research_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_lead_queue_source_entity",
        "lead_queue",
        "entities",
        ["source_entity_id"],
        ["id"],
    )

    op.create_index("idx_lead_queue_session_id", "lead_queue", ["session_id"])
    op.create_index("idx_lead_queue_status", "lead_queue", ["status"])
    op.create_index(
        "idx_lead_queue_session_status", "lead_queue", ["session_id", "status"]
    )
    op.create_index(
        "idx_lead_queue_priority",
        "lead_queue",
        [
            "session_id",
            "status",
            "priority",
            sa.text("confidence DESC"),
            "created_at",
        ],
    )
    op.create_index(
        "idx_lead_queue_target",
        "lead_queue",
        ["session_id", "target_identifier", "target_identifier_type"],
    )

    # =========================
    # Session Entities Junction Table
    # =========================
    op.create_table(
        "session_entities",
        sa.Column(
            "session_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "entity_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "added_via_lead_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("depth", sa.Integer, nullable=False, server_default=sa.text("0")),
        sa.Column(
            "relevance_score", sa.Float, nullable=False, server_default=sa.text("1.0")
        ),
        sa.PrimaryKeyConstraint("session_id", "entity_id"),
    )

    # Foreign keys for session_entities
    op.create_foreign_key(
        "fk_session_entities_session",
        "session_entities",
        "research_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_session_entities_entity",
        "session_entities",
        "entities",
        ["entity_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_session_entities_lead",
        "session_entities",
        "lead_queue",
        ["added_via_lead_id"],
        ["id"],
    )

    op.create_index("idx_session_entities_session", "session_entities", ["session_id"])
    op.create_index("idx_session_entities_entity", "session_entities", ["entity_id"])
    op.create_index(
        "idx_session_entities_depth", "session_entities", ["session_id", "depth"]
    )

    # =========================
    # Session Relationships Junction Table
    # =========================
    op.create_table(
        "session_relationships",
        sa.Column(
            "session_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "relationship_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "added_via_lead_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("session_id", "relationship_id"),
    )

    # Foreign keys for session_relationships
    op.create_foreign_key(
        "fk_session_relationships_session",
        "session_relationships",
        "research_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    # Note: relationship_id references Neo4j relationships (not a PostgreSQL table)
    # No foreign key constraint - the UUID refers to a Neo4j relationship ID
    op.create_foreign_key(
        "fk_session_relationships_lead",
        "session_relationships",
        "lead_queue",
        ["added_via_lead_id"],
        ["id"],
    )

    op.create_index(
        "idx_session_relationships_session", "session_relationships", ["session_id"]
    )
    op.create_index(
        "idx_session_relationships_relationship",
        "session_relationships",
        ["relationship_id"],
    )


def downgrade() -> None:
    op.drop_table("session_relationships")
    op.drop_table("session_entities")
    op.drop_table("lead_queue")
    op.drop_table("research_sessions")
