"""Add provincial_registry_id column to entities

Revision ID: 006_provincial_registry
Revises: 005_research_tables
Create Date: 2026-01-30

Adds provincial_registry_id column to entities table for tracking
provincial non-profit registry identifiers (e.g., AB:society_12345).
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision: str = "006_provincial_registry"
down_revision: Union[str, None] = "005_research_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add provincial_registry_id column
    op.add_column(
        "entities",
        sa.Column("provincial_registry_id", sa.String(100), nullable=True)
    )

    # Add index for provincial lookups
    op.create_index(
        "idx_entities_provincial_registry",
        "entities",
        ["provincial_registry_id"],
        postgresql_where=sa.text("provincial_registry_id IS NOT NULL")
    )


def downgrade() -> None:
    op.drop_index("idx_entities_provincial_registry", table_name="entities")
    op.drop_column("entities", "provincial_registry_id")
