"""Add log_output column to ingestion_runs

Revision ID: 004_ingestion_run_logs
Revises: 003_resolution_tables
Create Date: 2026-01-28

Adds a TEXT column to store captured log output for each ingestion run.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision: str = "004_ingestion_run_logs"
down_revision: Union[str, None] = "003_resolution_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("ingestion_runs", sa.Column("log_output", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("ingestion_runs", "log_output")
