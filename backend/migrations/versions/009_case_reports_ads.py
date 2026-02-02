"""Add ads_summary and similarity_leads columns to case_reports.

Revision ID: 009_case_reports_ads
Revises: 008_cases
Create Date: 2026-02-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

# revision identifiers
revision = "009_case_reports_ads"
down_revision = "008_cases"
branch_labels = None
depends_on = None


def column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [c["name"] for c in inspector.get_columns(table_name)]
    return column_name in columns


def upgrade() -> None:
    # Add ads_summary column if it doesn't exist
    if not column_exists("case_reports", "ads_summary"):
        op.add_column(
            "case_reports",
            sa.Column(
                "ads_summary",
                postgresql.JSONB,
                nullable=True,
                comment="Aggregated Meta Ads statistics",
            ),
        )

    # Add similarity_leads column if it doesn't exist
    if not column_exists("case_reports", "similarity_leads"):
        op.add_column(
            "case_reports",
            sa.Column(
                "similarity_leads",
                postgresql.JSONB,
                nullable=False,
                server_default="[]",
                comment="Suggested leads based on ad content similarity",
            ),
        )


def downgrade() -> None:
    def drop_column_safe(table_name: str, column_name: str) -> None:
        if column_exists(table_name, column_name):
            op.drop_column(table_name, column_name)

    drop_column_safe("case_reports", "similarity_leads")
    drop_column_safe("case_reports", "ads_summary")
