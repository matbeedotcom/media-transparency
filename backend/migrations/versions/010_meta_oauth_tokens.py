"""Create meta_oauth_tokens table for storing Facebook OAuth tokens.

Revision ID: 010_meta_oauth_tokens
Revises: 009_case_reports_ads
Create Date: 2026-02-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

# revision identifiers
revision = "010_meta_oauth_tokens"
down_revision = "009_case_reports_ads"
branch_labels = None
depends_on = None


def table_exists(table_name: str) -> bool:
    """Check if a table exists."""
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    if not table_exists("meta_oauth_tokens"):
        op.create_table(
            "meta_oauth_tokens",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("access_token", sa.Text(), nullable=False),
            sa.Column(
                "token_type",
                sa.String(50),
                nullable=False,
                server_default="user",
                comment="Token type: user, system, app",
            ),
            sa.Column(
                "expires_at",
                sa.DateTime(timezone=True),
                nullable=True,
                comment="When the token expires",
            ),
            sa.Column(
                "scopes",
                postgresql.ARRAY(sa.String()),
                nullable=True,
                comment="Granted permission scopes",
            ),
            sa.Column(
                "fb_user_id",
                sa.String(255),
                nullable=True,
                comment="Facebook user ID who authorized",
            ),
            sa.Column(
                "fb_user_name",
                sa.String(255),
                nullable=True,
                comment="Facebook user name who authorized",
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
                onupdate=sa.func.now(),
            ),
        )
        
        # Create index on token_type for quick lookups
        op.create_index(
            "ix_meta_oauth_tokens_token_type",
            "meta_oauth_tokens",
            ["token_type"],
        )


def downgrade() -> None:
    if table_exists("meta_oauth_tokens"):
        op.drop_index("ix_meta_oauth_tokens_token_type", table_name="meta_oauth_tokens")
        op.drop_table("meta_oauth_tokens")
