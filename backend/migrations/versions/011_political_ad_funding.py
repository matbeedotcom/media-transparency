"""Add indexes for political ad funding data sources.

Adds partial indexes on entities.metadata for registration_id and google_ad_id lookups,
and on evidence.evidence_type for court_decision and ppsa_registration filtering.

Revision ID: 011_political_ad_funding
Revises: 010_meta_oauth_tokens
Create Date: 2026-02-05
"""

from alembic import op
from sqlalchemy import inspect, text

# revision identifiers
revision = "011_political_ad_funding"
down_revision = "010_meta_oauth_tokens"
branch_labels = None
depends_on = None


def index_exists(index_name: str, table_name: str) -> bool:
    """Check if an index exists on a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    indexes = inspector.get_indexes(table_name)
    return any(idx["name"] == index_name for idx in indexes)


def upgrade() -> None:
    # Index on entities.metadata->>'registration_id' for third-party advertiser lookup
    if not index_exists("ix_entities_metadata_registration_id", "entities"):
        op.execute(
            text(
                """
                CREATE INDEX ix_entities_metadata_registration_id
                ON entities ((metadata->>'registration_id'))
                WHERE metadata->>'registration_id' IS NOT NULL
                """
            )
        )

    # Index on entities.metadata->>'google_ad_id' for Google ad lookup
    if not index_exists("ix_entities_metadata_google_ad_id", "entities"):
        op.execute(
            text(
                """
                CREATE INDEX ix_entities_metadata_google_ad_id
                ON entities ((metadata->>'google_ad_id'))
                WHERE metadata->>'google_ad_id' IS NOT NULL
                """
            )
        )

    # Partial index on evidence.evidence_type for court_decision and ppsa_registration
    if not index_exists("ix_evidence_type_court_ppsa", "evidence"):
        op.execute(
            text(
                """
                CREATE INDEX ix_evidence_type_court_ppsa
                ON evidence (evidence_type)
                WHERE evidence_type IN ('court_decision', 'ppsa_registration')
                """
            )
        )


def downgrade() -> None:
    if index_exists("ix_evidence_type_court_ppsa", "evidence"):
        op.drop_index("ix_evidence_type_court_ppsa", table_name="evidence")
    if index_exists("ix_entities_metadata_google_ad_id", "entities"):
        op.drop_index("ix_entities_metadata_google_ad_id", table_name="entities")
    if index_exists("ix_entities_metadata_registration_id", "entities"):
        op.drop_index("ix_entities_metadata_registration_id", table_name="entities")
