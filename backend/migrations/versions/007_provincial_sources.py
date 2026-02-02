"""Add provincial sources tracking table

Revision ID: 007_provincial_sources
Revises: 006_provincial_registry
Create Date: 2026-01-31

Adds provincial_sources table for tracking provincial corporation registry
data sources, their configuration, and ingestion status.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

# revision identifiers
revision: str = "007_provincial_sources"
down_revision: Union[str, None] = "006_provincial_registry"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create provincial_sources table
    op.create_table(
        "provincial_sources",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("province_code", sa.String(2), nullable=False, unique=True),
        sa.Column("province_name", sa.String(100), nullable=False),
        sa.Column("source_name", sa.String(200), nullable=False),
        sa.Column("source_url", sa.Text, nullable=False),
        sa.Column("data_format", sa.String(20), nullable=False),
        sa.Column("update_frequency", sa.String(50), nullable=True),
        sa.Column("bulk_available", sa.Boolean, server_default=sa.text("false")),
        sa.Column("enabled", sa.Boolean, server_default=sa.text("true")),
        sa.Column("last_ingested", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_record_count", sa.Integer, nullable=True),
        sa.Column("config", JSONB, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
    )

    # Index for lookups by province code
    op.create_index(
        "idx_provincial_sources_code",
        "provincial_sources",
        ["province_code"],
    )

    # Insert initial provincial sources data
    # Note: OpenCorporates removed - not available (paid API)
    op.execute("""
        INSERT INTO provincial_sources (
            province_code, province_name, source_name, source_url,
            data_format, update_frequency, bulk_available, enabled
        ) VALUES
        ('QC', 'Quebec', 'Registraire des Entreprises',
         'https://www.donneesquebec.ca/recherche/dataset/registre-des-entreprises',
         'csv', 'daily', true, true),
        ('AB', 'Alberta', 'Alberta Corporate Registry',
         'https://open.alberta.ca/opendata/alberta-non-profit-listing',
         'xlsx', 'monthly', true, true),
        ('ON', 'Ontario', 'Ontario Business Registry',
         'https://data.ontario.ca/',
         'csv', 'variable', false, true),
        ('BC', 'British Columbia', 'BC Registry Services',
         'https://www.bcregistry.gov.bc.ca/',
         'api', 'realtime', false, false),
        ('SK', 'Saskatchewan', 'Saskatchewan Corporate Registry',
         'https://corporateregistry.isc.ca/',
         'search', 'realtime', false, false),
        ('MB', 'Manitoba', 'Manitoba Companies Office',
         'https://companiesoffice.gov.mb.ca/',
         'search', 'realtime', false, false),
        ('NS', 'Nova Scotia', 'Registry of Joint Stock Companies',
         'https://data.novascotia.ca/',
         'csv', 'variable', true, true),
        ('NB', 'New Brunswick', 'Corporate Registry of New Brunswick',
         'https://www2.snb.ca/content/snb/en/sites/corporate-registry.html',
         'search', 'realtime', false, false),
        ('PE', 'Prince Edward Island', 'PEI Business Registry',
         'https://www.princeedwardisland.ca/en/feature/pei-business-corporate-registry',
         'search', 'realtime', false, false),
        ('NL', 'Newfoundland and Labrador', 'Companies and Deeds Online',
         'https://cado.eservices.gov.nl.ca/',
         'search', 'realtime', false, false),
        ('NT', 'Northwest Territories', 'NWT Corporate Registries',
         'https://www.justice.gov.nt.ca/en/corporate-registries/',
         'search', 'realtime', false, false),
        ('YT', 'Yukon', 'Yukon Corporate Online Registry',
         'https://ycor-reey.gov.yk.ca/',
         'search', 'realtime', false, false),
        ('NU', 'Nunavut', 'Nunavut Legal Registries',
         'https://www.gov.nu.ca/justice/information/legal-registries',
         'search', 'realtime', false, false)
    """)


def downgrade() -> None:
    op.drop_index("idx_provincial_sources_code", table_name="provincial_sources")
    op.drop_table("provincial_sources")
