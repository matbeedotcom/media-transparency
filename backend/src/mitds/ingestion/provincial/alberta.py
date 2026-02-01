"""Alberta Non-Profit Listing ingester.

Ingests data from the Alberta Open Data portal's Non-Profit Listing dataset.

Data source: https://open.alberta.ca/opendata/alberta-non-profit-listing
Format: XLSX
Update frequency: Monthly
License: Open Government Licence - Alberta

Fields:
- Organization Type
- Organization Name
- Current Status
- Registration Date
- City
- Postal Code
"""

from datetime import date, datetime
from typing import Any
from uuid import UUID

from .base import BaseProvincialIngester
from .models import ProvincialNonProfitRecord, ProvincialOrgStatus, ProvincialOrgType

# Alberta Open Data portal URL for non-profit listing
# Updated 2026-01-31: URL changed on Alberta Open Data portal
ALBERTA_DATA_URL = "https://open.alberta.ca/dataset/bcc15e72-fe46-4215-8de0-33951662465e/resource/48fee3cc-6a25-452e-be65-11bd05cc00ab/download/non_profit_name_list_for_open_data_portal.xlsx"

# Expected column headers for validation
# Updated 2026-01-31: Column names changed on Alberta Open Data portal
ALBERTA_EXPECTED_COLUMNS = [
    "Legal Entity Type Description",
    "Legal Entity Name",
    "Status",
    "Registration Date",
    "City",
    "Postal Code",
]


class AlbertaNonProfitIngester(BaseProvincialIngester):
    """Ingester for Alberta's Non-Profit Listing.

    Downloads and parses the XLSX file from Alberta Open Data,
    extracting non-profit organization information.

    Usage:
        ingester = AlbertaNonProfitIngester()
        result = await ingester.run(IngestionConfig(incremental=True))
    """

    @property
    def province(self) -> str:
        """Return the Alberta province code."""
        return "AB"

    def get_data_url(self) -> str:
        """Return the URL to download Alberta non-profit data."""
        return ALBERTA_DATA_URL

    def get_expected_columns(self) -> list[str]:
        """Return expected column names for validation."""
        return ALBERTA_EXPECTED_COLUMNS

    def get_header_row_index(self) -> int:
        """Return header row index (row 2 in the file, 0-indexed as 1)."""
        return 1

    def parse_record(self, row: tuple) -> ProvincialNonProfitRecord | None:
        """Parse a single row from the Alberta data file.

        Args:
            row: Tuple of (org_type, name, status, reg_date, city, postal)

        Returns:
            ProvincialNonProfitRecord if valid, None to skip the row
        """
        if len(row) < 6:
            return None

        org_type, name, status, reg_date, city, postal = row[:6]

        # Skip rows with missing required field (name)
        if not name or not str(name).strip():
            return None

        # Clean and normalize fields
        name = str(name).strip()
        org_type = str(org_type).strip() if org_type else "unknown"
        status = str(status).strip() if status else "unknown"
        city = str(city).strip() if city else None
        postal = str(postal).strip() if postal else None

        # Parse registration date
        registration_date = self._parse_date(reg_date)

        return ProvincialNonProfitRecord(
            name=name,
            org_type_raw=org_type,
            status_raw=status,
            registration_date=registration_date,
            city=city if city else None,
            postal_code=postal if postal else None,
            province=self.province,
            source_url=self.get_data_url(),
        )

    def _parse_date(self, value: Any) -> date | None:
        """Parse a date value from the spreadsheet.

        Handles various date formats that may appear in the data.

        Args:
            value: Date value (may be datetime, string, or None)

        Returns:
            Parsed date or None if invalid/empty
        """
        if value is None:
            return None

        # If already a datetime/date object (openpyxl may return these)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value

        # Try to parse string
        value_str = str(value).strip()
        if not value_str:
            return None

        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue

        # If all formats fail, log and return None
        self.logger.debug(f"Failed to parse date: {value}")
        return None

    def map_org_type(self, raw: str) -> ProvincialOrgType:
        """Map Alberta organization type to standard classification.

        Alberta uses specific act names as organization types:
        - Societies Act -> SOCIETY
        - Agricultural Societies Act -> AGRICULTURAL
        - Religious Societies Lands Act -> RELIGIOUS
        - Companies Act -> NONPROFIT_COMPANY
        - Business Corporations Act -> EXTRAPROVINCIAL

        Args:
            raw: Raw organization type string

        Returns:
            ProvincialOrgType enum value
        """
        raw_lower = raw.lower()

        if "societies act" in raw_lower and "agricultural" not in raw_lower and "religious" not in raw_lower:
            return ProvincialOrgType.SOCIETY
        elif "agricultural societies act" in raw_lower:
            return ProvincialOrgType.AGRICULTURAL
        elif "religious societies lands act" in raw_lower:
            return ProvincialOrgType.RELIGIOUS
        elif "companies act" in raw_lower:
            return ProvincialOrgType.NONPROFIT_COMPANY
        elif "business corporations act" in raw_lower:
            return ProvincialOrgType.EXTRAPROVINCIAL
        elif "private act" in raw_lower:
            return ProvincialOrgType.PRIVATE_ACT

        return ProvincialOrgType.UNKNOWN

    def map_status(self, raw: str) -> ProvincialOrgStatus:
        """Map Alberta status to standard classification.

        Alberta status values:
        - Active -> ACTIVE
        - Struck -> STRUCK
        - Dissolved -> DISSOLVED
        - Continued Out -> CONTINUED_OUT
        - Amalgamated -> AMALGAMATED

        Args:
            raw: Raw status string

        Returns:
            ProvincialOrgStatus enum value
        """
        raw_lower = raw.lower()

        if "active" in raw_lower:
            return ProvincialOrgStatus.ACTIVE
        elif "struck" in raw_lower:
            return ProvincialOrgStatus.STRUCK
        elif "dissolved" in raw_lower:
            return ProvincialOrgStatus.DISSOLVED
        elif "continued" in raw_lower:
            return ProvincialOrgStatus.CONTINUED_OUT
        elif "amalgamated" in raw_lower:
            return ProvincialOrgStatus.AMALGAMATED

        return ProvincialOrgStatus.UNKNOWN


async def run_alberta_nonprofits_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run Alberta non-profit ingestion.

    Main entry point for running the Alberta ingester. Can be called
    from CLI, API, or directly from Python code.

    Args:
        incremental: Use incremental sync (only changed records)
        limit: Maximum records to process (for testing)
        target_entities: Specific organization names to ingest
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary with statistics

    Example:
        result = await run_alberta_nonprofits_ingestion(
            incremental=True,
            limit=100,
        )
        print(f"Processed: {result['records_processed']}")
    """
    from ..base import IngestionConfig

    ingester = AlbertaNonProfitIngester()

    config = IngestionConfig(
        incremental=incremental,
        limit=limit,
        target_entities=target_entities,
    )

    result = await ingester.run(config, run_id=run_id)

    return {
        "run_id": str(result.run_id),
        "source": result.source,
        "status": result.status,
        "started_at": result.started_at.isoformat(),
        "completed_at": result.completed_at.isoformat() if result.completed_at else None,
        "records_processed": result.records_processed,
        "records_created": result.records_created,
        "records_updated": result.records_updated,
        "duplicates_found": result.duplicates_found,
        "errors": result.errors,
    }
