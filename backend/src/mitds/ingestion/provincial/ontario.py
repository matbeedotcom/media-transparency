"""Ontario Corporation Registry ingester.

Ingests data from the Ontario Business Registry via data.ontario.ca.

Data source: https://data.ontario.ca/
Format: CSV
Update frequency: Variable
License: Open Government Licence - Ontario

Note: Ontario has partial open data available. The full registry is not
accessible via bulk download. This ingester handles the available datasets.

Ontario corporation types:
- Business Corporation (OBCA)
- Not-for-Profit Corporation (ONCA)
- Co-operative Corporation
- Extra-Provincial Corporation
- Professional Corporation
"""

from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from .base import BaseProvincialCorpIngester
from .models import Address, ProvincialCorporationRecord, ProvincialCorpStatus, ProvincialCorpType

# Ontario Open Data portal URL
# Note: This URL may need to be updated based on actual available datasets
# Ontario's full registry is not available; this targets partial open datasets
ONTARIO_DATA_URL = "https://data.ontario.ca/dataset/active-business-entities/resource/download"

# Placeholder URL - actual URL depends on specific dataset availability
# Common datasets:
# - Licence and Registration Data
# - Business Registry Partner listings

# Expected column headers for validation (varies by dataset)
ONTARIO_EXPECTED_COLUMNS = [
    "Business Name",
    "Business Number",
    "Status",
    "Type",
    "Registration Date",
]


class OntarioCorporationIngester(BaseProvincialCorpIngester):
    """Ingester for Ontario's Business Registry (partial data).

    Downloads and parses available CSV data from data.ontario.ca.
    Note: Full registry not available; this processes partial open datasets.

    Usage:
        ingester = OntarioCorporationIngester()
        result = await ingester.run(IngestionConfig(incremental=True))
    """

    # Custom data URL can be set for different Ontario datasets
    _data_url: str | None = None

    @property
    def province(self) -> str:
        """Return the Ontario province code."""
        return "ON"

    @property
    def data_format(self) -> Literal["csv"]:
        """Return the data format (CSV)."""
        return "csv"

    def get_data_url(self) -> str:
        """Return the URL to download Ontario enterprise data."""
        return self._data_url or ONTARIO_DATA_URL

    def set_data_url(self, url: str) -> None:
        """Set a custom data URL for specific Ontario datasets.

        Ontario has multiple datasets; this allows targeting specific ones.

        Args:
            url: URL to the Ontario CSV dataset
        """
        self._data_url = url

    def get_expected_columns(self) -> list[str] | None:
        """Return expected column names for validation."""
        # Return None to handle varying column structures
        return None

    def get_csv_encoding(self) -> str:
        """Return CSV encoding (UTF-8)."""
        return "utf-8"

    def get_csv_delimiter(self) -> str:
        """Return CSV delimiter (comma)."""
        return ","

    def parse_record(self, row: tuple) -> ProvincialCorporationRecord | None:
        """Parse a single row from the Ontario CSV file.

        Ontario data structure may vary by dataset. This handles common patterns.

        Args:
            row: Tuple of values from the CSV row

        Returns:
            ProvincialCorporationRecord if valid, None to skip the row
        """
        if len(row) < 4:
            return None

        # Extract fields (indices based on typical Ontario CSV structure)
        # Adjust based on actual dataset columns
        name = str(row[0]).strip() if row[0] else None
        corp_number = str(row[1]).strip() if len(row) > 1 and row[1] else None
        status = str(row[2]).strip() if len(row) > 2 and row[2] else "unknown"
        corp_type = str(row[3]).strip() if len(row) > 3 and row[3] else "unknown"

        # Skip rows with missing required fields
        if not name:
            return None

        # Generate a registration number if not provided
        if not corp_number:
            import hashlib
            corp_number = hashlib.md5(name.encode()).hexdigest()[:12]

        # Parse registration date if available
        incorporation_date = None
        if len(row) > 4 and row[4]:
            incorporation_date = self._parse_date(str(row[4]))

        # Build address if available
        address = None
        if len(row) > 8:
            street = str(row[5]).strip() if row[5] else None
            city = str(row[6]).strip() if row[6] else None
            postal = str(row[7]).strip() if row[7] else None

            if city or postal:
                address = Address(
                    street_address=street,
                    city=city,
                    province="ON",
                    postal_code=postal,
                )

        # Extract business number if available
        business_number = None
        if len(row) > 9 and row[9]:
            bn = str(row[9]).strip()
            # Validate BN format (9 digits or full format)
            if bn and (bn.isdigit() or "RC" in bn.upper()):
                business_number = bn

        return ProvincialCorporationRecord(
            name=name,
            name_french=None,
            registration_number=corp_number,
            business_number=business_number,
            corp_type_raw=corp_type,
            status_raw=status,
            incorporation_date=incorporation_date,
            jurisdiction=self.province,
            registered_address=address,
            source_url=self.get_data_url(),
        )

    def _parse_date(self, value: str | None) -> date | None:
        """Parse a date value from the Ontario CSV.

        Args:
            value: Date string

        Returns:
            Parsed date or None if invalid/empty
        """
        if not value:
            return None

        value_str = str(value).strip()
        if not value_str:
            return None

        # Common date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue

        return None

    def map_corp_type(self, raw: str) -> ProvincialCorpType:
        """Map Ontario corporation type to standard classification.

        Ontario legal forms:
        - Business Corporation -> FOR_PROFIT
        - Not-for-Profit Corporation -> NOT_FOR_PROFIT
        - Co-operative Corporation -> COOPERATIVE
        - Extra-Provincial Corporation -> EXTRAPROVINCIAL
        - Professional Corporation -> PROFESSIONAL

        Args:
            raw: Raw corporation type string

        Returns:
            ProvincialCorpType enum value
        """
        raw_lower = raw.lower()

        # Not-for-profit (Ontario uses this specific term)
        if "not-for-profit" in raw_lower or "not for profit" in raw_lower:
            return ProvincialCorpType.NOT_FOR_PROFIT

        # Non-profit (general)
        if "non-profit" in raw_lower or "nonprofit" in raw_lower:
            return ProvincialCorpType.NONPROFIT

        # Cooperatives
        if "co-operative" in raw_lower or "cooperative" in raw_lower or "coop" in raw_lower:
            return ProvincialCorpType.COOPERATIVE

        # Professional corporations
        if "professional" in raw_lower:
            return ProvincialCorpType.PROFESSIONAL

        # Extraprovincial
        if "extra-provincial" in raw_lower or "extraprovincial" in raw_lower:
            return ProvincialCorpType.EXTRAPROVINCIAL

        # For-profit business corporations
        if any(kw in raw_lower for kw in [
            "business", "corporation", "inc.", "limited", "ltd",
            "company", "corp"
        ]):
            return ProvincialCorpType.FOR_PROFIT

        return ProvincialCorpType.UNKNOWN

    def map_status(self, raw: str) -> ProvincialCorpStatus:
        """Map Ontario status to standard classification.

        Ontario status values:
        - Active -> ACTIVE
        - Inactive -> INACTIVE
        - Dissolved -> DISSOLVED
        - Cancelled -> STRUCK
        - Amalgamated -> AMALGAMATED
        - Continued -> CONTINUED_OUT

        Args:
            raw: Raw status string

        Returns:
            ProvincialCorpStatus enum value
        """
        raw_lower = raw.lower()

        if "active" in raw_lower:
            return ProvincialCorpStatus.ACTIVE
        elif "inactive" in raw_lower:
            return ProvincialCorpStatus.INACTIVE
        elif "dissolved" in raw_lower:
            return ProvincialCorpStatus.DISSOLVED
        elif "cancelled" in raw_lower:
            return ProvincialCorpStatus.STRUCK
        elif "amalgamated" in raw_lower:
            return ProvincialCorpStatus.AMALGAMATED
        elif "continued" in raw_lower:
            return ProvincialCorpStatus.CONTINUED_OUT
        elif "revoked" in raw_lower:
            return ProvincialCorpStatus.REVOKED

        return ProvincialCorpStatus.UNKNOWN


async def run_ontario_corps_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
    data_url: str | None = None,
) -> dict[str, Any]:
    """Run Ontario corporation ingestion.

    Main entry point for running the Ontario ingester. Can be called
    from CLI, API, or directly from Python code.

    Note: Ontario has partial data availability. The full registry
    is not accessible via bulk download.

    Args:
        incremental: Use incremental sync (only changed records)
        limit: Maximum records to process (for testing)
        target_entities: Specific organization names to ingest
        run_id: Optional run ID from API layer
        data_url: Optional custom URL for specific Ontario dataset

    Returns:
        Ingestion result dictionary with statistics

    Example:
        result = await run_ontario_corps_ingestion(
            incremental=True,
            limit=100,
        )
        print(f"Processed: {result['records_processed']}")
    """
    from ..base import IngestionConfig

    ingester = OntarioCorporationIngester()

    if data_url:
        ingester.set_data_url(data_url)

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
