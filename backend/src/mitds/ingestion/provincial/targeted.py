"""Provincial corporation registry availability information.

This module documents which provinces have bulk data available and which do not.

Provinces WITH bulk data (implemented elsewhere):
- Quebec (QC) - Daily CSV from Données Québec -> quebec.py
- Alberta Non-profits (AB) - Monthly XLSX from Alberta Open Data -> alberta.py
- Nova Scotia Co-ops (NS) - CSV from NS Open Data -> nova_scotia.py

Provinces WITHOUT bulk data (no ingestion possible):
- British Columbia (BC) - Paid API only ($100+ BC OnLine subscription)
- Alberta For-profit - Not in open data (requires registry agent)
- Saskatchewan (SK) - Search-only registry
- Manitoba (MB) - Search-only registry
- New Brunswick (NB) - Search-only registry
- Prince Edward Island (PE) - Search-only registry
- Newfoundland and Labrador (NL) - Search-only registry
- Northwest Territories (NT) - Search-only registry
- Yukon (YT) - Search-only registry
- Nunavut (NU) - Search-only registry

For provinces without bulk data, use the cross-reference service to match
entities found through other sources (SEC, SEDAR, Elections Canada, etc.)
with federal corporation records.
"""

from typing import Any


class NoBulkDataError(Exception):
    """Raised when attempting to ingest from a province without bulk data access."""

    def __init__(self, province: str, reason: str, alternatives: list[str] | None = None):
        self.province = province
        self.reason = reason
        self.alternatives = alternatives or []

        message = f"{province}: {reason}"
        if alternatives:
            message += f"\n\nAlternatives:\n" + "\n".join(f"  - {alt}" for alt in alternatives)

        super().__init__(message)


# Province codes that have no bulk data available
NO_BULK_DATA_PROVINCES = {
    "ON": NoBulkDataError(
        province="Ontario",
        reason="Ontario Business Registry provides search-only access; bulk data requires custom arrangement with ministry fees",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.ontario.ca/page/ontario-business-registry",
        ],
    ),
    "BC": NoBulkDataError(
        province="British Columbia",
        reason="BC Registry requires paid BC OnLine subscription ($100+ setup + per-search fees)",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.bcregistry.gov.bc.ca/",
        ],
    ),
    "SK": NoBulkDataError(
        province="Saskatchewan",
        reason="ISC Online Services provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://corporateregistry.isc.ca/",
        ],
    ),
    "MB": NoBulkDataError(
        province="Manitoba",
        reason="Companies Office provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://companiesoffice.gov.mb.ca/",
        ],
    ),
    "NB": NoBulkDataError(
        province="New Brunswick",
        reason="Service New Brunswick provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.pxw1.snb.ca/snb7001/e/2000/2500e.asp",
        ],
    ),
    "PE": NoBulkDataError(
        province="Prince Edward Island",
        reason="Corporate Registry provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.princeedwardisland.ca/en/feature/corporate-registry",
        ],
    ),
    "PEI": None,  # Alias for PE, will be resolved below
    "NL": NoBulkDataError(
        province="Newfoundland and Labrador",
        reason="Registry of Companies provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://cado.eservices.gov.nl.ca/",
        ],
    ),
    "NT": NoBulkDataError(
        province="Northwest Territories",
        reason="MACA Registry provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.maca.gov.nt.ca/",
        ],
    ),
    "YT": NoBulkDataError(
        province="Yukon",
        reason="Corporate Affairs provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://corporateonline.gov.yk.ca/",
        ],
    ),
    "NU": NoBulkDataError(
        province="Nunavut",
        reason="Legal Registries provides search-only access, no bulk data export",
        alternatives=[
            "Use cross-reference service to match entities from other sources",
            "Manual lookup at https://www.nunavutlegalregistries.ca/",
        ],
    ),
}

# Set up alias
NO_BULK_DATA_PROVINCES["PEI"] = NO_BULK_DATA_PROVINCES["PE"]


def check_bulk_data_available(province: str) -> None:
    """Check if bulk data is available for a province.

    Args:
        province: Province code (e.g., 'SK', 'BC')

    Raises:
        NoBulkDataError: If the province does not have bulk data available
    """
    province_upper = province.upper()

    if province_upper in NO_BULK_DATA_PROVINCES:
        error = NO_BULK_DATA_PROVINCES[province_upper]
        if error is not None:
            raise error


def get_available_provinces() -> dict[str, str]:
    """Get list of provinces with bulk data available.

    Returns:
        Dictionary mapping province code to description
    """
    return {
        "QC": "Quebec - Registraire des Entreprises (daily CSV)",
        "AB": "Alberta - Non-profit organizations only (monthly XLSX)",
        "NS": "Nova Scotia - Co-operatives only (CSV)",
    }


def get_unavailable_provinces() -> dict[str, str]:
    """Get list of provinces without bulk data.

    Returns:
        Dictionary mapping province code to reason
    """
    return {
        code: error.reason
        for code, error in NO_BULK_DATA_PROVINCES.items()
        if error is not None and code != "PEI"  # Skip alias
    }


async def run_targeted_ingestion(
    province: str,
    target_entities: list[str] | None = None,
    from_csv: str | None = None,
    limit: int | None = None,
    headless: bool = True,
    save_to_db: bool = True,
) -> dict[str, Any]:
    """Run targeted search-based ingestion for a province.

    For provinces without bulk data, this uses Playwright browser automation
    to search the registry for specific company names.

    Args:
        province: Province code (e.g., 'ON', 'SK')
        target_entities: List of company names to search for
        from_csv: Path to CSV file with company names (column: 'name')
        limit: Maximum number of results to return
        headless: Whether to run browser in headless mode
        save_to_db: Whether to save results to database

    Returns:
        Dictionary with search results and statistics

    Raises:
        ValueError: If province has bulk data (use dedicated ingester instead)
        NoBulkDataError: If province code is invalid
    """
    province_upper = province.upper()

    # Check if this province has bulk data - if so, redirect to bulk ingester
    if province_upper in ("QC", "AB", "NS"):
        raise ValueError(
            f"Province {province_upper} has bulk data available through a dedicated ingester. "
            f"Use the appropriate command:\n"
            f"  - QC: mitds ingest quebec-corps\n"
            f"  - AB: mitds ingest alberta-nonprofits\n"
            f"  - NS: mitds ingest nova-scotia-coops"
        )

    # Build search terms list
    search_terms = []

    if target_entities:
        search_terms.extend(target_entities)

    if from_csv:
        import csv
        with open(from_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("name", "").strip()
                if name:
                    search_terms.append(name)

    if not search_terms:
        raise ValueError(
            "No search terms provided. Use --entity to specify company names "
            "or --from-csv to load from a CSV file."
        )

    # Apply limit if specified
    if limit and len(search_terms) > limit:
        search_terms = search_terms[:limit]

    # Import and run the search
    from .search import run_targeted_search

    return await run_targeted_search(
        province=province_upper,
        search_terms=search_terms,
        headless=headless,
        save_to_db=save_to_db,
    )
