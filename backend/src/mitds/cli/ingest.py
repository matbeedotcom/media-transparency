"""CLI commands for data ingestion.

Provides command-line interface for running data ingestion
pipelines manually.

Usage:
    mitds ingest irs990 [--start-year YEAR] [--end-year YEAR] [--limit N]
    mitds ingest cra [--limit N]
"""

import asyncio
import sys
from datetime import datetime
from typing import Any

import click

from ..logging import setup_logging


@click.group(name="ingest")
def cli():
    """Data ingestion commands."""
    # Initialize logging for CLI
    setup_logging()


@cli.command(name="irs990")
@click.option(
    "--start-year",
    type=int,
    default=None,
    help="Start year for ingestion (default: previous year)",
)
@click.option(
    "--end-year",
    type=int,
    default=None,
    help="End year for ingestion (default: current year)",
)
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--batched/--no-batch",
    default=True,
    help="Use batched processing for better performance (default: batched)",
)
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of records per batch when using --batched (default: 100)",
)
@click.option(
    "--skip",
    type=int,
    default=0,
    help="Skip first N records (for resuming from a specific point)",
)
@click.option(
    "--workers",
    type=int,
    default=None,
    help="Number of parallel XML parsing workers (default: auto based on CPU count, max 8)",
)
def ingest_irs990(
    start_year: int | None,
    end_year: int | None,
    incremental: bool,
    limit: int | None,
    verbose: bool,
    batched: bool,
    batch_size: int,
    skip: int,
    workers: int | None,
):
    """Ingest IRS 990 nonprofit filings.

    Downloads and processes IRS 990 XML filings from the IRS AWS S3 bucket.
    Extracts organizations, officers, and grant relationships.

    Examples:

        # Incremental sync for current and previous year (batched by default)
        mitds ingest irs990

        # Full sync for specific years
        mitds ingest irs990 --start-year 2020 --end-year 2023 --full

        # Test with limited records
        mitds ingest irs990 --limit 100 --verbose

        # Use sequential processing (slower, for debugging)
        mitds ingest irs990 --no-batch --limit 100

        # Customize batch size for batched processing
        mitds ingest irs990 --batch-size 200

        # Resume from record 11000 (skip first 11000 records)
        mitds ingest irs990 --skip 11000

        # Resume and limit (process records 11000-12000)
        mitds ingest irs990 --skip 11000 --limit 1000
    """
    from ..ingestion import run_irs990_ingestion

    mode_str = "batched" if batched else "sequential"
    click.echo(f"Starting IRS 990 ingestion ({mode_str} mode)...")

    import os
    actual_workers = workers or min(os.cpu_count() or 4, 8)

    if verbose:
        click.echo(f"  Start year: {start_year or 'previous year'}")
        click.echo(f"  End year: {end_year or 'current year'}")
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        click.echo(f"  Processing: {mode_str}")
        if batched:
            click.echo(f"  Batch size: {batch_size}")
        click.echo(f"  Parse workers: {actual_workers}")
        if skip:
            click.echo(f"  Skip: {skip} records")
        if limit:
            click.echo(f"  Limit: {limit} records")

    if skip:
        click.echo(f"Skipping first {skip} records...")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_irs990_ingestion(
                start_year=start_year,
                end_year=end_year,
                incremental=incremental,
                limit=limit,
                batched=batched,
                batch_size=batch_size,
                skip=skip,
                workers=workers,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="cra")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_cra(
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest CRA registered charities.

    Downloads and processes Canadian charity data from the CRA Open Data portal.
    Extracts organizations and gifts to qualified donees.

    Examples:

        # Incremental sync
        mitds ingest cra

        # Full sync
        mitds ingest cra --full

        # Test with limited records
        mitds ingest cra --limit 100 --verbose
    """
    from ..ingestion import run_cra_ingestion

    click.echo("Starting CRA ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_cra_ingestion(
                incremental=incremental,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="alberta-nonprofits")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_alberta_nonprofits(
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest Alberta non-profit organizations.

    Downloads and processes Alberta's Non-Profit Listing from the Open Data portal.
    Extracts organizations with type, status, registration date, and location.

    Examples:

        # Incremental sync (only changed records)
        mitds ingest alberta-nonprofits

        # Full sync
        mitds ingest alberta-nonprofits --full

        # Test with limited records
        mitds ingest alberta-nonprofits --limit 100 --verbose
    """
    from ..ingestion.provincial import run_alberta_nonprofits_ingestion

    click.echo("Starting Alberta non-profit ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_alberta_nonprofits_ingestion(
                incremental=incremental,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="quebec-corps")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_quebec_corps(
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest Quebec corporations from Registraire des Entreprises.

    Downloads and processes Quebec's enterprise registry from Données Québec.
    Includes all corporation types: for-profit, non-profit, cooperatives, etc.

    Quebec has the best bulk data availability among Canadian provinces
    with daily CSV updates.

    Examples:

        # Incremental sync (only changed records)
        mitds ingest quebec-corps

        # Full sync
        mitds ingest quebec-corps --full

        # Test with limited records
        mitds ingest quebec-corps --limit 100 --verbose
    """
    from ..ingestion.provincial.quebec import run_quebec_corps_ingestion

    click.echo("Starting Quebec corporation ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        click.echo("  Source: Données Québec (daily CSV)")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_quebec_corps_ingestion(
                incremental=incremental,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="nova-scotia-coops")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_nova_scotia_coops(
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest Nova Scotia co-operatives from the Registry of Joint Stock Companies.

    Downloads and processes the list of co-operatives registered in Nova Scotia
    from the NS Open Data Portal.

    Data includes: registry ID, name, year incorporated, address,
    non-profit/for-profit classification, and co-op type.

    Note: This only includes co-operatives, not all corporations.
    Nova Scotia does not provide bulk data for other corporation types.

    Examples:

        # Incremental sync (only changed records)
        mitds ingest nova-scotia-coops

        # Full sync
        mitds ingest nova-scotia-coops --full

        # Test with limited records
        mitds ingest nova-scotia-coops --limit 100 --verbose
    """
    from ..ingestion.provincial.nova_scotia import run_nova_scotia_coops_ingestion

    click.echo("Starting Nova Scotia co-operatives ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        click.echo("  Source: NS Open Data Portal (co-operatives only)")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_nova_scotia_coops_ingestion(
                incremental=incremental,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="cross-reference")
@click.option(
    "--provinces",
    type=str,
    default=None,
    help="Comma-separated province codes to cross-reference (default: all)",
)
@click.option(
    "--auto-link-threshold",
    type=float,
    default=0.95,
    help="Threshold for automatic linking (default: 0.95)",
)
@click.option(
    "--review-threshold",
    type=float,
    default=0.85,
    help="Threshold for flagging for review (default: 0.85)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def cross_reference_provincial(
    provinces: str | None,
    auto_link_threshold: float,
    review_threshold: float,
    verbose: bool,
):
    """Cross-reference provincial corporations with federal registry.

    Matches provincial corporation records with federal registry data
    using business number and name matching strategies.

    Match results are classified by confidence:
    - Auto-link (>=95%): Automatically create SAME_AS relationship
    - Flag for review (85-95%): Requires manual verification
    - No match (<85%): No relationship created

    Examples:

        # Cross-reference all provinces
        mitds ingest cross-reference

        # Cross-reference specific provinces
        mitds ingest cross-reference --provinces QC,ON,AB

        # Adjust thresholds
        mitds ingest cross-reference --auto-link-threshold 0.90 --review-threshold 0.80
    """
    from ..ingestion.provincial.cross_reference import run_cross_reference

    click.echo("Starting provincial cross-referencing...")

    provinces_list = None
    if provinces:
        provinces_list = [p.strip().upper() for p in provinces.split(",")]

    if verbose:
        click.echo(f"  Provinces: {provinces_list or 'all'}")
        click.echo(f"  Auto-link threshold: {auto_link_threshold}")
        click.echo(f"  Review threshold: {review_threshold}")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_cross_reference(
                provinces=provinces_list,
                auto_link_threshold=auto_link_threshold,
                review_threshold=review_threshold,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        click.echo("\n" + "=" * 40)
        click.secho("Cross-Reference Complete", fg="green")
        click.echo("=" * 40)
        click.echo(f"Duration: {duration:.1f} seconds")
        click.echo(f"Total processed: {result.get('total_processed', 0)}")
        click.echo(f"Matched by BN: {result.get('matched_by_bn', 0)}")
        click.echo(f"Matched by exact name: {result.get('matched_by_exact_name', 0)}")
        click.echo(f"Matched by fuzzy name: {result.get('matched_by_fuzzy_name', 0)}")
        click.echo(f"Auto-linked: {result.get('auto_linked', 0)}")
        click.echo(f"Flagged for review: {result.get('flagged_for_review', 0)}")
        click.echo(f"No match: {result.get('no_match', 0)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="provincial-availability")
def provincial_availability():
    """Show which provinces have bulk data available for ingestion.

    Lists all Canadian provinces and territories with their data availability
    status. Only provinces with bulk open data can be ingested automatically.

    Provinces WITHOUT bulk data require manual research through their
    respective online registries and can be matched using the cross-reference
    service after being found through other sources (SEC, SEDAR, etc.).
    """
    from ..ingestion.provincial.targeted import get_available_provinces, get_unavailable_provinces

    click.echo("\n" + "=" * 60)
    click.secho("Provincial Corporation Data Availability", fg="cyan", bold=True)
    click.echo("=" * 60)

    click.echo("\n" + click.style("[+] BULK DATA AVAILABLE:", fg="green", bold=True))
    click.echo("-" * 40)

    available = get_available_provinces()
    for code, desc in available.items():
        click.echo(f"  {code}: {desc}")

    click.echo(f"\n  Commands:")
    click.echo(f"    mitds ingest quebec-corps")
    click.echo(f"    mitds ingest alberta-nonprofits")
    click.echo(f"    mitds ingest nova-scotia-coops")

    click.echo("\n" + click.style("[-] NO BULK DATA (search-only registries):", fg="red", bold=True))
    click.echo("-" * 40)

    unavailable = get_unavailable_provinces()
    for code, reason in unavailable.items():
        click.echo(f"  {code}: {reason}")

    click.echo("\n" + click.style("[*] SEARCH-ONLY REGISTRIES:", fg="yellow", bold=True))
    click.echo("-" * 40)

    from ..ingestion.provincial.search import get_registry_access_info
    access_info = get_registry_access_info()

    # Split into public search vs account required
    public_search = [(code, info) for code, info in access_info.items() if info.get("public_search")]
    account_required = [(code, info) for code, info in access_info.items() if info.get("requires_account")]

    if public_search:
        click.echo("\n  " + click.style("Public search available (Playwright):", fg="green"))
        for code, info in public_search:
            click.echo(f"    {code}: {info['name']} - {info['notes']}")
        click.echo("\n  Example:")
        click.echo("    mitds ingest provincial-search -p ON -e 'Company Name'")

    if account_required:
        click.echo("\n  " + click.style("Account required (manual lookup only):", fg="red"))
        for code, info in account_required:
            click.echo(f"    {code}: {info['name']} - {info['notes']}")

    click.echo("\n" + click.style("Alternative approach:", fg="cyan"))
    click.echo("  Entities can also be discovered through:")
    click.echo("    - SEC EDGAR (US-listed Canadian companies)")
    click.echo("    - SEDAR+ (Canadian securities filings)")
    click.echo("    - Elections Canada (third party advertisers)")
    click.echo("    - Lobbying Registry (registered lobbyists)")
    click.echo("  Then cross-referenced with: mitds ingest cross-reference")


@cli.command(name="provincial-search")
@click.option(
    "--province",
    "-p",
    required=True,
    help="Province code to search (e.g., ON, SK, MB, BC)",
)
@click.option(
    "--entity",
    "-e",
    multiple=True,
    help="Company name to search for (can be specified multiple times)",
)
@click.option(
    "--from-csv",
    type=click.Path(exists=True),
    help="CSV file with company names (must have 'name' column)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of entities to search",
)
@click.option(
    "--headless/--no-headless",
    default=True,
    help="Run browser in headless mode (default: headless)",
)
@click.option(
    "--save/--no-save",
    default=True,
    help="Save results to database (default: save)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_provincial_search(
    province: str,
    entity: tuple[str, ...],
    from_csv: str | None,
    limit: int | None,
    headless: bool,
    save: bool,
    verbose: bool,
):
    """Search provincial registries for specific companies using Playwright.

    For provinces that don't provide bulk data downloads, this command uses
    browser automation to search their online registries.

    Requires Playwright: pip install playwright && playwright install chromium

    Supported provinces: ON, SK, MB, BC, NB, PE, NL, NT, YT, NU

    Examples:

        # Search Ontario for a specific company
        mitds ingest provincial-search -p ON -e "Postmedia Network Inc."

        # Search multiple companies
        mitds ingest provincial-search -p SK -e "SaskTel" -e "Corus Entertainment"

        # Search from CSV file (must have 'name' column)
        mitds ingest provincial-search -p MB --from-csv companies.csv

        # Run with visible browser (for debugging)
        mitds ingest provincial-search -p ON -e "Test Corp" --no-headless
    """
    from ..ingestion.provincial.targeted import run_targeted_ingestion
    from ..ingestion.provincial.search import get_registry_access_info, get_public_search_provinces

    # Validate province
    search_provinces = {"ON", "SK", "MB", "BC", "NB", "PE", "PEI", "NL", "NT", "YT", "NU"}
    bulk_provinces = {"QC", "AB", "NS"}
    public_search_provinces = set(get_public_search_provinces())

    province_upper = province.upper()
    if province_upper == "PEI":
        province_upper = "PE"

    if province_upper in bulk_provinces:
        click.echo(
            f"Error: Province {province_upper} has bulk data available. "
            f"Use the dedicated ingester instead:",
            err=True,
        )
        click.echo(f"  - QC: mitds ingest quebec-corps", err=True)
        click.echo(f"  - AB: mitds ingest alberta-nonprofits", err=True)
        click.echo(f"  - NS: mitds ingest nova-scotia-coops", err=True)
        sys.exit(1)

    if province_upper not in search_provinces:
        click.echo(
            f"Error: Province '{province}' is not supported.", err=True
        )
        click.echo(f"Supported: {', '.join(sorted(search_provinces))}", err=True)
        sys.exit(1)

    # Check if province requires account
    if province_upper not in public_search_provinces:
        access_info = get_registry_access_info()
        info = access_info.get(province_upper, {})
        click.echo(
            f"Error: {info.get('name', province_upper)} registry requires an account.",
            err=True,
        )
        click.echo(f"  {info.get('notes', 'No public search available')}", err=True)
        click.echo(f"  Registry URL: {info.get('url', 'N/A')}", err=True)
        click.echo("\nUse 'mitds ingest provincial-availability' to see all options.", err=True)
        sys.exit(1)

    # Build search terms
    search_terms = list(entity)

    if not search_terms and not from_csv:
        click.echo(
            "Error: No search terms provided. Use --entity or --from-csv.",
            err=True,
        )
        sys.exit(1)

    click.echo(f"Starting {province_upper} registry search...")

    if verbose:
        click.echo(f"  Province: {province_upper}")
        click.echo(f"  Headless: {headless}")
        click.echo(f"  Save to DB: {save}")
        if entity:
            click.echo(f"  Entities: {len(entity)}")
        if from_csv:
            click.echo(f"  CSV file: {from_csv}")
        if limit:
            click.echo(f"  Limit: {limit}")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_targeted_ingestion(
                province=province_upper,
                target_entities=search_terms if search_terms else None,
                from_csv=from_csv,
                limit=limit,
                headless=headless,
                save_to_db=save,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        click.echo(f"\n{'=' * 40}")
        click.echo(f"Search completed in {duration:.1f}s")
        click.echo(f"  Results found: {result.get('results_found', 0)}")
        click.echo(f"  Records created: {result.get('records_created', 0)}")
        click.echo(f"  Records updated: {result.get('records_updated', 0)}")

        if verbose and result.get("results"):
            click.echo(f"\nResults:")
            for r in result["results"]:
                click.echo(f"  - {r['name']} ({r['registration_number']}) - {r['status']}")

        if result.get("errors"):
            click.echo(f"\nErrors ({len(result['errors'])}):")
            for err in result["errors"][:5]:
                click.echo(f"  - {err}")

    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo(
            "\nTo use provincial search, install Playwright:",
            err=True,
        )
        click.echo("  pip install playwright", err=True)
        click.echo("  playwright install chromium", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="provincial-corps")
@click.option(
    "--provinces",
    type=str,
    default=None,
    help="Comma-separated province codes to ingest (default: all bulk-data provinces)",
)
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process per province",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_provincial_corps_batch(
    provinces: str | None,
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Batch ingest corporations from all provinces with bulk data.

    Runs ingestion for all specified provinces sequentially.
    Only provinces with bulk data access are supported.

    Currently supported bulk-data provinces:
    - QC: Quebec (Registraire des Entreprises - daily CSV, all corp types)
    - AB: Alberta (Non-profit organizations only - monthly XLSX)
    - NS: Nova Scotia (Co-operatives only - CSV)

    Provinces WITHOUT bulk data (BC, SK, MB, ON, NB, PE, NL, NT, YT, NU)
    do not provide open data exports. Use 'mitds ingest provincial-availability'
    to see alternatives.

    Examples:

        # Ingest all bulk-data provinces
        mitds ingest provincial-corps

        # Ingest specific provinces
        mitds ingest provincial-corps --provinces QC,AB,NS

        # Test with limited records
        mitds ingest provincial-corps --limit 100 --verbose
    """
    from ..ingestion.provincial import run_quebec_corps_ingestion
    from ..ingestion.provincial import run_alberta_nonprofits_ingestion
    from ..ingestion.provincial.nova_scotia import run_nova_scotia_coops_ingestion

    # Provinces with bulk data and their ingestion functions
    BULK_DATA_PROVINCES = {
        "QC": ("Quebec corporations", run_quebec_corps_ingestion),
        "AB": ("Alberta non-profits", run_alberta_nonprofits_ingestion),
        "NS": ("Nova Scotia co-ops", run_nova_scotia_coops_ingestion),
    }

    if provinces:
        province_list = [p.strip().upper() for p in provinces.split(",")]
        # Validate provinces
        invalid = [p for p in province_list if p not in BULK_DATA_PROVINCES]
        if invalid:
            click.echo(
                f"Error: Provinces {invalid} don't have bulk data access.", err=True
            )
            click.echo(f"Valid bulk-data provinces: {list(BULK_DATA_PROVINCES.keys())}", err=True)
            click.echo(
                "\nRun 'mitds ingest provincial-availability' to see all options.",
                err=True,
            )
            sys.exit(1)
    else:
        province_list = list(BULK_DATA_PROVINCES.keys())

    click.echo(f"Starting batch provincial corporation ingestion...")
    click.echo(f"  Provinces: {province_list}")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} records per province")

    total_start = datetime.now()
    results = []

    for province in province_list:
        desc, ingestion_func = BULK_DATA_PROVINCES[province]

        click.echo(f"\n{'=' * 40}")
        click.echo(f"Processing {province} ({desc})...")
        click.echo("=" * 40)

        start_time = datetime.now()

        try:
            result = asyncio.run(
                ingestion_func(
                    incremental=incremental,
                    limit=limit,
                )
            )

            duration = (datetime.now() - start_time).total_seconds()
            result["province"] = province
            result["duration"] = duration
            results.append(result)

            click.echo(f"\n{province} completed in {duration:.1f}s")
            click.echo(f"  Processed: {result.get('records_processed', 0)}")
            click.echo(f"  Created: {result.get('records_created', 0)}")

        except Exception as e:
            click.echo(f"Error processing {province}: {e}", err=True)
            results.append({
                "province": province,
                "status": "failed",
                "error": str(e),
            })

    # Print summary
    total_duration = (datetime.now() - total_start).total_seconds()

    click.echo("\n" + "=" * 50)
    click.secho("BATCH INGESTION COMPLETE", fg="green")
    click.echo("=" * 50)
    click.echo(f"Total duration: {total_duration:.1f} seconds")
    click.echo(f"Provinces processed: {len(results)}")

    total_processed = sum(r.get("records_processed", 0) for r in results)
    total_created = sum(r.get("records_created", 0) for r in results)
    click.echo(f"Total records processed: {total_processed}")
    click.echo(f"Total records created: {total_created}")

    # Check for failures
    failures = [r for r in results if r.get("status") == "failed"]
    if failures:
        click.secho(f"\nFailed provinces: {len(failures)}", fg="red")
        for f in failures:
            click.echo(f"  - {f['province']}: {f.get('error', 'Unknown error')}")


@cli.command(name="opencorporates")
@click.option(
    "--search",
    type=str,
    default=None,
    help="Search query for companies",
)
@click.option(
    "--entity-names",
    type=str,
    default=None,
    help="Comma-separated entity names to search for",
)
@click.option(
    "--jurisdiction",
    type=str,
    default=None,
    help="Filter by jurisdiction (e.g., us_de, gb)",
)
@click.option(
    "--max-companies",
    type=int,
    default=100,
    help="Maximum number of companies to ingest",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_opencorporates(
    search: str | None,
    entity_names: str | None,
    jurisdiction: str | None,
    max_companies: int,
    verbose: bool,
):
    """Ingest company data from OpenCorporates.

    Fetches company and officer information from the OpenCorporates API.
    Creates organizations, persons, and their relationships.

    Examples:

        # Search for specific companies
        mitds ingest opencorporates --search "Acme Corporation"

        # Enrich existing entities
        mitds ingest opencorporates --entity-names "Foundation X,Institute Y"

        # Filter by jurisdiction
        mitds ingest opencorporates --search "Tech Corp" --jurisdiction us_de

        # Limit results
        mitds ingest opencorporates --search "Media" --max-companies 50 --verbose
    """
    from ..ingestion.opencorp import run_opencorporates_ingestion

    if not search and not entity_names:
        click.echo("Error: Either --search or --entity-names is required", err=True)
        sys.exit(1)

    click.echo("Starting OpenCorporates ingestion...")

    if verbose:
        if search:
            click.echo(f"  Search query: {search}")
        if entity_names:
            click.echo(f"  Entity names: {entity_names}")
        if jurisdiction:
            click.echo(f"  Jurisdiction: {jurisdiction}")
        click.echo(f"  Max companies: {max_companies}")

    start_time = datetime.now()

    try:
        # Parse entity names if provided
        names_list = None
        if entity_names:
            names_list = [n.strip() for n in entity_names.split(",")]

        # Parse jurisdictions
        jurisdictions = [jurisdiction] if jurisdiction else None

        result = asyncio.run(
            run_opencorporates_ingestion(
                entity_names=names_list,
                search_query=search,
                jurisdiction_codes=jurisdictions,
                max_companies=max_companies,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="sec-edgar")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of companies to process",
)
@click.option(
    "--target",
    type=str,
    default=None,
    help="Comma-separated list of CIKs to target",
)
@click.option(
    "--with-ownership/--no-ownership",
    default=True,
    help="Parse 13D/13G ownership filings (default: enabled)",
)
@click.option(
    "--with-insiders/--no-insiders",
    default=True,
    help="Parse Form 4 insider transaction filings (default: enabled)",
)
@click.option(
    "--flag-canadian/--no-flag-canadian",
    default=True,
    help="Detect and flag Canadian companies in ownership filings (default: enabled)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_sec_edgar(
    incremental: bool,
    limit: int | None,
    target: str | None,
    with_ownership: bool,
    with_insiders: bool,
    flag_canadian: bool,
    verbose: bool,
):
    """Ingest SEC EDGAR company filings.

    Downloads company information from the SEC EDGAR database.
    Includes public companies, investment funds, and their filings.
    Automatically creates Neo4j graph nodes and OWNS relationships
    from SC 13D/13G beneficial ownership filings, and DIRECTOR_OF /
    EMPLOYED_BY relationships from Form 4 insider filings.

    With --flag-canadian (default), detects Canadian companies in 13D/13G
    filings and flags them with jurisdiction: CA in the graph.

    Free API - no key required.

    Examples:

        # Incremental sync with ownership + insider parsing
        mitds ingest sec-edgar

        # Test with limited records
        mitds ingest sec-edgar --limit 100 --verbose

        # Target specific CIKs (e.g., Chatham Asset Management)
        mitds ingest sec-edgar --target 0001633336 --limit 5 -v

        # Skip ownership parsing for faster ingestion
        mitds ingest sec-edgar --no-ownership --limit 50

        # Skip insider parsing
        mitds ingest sec-edgar --no-insiders --limit 50

        # Disable Canadian detection
        mitds ingest sec-edgar --no-flag-canadian --limit 50
    """
    from ..ingestion.edgar import run_sec_edgar_ingestion

    click.echo("Starting SEC EDGAR ingestion...")

    target_entities = None
    if target:
        target_entities = [t.strip() for t in target.split(",")]

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        click.echo(f"  Ownership parsing: {'enabled' if with_ownership else 'disabled'}")
        click.echo(f"  Insider parsing: {'enabled' if with_insiders else 'disabled'}")
        click.echo(f"  Canadian detection: {'enabled' if flag_canadian else 'disabled'}")
        if target_entities:
            click.echo(f"  Target CIKs: {target_entities}")
        if limit:
            click.echo(f"  Limit: {limit} companies")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_sec_edgar_ingestion(
                incremental=incremental,
                limit=limit,
                target_entities=target_entities,
                parse_ownership=with_ownership,
                parse_insiders=with_insiders,
                flag_canadian=flag_canadian,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="sedar")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of filings to process",
)
@click.option(
    "--target",
    type=str,
    default=None,
    help="Comma-separated list of company names, SEDAR profiles, or document URLs",
)
@click.option(
    "--csv-path",
    type=str,
    default=None,
    help="Path to CSV export from SEDAR+ web interface",
)
@click.option(
    "--from-date",
    type=str,
    default=None,
    help="Start date for filing search (YYYY-MM-DD)",
)
@click.option(
    "--to-date",
    type=str,
    default=None,
    help="End date for filing search (YYYY-MM-DD)",
)
@click.option(
    "--doc-types",
    type=str,
    default=None,
    help="Comma-separated document types: early_warning, alternative_monthly",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_sedar(
    incremental: bool,
    limit: int | None,
    target: str | None,
    csv_path: str | None,
    from_date: str | None,
    to_date: str | None,
    doc_types: str | None,
    verbose: bool,
):
    """Ingest SEDAR+ Canadian securities filings.

    Downloads and processes Early Warning Reports and Alternative Monthly
    Reports from SEDAR+ to track Canadian corporate ownership.

    Creates:
    - Organization nodes for Canadian acquirers and issuers
    - OWNS relationships with ownership percentage and filing details

    Note: SEDAR+ does not have a public API. Ingestion modes:
    1. --target: Process specific document URLs
    2. --csv-path: Process exported CSV from SEDAR+ web interface
    3. Manual: Visit sedarplus.ca, search, export, then use --csv-path

    Free data - no key required.

    Examples:

        # Process a specific SEDAR+ document URL
        mitds ingest sedar --target "https://www.sedarplus.ca/csa-party/records/document.html?id=..."

        # Process CSV export from SEDAR+ web search
        mitds ingest sedar --csv-path /path/to/sedar_export.csv

        # Target multiple companies (for manual lookup guidance)
        mitds ingest sedar --target "Postmedia Network,Corus Entertainment" -v

        # Limit processing
        mitds ingest sedar --csv-path export.csv --limit 50 --verbose
    """
    from ..ingestion.sedar import run_sedar_ingestion

    click.echo("Starting SEDAR+ ingestion...")

    target_entities = None
    if target:
        target_entities = [t.strip() for t in target.split(",")]

    document_types = None
    if doc_types:
        document_types = [t.strip() for t in doc_types.split(",")]

    # Parse dates
    date_from = None
    date_to = None
    if from_date:
        try:
            date_from = datetime.strptime(from_date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(f"Invalid from-date format: {from_date}. Use YYYY-MM-DD", err=True)
            sys.exit(1)
    if to_date:
        try:
            date_to = datetime.strptime(to_date, "%Y-%m-%d").date()
        except ValueError:
            click.echo(f"Invalid to-date format: {to_date}. Use YYYY-MM-DD", err=True)
            sys.exit(1)

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if target_entities:
            click.echo(f"  Targets: {target_entities}")
        if csv_path:
            click.echo(f"  CSV path: {csv_path}")
        if date_from:
            click.echo(f"  From date: {date_from}")
        if date_to:
            click.echo(f"  To date: {date_to}")
        if document_types:
            click.echo(f"  Document types: {document_types}")
        if limit:
            click.echo(f"  Limit: {limit} filings")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_sedar_ingestion(
                incremental=incremental,
                limit=limit,
                target_entities=target_entities,
                csv_path=csv_path,
                document_types=document_types,
                date_from=date_from,
                date_to=date_to,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="canada-corps")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of corporations to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_canada_corps(
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest Canada federal corporations.

    Downloads corporation data from the ISED Open Government Portal.
    Includes CBCA corporations, not-for-profits, and cooperatives.

    Free data - no key required.

    Examples:

        # Incremental sync
        mitds ingest canada-corps

        # Test with limited records
        mitds ingest canada-corps --limit 100 --verbose
    """
    from ..ingestion.canada_corps import run_canada_corps_ingestion

    click.echo("Starting Canada Corporations ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} corporations")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_canada_corps_ingestion(
                incremental=incremental,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="lobbying")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of registrations to process",
)
@click.option(
    "--target",
    type=str,
    default=None,
    help="Comma-separated client/registrant names to filter",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_lobbying(
    incremental: bool,
    limit: int | None,
    target: str | None,
    verbose: bool,
):
    """Ingest Canada Lobbying Registry data.

    Downloads lobbying registrations and communication reports from
    the Office of the Commissioner of Lobbying of Canada.

    Creates relationships between lobbyists, their clients, and
    government institutions being lobbied.

    Free data - no key required.

    Examples:

        # Full sync
        mitds ingest lobbying

        # Test with limited records
        mitds ingest lobbying --limit 100 --verbose

        # Target specific organizations
        mitds ingest lobbying --target "National Citizens Coalition,Fraser Institute"
    """
    from ..ingestion.lobbying import run_lobbying_ingestion

    click.echo("Starting Lobbying Registry ingestion...")

    target_entities = None
    if target:
        target_entities = [t.strip() for t in target.split(",")]

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} registrations")
        if target_entities:
            click.echo(f"  Target entities: {target_entities}")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_lobbying_ingestion(
                incremental=incremental,
                limit=limit,
                target_entities=target_entities,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="elections-canada")
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of third parties to process",
)
@click.option(
    "--target",
    type=str,
    default=None,
    help="Comma-separated third party names to filter",
)
@click.option(
    "--elections",
    type=str,
    default=None,
    help="Comma-separated election IDs (e.g., '44,45' for 44th and 45th GE)",
)
@click.option(
    "--parse-pdfs",
    is_flag=True,
    help="Download and parse PDF financial returns for detailed expenses/suppliers",
)
@click.option(
    "--enrich-vendors/--no-enrich-vendors",
    default=False,
    help="Search external sources (Canada Corps, SEC, CRA) for vendor matches",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_elections_canada(
    incremental: bool,
    limit: int | None,
    target: str | None,
    elections: str | None,
    parse_pdfs: bool,
    enrich_vendors: bool,
    verbose: bool,
):
    """Ingest Elections Canada third party data.

    Downloads third party registration and advertising expense data
    from Elections Canada for federal elections.

    Creates:
    - Organization nodes for third parties
    - Election nodes for federal elections
    - REGISTERED_FOR relationships between organizations and elections
    - ADVERTISED_ON relationships for advertising expenses by media type
    - Person/Organization nodes for financial agents and auditors

    With --parse-pdfs (requires pdfplumber):
    - Vendor nodes for advertising suppliers (Facebook, radio stations, etc.)
    - PAID_BY relationships showing money flow from third parties to vendors
    - Person nodes for individual contributors (>$200)
    - CONTRIBUTED_TO relationships showing who funded the third party

    Free data - no key required.

    Examples:

        # Process all elections
        mitds ingest elections-canada

        # Process specific elections
        mitds ingest elections-canada --elections 44,45

        # Target specific organizations with full expense/supplier data
        mitds ingest elections-canada --target "National Citizens Coalition" --parse-pdfs

        # Test with limited records
        mitds ingest elections-canada --limit 10 --verbose
    """
    from ..ingestion.elections_canada import run_elections_canada_ingestion

    click.echo("Starting Elections Canada ingestion...")

    target_entities = None
    if target:
        target_entities = [t.strip() for t in target.split(",")]

    election_ids = None
    if elections:
        election_ids = [e.strip() for e in elections.split(",")]

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} third parties")
        if target_entities:
            click.echo(f"  Target entities: {target_entities}")
        if election_ids:
            click.echo(f"  Elections: {election_ids}")
        if parse_pdfs:
            click.echo("  Parse PDFs: enabled (will extract suppliers/contributors)")
        if enrich_vendors:
            click.echo("  Enrich vendors: enabled (will search external sources)")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_elections_canada_ingestion(
                incremental=incremental,
                limit=limit,
                target_entities=target_entities,
                elections=election_ids,
                parse_pdfs=parse_pdfs,
                enrich_vendors=enrich_vendors,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="meta-ads")
@click.option(
    "--days-back",
    type=int,
    default=7,
    help="Number of days to look back (default: 7)",
)
@click.option(
    "--countries",
    type=str,
    default=None,
    help="Comma-separated country codes (default: US,CA)",
)
@click.option(
    "--search-terms",
    type=str,
    default=None,
    help="Comma-separated search terms to filter ads",
)
@click.option(
    "--page-ids",
    type=str,
    default=None,
    help="Comma-separated Meta page IDs to filter ads",
)
@click.option(
    "--incremental/--full",
    default=True,
    help="Incremental or full sync (default: incremental)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of ads to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--minimal-fields",
    is_flag=True,
    help="Use minimal fields (for debugging permission issues)",
)
@click.option(
    "--enrich-page-details",
    is_flag=True,
    help="Fetch Facebook Page details (contact info, social links, managing orgs)",
)
def ingest_meta_ads(
    days_back: int,
    countries: str | None,
    search_terms: str | None,
    page_ids: str | None,
    incremental: bool,
    limit: int | None,
    verbose: bool,
    minimal_fields: bool,
    enrich_page_details: bool,
):
    """Ingest political ads from Meta Ad Library.

    Fetches political and social issue ads from Meta's Ad Library API
    for the US and Canada. Creates Ad nodes, Sponsor entities, and
    SPONSORED_BY relationships.

    With --enrich-page-details, also fetches Facebook Page information:
    - Contact info (website, email, phone, WhatsApp)
    - Social links (Instagram, YouTube, Twitter/X)
    - Managing organizations (agencies, businesses)
    - Page stats (followers, verification status)

    Requires META_ACCESS_TOKEN environment variable (or META_APP_ID +
    META_APP_SECRET for token refresh). Your app must also have completed
    Meta's App Review for the ads_read permission.

    Rate limited to 200 calls/hour by Meta.

    Examples:

        # Search for election-related ads (search term required)
        mitds ingest meta-ads --search-terms "election"

        # Fetch last 30 days
        mitds ingest meta-ads --search-terms "vote" --days-back 30

        # Filter by country
        mitds ingest meta-ads --search-terms "candidate" --countries US

        # Filter by page ID (alternative to search terms)
        mitds ingest meta-ads --page-ids "123456789,987654321"

        # Debug permission issues with minimal fields
        mitds ingest meta-ads --search-terms "test" --minimal-fields --limit 5
        
        # Fetch ads with full Facebook Page details
        mitds ingest meta-ads --search-terms "coalition" --enrich-page-details
    """
    from ..ingestion.meta_ads import run_meta_ads_ingestion

    click.echo("Starting Meta Ad Library ingestion...")

    countries_list = None
    if countries:
        countries_list = [c.strip() for c in countries.split(",")]

    search_terms_list = None
    if search_terms:
        search_terms_list = [t.strip() for t in search_terms.split(",")]

    page_ids_list = None
    if page_ids:
        page_ids_list = [p.strip() for p in page_ids.split(",")]

    if verbose:
        click.echo(f"  Days back: {days_back}")
        click.echo(f"  Countries: {countries_list or ['US', 'CA']}")
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if search_terms_list:
            click.echo(f"  Search terms: {search_terms_list}")
        if page_ids_list:
            click.echo(f"  Page IDs: {page_ids_list}")
        if limit:
            click.echo(f"  Limit: {limit} ads")
        if minimal_fields:
            click.echo("  Minimal fields: enabled (debugging mode)")
        if enrich_page_details:
            click.echo("  Page details enrichment: enabled")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_meta_ads_ingestion(
                countries=countries_list,
                days_back=days_back,
                incremental=incremental,
                limit=limit,
                search_terms=search_terms_list,
                page_ids=page_ids_list,
                minimal_fields=minimal_fields,
                enrich_page_details=enrich_page_details,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        _print_result(result, duration, verbose)

    except ValueError as e:
        # Likely missing credentials
        click.echo(f"Configuration error: {e}", err=True)
        click.echo("\nTo use the Meta Ad Library API, set one of:", err=True)
        click.echo("  - META_APP_ID + META_APP_SECRET (recommended - auto-generates token)", err=True)
        click.echo("  - META_ACCESS_TOKEN (for User/System User tokens)", err=True)
        click.echo("\nGet your App ID and Secret from: https://developers.facebook.com/apps/", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="meta-token")
@click.option(
    "--extend",
    type=str,
    default=None,
    help="Short-lived token to exchange for a long-lived token (60 days)",
)
@click.option(
    "--debug",
    type=str,
    default=None,
    help="Token to debug/inspect",
)
@click.option(
    "--save-to-env",
    is_flag=True,
    help="Save the new token to backend/.env file",
)
def meta_token_command(
    extend: str | None,
    debug: str | None,
    save_to_env: bool,
):
    """Manage Meta (Facebook) access tokens.

    The Meta Ad Library API requires a User Access Token (not just App credentials).
    This command helps you:
    
    1. Exchange a short-lived token for a long-lived token (60 days)
    2. Debug/inspect a token to see its expiration and permissions
    3. Optionally save the new token to your .env file

    HOW TO GET A SHORT-LIVED TOKEN:
    
    1. Go to: https://developers.facebook.com/tools/explorer/
    2. Select your app from the dropdown
    3. Click "Generate Access Token"
    4. Grant the required permissions (ads_read)
    5. Copy the token and use with --extend

    Examples:

        # Exchange short-lived token for long-lived (60 days):
        mitds ingest meta-token --extend "EAAxxxxxx..."

        # Exchange and save to .env automatically:
        mitds ingest meta-token --extend "EAAxxxxxx..." --save-to-env

        # Debug a token to see its expiration:
        mitds ingest meta-token --debug "EAAxxxxxx..."
    """
    import httpx
    from ..config import get_settings
    
    settings = get_settings()
    
    if not settings.meta_app_id or not settings.meta_app_secret:
        click.echo("Error: META_APP_ID and META_APP_SECRET must be set in .env", err=True)
        sys.exit(1)
    
    if extend:
        click.echo("Exchanging short-lived token for long-lived token...")
        
        # Exchange for long-lived token
        url = "https://graph.facebook.com/v24.0/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "fb_exchange_token": extend,
        }
        
        try:
            response = httpx.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                try:
                    error = response.json().get("error", {})
                    click.echo(f"\nError: {error.get('message', response.text)}", err=True)
                    click.echo(f"Error code: {error.get('code')}", err=True)
                except Exception:
                    click.echo(f"\nError: {response.text}", err=True)
                sys.exit(1)
            
            data = response.json()
            new_token = data["access_token"]
            expires_in = data.get("expires_in", 5184000)  # Default 60 days
            
            # Calculate expiry date
            from datetime import timedelta
            expiry_date = datetime.now() + timedelta(seconds=expires_in)
            
            click.echo(f"\n{'='*60}")
            click.echo("SUCCESS! Long-lived token generated")
            click.echo(f"{'='*60}")
            click.echo(f"\nExpires: {expiry_date.strftime('%Y-%m-%d %H:%M')} ({expires_in // 86400} days)")
            click.echo(f"\nToken:\n{new_token}")
            
            if save_to_env:
                # Update .env file
                env_path = "backend/.env"
                try:
                    with open(env_path, "r") as f:
                        env_content = f.read()
                    
                    # Check if META_ACCESS_TOKEN exists
                    import re
                    if re.search(r"^#?\s*META_ACCESS_TOKEN=", env_content, re.MULTILINE):
                        # Replace existing (including commented out)
                        env_content = re.sub(
                            r"^#?\s*META_ACCESS_TOKEN=.*$",
                            f"META_ACCESS_TOKEN={new_token}",
                            env_content,
                            flags=re.MULTILINE
                        )
                    else:
                        # Add new line after META_APP_SECRET
                        env_content = re.sub(
                            r"(META_APP_SECRET=.*?)(\n)",
                            f"\\1\\2META_ACCESS_TOKEN={new_token}\\2",
                            env_content
                        )
                    
                    with open(env_path, "w") as f:
                        f.write(env_content)
                    
                    click.echo(f"\n✓ Token saved to {env_path}")
                    click.echo("  Restart your server to use the new token.")
                except Exception as e:
                    click.echo(f"\nWarning: Could not save to {env_path}: {e}", err=True)
                    click.echo("Please manually add to your .env:")
                    click.echo(f"META_ACCESS_TOKEN={new_token}")
            else:
                click.echo(f"\nTo use this token, add to your .env file:")
                click.echo(f"META_ACCESS_TOKEN={new_token}")
                click.echo(f"\nOr run with --save-to-env to auto-update .env")
                
        except httpx.RequestError as e:
            click.echo(f"Network error: {e}", err=True)
            sys.exit(1)
    
    elif debug:
        click.echo("Debugging token...")
        
        url = "https://graph.facebook.com/debug_token"
        params = {
            "input_token": debug,
            "access_token": f"{settings.meta_app_id}|{settings.meta_app_secret}",
        }
        
        try:
            response = httpx.get(url, params=params, timeout=30)
            data = response.json()
            
            if "error" in data:
                click.echo(f"\nError: {data['error'].get('message')}", err=True)
                sys.exit(1)
            
            token_data = data.get("data", {})
            
            click.echo(f"\n{'='*60}")
            click.echo("TOKEN INFO")
            click.echo(f"{'='*60}")
            click.echo(f"App ID: {token_data.get('app_id', 'N/A')}")
            click.echo(f"Type: {token_data.get('type', 'N/A')}")
            click.echo(f"Valid: {token_data.get('is_valid', False)}")
            
            if token_data.get("expires_at"):
                expiry = datetime.fromtimestamp(token_data["expires_at"])
                days_left = (expiry - datetime.now()).days
                click.echo(f"Expires: {expiry.strftime('%Y-%m-%d %H:%M')} ({days_left} days left)")
            elif token_data.get("data_access_expires_at"):
                expiry = datetime.fromtimestamp(token_data["data_access_expires_at"])
                click.echo(f"Data access expires: {expiry.strftime('%Y-%m-%d %H:%M')}")
            else:
                click.echo("Expires: Never (or not specified)")
            
            if token_data.get("scopes"):
                click.echo(f"Permissions: {', '.join(token_data['scopes'])}")
            
            if not token_data.get("is_valid"):
                error = token_data.get("error", {})
                click.echo(f"\nInvalid reason: {error.get('message', 'Unknown')}")
                
        except httpx.RequestError as e:
            click.echo(f"Network error: {e}", err=True)
            sys.exit(1)
    
    else:
        # Show help on how to get a token
        click.echo("Meta Ad Library Token Helper")
        click.echo("="*60)
        click.echo("\nThe Meta Ad Library API requires a User Access Token.")
        click.echo("Here's how to get one:\n")
        click.echo("1. Go to: https://developers.facebook.com/tools/explorer/")
        click.echo("2. Select your app from the 'Meta App' dropdown")
        click.echo("3. Click 'Generate Access Token'")
        click.echo("4. Approve permissions if prompted")
        click.echo("5. Copy the generated token\n")
        click.echo("Then exchange it for a long-lived token (60 days):")
        click.echo('  mitds ingest meta-token --extend "YOUR_TOKEN" --save-to-env\n')
        click.echo("To check when your current token expires:")
        click.echo('  mitds ingest meta-token --debug "YOUR_TOKEN"')


@cli.command(name="meta-page-details")
@click.option(
    "--page-id",
    type=str,
    default=None,
    help="Facebook Page ID to fetch details for",
)
@click.option(
    "--sponsor-name",
    type=str,
    default=None,
    help="Sponsor name to search for and enrich",
)
@click.option(
    "--include-agencies/--no-agencies",
    default=True,
    help="Include managing organizations (default: yes)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def fetch_meta_page_details(
    page_id: str | None,
    sponsor_name: str | None,
    include_agencies: bool,
    verbose: bool,
):
    """Fetch Facebook Page details for a sponsor.

    Retrieves detailed information about a Facebook Page including:
    - Contact info (website, email, phone, WhatsApp)
    - Social links (Instagram, YouTube, Twitter/X)
    - Managing organizations (agencies, businesses)
    - Page stats (followers, verification status)

    You can either:
    - Provide a --page-id directly
    - Provide a --sponsor-name to search for and enrich

    Examples:

        # Fetch details by page ID
        mitds ingest meta-page-details --page-id "123456789"

        # Search for a sponsor and fetch their page details
        mitds ingest meta-page-details --sponsor-name "National Citizens Coalition"

        # Verbose output with all details
        mitds ingest meta-page-details --page-id "123456789" -v
    """
    from ..ingestion.meta_ads import (
        fetch_facebook_page_details,
        search_and_enrich_sponsor,
    )

    if not page_id and not sponsor_name:
        click.echo("Error: Either --page-id or --sponsor-name is required", err=True)
        sys.exit(1)

    try:
        if page_id:
            click.echo(f"Fetching Facebook Page details for ID: {page_id}")
            details = asyncio.run(fetch_facebook_page_details(page_id, include_agencies))

            if details:
                click.echo(f"\nPage: {details.page_name}")
                if details.username:
                    click.echo(f"Username: @{details.username}")
                if details.category:
                    click.echo(f"Category: {details.category}")
                if details.verification_status:
                    click.echo(f"Verification: {details.verification_status}")
                if details.followers_count:
                    click.echo(f"Followers: {details.followers_count:,}")

                # Contact info
                click.echo("\nContact Information:")
                if details.contact.website:
                    click.echo(f"  Website: {details.contact.website}")
                if details.contact.websites and len(details.contact.websites) > 1:
                    click.echo(f"  All websites: {', '.join(details.contact.websites)}")
                if details.contact.emails:
                    click.echo(f"  Emails: {', '.join(details.contact.emails)}")
                if details.contact.phone:
                    click.echo(f"  Phone: {details.contact.phone}")
                if details.contact.whatsapp_number:
                    click.echo(f"  WhatsApp: {details.contact.whatsapp_number}")
                if details.contact.address:
                    click.echo(f"  Address: {details.contact.address}")

                # Social links
                click.echo("\nSocial Links:")
                if details.social_links.facebook_url:
                    click.echo(f"  Facebook: {details.social_links.facebook_url}")
                if details.social_links.instagram_username:
                    click.echo(f"  Instagram: @{details.social_links.instagram_username}")
                if details.social_links.twitter_handle:
                    click.echo(f"  Twitter/X: @{details.social_links.twitter_handle}")
                if details.social_links.youtube_url:
                    click.echo(f"  YouTube: {details.social_links.youtube_url}")

                # Managing organizations
                if details.managing_organizations:
                    click.echo("\nManaging Organizations:")
                    for org in details.managing_organizations:
                        click.echo(f"  - {org.name} ({org.type})")

                if verbose:
                    click.echo("\nAbout:")
                    if details.about:
                        click.echo(f"  {details.about}")
                    if details.description:
                        click.echo(f"\nDescription:\n  {details.description}")
            else:
                click.echo("Could not fetch page details (may require additional permissions)")

        elif sponsor_name:
            click.echo(f"Searching for sponsor: {sponsor_name}")
            result = asyncio.run(search_and_enrich_sponsor(sponsor_name))

            click.echo(f"\nAds found: {result['ads_found']}")
            if result['page_id']:
                click.echo(f"Page ID: {result['page_id']}")
            click.echo(f"Enriched: {'Yes' if result['enriched'] else 'No'}")

            if result['page_details'] and verbose:
                details = result['page_details']
                click.echo(f"\nPage Details:")
                click.echo(f"  Name: {details.get('page_name', 'N/A')}")
                if details.get('contact', {}).get('website'):
                    click.echo(f"  Website: {details['contact']['website']}")
                if details.get('social_links', {}).get('twitter_handle'):
                    click.echo(f"  Twitter: @{details['social_links']['twitter_handle']}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="littlesis")
@click.option(
    "--entities/--no-entities",
    default=True,
    help="Ingest entities (default: yes)",
)
@click.option(
    "--relationships/--no-relationships",
    default=True,
    help="Ingest relationships (default: yes)",
)
@click.option(
    "--force-refresh",
    is_flag=True,
    help="Force re-download of bulk data files even if cached",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_littlesis(
    entities: bool,
    relationships: bool,
    force_refresh: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest LittleSis bulk data (entities and relationships).

    Downloads and processes the LittleSis bulk data exports containing
    curated data on U.S. political and corporate power structures.

    Data is cached locally and in S3 for 7 days before re-downloading.

    Creates:
    - Person and Organization entities from LittleSis
    - Relationships: FUNDED_BY, DIRECTOR_OF, EMPLOYED_BY, OWNS

    License: CC BY-SA 4.0 (attribution required)

    Examples:

        # Full import (entities + relationships)
        mitds ingest littlesis

        # Entities only
        mitds ingest littlesis --no-relationships

        # Force fresh download
        mitds ingest littlesis --force-refresh

        # Test with limited records
        mitds ingest littlesis --limit 1000 --verbose
    """
    from ..ingestion.littlesis import get_littlesis_stats, run_littlesis_ingestion

    click.echo("Starting LittleSis bulk data ingestion...")

    if verbose:
        click.echo(f"  Entities: {'yes' if entities else 'no'}")
        click.echo(f"  Relationships: {'yes' if relationships else 'no'}")
        click.echo(f"  Force refresh: {'yes' if force_refresh else 'no'}")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    # Combine stats + ingestion in single async function to avoid event loop issues
    async def _run_with_stats():
        stats = None
        if verbose:
            try:
                stats = await get_littlesis_stats()
            except Exception:
                pass

        result = await run_littlesis_ingestion(
            entities=entities,
            relationships=relationships,
            force_refresh=force_refresh,
            limit=limit,
        )
        return stats, result

    try:
        stats, result = asyncio.run(_run_with_stats())

        if verbose and stats:
            click.echo("\nPre-ingestion cache status:")
            click.echo(f"  Cache valid: {stats.get('cache_valid', False)}")
            click.echo(f"  Entities in DB: {stats.get('entity_count', 0)}")
            click.echo(f"  Relationships in DB: {stats.get('relationship_count', 0)}")

        duration = (datetime.now() - start_time).total_seconds()

        click.echo("\n" + "=" * 40)
        click.secho("LittleSis Ingestion Complete", fg="green")
        click.echo("=" * 40)
        click.echo(f"Duration: {duration:.1f} seconds")

        if result.get("entities"):
            click.echo("\nEntities:")
            _print_result(result["entities"], 0, verbose)

        if result.get("relationships"):
            click.echo("\nRelationships:")
            _print_result(result["relationships"], 0, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="linkedin")
@click.option(
    "--company",
    "-c",
    type=str,
    default=None,
    help="Company name to filter results or search for",
)
@click.option(
    "--company-url",
    type=str,
    default=None,
    help="LinkedIn company URL (e.g., https://www.linkedin.com/company/postmedia)",
)
@click.option(
    "--company-entity-id",
    type=str,
    default=None,
    help="UUID of existing organization entity to link members to",
)
@click.option(
    "--from-csv",
    type=click.Path(exists=True),
    help="CSV file with LinkedIn data (recommended)",
)
@click.option(
    "--scrape/--no-scrape",
    default=False,
    help="Enable browser scraping mode (requires authentication)",
)
@click.option(
    "--session-cookie",
    type=str,
    default=None,
    envvar="LINKEDIN_SESSION_COOKIE",
    help="LinkedIn li_at session cookie for authentication",
)
@click.option(
    "--cookies-file",
    type=click.Path(exists=True),
    help="JSON file with LinkedIn cookies (exported from browser)",
)
@click.option(
    "--titles",
    type=str,
    default=None,
    help="Comma-separated title keywords to filter (e.g., 'CEO,Director,VP')",
)
@click.option(
    "--headless/--no-headless",
    default=True,
    help="Run browser in headless mode (default: headless)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of profiles to process",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_linkedin(
    company: str | None,
    company_url: str | None,
    company_entity_id: str | None,
    from_csv: str | None,
    scrape: bool,
    session_cookie: str | None,
    cookies_file: str | None,
    titles: str | None,
    headless: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest LinkedIn company members for network research.

    Maps out organizational networks by extracting member data from LinkedIn.
    Supports two primary modes:

    1. CSV IMPORT (Recommended):
       Import data exported from LinkedIn or Sales Navigator.
       No authentication required, no rate limits.

    2. BROWSER SCRAPING:
       Automated scraping of company pages using Playwright.
       Requires LinkedIn authentication (session cookie).
       Use with caution - may violate LinkedIn ToS.

    Creates Person nodes in Neo4j with EMPLOYED_BY relationships
    to organizations. Executives and board members are flagged.

    Examples:

        # Import from CSV (recommended)
        mitds ingest linkedin --from-csv members.csv --company "Postmedia Network"

        # Import and link to existing organization entity
        mitds ingest linkedin --from-csv members.csv --company-entity-id UUID

        # Scrape company page (requires auth)
        mitds ingest linkedin --company-url "https://linkedin.com/company/postmedia" \\
            --scrape --session-cookie "your_li_at_cookie"

        # Filter by executive titles
        mitds ingest linkedin --from-csv members.csv --titles "CEO,CFO,Director,VP"

        # Scrape with visible browser (for debugging)
        mitds ingest linkedin --company "Corus Entertainment" --scrape --no-headless

    Authentication for Scraping:

        To get your LinkedIn session cookie:
        1. Log into LinkedIn in your browser
        2. Open Developer Tools (F12) > Application > Cookies
        3. Find the 'li_at' cookie and copy its value
        4. Pass via --session-cookie or LINKEDIN_SESSION_COOKIE env var

        Or export all cookies as JSON and use --cookies-file
    """
    from ..ingestion.linkedin import run_linkedin_ingestion

    if not from_csv and not scrape:
        click.echo(
            "Error: Must specify either --from-csv or --scrape mode",
            err=True,
        )
        click.echo("\nUse --from-csv to import LinkedIn data from a CSV file", err=True)
        click.echo("Use --scrape with --session-cookie to scrape LinkedIn directly", err=True)
        sys.exit(1)

    if scrape and not session_cookie and not cookies_file:
        click.echo(
            "Error: Scraping requires authentication.",
            err=True,
        )
        click.echo("\nProvide LinkedIn session cookie via:", err=True)
        click.echo("  --session-cookie 'your_li_at_cookie_value'", err=True)
        click.echo("  or --cookies-file path/to/cookies.json", err=True)
        click.echo("  or LINKEDIN_SESSION_COOKIE environment variable", err=True)
        sys.exit(1)

    if scrape and not company and not company_url:
        click.echo(
            "Error: Scraping requires --company or --company-url",
            err=True,
        )
        sys.exit(1)

    # Parse titles filter
    titles_filter = None
    if titles:
        titles_filter = [t.strip() for t in titles.split(",")]

    click.echo("Starting LinkedIn member ingestion...")

    if verbose:
        click.echo(f"  Mode: {'scraping' if scrape else 'CSV import'}")
        if company:
            click.echo(f"  Company: {company}")
        if company_url:
            click.echo(f"  Company URL: {company_url}")
        if company_entity_id:
            click.echo(f"  Link to entity: {company_entity_id}")
        if from_csv:
            click.echo(f"  CSV file: {from_csv}")
        if titles_filter:
            click.echo(f"  Title filter: {titles_filter}")
        if limit:
            click.echo(f"  Limit: {limit} profiles")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_linkedin_ingestion(
                csv_path=from_csv,
                company_name=company,
                company_url=company_url,
                company_entity_id=company_entity_id,
                scrape=scrape,
                headless=headless,
                session_cookie=session_cookie,
                cookies_file=cookies_file,
                titles_filter=titles_filter,
                limit=limit,
            )
        )

        duration = (datetime.now() - start_time).total_seconds()

        click.echo("\n" + "=" * 40)
        click.secho("LinkedIn Ingestion Complete", fg="green")
        click.echo("=" * 40)
        click.echo(f"Duration: {duration:.1f} seconds")
        click.echo(f"Profiles processed: {result.get('records_processed', 0)}")
        click.echo(f"Profiles created: {result.get('records_created', 0)}")
        click.echo(f"Profiles updated: {result.get('records_updated', 0)}")

        if result.get("errors"):
            click.echo(f"\nErrors: {len(result['errors'])}")
            if verbose:
                for i, err in enumerate(result["errors"][:5], 1):
                    click.echo(f"  {i}. {err}")

    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        if "playwright" in str(e).lower():
            click.echo(
                "\nTo use LinkedIn scraping, install Playwright:",
                err=True,
            )
            click.echo("  pip install playwright", err=True)
            click.echo("  playwright install chromium", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="status")
def ingestion_status():
    """Show status of all ingestion pipelines."""
    from sqlalchemy import text

    from ..db import get_db_session

    async def _get_status():
        async with get_db_session() as db:
            query = text("""
                SELECT DISTINCT ON (source)
                    source,
                    status,
                    started_at,
                    completed_at,
                    records_processed,
                    records_created
                FROM ingestion_runs
                ORDER BY source, completed_at DESC NULLS LAST
            """)

            result = await db.execute(query)
            return result.fetchall()

    try:
        runs = asyncio.run(_get_status())

        click.echo("\nIngestion Pipeline Status")
        click.echo("=" * 60)

        sources = ["irs990", "cra", "sec_edgar", "canada_corps", "sedar", "opencorporates", "meta_ads", "lobbying", "elections_canada", "littlesis"]
        run_by_source = {r.source: r for r in runs}

        for source in sources:
            run = run_by_source.get(source)

            if run:
                status_color = {
                    "completed": "green",
                    "partial": "yellow",
                    "running": "blue",
                    "failed": "red",
                }.get(run.status, "white")

                click.echo(f"\n{source.upper()}")
                click.echo("  Status: ", nl=False)
                click.secho(run.status, fg=status_color)
                click.echo(f"  Last run: {run.completed_at or 'N/A'}")
                click.echo(f"  Records processed: {run.records_processed or 0}")
                click.echo(f"  Records created: {run.records_created or 0}")
            else:
                click.echo(f"\n{source.upper()}")
                click.secho("  Status: never_run", fg="yellow")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _print_result(result: dict[str, Any], duration: float, verbose: bool):
    """Print ingestion result."""
    status = result.get("status", "unknown")
    status_color = {
        "completed": "green",
        "partial": "yellow",
        "failed": "red",
    }.get(status, "white")

    click.echo("\n" + "=" * 40)
    click.echo("Ingestion ", nl=False)
    click.secho(status.upper(), fg=status_color)
    click.echo("=" * 40)

    click.echo(f"Duration: {duration:.1f} seconds")
    click.echo(f"Records processed: {result.get('records_processed', 0)}")
    click.echo(f"Records created: {result.get('records_created', 0)}")
    click.echo(f"Records updated: {result.get('records_updated', 0)}")
    click.echo(f"Duplicates found: {result.get('duplicates_found', 0)}")

    errors = result.get("errors", [])
    if errors:
        # errors can be an integer (count) or a list (actual error objects)
        error_count = errors if isinstance(errors, int) else len(errors)
        click.echo(f"\nErrors: {error_count}")
        if verbose and isinstance(errors, list):
            for i, error in enumerate(errors[:10], 1):
                if isinstance(error, dict):
                    click.echo(f"  {i}. {error.get('error', 'Unknown error')}")
                else:
                    click.echo(f"  {i}. {error}")
            if len(errors) > 10:
                click.echo(f"  ... and {len(errors) - 10} more")


# Main CLI entry point
@click.group()
def main():
    """MITDS Command Line Interface."""
    pass


main.add_command(cli)


if __name__ == "__main__":
    # When run directly as a module, use the ingest group directly
    cli()
