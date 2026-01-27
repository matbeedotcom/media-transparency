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


@click.group(name="ingest")
def cli():
    """Data ingestion commands."""
    pass


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
def ingest_irs990(
    start_year: int | None,
    end_year: int | None,
    incremental: bool,
    limit: int | None,
    verbose: bool,
):
    """Ingest IRS 990 nonprofit filings.

    Downloads and processes IRS 990 XML filings from the IRS AWS S3 bucket.
    Extracts organizations, officers, and grant relationships.

    Examples:

        # Incremental sync for current and previous year
        mitds ingest irs990

        # Full sync for specific years
        mitds ingest irs990 --start-year 2020 --end-year 2023 --full

        # Test with limited records
        mitds ingest irs990 --limit 100 --verbose
    """
    from ..ingestion import run_irs990_ingestion

    click.echo(f"Starting IRS 990 ingestion...")

    if verbose:
        click.echo(f"  Start year: {start_year or 'previous year'}")
        click.echo(f"  End year: {end_year or 'current year'}")
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        if limit:
            click.echo(f"  Limit: {limit} records")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_irs990_ingestion(
                start_year=start_year,
                end_year=end_year,
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

    click.echo(f"Starting CRA ingestion...")

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
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def ingest_sec_edgar(
    incremental: bool,
    limit: int | None,
    with_ownership: bool,
    with_insiders: bool,
    verbose: bool,
):
    """Ingest SEC EDGAR company filings.

    Downloads company information from the SEC EDGAR database.
    Includes public companies, investment funds, and their filings.
    Automatically creates Neo4j graph nodes and OWNS relationships
    from SC 13D/13G beneficial ownership filings, and DIRECTOR_OF /
    EMPLOYED_BY relationships from Form 4 insider filings.

    Free API - no key required.

    Examples:

        # Incremental sync with ownership + insider parsing
        mitds ingest sec-edgar

        # Test with limited records
        mitds ingest sec-edgar --limit 100 --verbose

        # Skip ownership parsing for faster ingestion
        mitds ingest sec-edgar --no-ownership --limit 50

        # Skip insider parsing
        mitds ingest sec-edgar --no-insiders --limit 50
    """
    from ..ingestion.edgar import run_sec_edgar_ingestion

    click.echo("Starting SEC EDGAR ingestion...")

    if verbose:
        click.echo(f"  Mode: {'incremental' if incremental else 'full'}")
        click.echo(f"  Ownership parsing: {'enabled' if with_ownership else 'disabled'}")
        click.echo(f"  Insider parsing: {'enabled' if with_insiders else 'disabled'}")
        if limit:
            click.echo(f"  Limit: {limit} companies")

    start_time = datetime.now()

    try:
        result = asyncio.run(
            run_sec_edgar_ingestion(
                incremental=incremental,
                limit=limit,
                parse_ownership=with_ownership,
                parse_insiders=with_insiders,
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

        sources = ["irs990", "cra", "sec_edgar", "canada_corps", "opencorporates", "meta_ads"]
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
                click.echo(f"  Status: ", nl=False)
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
        click.echo(f"\nErrors: {len(errors)}")
        if verbose:
            for i, error in enumerate(errors[:10], 1):
                click.echo(f"  {i}. {error.get('error', 'Unknown error')}")
            if len(errors) > 10:
                click.echo(f"  ... and {len(errors) - 10} more")


# Main CLI entry point
@click.group()
def main():
    """MITDS Command Line Interface."""
    pass


main.add_command(cli)


if __name__ == "__main__":
    main()
