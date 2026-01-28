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
    from ..ingestion.littlesis import run_littlesis_ingestion, get_littlesis_stats

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
            click.echo(f"\nPre-ingestion cache status:")
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

        sources = ["irs990", "cra", "sec_edgar", "canada_corps", "opencorporates", "meta_ads", "lobbying", "elections_canada", "littlesis"]
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
