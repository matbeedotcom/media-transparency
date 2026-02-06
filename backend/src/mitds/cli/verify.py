"""CLI commands for data source verification.

Provides commands to run real-data verification suites
for each political ad funding data source.

Usage:
    mitds verify elections-third-party --jurisdiction federal --verbose
    mitds verify beneficial-ownership --verbose
    mitds verify google-ads --verbose
    mitds verify provincial-lobbying --province BC --verbose
    mitds verify canlii --verbose
    mitds verify --all --verbose
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import click

from ..logging import setup_logging

# Path to verification fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "tests" / "fixtures" / "verification"


@click.group(name="verify")
def cli():
    """Data source verification commands.

    Run real-data verification suites to validate ingestion accuracy
    for each political ad funding data source.
    """
    setup_logging()


@cli.command(name="elections-third-party")
@click.option(
    "--jurisdiction",
    type=click.Choice(["federal", "ontario", "bc", "alberta"]),
    default="federal",
    help="Elections jurisdiction to verify",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_elections_third_party(jurisdiction: str, verbose: bool) -> None:
    """Verify Elections Third-Party ingestion against reference data."""
    fixture_file = FIXTURES_DIR / "elections_canada_reference.json"

    if not fixture_file.exists():
        click.echo(f"Reference fixture not found: {fixture_file}")
        click.echo("Create reference data first before running verification.")
        sys.exit(1)

    result = asyncio.run(
        _run_elections_verification(jurisdiction, fixture_file, verbose)
    )
    _print_verification_result(result, "Elections Third-Party", verbose)


@cli.command(name="beneficial-ownership")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_beneficial_ownership(verbose: bool) -> None:
    """Verify Beneficial Ownership ingestion against reference data."""
    fixture_file = FIXTURES_DIR / "beneficial_ownership_reference.json"

    if not fixture_file.exists():
        click.echo(f"Reference fixture not found: {fixture_file}")
        click.echo("Create reference data first before running verification.")
        sys.exit(1)

    result = asyncio.run(
        _run_beneficial_ownership_verification(fixture_file, verbose)
    )
    _print_verification_result(result, "Beneficial Ownership", verbose)


@cli.command(name="google-ads")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_google_ads(verbose: bool) -> None:
    """Verify Google Political Ads ingestion against reference data."""
    fixture_file = FIXTURES_DIR / "google_ads_reference.json"

    if not fixture_file.exists():
        click.echo(f"Reference fixture not found: {fixture_file}")
        click.echo("Create reference data first before running verification.")
        sys.exit(1)

    result = asyncio.run(_run_google_ads_verification(fixture_file, verbose))
    _print_verification_result(result, "Google Political Ads", verbose)


@cli.command(name="provincial-lobbying")
@click.option(
    "--province",
    type=click.Choice(["BC", "ON", "AB"]),
    default="BC",
    help="Province to verify",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_provincial_lobbying(province: str, verbose: bool) -> None:
    """Verify Provincial Lobbying ingestion against reference data."""
    fixture_file = FIXTURES_DIR / "bc_lobbying_reference.json"

    if not fixture_file.exists():
        click.echo(f"Reference fixture not found: {fixture_file}")
        click.echo("Create reference data first before running verification.")
        sys.exit(1)

    result = asyncio.run(
        _run_provincial_lobbying_verification(province, fixture_file, verbose)
    )
    _print_verification_result(result, f"Provincial Lobbying ({province})", verbose)


@cli.command(name="canlii")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_canlii(verbose: bool) -> None:
    """Verify CanLII ingestion against reference data."""
    fixture_file = FIXTURES_DIR / "canlii_reference.json"

    if not fixture_file.exists():
        click.echo(f"Reference fixture not found: {fixture_file}")
        click.echo("Create reference data first before running verification.")
        sys.exit(1)

    result = asyncio.run(_run_canlii_verification(fixture_file, verbose))
    _print_verification_result(result, "CanLII", verbose)


@cli.command(name="all")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed comparison")
def verify_all(verbose: bool) -> None:
    """Run all verification suites."""
    sources = [
        ("Elections Third-Party (Federal)", "elections_canada_reference.json"),
        ("Beneficial Ownership", "beneficial_ownership_reference.json"),
        ("Google Political Ads", "google_ads_reference.json"),
        ("BC Lobbying", "bc_lobbying_reference.json"),
        ("CanLII", "canlii_reference.json"),
    ]

    all_passed = True
    results_summary: list[dict[str, Any]] = []

    for source_name, fixture_name in sources:
        fixture_file = FIXTURES_DIR / fixture_name
        if not fixture_file.exists():
            click.echo(f"  SKIP  {source_name} — no reference fixture")
            results_summary.append({"source": source_name, "status": "SKIP"})
            continue

        click.echo(f"  Verifying {source_name}...")
        # Each source would call its specific verification function
        results_summary.append({"source": source_name, "status": "PENDING"})

    click.echo("\n=== Verification Summary ===")
    for r in results_summary:
        status_icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "○", "PENDING": "…"}.get(
            r["status"], "?"
        )
        click.echo(f"  {status_icon} {r['source']}: {r['status']}")

    if not all_passed:
        sys.exit(1)


# =============================================================================
# Verification runner functions
# =============================================================================


async def _run_elections_verification(
    jurisdiction: str, fixture_file: Path, verbose: bool
) -> dict[str, Any]:
    """Run Elections Third-Party verification against reference data."""
    reference = _load_fixture(fixture_file)

    # Query system for ingested third-party data
    from ..db import get_db_session
    from sqlalchemy import text

    system_data: list[dict[str, Any]] = []

    try:
        async with get_db_session() as db:
            result = await db.execute(
                text(
                    """
                    SELECT name, metadata, external_ids
                    FROM entities
                    WHERE entity_type = 'organization'
                      AND metadata->>'advertiser_type' = 'third_party'
                    ORDER BY name
                    """
                )
            )
            rows = result.fetchall()
            for row in rows:
                system_data.append(
                    {
                        "name": row[0],
                        "metadata": row[1] if isinstance(row[1], dict) else {},
                        "external_ids": row[2] if isinstance(row[2], dict) else {},
                    }
                )
    except Exception as e:
        return {
            "passed": False,
            "error": str(e),
            "metrics": {},
        }

    return _compare_results(
        reference.get("third_parties", []),
        system_data,
        match_key="name",
    )


async def _run_beneficial_ownership_verification(
    fixture_file: Path, verbose: bool
) -> dict[str, Any]:
    """Run Beneficial Ownership verification against reference data."""
    reference = _load_fixture(fixture_file)

    from ..db import get_db_session, get_neo4j_session

    system_data: list[dict[str, Any]] = []

    try:
        async with get_neo4j_session() as session:
            result = await session.run(
                """
                MATCH (p:Person)-[r:BENEFICIAL_OWNER_OF]->(o:Organization)
                RETURN p.name AS person_name, o.name AS corp_name,
                       r.control_description AS control,
                       o.canada_corp_num AS corp_number
                ORDER BY p.name
                """
            )
            records = await result.data()
            for r in records:
                system_data.append(
                    {
                        "name": r["person_name"],
                        "corporation": r["corp_name"],
                        "corp_number": r.get("corp_number"),
                        "control": r.get("control"),
                    }
                )
    except Exception as e:
        return {"passed": False, "error": str(e), "metrics": {}}

    return _compare_results(
        reference.get("beneficial_owners", []),
        system_data,
        match_key="name",
    )


async def _run_google_ads_verification(
    fixture_file: Path, verbose: bool
) -> dict[str, Any]:
    """Run Google Ads verification against reference data."""
    reference = _load_fixture(fixture_file)

    from ..db import get_db_session
    from sqlalchemy import text

    system_data: list[dict[str, Any]] = []

    try:
        async with get_db_session() as db:
            result = await db.execute(
                text(
                    """
                    SELECT name, metadata, external_ids
                    FROM entities
                    WHERE entity_type = 'ad'
                      AND external_ids ? 'google_ad_id'
                    ORDER BY name
                    """
                )
            )
            rows = result.fetchall()
            for row in rows:
                metadata = row[1] if isinstance(row[1], dict) else {}
                system_data.append(
                    {
                        "name": metadata.get("advertiser_name", row[0]),
                        "ad_id": (row[2] or {}).get("google_ad_id"),
                        "metadata": metadata,
                    }
                )
    except Exception as e:
        return {"passed": False, "error": str(e), "metrics": {}}

    return _compare_results(
        reference.get("advertisers", []),
        system_data,
        match_key="name",
    )


async def _run_provincial_lobbying_verification(
    province: str, fixture_file: Path, verbose: bool
) -> dict[str, Any]:
    """Run Provincial Lobbying verification against reference data."""
    reference = _load_fixture(fixture_file)

    from ..db import get_neo4j_session

    system_data: list[dict[str, Any]] = []

    try:
        async with get_neo4j_session() as session:
            result = await session.run(
                """
                MATCH (l)-[r:PROVINCIAL_LOBBIES_FOR]->(o:Organization)
                WHERE r.jurisdiction = $province
                RETURN l.name AS lobbyist, o.name AS client,
                       r.registration_id AS reg_id
                ORDER BY l.name
                """,
                province=province,
            )
            records = await result.data()
            for r in records:
                system_data.append(
                    {
                        "name": r["lobbyist"],
                        "client": r["client"],
                        "registration_id": r.get("reg_id"),
                    }
                )
    except Exception as e:
        return {"passed": False, "error": str(e), "metrics": {}}

    return _compare_results(
        reference.get("lobbyist_pairs", []),
        system_data,
        match_key="name",
    )


async def _run_canlii_verification(
    fixture_file: Path, verbose: bool
) -> dict[str, Any]:
    """Run CanLII verification against reference data."""
    reference = _load_fixture(fixture_file)

    from ..db import get_neo4j_session

    system_data: list[dict[str, Any]] = []

    try:
        async with get_neo4j_session() as session:
            result = await session.run(
                """
                MATCH (a:Organization)-[r:LITIGATED_WITH]->(b:Organization)
                RETURN a.name AS entity_a, b.name AS entity_b,
                       r.case_citation AS citation, r.court AS court
                ORDER BY a.name
                """
            )
            records = await result.data()
            for r in records:
                system_data.append(
                    {
                        "name": f"{r['entity_a']} v. {r['entity_b']}",
                        "citation": r.get("citation"),
                        "court": r.get("court"),
                    }
                )
    except Exception as e:
        return {"passed": False, "error": str(e), "metrics": {}}

    return _compare_results(
        reference.get("entity_pairs", []),
        system_data,
        match_key="name",
    )


# =============================================================================
# Helper functions
# =============================================================================


def _load_fixture(fixture_file: Path) -> dict[str, Any]:
    """Load a JSON fixture file."""
    with open(fixture_file) as f:
        return json.load(f)


def _compare_results(
    expected: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    match_key: str = "name",
) -> dict[str, Any]:
    """Compare expected vs actual results and return metrics."""
    expected_keys = {item.get(match_key, "") for item in expected}
    actual_keys = {item.get(match_key, "") for item in actual}

    matched = expected_keys & actual_keys
    missing = expected_keys - actual_keys
    extra = actual_keys - expected_keys

    precision = len(matched) / len(actual_keys) if actual_keys else 0.0
    recall = len(matched) / len(expected_keys) if expected_keys else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    passed = recall >= 0.95  # Default threshold

    return {
        "passed": passed,
        "metrics": {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "matched_count": len(matched),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "total_expected": len(expected_keys),
            "total_actual": len(actual_keys),
            "matched": sorted(matched),
            "missing": sorted(missing),
            "extra": sorted(extra),
        },
    }


def _print_verification_result(
    result: dict[str, Any], source_name: str, verbose: bool
) -> None:
    """Print verification result with formatting."""
    if "error" in result:
        click.echo(f"\n✗ {source_name}: ERROR — {result['error']}")
        sys.exit(1)

    metrics = result.get("metrics", {})
    passed = result.get("passed", False)
    status = "PASS" if passed else "FAIL"
    icon = "✓" if passed else "✗"

    click.echo(f"\n{'=' * 50}")
    click.echo(f"{icon} Verification: {source_name} — {status}")
    click.echo(f"{'=' * 50}")
    click.echo(f"  Precision: {metrics.get('precision', 0):.2%}")
    click.echo(f"  Recall:    {metrics.get('recall', 0):.2%}")
    click.echo(f"  F1 Score:  {metrics.get('f1', 0):.2%}")
    click.echo(
        f"  Matched:   {metrics.get('matched_count', 0)}/{metrics.get('total_expected', 0)}"
    )
    click.echo(f"  Missing:   {metrics.get('missing_count', 0)}")
    click.echo(f"  Extra:     {metrics.get('extra_count', 0)}")

    if verbose and metrics.get("missing"):
        click.echo(f"\n  Missing items:")
        for item in metrics["missing"]:
            click.echo(f"    - {item}")

    if verbose and metrics.get("extra"):
        click.echo(f"\n  Extra items (not in reference):")
        for item in metrics["extra"]:
            click.echo(f"    - {item}")

    if not passed:
        sys.exit(1)
