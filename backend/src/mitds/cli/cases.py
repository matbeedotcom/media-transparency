"""CLI commands for Case Intake System.

Provides commands for case management, status monitoring, and report generation.
"""

import asyncio
import json
import sys
from uuid import UUID

import click

from ..cases.manager import CaseManager, get_case_manager
from ..cases.models import CaseConfig, CaseStatus, CreateCaseRequest, EntryPointType
from ..cases.reports.generator import ReportGenerator, get_report_generator
from ..cases.reports.templates import export_report
from ..cases.review.queue import EntityMatchQueue, get_match_queue


@click.group("case")
def case_group() -> None:
    """Manage research cases."""
    pass


@case_group.command("create")
@click.option("--name", "-n", required=True, help="Case name")
@click.option(
    "--type",
    "-t",
    "entry_type",
    required=True,
    type=click.Choice(["meta_ad", "corporation", "url", "text"]),
    help="Entry point type",
)
@click.option("--value", "-v", required=True, help="Entry point value")
@click.option("--max-depth", default=2, type=int, help="Maximum depth (default: 2)")
@click.option("--max-entities", default=100, type=int, help="Maximum entities (default: 100)")
@click.option("--start", is_flag=True, help="Start processing immediately")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def create_case(
    name: str,
    entry_type: str,
    value: str,
    max_depth: int,
    max_entities: int,
    start: bool,
    as_json: bool,
) -> None:
    """Create a new case from an entry point."""

    async def _create() -> None:
        manager = get_case_manager()

        config = CaseConfig(
            max_depth=max_depth,
            max_entities=max_entities,
        )

        request = CreateCaseRequest(
            name=name,
            entry_point_type=EntryPointType(entry_type),
            entry_point_value=value,
            config=config,
        )

        case = await manager.create_case(request)

        if start:
            case = await manager.start_processing(case.id)

        if as_json:
            click.echo(json.dumps(case.model_dump(mode="json"), indent=2))
        else:
            click.echo(f"Created case: {case.id}")
            click.echo(f"  Name: {case.name}")
            click.echo(f"  Type: {case.entry_point_type}")
            click.echo(f"  Status: {case.status}")
            if start:
                click.echo(f"  Research Session: {case.research_session_id}")

    asyncio.run(_create())


@case_group.command("list")
@click.option("--status", "-s", type=click.Choice([s.value for s in CaseStatus]), help="Filter by status")
@click.option("--limit", "-l", default=20, type=int, help="Maximum results")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def list_cases(status: str | None, limit: int, as_json: bool) -> None:
    """List cases."""

    async def _list() -> None:
        manager = get_case_manager()
        case_status = CaseStatus(status) if status else None
        cases, total = await manager.list_cases(status=case_status, limit=limit)

        if as_json:
            data = {
                "items": [c.model_dump(mode="json") for c in cases],
                "total": total,
            }
            click.echo(json.dumps(data, indent=2))
        else:
            click.echo(f"Cases ({total} total):")
            click.echo("")
            for case in cases:
                click.echo(f"  {case.id}")
                click.echo(f"    Name: {case.name}")
                click.echo(f"    Type: {case.entry_point_type}")
                click.echo(f"    Status: {case.status}")
                click.echo(f"    Created: {case.created_at}")
                click.echo("")

    asyncio.run(_list())


@case_group.command("status")
@click.argument("case_id")
@click.option("--watch", "-w", is_flag=True, help="Watch for updates")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def case_status(case_id: str, watch: bool, as_json: bool) -> None:
    """Get case status."""

    async def _status() -> None:
        manager = get_case_manager()
        case = await manager.get_case(UUID(case_id))

        if case is None:
            click.echo(f"Case not found: {case_id}", err=True)
            sys.exit(1)

        if as_json:
            click.echo(json.dumps(case.model_dump(mode="json"), indent=2))
        else:
            click.echo(f"Case: {case.id}")
            click.echo(f"  Name: {case.name}")
            click.echo(f"  Type: {case.entry_point_type}")
            click.echo(f"  Value: {case.entry_point_value[:50]}...")
            click.echo(f"  Status: {case.status}")
            click.echo("")
            click.echo("  Stats:")
            click.echo(f"    Entities: {case.stats.entity_count}")
            click.echo(f"    Relationships: {case.stats.relationship_count}")
            click.echo(f"    Leads processed: {case.stats.leads_processed}")
            click.echo(f"    Leads pending: {case.stats.leads_pending}")
            click.echo(f"    Pending matches: {case.stats.pending_matches}")
            click.echo("")
            if case.research_session_id:
                click.echo(f"  Research Session: {case.research_session_id}")

    if watch:
        import time
        try:
            while True:
                click.clear()
                asyncio.run(_status())
                time.sleep(5)
        except KeyboardInterrupt:
            pass
    else:
        asyncio.run(_status())


@case_group.command("pause")
@click.argument("case_id")
def pause_case(case_id: str) -> None:
    """Pause case processing."""

    async def _pause() -> None:
        manager = get_case_manager()
        try:
            case = await manager.pause_case(UUID(case_id))
            click.echo(f"Paused case: {case.id}")
            click.echo(f"  Status: {case.status}")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    asyncio.run(_pause())


@case_group.command("resume")
@click.argument("case_id")
def resume_case(case_id: str) -> None:
    """Resume paused case."""

    async def _resume() -> None:
        manager = get_case_manager()
        try:
            case = await manager.resume_case(UUID(case_id))
            click.echo(f"Resumed case: {case.id}")
            click.echo(f"  Status: {case.status}")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    asyncio.run(_resume())


@case_group.command("delete")
@click.argument("case_id")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def delete_case(case_id: str, force: bool) -> None:
    """Delete a case."""

    async def _delete() -> None:
        manager = get_case_manager()

        if not force:
            if not click.confirm(f"Delete case {case_id}?"):
                return

        deleted = await manager.delete_case(UUID(case_id))
        if deleted:
            click.echo(f"Deleted case: {case_id}")
        else:
            click.echo(f"Case not found: {case_id}", err=True)
            sys.exit(1)

    asyncio.run(_delete())


@case_group.command("report")
@click.argument("case_id")
@click.option("--format", "-f", "output_format", default="markdown", type=click.Choice(["json", "markdown"]))
@click.option("--output", "-o", type=click.Path(), help="Output file (default: stdout)")
@click.option("--regenerate", is_flag=True, help="Force regenerate report")
def case_report(case_id: str, output_format: str, output: str | None, regenerate: bool) -> None:
    """Generate or view case report."""

    async def _report() -> None:
        manager = get_case_manager()
        generator = get_report_generator()

        case = await manager.get_case(UUID(case_id))
        if case is None:
            click.echo(f"Case not found: {case_id}", err=True)
            sys.exit(1)

        # Get or generate report
        report = None
        if not regenerate:
            report = await generator.get_report(UUID(case_id))

        if report is None or regenerate:
            click.echo("Generating report...", err=True)
            report = await generator.generate(case)

        # Export
        content = export_report(report, output_format)

        if output:
            with open(output, "w") as f:
                f.write(content)
            click.echo(f"Report saved to: {output}", err=True)
        else:
            click.echo(content)

    asyncio.run(_report())


@case_group.command("reviews")
@click.argument("case_id")
@click.option("--limit", "-l", default=20, type=int, help="Maximum results")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def list_reviews(case_id: str, limit: int, as_json: bool) -> None:
    """List pending entity matches for review."""

    async def _reviews() -> None:
        queue = get_match_queue()
        matches, total = await queue.get_pending(UUID(case_id), limit=limit)

        if as_json:
            items = []
            for match in matches:
                response = await queue.get_match_with_entities(match.id)
                if response:
                    items.append(response.model_dump(mode="json"))
            click.echo(json.dumps({"items": items, "total": total}, indent=2))
        else:
            click.echo(f"Pending matches ({total} total):")
            click.echo("")
            for match in matches:
                response = await queue.get_match_with_entities(match.id)
                if response:
                    click.echo(f"  {match.id}")
                    click.echo(f"    Source: {response.source_entity.name} ({response.source_entity.entity_type})")
                    click.echo(f"    Target: {response.target_entity.name} ({response.target_entity.entity_type})")
                    click.echo(f"    Confidence: {match.confidence:.2f}")
                    click.echo("")

    asyncio.run(_reviews())


@case_group.command("review")
@click.argument("match_id")
@click.option("--approve", "-a", is_flag=True, help="Approve the match")
@click.option("--reject", "-r", is_flag=True, help="Reject the match")
@click.option("--defer", "-d", is_flag=True, help="Defer the match")
@click.option("--notes", "-n", help="Review notes")
@click.option("--reviewer", default="cli", help="Reviewer identifier")
def review_match(
    match_id: str,
    approve: bool,
    reject: bool,
    defer: bool,
    notes: str | None,
    reviewer: str,
) -> None:
    """Review an entity match."""

    if sum([approve, reject, defer]) != 1:
        click.echo("Error: Must specify exactly one of --approve, --reject, or --defer", err=True)
        sys.exit(1)

    async def _review() -> None:
        queue = get_match_queue()

        try:
            if approve:
                match = await queue.approve(UUID(match_id), reviewer, notes)
                click.echo(f"Approved match: {match.id}")
            elif reject:
                match = await queue.reject(UUID(match_id), reviewer, notes)
                click.echo(f"Rejected match: {match.id}")
            else:
                match = await queue.defer(UUID(match_id), reviewer, notes)
                click.echo(f"Deferred match: {match.id}")

            click.echo(f"  Status: {match.status}")
            if notes:
                click.echo(f"  Notes: {notes}")
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

    asyncio.run(_review())
