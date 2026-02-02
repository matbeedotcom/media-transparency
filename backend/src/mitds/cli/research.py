"""CLI commands for research investigations.

Provides commands for managing research sessions that
"follow the leads" through entity networks.
"""

import asyncio
import json
import sys
from datetime import datetime
from uuid import UUID

import click
from tqdm import tqdm

from ..research import (
    EntryPointType,
    LeadType,
    ResearchSessionConfig,
    SessionStatus,
    get_processor,
    get_queue_manager,
    get_session_manager,
)


def run_async(coro):
    """Run an async coroutine in a new event loop."""
    return asyncio.get_event_loop().run_until_complete(coro)


@click.group(name="research")
def cli():
    """Research investigation commands.

    Start investigations from various entry points and
    automatically discover related entities.
    """
    pass


@cli.command(name="start")
@click.option("--name", "-n", required=True, help="Session name")
@click.option(
    "--entry-type",
    "-t",
    type=click.Choice(["meta-ads", "company", "ein", "bn", "nonprofit", "entity-id"]),
    required=True,
    help="Entry point type",
)
@click.option("--entry-value", "-v", required=True, help="Search query or identifier")
@click.option("--description", "-d", help="Session description")
@click.option("--max-depth", default=3, help="Maximum hops from entry point")
@click.option("--max-entities", default=500, help="Maximum entities to discover")
@click.option("--jurisdictions", default="US,CA", help="Jurisdictions to include (comma-separated)")
@click.option(
    "--lead-types",
    default="all",
    help="Lead types to follow (comma-separated or 'all')",
)
@click.option("--min-confidence", default=0.5, help="Minimum confidence threshold")
@click.option("--background/--foreground", default=False, help="Run in background")
@click.option("--verbose", "-V", is_flag=True, help="Verbose output")
def start_research(
    name: str,
    entry_type: str,
    entry_value: str,
    description: str | None,
    max_depth: int,
    max_entities: int,
    jurisdictions: str,
    lead_types: str,
    min_confidence: float,
    background: bool,
    verbose: bool,
):
    """Start a new research investigation.

    Examples:

        # Start from Meta ad search
        mitds research start --name "PAC Investigation" \\
            --entry-type meta-ads --entry-value "election 2024"

        # Start from company EIN
        mitds research start --name "Foundation Network" \\
            --entry-type ein --entry-value "13-1837418"

        # Start from Canadian business number
        mitds research start --name "Canadian Media" \\
            --entry-type bn --entry-value "891234567RR0001"

        # Start with custom limits
        mitds research start --name "Deep Dive" \\
            --entry-type company --entry-value "Postmedia" \\
            --max-depth 5 --max-entities 1000
    """
    # Map CLI entry type to enum
    entry_type_map = {
        "meta-ads": EntryPointType.META_ADS,
        "company": EntryPointType.COMPANY,
        "ein": EntryPointType.EIN,
        "bn": EntryPointType.BN,
        "nonprofit": EntryPointType.NONPROFIT,
        "entity-id": EntryPointType.ENTITY_ID,
    }
    entry_point_type = entry_type_map[entry_type]

    # Parse jurisdictions
    jurisdiction_list = [j.strip().upper() for j in jurisdictions.split(",")]

    # Parse lead types
    if lead_types.lower() == "all":
        enabled_lead_types = list(LeadType)
    else:
        lead_type_map = {
            "ownership": LeadType.OWNERSHIP,
            "funding": LeadType.FUNDING,
            "sponsorship": LeadType.SPONSORSHIP,
            "board": LeadType.BOARD_INTERLOCK,
            "cross-border": LeadType.CROSS_BORDER,
            "infrastructure": LeadType.INFRASTRUCTURE,
        }
        enabled_lead_types = [
            lead_type_map[lt.strip().lower()]
            for lt in lead_types.split(",")
            if lt.strip().lower() in lead_type_map
        ]

    # Create config
    config = ResearchSessionConfig(
        max_depth=max_depth,
        max_entities=max_entities,
        jurisdictions=jurisdiction_list,
        enabled_lead_types=enabled_lead_types,
        min_confidence=min_confidence,
    )

    async def _create_and_run():
        manager = get_session_manager()

        # Create session
        session = await manager.create_session(
            name=name,
            entry_point_type=entry_point_type,
            entry_point_value=entry_value,
            config=config,
            description=description,
        )

        click.echo(f"Created research session: {session.id}")
        click.echo(f"  Name: {name}")
        click.echo(f"  Entry: {entry_type} = {entry_value}")
        click.echo(f"  Max depth: {max_depth}, Max entities: {max_entities}")

        if not background:
            # Run in foreground
            click.echo("\nStarting research (Ctrl+C to pause)...")
            processor = get_processor()

            try:
                stats = await processor.process_session(session.id)
                click.echo(f"\nSession completed:")
                click.echo(f"  Entities discovered: {stats.total_entities}")
                click.echo(f"  Relationships found: {stats.total_relationships}")
                click.echo(f"  Leads processed: {stats.leads_completed}")
            except KeyboardInterrupt:
                click.echo("\nPausing session...")
                await manager.pause_session(session.id)
                click.echo(f"Session paused. Resume with: mitds research resume {session.id}")
        else:
            click.echo(f"\nRun in foreground with: mitds research resume {session.id}")

        return session.id

    session_id = run_async(_create_and_run())
    click.echo(f"\nSession ID: {session_id}")


@cli.command(name="status")
@click.argument("session_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def research_status(session_id: str, as_json: bool):
    """Get status of a research session."""
    async def _get_status():
        manager = get_session_manager()
        queue = get_queue_manager()

        session = await manager.get_session(UUID(session_id))
        if not session:
            click.echo(f"Session not found: {session_id}", err=True)
            return None

        queue_stats = await queue.get_queue_stats(UUID(session_id))

        if as_json:
            output = {
                "id": str(session.id),
                "name": session.name,
                "status": session.status.value if isinstance(session.status, SessionStatus) else session.status,
                "entry_point_type": session.entry_point_type.value if isinstance(session.entry_point_type, EntryPointType) else session.entry_point_type,
                "entry_point_value": session.entry_point_value,
                "created_at": session.created_at.isoformat() if session.created_at else None,
                "stats": {
                    "entities": session.stats.total_entities,
                    "relationships": session.stats.total_relationships,
                    "leads_pending": queue_stats.pending,
                    "leads_completed": queue_stats.completed,
                    "leads_failed": queue_stats.failed,
                },
            }
            click.echo(json.dumps(output, indent=2))
        else:
            status_val = session.status.value if isinstance(session.status, SessionStatus) else session.status
            entry_type_val = session.entry_point_type.value if isinstance(session.entry_point_type, EntryPointType) else session.entry_point_type

            click.echo(f"Research Session: {session.name}")
            click.echo(f"  ID: {session.id}")
            click.echo(f"  Status: {status_val}")
            click.echo(f"  Entry Point: {entry_type_val} = {session.entry_point_value}")
            click.echo(f"  Created: {session.created_at}")
            click.echo()
            click.echo("Progress:")
            click.echo(f"  Entities discovered: {session.stats.total_entities}")
            click.echo(f"  Relationships found: {session.stats.total_relationships}")
            click.echo()
            click.echo("Lead Queue:")
            click.echo(f"  Pending: {queue_stats.pending}")
            click.echo(f"  In progress: {queue_stats.in_progress}")
            click.echo(f"  Completed: {queue_stats.completed}")
            click.echo(f"  Skipped: {queue_stats.skipped}")
            click.echo(f"  Failed: {queue_stats.failed}")

        return session

    run_async(_get_status())


@cli.command(name="pause")
@click.argument("session_id")
def pause_research(session_id: str):
    """Pause a running research session."""
    async def _pause():
        manager = get_session_manager()
        session = await manager.pause_session(UUID(session_id))

        if session:
            click.echo(f"Paused session: {session_id}")
        else:
            click.echo(f"Could not pause session (not running?): {session_id}", err=True)

    run_async(_pause())


@cli.command(name="resume")
@click.argument("session_id")
@click.option("--max-iterations", default=None, type=int, help="Max iterations before stopping")
def resume_research(session_id: str, max_iterations: int | None):
    """Resume a paused research session."""
    async def _resume():
        manager = get_session_manager()
        processor = get_processor()

        # Resume the session
        session = await manager.resume_session(UUID(session_id))
        if not session:
            # Try starting if it was just created
            session = await manager.get_session(UUID(session_id))
            if not session:
                click.echo(f"Session not found: {session_id}", err=True)
                return

            if session.status == SessionStatus.INITIALIZING:
                session = await manager.start_session(UUID(session_id))
            elif session.status != SessionStatus.PAUSED:
                click.echo(f"Session is not paused: {session.status}", err=True)
                return

        click.echo(f"Resuming session: {session.name}")
        click.echo("Press Ctrl+C to pause...")

        try:
            stats = await processor.process_session(
                UUID(session_id),
                max_iterations=max_iterations,
            )
            click.echo(f"\nSession completed:")
            click.echo(f"  Entities discovered: {stats.total_entities}")
            click.echo(f"  Relationships found: {stats.total_relationships}")
            click.echo(f"  Leads processed: {stats.leads_completed}")
        except KeyboardInterrupt:
            click.echo("\nPausing session...")
            await manager.pause_session(UUID(session_id))
            click.echo(f"Session paused. Resume with: mitds research resume {session_id}")

    run_async(_resume())


@cli.command(name="list")
@click.option("--status", help="Filter by status (running, paused, completed, failed)")
@click.option("--limit", default=20, help="Maximum sessions to show")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def list_sessions(status: str | None, limit: int, as_json: bool):
    """List research sessions."""
    async def _list():
        manager = get_session_manager()

        status_filter = None
        if status:
            try:
                status_filter = SessionStatus(status)
            except ValueError:
                click.echo(f"Invalid status: {status}", err=True)
                return

        sessions = await manager.list_sessions(status=status_filter, limit=limit)

        if as_json:
            output = [
                {
                    "id": str(s.id),
                    "name": s.name,
                    "status": s.status.value if isinstance(s.status, SessionStatus) else s.status,
                    "entry_point": s.entry_point_value,
                    "entities": s.stats.total_entities,
                    "created_at": s.created_at.isoformat() if s.created_at else None,
                }
                for s in sessions
            ]
            click.echo(json.dumps(output, indent=2))
        else:
            if not sessions:
                click.echo("No research sessions found.")
                return

            click.echo(f"{'ID':<38} {'Name':<25} {'Status':<12} {'Entities':<10} {'Created':<20}")
            click.echo("-" * 105)

            for s in sessions:
                status_val = s.status.value if isinstance(s.status, SessionStatus) else s.status
                created = s.created_at.strftime("%Y-%m-%d %H:%M") if s.created_at else "N/A"
                click.echo(
                    f"{str(s.id):<38} {s.name[:24]:<25} {status_val:<12} "
                    f"{s.stats.total_entities:<10} {created:<20}"
                )

    run_async(_list())


@cli.command(name="export")
@click.argument("session_id")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "csv", "graphml"]),
    default="json",
    help="Export format",
)
@click.option("--output", "-o", help="Output file path (stdout if not specified)")
def export_research(session_id: str, format: str, output: str | None):
    """Export research session results."""
    async def _export():
        manager = get_session_manager()

        session = await manager.get_session(UUID(session_id))
        if not session:
            click.echo(f"Session not found: {session_id}", err=True)
            return

        entities = await manager.get_session_entities(UUID(session_id), limit=10000)

        if format == "json":
            export_data = {
                "session": {
                    "id": str(session.id),
                    "name": session.name,
                    "entry_point_type": session.entry_point_type.value if isinstance(session.entry_point_type, EntryPointType) else session.entry_point_type,
                    "entry_point_value": session.entry_point_value,
                    "status": session.status.value if isinstance(session.status, SessionStatus) else session.status,
                    "created_at": session.created_at.isoformat() if session.created_at else None,
                    "completed_at": session.completed_at.isoformat() if session.completed_at else None,
                },
                "stats": {
                    "total_entities": session.stats.total_entities,
                    "total_relationships": session.stats.total_relationships,
                    "leads_completed": session.stats.leads_completed,
                },
                "entities": [
                    {
                        "id": str(e["id"]),
                        "name": e["name"],
                        "entity_type": e["entity_type"],
                        "depth": e["depth"],
                        "relevance_score": e["relevance_score"],
                    }
                    for e in entities
                ],
            }
            content = json.dumps(export_data, indent=2)

        elif format == "csv":
            lines = ["id,name,entity_type,depth,relevance_score"]
            for e in entities:
                name = e["name"].replace('"', '""')
                lines.append(f'"{e["id"]}","{name}","{e["entity_type"]}",{e["depth"]},{e["relevance_score"]}')
            content = "\n".join(lines)

        elif format == "graphml":
            # Basic GraphML export
            lines = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<graphml xmlns="http://graphml.graphdrawing.org/xmlns">',
                '  <graph id="G" edgedefault="directed">',
            ]
            for e in entities:
                lines.append(f'    <node id="{e["id"]}">')
                lines.append(f'      <data key="name">{e["name"]}</data>')
                lines.append(f'      <data key="type">{e["entity_type"]}</data>')
                lines.append(f'    </node>')
            lines.append('  </graph>')
            lines.append('</graphml>')
            content = "\n".join(lines)

        if output:
            with open(output, "w", encoding="utf-8") as f:
                f.write(content)
            click.echo(f"Exported to: {output}")
        else:
            click.echo(content)

    run_async(_export())


@cli.command(name="delete")
@click.argument("session_id")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def delete_session(session_id: str, force: bool):
    """Delete a research session and all its data."""
    async def _delete():
        manager = get_session_manager()

        session = await manager.get_session(UUID(session_id))
        if not session:
            click.echo(f"Session not found: {session_id}", err=True)
            return

        if not force:
            click.echo(f"This will delete session '{session.name}' and all associated data.")
            if not click.confirm("Are you sure?"):
                click.echo("Cancelled.")
                return

        deleted = await manager.delete_session(UUID(session_id))
        if deleted:
            click.echo(f"Deleted session: {session_id}")
        else:
            click.echo(f"Failed to delete session: {session_id}", err=True)

    run_async(_delete())


@cli.command(name="leads")
@click.argument("session_id")
@click.option("--status", help="Filter by status (pending, completed, skipped, failed)")
@click.option("--limit", default=20, help="Maximum leads to show")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def show_leads(session_id: str, status: str | None, limit: int, as_json: bool):
    """Show leads in a research session's queue."""
    async def _show():
        queue = get_queue_manager()

        if status == "pending":
            leads = await queue.get_pending_leads(UUID(session_id), limit=limit)
        else:
            # Get all leads (would need a new method for filtered access)
            leads = await queue.get_pending_leads(UUID(session_id), limit=limit)

        if as_json:
            output = [
                {
                    "id": str(lead.id),
                    "lead_type": lead.lead_type.value if hasattr(lead.lead_type, 'value') else lead.lead_type,
                    "target": lead.target_identifier,
                    "identifier_type": lead.target_identifier_type.value if hasattr(lead.target_identifier_type, 'value') else lead.target_identifier_type,
                    "priority": lead.priority,
                    "confidence": lead.confidence,
                    "depth": lead.depth,
                    "status": lead.status.value if hasattr(lead.status, 'value') else lead.status,
                }
                for lead in leads
            ]
            click.echo(json.dumps(output, indent=2))
        else:
            if not leads:
                click.echo("No leads found.")
                return

            click.echo(f"{'Type':<15} {'Target':<35} {'Priority':<10} {'Confidence':<12} {'Depth':<6}")
            click.echo("-" * 78)

            for lead in leads:
                lead_type = lead.lead_type.value if hasattr(lead.lead_type, 'value') else lead.lead_type
                target = lead.target_identifier[:34]
                click.echo(
                    f"{lead_type:<15} {target:<35} {lead.priority:<10} "
                    f"{lead.confidence:<12.2f} {lead.depth:<6}"
                )

    run_async(_show())
