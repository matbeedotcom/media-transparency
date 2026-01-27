"""CLI commands for entity resolution and reconciliation.

Provides command-line interface for managing entity matching
and human-in-the-loop reconciliation.

Usage:
    mitds resolve list [--status STATUS] [--limit N]
    mitds resolve review TASK_ID --action ACTION
    mitds resolve stats
    mitds resolve match --source-id ID --candidates FILE
"""

import asyncio
import sys
from datetime import datetime
from typing import Any
from uuid import UUID

import click


@click.group(name="resolve")
def cli():
    """Entity resolution and reconciliation commands."""
    pass


@cli.command(name="list")
@click.option(
    "--status",
    type=click.Choice(["pending", "in_progress", "all"]),
    default="pending",
    help="Filter by status",
)
@click.option(
    "--priority",
    type=click.Choice(["low", "medium", "high", "critical"]),
    default=None,
    help="Filter by priority",
)
@click.option(
    "--strategy",
    type=click.Choice(["deterministic", "fuzzy", "embedding"]),
    default=None,
    help="Filter by match strategy",
)
@click.option(
    "--limit",
    type=int,
    default=20,
    help="Maximum tasks to show",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed information",
)
def list_tasks(
    status: str,
    priority: str | None,
    strategy: str | None,
    limit: int,
    verbose: bool,
):
    """List reconciliation tasks.

    Shows tasks in the reconciliation queue that need human review.

    Examples:

        # List pending tasks
        mitds resolve list

        # List high priority tasks
        mitds resolve list --priority high

        # List tasks using fuzzy matching
        mitds resolve list --strategy fuzzy --verbose
    """
    from ..resolution.reconcile import (
        ReconciliationQueue,
        ReconciliationPriority,
    )
    from ..resolution.matcher import MatchStrategy

    async def _list():
        queue = ReconciliationQueue()

        priority_filter = None
        if priority:
            priority_filter = ReconciliationPriority(priority)

        strategy_filter = None
        if strategy:
            strategy_filter = MatchStrategy(strategy)

        tasks = await queue.get_pending_tasks(
            limit=limit,
            priority=priority_filter,
            strategy=strategy_filter,
        )

        return tasks

    try:
        tasks = asyncio.run(_list())

        if not tasks:
            click.echo("No tasks found matching the criteria.")
            return

        click.echo(f"\nReconciliation Tasks ({len(tasks)} found)")
        click.echo("=" * 70)

        for task in tasks:
            priority_colors = {
                "critical": "red",
                "high": "yellow",
                "medium": "white",
                "low": "green",
            }

            click.echo(f"\nTask: {task.id}")
            click.echo(f"  Status: ", nl=False)
            click.secho(task.status.value, fg="blue")
            click.echo(f"  Priority: ", nl=False)
            click.secho(
                task.priority.value,
                fg=priority_colors.get(task.priority.value, "white"),
            )
            click.echo(f"  Confidence: {task.match_confidence:.2f}")
            click.echo(f"  Strategy: {task.match_strategy.value}")
            click.echo(f"  Source: {task.source_entity_name} ({task.source_entity_type})")
            click.echo(f"  Candidate: {task.candidate_entity_name} ({task.candidate_entity_type})")

            if verbose and task.match_details:
                click.echo("  Match Details:")
                for key, value in task.match_details.items():
                    click.echo(f"    {key}: {value}")

            if task.assigned_to:
                click.echo(f"  Assigned to: {task.assigned_to}")

        click.echo("\n" + "=" * 70)
        click.echo(f"Use 'mitds resolve review <task_id> --action <action>' to resolve")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="review")
@click.argument("task_id")
@click.option(
    "--action",
    type=click.Choice(["same", "different", "merge-left", "merge-right", "skip"]),
    required=True,
    help="Resolution action",
)
@click.option(
    "--reviewer",
    type=str,
    default="cli-user",
    help="Reviewer username",
)
@click.option(
    "--notes",
    type=str,
    default=None,
    help="Review notes",
)
def review_task(
    task_id: str,
    action: str,
    reviewer: str,
    notes: str | None,
):
    """Review and resolve a reconciliation task.

    Actions:
        same: Confirm entities are the same (creates link)
        different: Confirm entities are different (prevents future matching)
        merge-left: Merge candidate into source entity
        merge-right: Merge source into candidate entity
        skip: Skip for now (task remains pending)

    Examples:

        # Confirm entities are the same
        mitds resolve review abc123 --action same --notes "Verified via website"

        # Mark as different
        mitds resolve review abc123 --action different

        # Merge entities
        mitds resolve review abc123 --action merge-left --reviewer analyst1
    """
    from ..resolution.reconcile import ReconciliationQueue

    # Map action names
    action_map = {
        "same": "same_entity",
        "different": "different",
        "merge-left": "merge_left",
        "merge-right": "merge_right",
        "skip": "skip",
    }

    resolution = action_map[action]

    async def _review():
        queue = ReconciliationQueue()

        task = await queue.get_task(UUID(task_id))
        if not task:
            return None

        resolved = await queue.resolve_task(
            task_id=UUID(task_id),
            resolution=resolution,
            reviewer=reviewer,
            notes=notes,
        )

        return resolved

    try:
        resolved = asyncio.run(_review())

        if not resolved:
            click.echo(f"Task not found: {task_id}", err=True)
            sys.exit(1)

        click.echo(f"\nTask resolved successfully!")
        click.echo(f"  Task ID: {resolved.id}")
        click.echo(f"  Resolution: {resolution}")
        click.echo(f"  Reviewer: {reviewer}")
        click.echo(f"  Status: ", nl=False)
        click.secho(resolved.status.value, fg="green")

    except ValueError as e:
        click.echo(f"Invalid task ID: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="stats")
def queue_stats():
    """Show reconciliation queue statistics.

    Displays counts of tasks by status, priority, and matching strategy.

    Example:

        mitds resolve stats
    """
    from ..resolution.reconcile import ReconciliationQueue

    async def _stats():
        queue = ReconciliationQueue()
        return await queue.get_stats()

    try:
        stats = asyncio.run(_stats())

        click.echo("\nReconciliation Queue Statistics")
        click.echo("=" * 50)

        click.echo("\nBy Status:")
        click.echo(f"  Pending: {stats.total_pending}")
        click.echo(f"  In Progress: {stats.total_in_progress}")
        click.echo(f"  Completed: {stats.total_completed}")
        click.echo(f"    - Approved: {stats.total_approved}")
        click.echo(f"    - Rejected: {stats.total_rejected}")
        click.echo(f"    - Merged: {stats.total_merged}")

        if stats.by_priority:
            click.echo("\nPending by Priority:")
            for priority, count in sorted(stats.by_priority.items()):
                click.echo(f"  {priority}: {count}")

        if stats.by_strategy:
            click.echo("\nPending by Strategy:")
            for strategy, count in sorted(stats.by_strategy.items()):
                click.echo(f"  {strategy}: {count}")

        if stats.avg_confidence > 0:
            click.echo(f"\nAverage Confidence (pending): {stats.avg_confidence:.2f}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="run")
@click.option(
    "--entity-type",
    type=click.Choice(["Organization", "Person", "Outlet", "all"]),
    default="all",
    help="Entity type to resolve",
)
@click.option(
    "--auto-merge-threshold",
    type=float,
    default=0.95,
    help="Confidence threshold for automatic merging (default: 0.95)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Find candidates without merging",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed output",
)
def run_resolution(
    entity_type: str,
    auto_merge_threshold: float,
    dry_run: bool,
    verbose: bool,
):
    """Run entity resolution across all entities.

    Finds potential duplicates using deterministic and fuzzy matching,
    then either auto-merges high-confidence matches or queues them
    for human review.

    Examples:

        # Dry run to see candidates
        mitds resolve run --dry-run --verbose

        # Run resolution for organizations only
        mitds resolve run --entity-type Organization

        # Lower auto-merge threshold
        mitds resolve run --auto-merge-threshold 0.90

        # Full run with verbose output
        mitds resolve run --verbose
    """
    from ..resolution.resolver import EntityResolver
    from ..resolution.reconcile import ReconciliationQueue

    entity_types = (
        ["Organization", "Person", "Outlet"]
        if entity_type == "all"
        else [entity_type]
    )

    async def _run():
        resolver = EntityResolver(
            auto_merge_threshold=auto_merge_threshold,
        )

        total_candidates = 0
        total_auto_merged = 0
        total_queued = 0

        for etype in entity_types:
            click.echo(f"\nResolving {etype} entities...")

            duplicates = await resolver.find_duplicates(etype)

            if not duplicates:
                click.echo(f"  No duplicates found for {etype}")
                continue

            click.echo(f"  Found {len(duplicates)} potential duplicates")
            total_candidates += len(duplicates)

            for dup in duplicates:
                if verbose:
                    click.echo(
                        f"    {dup.source_id} <-> {dup.target_id} "
                        f"(confidence: {dup.confidence:.2f}, "
                        f"strategy: {dup.strategy.value if dup.strategy else 'unknown'})"
                    )

                if dry_run:
                    continue

                if dup.confidence >= auto_merge_threshold:
                    # Auto-merge
                    merged = await resolver.merge_entities(
                        dup.source_id,
                        dup.target_id,
                        user_id="system:auto-resolve",
                    )
                    if merged:
                        total_auto_merged += 1
                        if verbose:
                            click.echo(
                                f"      -> Auto-merged (confidence: {dup.confidence:.2f})"
                            )
                else:
                    # Queue for review
                    queue = ReconciliationQueue()
                    await queue.create_task(
                        source_entity_id=dup.source_id,
                        candidate_entity_id=dup.target_id,
                        match_confidence=dup.confidence,
                        match_strategy=dup.strategy,
                        match_details=dup.match_details,
                    )
                    total_queued += 1
                    if verbose:
                        click.echo(
                            f"      -> Queued for review (confidence: {dup.confidence:.2f})"
                        )

        return total_candidates, total_auto_merged, total_queued

    try:
        candidates, merged, queued = asyncio.run(_run())

        click.echo("\n" + "=" * 50)
        click.echo("Resolution Summary")
        click.echo("=" * 50)
        click.echo(f"  Candidates found: {candidates}")
        if not dry_run:
            click.echo(f"  Auto-merged: {merged}")
            click.echo(f"  Queued for review: {queued}")
        else:
            click.echo("  (dry run - no changes made)")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command(name="match")
@click.option(
    "--source-id",
    type=str,
    required=True,
    help="Source entity ID to find matches for",
)
@click.option(
    "--search-query",
    type=str,
    default=None,
    help="Search query to find candidate entities",
)
@click.option(
    "--threshold",
    type=float,
    default=0.7,
    help="Minimum match confidence threshold",
)
@click.option(
    "--use-embedding/--no-embedding",
    default=False,
    help="Use embedding-based matching",
)
@click.option(
    "--queue-low-confidence/--no-queue",
    default=True,
    help="Queue low-confidence matches for review",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed match information",
)
def find_matches(
    source_id: str,
    search_query: str | None,
    threshold: float,
    use_embedding: bool,
    queue_low_confidence: bool,
    verbose: bool,
):
    """Find matches for an entity.

    Searches for potential matches using multiple matching strategies
    and optionally queues low-confidence matches for review.

    Examples:

        # Find matches for an entity
        mitds resolve match --source-id abc123 --search-query "Acme"

        # Use embedding matching
        mitds resolve match --source-id abc123 --search-query "Acme" --use-embedding

        # Higher threshold
        mitds resolve match --source-id abc123 --search-query "Acme" --threshold 0.85
    """
    from ..resolution.matcher import (
        HybridMatcher,
        MatchCandidate,
    )
    from ..resolution.reconcile import queue_low_confidence_matches
    from ..db import get_neo4j_session

    async def _find_matches():
        # Get source entity
        async with get_neo4j_session() as session:
            query = """
            MATCH (e {id: $entity_id})
            RETURN e
            """
            result = await session.run(query, entity_id=source_id)
            record = await result.single()

            if not record:
                return None, None

            source_data = dict(record["e"])
            source = MatchCandidate(
                entity_id=UUID(source_data["id"]),
                entity_type=source_data.get("entity_type", "UNKNOWN"),
                name=source_data.get("name", "Unknown"),
                identifiers={
                    k: v for k, v in source_data.items()
                    if k in ["ein", "bn", "opencorp_id"] and v
                },
                attributes=source_data,
            )

            # Get candidates
            candidates = []
            if search_query:
                search_cypher = """
                MATCH (e)
                WHERE (e:Organization OR e:Person OR e:Outlet)
                AND e.id <> $source_id
                AND toLower(e.name) CONTAINS toLower($query)
                RETURN e
                LIMIT 100
                """
                cand_result = await session.run(
                    search_cypher,
                    source_id=source_id,
                    query=search_query,
                )
                cand_records = await cand_result.data()

                for rec in cand_records:
                    cand_data = dict(rec["e"])
                    candidates.append(MatchCandidate(
                        entity_id=UUID(cand_data["id"]),
                        entity_type=cand_data.get("entity_type", "UNKNOWN"),
                        name=cand_data.get("name", "Unknown"),
                        identifiers={
                            k: v for k, v in cand_data.items()
                            if k in ["ein", "bn", "opencorp_id"] and v
                        },
                        attributes=cand_data,
                    ))

        if not candidates:
            return source, []

        # Run matching
        matcher = HybridMatcher(use_embedding=use_embedding)
        matches = matcher.find_matches(source, candidates, threshold=threshold)

        return source, matches

    async def _queue_matches(matches):
        if queue_low_confidence and matches:
            tasks = await queue_low_confidence_matches(matches, confidence_threshold=0.9)
            return tasks
        return []

    try:
        source, matches = asyncio.run(_find_matches())

        if source is None:
            click.echo(f"Source entity not found: {source_id}", err=True)
            sys.exit(1)

        click.echo(f"\nSource Entity: {source.name}")
        click.echo(f"  Type: {source.entity_type}")
        click.echo(f"  ID: {source.entity_id}")

        if not matches:
            click.echo("\nNo matches found above threshold.")
            return

        click.echo(f"\nMatches Found: {len(matches)}")
        click.echo("=" * 60)

        for i, match in enumerate(matches, 1):
            confidence_color = "green" if match.confidence >= 0.9 else (
                "yellow" if match.confidence >= 0.7 else "red"
            )

            click.echo(f"\n{i}. {match.target.name}")
            click.echo(f"   Type: {match.target.entity_type}")
            click.echo(f"   ID: {match.target.entity_id}")
            click.echo(f"   Strategy: {match.strategy.value}")
            click.echo("   Confidence: ", nl=False)
            click.secho(f"{match.confidence:.2f}", fg=confidence_color)

            if verbose and match.match_details:
                click.echo("   Details:")
                for key, value in match.match_details.items():
                    click.echo(f"     {key}: {value}")

        # Queue low-confidence matches
        if queue_low_confidence:
            low_conf = [m for m in matches if m.confidence < 0.9]
            if low_conf:
                tasks = asyncio.run(_queue_matches(low_conf))
                if tasks:
                    click.echo(f"\n{len(tasks)} low-confidence matches queued for review.")

    except ValueError as e:
        click.echo(f"Invalid entity ID: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


# Main CLI entry point
@click.group()
def main():
    """MITDS Entity Resolution CLI."""
    pass


main.add_command(cli)


if __name__ == "__main__":
    main()
