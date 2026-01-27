"""Detection CLI commands for MITDS.

Provides command-line access to all detection engines:
- Temporal coordination analysis
- Funding cluster detection
- Infrastructure sharing detection
- Composite scoring
"""

import asyncio
import json
import sys

import click

from ..logging import get_context_logger

logger = get_context_logger(__name__)


def _run_async(coro):
    """Run an async coroutine from sync Click context."""
    return asyncio.get_event_loop().run_until_complete(coro)


@click.group("detect")
def cli():
    """Run detection analyses from the command line."""
    pass


@cli.command("temporal")
@click.option(
    "--entity-ids",
    required=True,
    help="Comma-separated entity UUIDs to analyze",
)
@click.option(
    "--start-date",
    required=True,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    required=True,
    help="End date (YYYY-MM-DD)",
)
@click.option(
    "--event-types",
    default=None,
    help="Comma-separated event types to include",
)
@click.option(
    "--no-hard-negatives",
    is_flag=True,
    default=False,
    help="Disable hard negative filtering",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output results as JSON",
)
def temporal(
    entity_ids: str,
    start_date: str,
    end_date: str,
    event_types: str | None,
    no_hard_negatives: bool,
    output_json: bool,
):
    """Analyze temporal coordination between entities.

    Detects publication bursts, lead-lag relationships, and
    synchronized timing distributions.

    Examples:
        mitds detect temporal --entity-ids id1,id2 --start-date 2024-01-01 --end-date 2025-01-01
        mitds detect temporal --entity-ids id1,id2 --start-date 2024-01-01 --end-date 2025-01-01 --json
    """
    from datetime import datetime
    from uuid import UUID

    from ..api.detection import _fetch_timing_events
    from ..detection.temporal import TemporalCoordinationDetector

    eids = [e.strip() for e in entity_ids.split(",") if e.strip()]
    if len(eids) < 2:
        click.echo("Error: At least 2 entity IDs required", err=True)
        sys.exit(1)

    try:
        uuid_eids = [UUID(e) for e in eids]
    except ValueError as e:
        click.echo(f"Error: Invalid UUID: {e}", err=True)
        sys.exit(1)

    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
    except ValueError as e:
        click.echo(f"Error: Invalid date format: {e}", err=True)
        sys.exit(1)

    et_list = [t.strip() for t in event_types.split(",")] if event_types else None

    async def run():
        events = await _fetch_timing_events(
            entity_ids=uuid_eids,
            start_date=start_dt,
            end_date=end_dt,
            event_types=et_list,
        )

        click.echo(f"Fetched {len(events)} events for {len(eids)} entities")

        detector = TemporalCoordinationDetector()
        result = await detector.detect_coordination(
            events=events,
            entity_ids=eids,
            exclude_hard_negatives=not no_hard_negatives,
        )
        return result

    try:
        result = _run_async(run())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if output_json:
        click.echo(result.model_dump_json(indent=2))
    else:
        click.echo(f"\nTemporal Coordination Analysis")
        click.echo(f"{'=' * 40}")
        click.echo(f"Entities:            {result.entity_count}")
        click.echo(f"Events:              {result.event_count}")
        click.echo(f"Coordination Score:  {result.coordination_score:.2%}")
        click.echo(f"Confidence:          {result.confidence:.2%}")
        click.echo(f"Coordinated:         {'YES' if result.is_coordinated else 'No'}")
        click.echo(f"\nExplanation: {result.explanation}")

        if result.bursts:
            click.echo(f"\nBursts Detected: {len(result.bursts)}")
            for b in result.bursts:
                click.echo(f"  Entity {b.entity_id}: {b.burst_count} bursts, {b.total_events} events")

        if result.lead_lag_pairs:
            click.echo(f"\nLead-Lag Pairs: {len(result.lead_lag_pairs)}")
            for p in result.lead_lag_pairs:
                sig = "***" if p.is_significant else ""
                click.echo(f"  {p.leader_entity_id} -> {p.follower_entity_id}: {p.lag_minutes}min (r={p.correlation:.3f}) {sig}")

        if result.synchronized_groups:
            click.echo(f"\nSynchronized Groups: {len(result.synchronized_groups)}")
            for g in result.synchronized_groups:
                click.echo(f"  {', '.join(g.entity_ids)}: sync={g.sync_score:.2%}, confidence={g.confidence:.2%}")


@cli.command("funding")
@click.option(
    "--entity-type",
    default=None,
    type=click.Choice(["Organization", "Outlet", "Person"], case_sensitive=False),
    help="Filter by entity type",
)
@click.option(
    "--fiscal-year",
    default=None,
    type=int,
    help="Filter by fiscal year",
)
@click.option(
    "--min-shared",
    default=2,
    type=int,
    help="Minimum shared funders (default: 2)",
)
@click.option(
    "--limit",
    default=50,
    type=int,
    help="Maximum clusters to return",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output results as JSON",
)
def funding(
    entity_type: str | None,
    fiscal_year: int | None,
    min_shared: int,
    limit: int,
    output_json: bool,
):
    """Detect funding clusters among entities.

    Identifies groups sharing common funders.

    Examples:
        mitds detect funding --entity-type Organization --fiscal-year 2023
        mitds detect funding --min-shared 3 --json
    """
    from ..detection.funding import FundingClusterDetector

    async def run():
        detector = FundingClusterDetector(min_shared_funders=min_shared)
        return await detector.detect_clusters(
            entity_type=entity_type,
            fiscal_year=fiscal_year,
            limit=limit,
        )

    try:
        results = _run_async(run())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if output_json:
        output = [r.model_dump() for r in results]
        click.echo(json.dumps(output, indent=2, default=str))
    else:
        click.echo(f"\nFunding Cluster Detection")
        click.echo(f"{'=' * 40}")
        click.echo(f"Clusters Found: {len(results)}")

        if not results:
            click.echo("No funding clusters found.")
            return

        for i, cluster in enumerate(results, 1):
            click.echo(f"\n--- Cluster {i} (score: {cluster.score:.2%}) ---")
            funder_name = cluster.shared_funder.name if hasattr(cluster.shared_funder, "name") else str(cluster.shared_funder)
            click.echo(f"  Shared Funder: {funder_name}")
            click.echo(f"  Members: {len(cluster.members)}")
            for m in cluster.members:
                name = m.name if hasattr(m, "name") else str(m)
                click.echo(f"    - {name}")
            click.echo(f"  Total Funding: ${cluster.total_funding:,.2f}")
            if cluster.evidence_summary:
                click.echo(f"  Evidence: {cluster.evidence_summary}")


@cli.command("infrastructure")
@click.option(
    "--domains",
    required=True,
    help="Comma-separated domain names to scan",
)
@click.option(
    "--min-score",
    default=1.0,
    type=float,
    help="Minimum match score (default: 1.0)",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output results as JSON",
)
def infrastructure(
    domains: str,
    min_score: float,
    output_json: bool,
):
    """Detect shared infrastructure between domains.

    Scans DNS, WHOIS, hosting, analytics, and SSL to find shared infrastructure.

    Examples:
        mitds detect infrastructure --domains example1.com,example2.com
        mitds detect infrastructure --domains example1.com,example2.com --min-score 2.0 --json
    """
    from ..detection.infra import InfrastructureDetector

    domain_list = [d.strip() for d in domains.split(",") if d.strip()]
    if len(domain_list) < 2:
        click.echo("Error: At least 2 domains required", err=True)
        sys.exit(1)

    async def run():
        detector = InfrastructureDetector()
        try:
            matches = await detector.find_shared_infrastructure(
                domains=domain_list,
                min_score=min_score,
            )
            return matches
        finally:
            await detector.close()

    try:
        matches = _run_async(run())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if output_json:
        output = []
        for m in matches:
            output.append({
                "domain_a": m.domain_a,
                "domain_b": m.domain_b,
                "signals": [
                    {"signal_type": s.signal_type.value, "value": s.value, "weight": s.weight, "description": s.description}
                    for s in m.signals
                ],
                "total_score": m.total_score,
                "confidence": m.confidence,
            })
        click.echo(json.dumps(output, indent=2, default=str))
    else:
        click.echo(f"\nInfrastructure Sharing Detection")
        click.echo(f"{'=' * 40}")
        click.echo(f"Domains Scanned: {len(domain_list)}")
        click.echo(f"Matches Found:   {len(matches)}")

        if not matches:
            click.echo(f"No shared infrastructure found above min_score {min_score}.")
            return

        for m in matches:
            click.echo(f"\n  {m.domain_a} <-> {m.domain_b}")
            click.echo(f"    Score: {m.total_score:.1f}, Confidence: {m.confidence:.2%}")
            for s in m.signals:
                click.echo(f"    [{s.signal_type.value}] {s.description} (weight: {s.weight})")


@cli.command("composite")
@click.option(
    "--entity-ids",
    required=True,
    help="Comma-separated entity UUIDs to analyze",
)
@click.option(
    "--no-temporal",
    is_flag=True,
    default=False,
    help="Exclude temporal analysis",
)
@click.option(
    "--no-funding",
    is_flag=True,
    default=False,
    help="Exclude funding analysis",
)
@click.option(
    "--no-infrastructure",
    is_flag=True,
    default=False,
    help="Exclude infrastructure analysis",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output results as JSON",
)
def composite(
    entity_ids: str,
    no_temporal: bool,
    no_funding: bool,
    no_infrastructure: bool,
    output_json: bool,
):
    """Calculate composite coordination score.

    Combines temporal, funding, and infrastructure signals with
    correlation-aware weighting. Requires 2+ signals from 2+ categories
    to flag.

    Examples:
        mitds detect composite --entity-ids id1,id2
        mitds detect composite --entity-ids id1,id2 --no-infrastructure --json
    """
    from datetime import datetime
    from uuid import UUID

    from ..detection.composite import (
        CompositeScoreCalculator,
        DetectedSignal,
        SignalType,
    )
    from ..detection.temporal import TemporalCoordinationDetector
    from ..detection.funding import FundingClusterDetector
    from ..api.detection import _fetch_timing_events

    eids = [e.strip() for e in entity_ids.split(",") if e.strip()]
    if len(eids) < 2:
        click.echo("Error: At least 2 entity IDs required", err=True)
        sys.exit(1)

    try:
        uuid_eids = [UUID(e) for e in eids]
    except ValueError as e:
        click.echo(f"Error: Invalid UUID: {e}", err=True)
        sys.exit(1)

    async def run():
        signals: list[DetectedSignal] = []
        signal_scores: dict[str, float] = {}
        errors: list[str] = []

        # Temporal
        if not no_temporal:
            try:
                end_date = datetime.utcnow()
                start_date = end_date.replace(year=end_date.year - 1)
                events = await _fetch_timing_events(
                    entity_ids=uuid_eids,
                    start_date=start_date,
                    end_date=end_date,
                )
                if events:
                    detector = TemporalCoordinationDetector()
                    result = await detector.detect_coordination(events=events, entity_ids=eids)
                    signal_scores["temporal"] = result.coordination_score
                    if result.coordination_score > 0:
                        signals.append(DetectedSignal(
                            signal_type=SignalType.TEMPORAL_COORDINATION,
                            strength=result.coordination_score,
                            confidence=result.confidence,
                            entity_ids=uuid_eids,
                        ))
                else:
                    signal_scores["temporal"] = 0.0
            except Exception as e:
                signal_scores["temporal"] = 0.0
                errors.append(f"Temporal: {e}")

        # Funding
        if not no_funding:
            try:
                detector = FundingClusterDetector(min_shared_funders=1)
                shared_funders = await detector.find_shared_funders(
                    entity_ids=uuid_eids, min_recipients=2,
                )
                if shared_funders:
                    max_conc = max(sf.funding_concentration for sf in shared_funders)
                    strength = min(1.0, len(shared_funders) * 0.2 + max_conc * 0.5)
                    signal_scores["funding"] = strength
                    signals.append(DetectedSignal(
                        signal_type=SignalType.SHARED_FUNDER,
                        strength=strength,
                        confidence=0.9,
                        entity_ids=uuid_eids,
                    ))
                else:
                    signal_scores["funding"] = 0.0
            except Exception as e:
                signal_scores["funding"] = 0.0
                errors.append(f"Funding: {e}")

        # Infrastructure
        if not no_infrastructure:
            try:
                from ..detection.infra import InfrastructureDetector
                from ..db import get_neo4j_session

                domains: list[str] = []
                async with get_neo4j_session() as neo4j:
                    for eid in uuid_eids:
                        result = await neo4j.run(
                            "MATCH (n {id: $eid}) WHERE n:Outlet OR n:Organization RETURN n.domain AS domain, n.domains AS domains",
                            eid=str(eid),
                        )
                        record = await result.single()
                        if record:
                            if record["domain"]:
                                domains.append(record["domain"])
                            elif record["domains"]:
                                domains.extend(record["domains"])

                if len(domains) >= 2:
                    infra_det = InfrastructureDetector()
                    try:
                        matches = await infra_det.find_shared_infrastructure(domains=domains, min_score=1.0)
                        if matches:
                            top = matches[0].total_score
                            strength = min(1.0, top / 10.0)
                            signal_scores["infrastructure"] = strength
                            signals.append(DetectedSignal(
                                signal_type=SignalType.INFRASTRUCTURE_SHARING,
                                strength=strength,
                                confidence=matches[0].confidence,
                                entity_ids=uuid_eids,
                            ))
                        else:
                            signal_scores["infrastructure"] = 0.0
                    finally:
                        await infra_det.close()
                else:
                    signal_scores["infrastructure"] = 0.0
            except Exception as e:
                signal_scores["infrastructure"] = 0.0
                errors.append(f"Infrastructure: {e}")

        calculator = CompositeScoreCalculator()
        composite_result = calculator.calculate(signals)
        return composite_result, signal_scores, errors

    try:
        composite_result, signal_scores, errors = _run_async(run())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if output_json:
        output = composite_result.to_dict()
        output["signal_scores"] = signal_scores
        if errors:
            output["errors"] = errors
        click.echo(json.dumps(output, indent=2, default=str))
    else:
        click.echo(f"\nComposite Coordination Score")
        click.echo(f"{'=' * 40}")
        click.echo(f"Entities:        {len(eids)}")
        click.echo(f"Raw Score:       {composite_result.raw_score:.2%}")
        click.echo(f"Adjusted Score:  {composite_result.adjusted_score:.2%}")
        click.echo(f"Flagged:         {'YES' if composite_result.is_flagged else 'No'}")
        if composite_result.flag_reason:
            click.echo(f"Flag Reason:     {composite_result.flag_reason}")
        click.echo(f"Confidence:      [{composite_result.confidence_band.lower_bound:.2f} - {composite_result.confidence_band.upper_bound:.2f}]")
        click.echo(f"Validation:      {'PASS' if composite_result.validation_passed else 'FAIL'}")
        for msg in composite_result.validation_messages:
            click.echo(f"  - {msg}")

        click.echo(f"\nSignal Breakdown:")
        for k, v in signal_scores.items():
            click.echo(f"  {k}: {v:.2%}")

        click.echo(f"\nCategory Breakdown:")
        for k, v in composite_result.category_breakdown.items():
            click.echo(f"  {k}: {v:.2%}")

        if errors:
            click.echo(f"\nErrors:")
            for err in errors:
                click.echo(f"  - {err}")
