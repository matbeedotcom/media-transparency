"""Report export templates.

Provides JSON and Markdown export formats for case reports.
"""

import json
from datetime import datetime
from typing import Any

from ..models import CaseReport


def export_json(report: CaseReport, pretty: bool = True) -> str:
    """Export a report as JSON.

    Args:
        report: The case report to export
        pretty: Whether to pretty-print the JSON

    Returns:
        JSON string
    """
    data = report.model_dump(mode="json")

    if pretty:
        return json.dumps(data, indent=2, default=str)
    return json.dumps(data, default=str)


def export_markdown(report: CaseReport) -> str:
    """Export a report as Markdown.

    Args:
        report: The case report to export

    Returns:
        Markdown string
    """
    lines: list[str] = []

    # Title
    lines.append(f"# Case Report")
    lines.append("")
    lines.append(f"**Generated**: {report.generated_at.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"**Version**: {report.report_version}")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Entry Point**: {report.summary.entry_point}")
    lines.append(f"- **Entities Discovered**: {report.summary.entity_count}")
    lines.append(f"- **Relationships Found**: {report.summary.relationship_count}")
    lines.append(f"- **Cross-Border Connections**: {report.summary.cross_border_count}")
    lines.append(f"- **Processing Time**: {report.summary.processing_time_seconds:.1f}s")
    if report.summary.has_unresolved_matches:
        lines.append("- **⚠️ Has unresolved entity matches requiring review**")
    lines.append("")

    # Cross-border flags (important, show first)
    if report.cross_border_flags:
        lines.append("## ⚠️ Cross-Border Connections")
        lines.append("")
        lines.append("| US Entity | CA Entity | Relationship | Amount |")
        lines.append("|-----------|-----------|--------------|--------|")
        for flag in report.cross_border_flags:
            amount = f"${flag.amount:,.0f}" if flag.amount else "N/A"
            lines.append(
                f"| {flag.us_entity_name} | {flag.ca_entity_name} | "
                f"{flag.relationship_type} | {amount} |"
            )
        lines.append("")

    # Top entities
    if report.top_entities:
        lines.append("## Top Entities")
        lines.append("")
        lines.append("| # | Name | Type | Jurisdiction | Relevance |")
        lines.append("|---|------|------|--------------|-----------|")
        for i, entity in enumerate(report.top_entities, 1):
            jurisdiction = entity.jurisdiction or "Unknown"
            lines.append(
                f"| {i} | {entity.name} | {entity.entity_type} | "
                f"{jurisdiction} | {entity.relevance_score:.2f} |"
            )
        lines.append("")

    # Top relationships
    if report.top_relationships:
        lines.append("## Key Relationships")
        lines.append("")
        lines.append("| Source | Target | Type | Amount | Significance |")
        lines.append("|--------|--------|------|--------|--------------|")
        for rel in report.top_relationships:
            amount = f"${rel.amount:,.0f}" if rel.amount else "N/A"
            lines.append(
                f"| {rel.source_name} | {rel.target_name} | "
                f"{rel.relationship_type} | {amount} | {rel.significance_score:.2f} |"
            )
        lines.append("")

    # Unknowns
    if report.unknowns:
        lines.append("## Unknowns")
        lines.append("")
        lines.append("The following entities or relationships could not be fully traced:")
        lines.append("")
        for unknown in report.unknowns:
            lines.append(f"- **{unknown.entity_name}**: {unknown.reason}")
            if unknown.attempted_sources:
                lines.append(f"  - Attempted sources: {', '.join(unknown.attempted_sources)}")
        lines.append("")

    # Evidence index
    if report.evidence_index:
        lines.append("## Evidence Sources")
        lines.append("")
        lines.append("| # | Type | Source URL | Retrieved |")
        lines.append("|---|------|------------|-----------|")
        for i, citation in enumerate(report.evidence_index, 1):
            url = citation.source_url or "N/A"
            if len(url) > 50:
                url = url[:47] + "..."
            retrieved = citation.retrieved_at.strftime('%Y-%m-%d %H:%M')
            lines.append(f"| {i} | {citation.source_type} | {url} | {retrieved} |")
        lines.append("")

    # Footer
    lines.append("---")
    lines.append("")
    lines.append(f"*Report ID: {report.id}*")
    lines.append(f"*Case ID: {report.case_id}*")

    return "\n".join(lines)


def export_report(report: CaseReport, format: str = "json") -> str:
    """Export a report in the specified format.

    Args:
        report: The case report to export
        format: Export format ('json' or 'markdown')

    Returns:
        Formatted string

    Raises:
        ValueError: If format is not supported
    """
    if format == "json":
        return export_json(report)
    elif format in ("markdown", "md"):
        return export_markdown(report)
    else:
        raise ValueError(f"Unsupported export format: {format}")
