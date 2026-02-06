"""Report API endpoints for MITDS."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from . import NotFoundError
from .auth import CurrentUser, OptionalUser
from ..cases.reports.funding_chain import FundingChainReportGenerator
from ..reporting.templates import (
    REPORT_TEMPLATES,
    ReportStatus,
    ReportType,
    StructuralRiskReport,
    TimelineNarrativeReport,
    TopologySummaryReport,
    create_report,
)

router = APIRouter(prefix="/reports")

# In-memory storage for demo purposes
# In production, use Redis or database
_report_store: dict[str, dict[str, Any]] = {}


# =========================
# Request/Response Models
# =========================


class ReportRequest(BaseModel):
    """Request for report generation."""

    report_type: str = Field(..., description="Type of report to generate")
    entity_ids: list[UUID] = Field(..., min_length=1, description="Entity IDs to analyze")
    title: str | None = Field(None, description="Custom report title")
    date_range_start: datetime | None = Field(None, description="Start of analysis period")
    date_range_end: datetime | None = Field(None, description="End of analysis period")
    options: dict[str, Any] | None = Field(None, description="Additional options")


class ReportResponse(BaseModel):
    """Response for report generation request."""

    report_id: UUID
    status: str
    status_url: str
    estimated_completion: str | None = None


class TemplateResponse(BaseModel):
    """Report template information."""

    id: str
    name: str
    description: str
    sections: list[str]
    required_data: list[str]
    output_formats: list[str]


# =========================
# List Templates (T117)
# =========================


@router.get("/templates")
async def list_report_templates(
    user: OptionalUser = None,
) -> list[TemplateResponse]:
    """List available report templates.

    Returns information about each report type including
    required data and available output formats.
    """
    templates = []

    for report_type, info in REPORT_TEMPLATES.items():
        templates.append(TemplateResponse(
            id=report_type.value,
            name=info["name"],
            description=info["description"],
            sections=info["sections"],
            required_data=info["required_data"],
            output_formats=info["output_formats"],
        ))

    return templates


@router.get("/templates/{template_id}")
async def get_template_details(
    template_id: str,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get detailed information about a specific template."""
    try:
        report_type = ReportType(template_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Template not found: {template_id}")

    info = REPORT_TEMPLATES.get(report_type)
    if not info:
        raise HTTPException(status_code=404, detail=f"Template not found: {template_id}")

    return {
        "id": report_type.value,
        **info,
        "example_request": {
            "report_type": report_type.value,
            "entity_ids": ["<uuid>", "<uuid>"],
            "title": f"Example {info['name']}",
            "date_range_start": "2020-01-01T00:00:00Z",
            "date_range_end": "2024-12-31T23:59:59Z",
        },
    }


# =========================
# Generate Report (T118)
# =========================


async def _generate_report_async(
    report_id: str,
    report_type: ReportType,
    entity_ids: list[UUID],
    title: str | None,
    date_range_start: datetime | None,
    date_range_end: datetime | None,
) -> None:
    """Background task to generate a report."""
    try:
        # Update status to generating
        _report_store[report_id]["status"] = ReportStatus.GENERATING.value

        # Create the appropriate report
        report = create_report(
            report_type=report_type,
            entity_ids=entity_ids,
            title=title,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )

        # For demo, add some sample findings based on report type
        if isinstance(report, StructuralRiskReport):
            report.entities_analyzed = len(entity_ids)
            report.finalize()

        # Store the completed report
        _report_store[report_id]["status"] = ReportStatus.COMPLETED.value
        _report_store[report_id]["completed_at"] = datetime.utcnow().isoformat()
        _report_store[report_id]["report_data"] = report.to_dict()

    except Exception as e:
        _report_store[report_id]["status"] = ReportStatus.FAILED.value
        _report_store[report_id]["error"] = str(e)


@router.post("")
async def generate_report(
    request: ReportRequest,
    background_tasks: BackgroundTasks,
    user: OptionalUser = None,
) -> ReportResponse:
    """Generate a report asynchronously.

    Starts report generation in the background and returns
    a report ID for tracking progress.

    Args:
        request: Report generation request with type and entities

    Returns:
        Report ID and status URL for tracking
    """
    # Validate report type
    try:
        report_type = ReportType(request.report_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report type: {request.report_type}. "
                   f"Valid types: {[rt.value for rt in ReportType]}"
        )

    # Create report record
    report_id = uuid4()
    _report_store[str(report_id)] = {
        "id": str(report_id),
        "report_type": request.report_type,
        "status": ReportStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
        "entity_ids": [str(eid) for eid in request.entity_ids],
        "title": request.title,
    }

    # Start background generation
    background_tasks.add_task(
        _generate_report_async,
        str(report_id),
        report_type,
        request.entity_ids,
        request.title,
        request.date_range_start,
        request.date_range_end,
    )

    return ReportResponse(
        report_id=report_id,
        status="pending",
        status_url=f"/api/reports/{report_id}",
        estimated_completion="Processing initiated",
    )


# =========================
# Get Report (T119)
# =========================


@router.get("/{report_id}")
async def get_report(
    report_id: UUID,
    format: str = Query("json", pattern="^(json|html|pdf|markdown)$"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get a generated report.

    Args:
        report_id: UUID of the report
        format: Output format (json, html, pdf, markdown)

    Returns:
        Report data in requested format
    """
    report_key = str(report_id)

    if report_key not in _report_store:
        raise NotFoundError("Report", report_id)

    report_record = _report_store[report_key]

    # If still processing, return status
    if report_record["status"] != ReportStatus.COMPLETED.value:
        return {
            "id": report_record["id"],
            "status": report_record["status"],
            "created_at": report_record["created_at"],
            "error": report_record.get("error"),
        }

    # Return completed report
    report_data = report_record.get("report_data", {})

    if format == "json":
        return {
            "id": report_record["id"],
            "status": report_record["status"],
            "created_at": report_record["created_at"],
            "completed_at": report_record.get("completed_at"),
            "format": "json",
            "report": report_data,
        }

    elif format == "markdown":
        # Convert to markdown
        markdown = _convert_to_markdown(report_data)
        return {
            "id": report_record["id"],
            "status": report_record["status"],
            "format": "markdown",
            "content": markdown,
        }

    elif format == "html":
        # Convert to HTML
        html = _convert_to_html(report_data)
        return {
            "id": report_record["id"],
            "status": report_record["status"],
            "format": "html",
            "content": html,
        }

    else:
        # PDF not implemented in demo
        raise HTTPException(
            status_code=501,
            detail="PDF export not implemented"
        )


@router.get("/{report_id}/status")
async def get_report_status(
    report_id: UUID,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get status of a report generation job."""
    report_key = str(report_id)

    if report_key not in _report_store:
        raise NotFoundError("Report", report_id)

    record = _report_store[report_key]

    return {
        "id": record["id"],
        "status": record["status"],
        "created_at": record["created_at"],
        "completed_at": record.get("completed_at"),
        "error": record.get("error"),
    }


# =========================
# Format Conversion Helpers
# =========================


def _convert_to_markdown(report_data: dict[str, Any]) -> str:
    """Convert report data to markdown format."""
    lines = []

    # Title
    metadata = report_data.get("metadata", {})
    lines.append(f"# {metadata.get('title', 'Report')}")
    lines.append("")

    if metadata.get("subtitle"):
        lines.append(f"*{metadata['subtitle']}*")
        lines.append("")

    # Executive summary
    if report_data.get("executive_summary"):
        lines.append("## Executive Summary")
        lines.append("")
        lines.append(report_data["executive_summary"])
        lines.append("")

    # Methodology
    if report_data.get("methodology"):
        lines.append("## Methodology")
        lines.append("")
        lines.append(report_data["methodology"])
        lines.append("")

    # Sections
    for section in report_data.get("sections", []):
        lines.append(f"## {section.get('title', 'Section')}")
        lines.append("")
        lines.append(section.get("content", ""))
        lines.append("")

        # Findings in section
        for finding in section.get("findings", []):
            lines.append(f"### {finding.get('title', 'Finding')}")
            lines.append("")
            lines.append(f"**Severity**: {finding.get('severity', 'unknown')}")
            lines.append(f"**Confidence**: {finding.get('confidence', 0):.0%}")
            lines.append("")
            lines.append(finding.get("description", ""))
            lines.append("")

            if finding.get("why_flagged"):
                lines.append("**Why Flagged**: " + finding["why_flagged"])
                lines.append("")

    # Limitations
    if report_data.get("limitations"):
        lines.append("## Limitations")
        lines.append("")
        for limitation in report_data["limitations"]:
            lines.append(f"- {limitation}")
        lines.append("")

    # Footer
    lines.append("---")
    lines.append(f"*Generated by {metadata.get('generated_by', 'MITDS')} "
                 f"on {metadata.get('generated_at', 'unknown')}*")

    return "\n".join(lines)


def _convert_to_html(report_data: dict[str, Any]) -> str:
    """Convert report data to HTML format."""
    metadata = report_data.get("metadata", {})

    html_parts = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        f"<title>{metadata.get('title', 'Report')}</title>",
        "<style>",
        "body { font-family: system-ui, sans-serif; max-width: 800px; margin: 0 auto; padding: 2rem; }",
        "h1 { color: #1a1a1a; }",
        "h2 { color: #333; border-bottom: 1px solid #ddd; padding-bottom: 0.5rem; }",
        ".finding { background: #f9f9f9; padding: 1rem; margin: 1rem 0; border-radius: 4px; }",
        ".severity-high { border-left: 4px solid #dc2626; }",
        ".severity-medium { border-left: 4px solid #f59e0b; }",
        ".severity-low { border-left: 4px solid #10b981; }",
        ".metadata { color: #666; font-size: 0.875rem; }",
        ".limitation { color: #666; }",
        "</style>",
        "</head>",
        "<body>",
        f"<h1>{metadata.get('title', 'Report')}</h1>",
    ]

    if metadata.get("subtitle"):
        html_parts.append(f"<p class='subtitle'><em>{metadata['subtitle']}</em></p>")

    if report_data.get("executive_summary"):
        html_parts.append("<h2>Executive Summary</h2>")
        html_parts.append(f"<p>{report_data['executive_summary']}</p>")

    for section in report_data.get("sections", []):
        html_parts.append(f"<h2>{section.get('title', 'Section')}</h2>")
        html_parts.append(f"<p>{section.get('content', '')}</p>")

        for finding in section.get("findings", []):
            severity = finding.get("severity", "low")
            html_parts.append(f"<div class='finding severity-{severity}'>")
            html_parts.append(f"<h3>{finding.get('title', 'Finding')}</h3>")
            html_parts.append(f"<p class='metadata'>Severity: {severity} | "
                            f"Confidence: {finding.get('confidence', 0):.0%}</p>")
            html_parts.append(f"<p>{finding.get('description', '')}</p>")
            if finding.get("why_flagged"):
                html_parts.append(f"<p><strong>Why Flagged:</strong> {finding['why_flagged']}</p>")
            html_parts.append("</div>")

    if report_data.get("limitations"):
        html_parts.append("<h2>Limitations</h2>")
        html_parts.append("<ul class='limitation'>")
        for limitation in report_data["limitations"]:
            html_parts.append(f"<li>{limitation}</li>")
        html_parts.append("</ul>")

    html_parts.append(f"<hr><p class='metadata'>Generated by {metadata.get('generated_by', 'MITDS')} "
                     f"on {metadata.get('generated_at', 'unknown')}</p>")
    html_parts.append("</body></html>")

    return "\n".join(html_parts)


# =========================
# Funding Chain Report (007)
# =========================


@router.get("/funding-chain/{case_id}")
async def get_funding_chain_report(
    case_id: UUID,
    format: str = Query("json", pattern="^(json|markdown)$"),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get Funding Chain Report for a case.

    Generates a funding chain report tracing the path from a political
    ad to its ultimate corporate funders. Integrates evidence from
    all available data sources with confidence scores.

    Args:
        case_id: UUID of the case
        format: Output format ("json" or "markdown")

    Returns:
        Funding chain report data
    """
    generator = FundingChainReportGenerator()
    report = await generator.generate(case_id, format=format)
    
    if format == "markdown":
        return {
            "case_id": str(case_id),
            "format": "markdown",
            "content": report,
        }
    
    return report
