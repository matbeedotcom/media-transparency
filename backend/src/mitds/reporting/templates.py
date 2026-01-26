"""Report templates for MITDS.

Provides structured report templates for different analysis types:
- Structural Risk Report: Comprehensive assessment of influence networks
- Influence Topology Summary: Visual summary of entity connections
- Timeline Narrative: Temporal analysis of relationship changes

All reports use non-accusatory language and include evidence links.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


class ReportType(str, Enum):
    """Types of reports available in MITDS."""

    STRUCTURAL_RISK = "structural_risk"
    TOPOLOGY_SUMMARY = "topology_summary"
    TIMELINE_NARRATIVE = "timeline_narrative"
    FUNDING_ANALYSIS = "funding_analysis"
    INFRASTRUCTURE_OVERLAP = "infrastructure_overlap"


class ReportStatus(str, Enum):
    """Report generation status."""

    PENDING = "pending"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


class FindingSeverity(str, Enum):
    """Severity levels for findings."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class EvidenceType(str, Enum):
    """Types of evidence that support findings."""

    FUNDING_RECORD = "funding_record"
    BOARD_OVERLAP = "board_overlap"
    TEMPORAL_PATTERN = "temporal_pattern"
    INFRASTRUCTURE_MATCH = "infrastructure_match"
    OWNERSHIP_CHAIN = "ownership_chain"
    DOCUMENT_REFERENCE = "document_reference"


# =============================================================================
# Evidence and Citation Data Classes
# =============================================================================


@dataclass
class EvidenceCitation:
    """A single piece of evidence supporting a finding."""

    evidence_type: EvidenceType
    source_name: str
    source_url: str | None = None
    excerpt: str | None = None
    timestamp: datetime | None = None
    confidence: float = 1.0
    evidence_id: UUID | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_type": self.evidence_type.value,
            "source_name": self.source_name,
            "source_url": self.source_url,
            "excerpt": self.excerpt,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "confidence": self.confidence,
            "evidence_id": str(self.evidence_id) if self.evidence_id else None,
        }


@dataclass
class EntityReference:
    """Reference to an entity within a report."""

    entity_id: UUID
    entity_type: str
    name: str
    role_in_finding: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": str(self.entity_id),
            "entity_type": self.entity_type,
            "name": self.name,
            "role_in_finding": self.role_in_finding,
        }


# =============================================================================
# Finding Data Classes
# =============================================================================


@dataclass
class Finding:
    """A single finding in a report.

    Findings describe patterns or relationships observed,
    using non-accusatory language and linking to evidence.
    """

    id: UUID = field(default_factory=uuid4)
    title: str = ""
    description: str = ""
    severity: FindingSeverity = FindingSeverity.LOW
    confidence: float = 0.0
    confidence_lower: float = 0.0
    confidence_upper: float = 0.0

    # Entities involved
    entities: list[EntityReference] = field(default_factory=list)

    # Supporting evidence
    evidence: list[EvidenceCitation] = field(default_factory=list)

    # Explanation
    why_flagged: str = ""
    limitations: list[str] = field(default_factory=list)

    # Metadata
    signal_types: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "confidence": self.confidence,
            "confidence_band": {
                "lower": self.confidence_lower,
                "upper": self.confidence_upper,
            },
            "entities": [e.to_dict() for e in self.entities],
            "evidence": [e.to_dict() for e in self.evidence],
            "why_flagged": self.why_flagged,
            "limitations": self.limitations,
            "signal_types": self.signal_types,
            "created_at": self.created_at.isoformat(),
        }


# =============================================================================
# Report Section Data Classes
# =============================================================================


@dataclass
class ReportSection:
    """A section within a report."""

    title: str
    content: str
    findings: list[Finding] = field(default_factory=list)
    subsections: list["ReportSection"] = field(default_factory=list)
    visualizations: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "content": self.content,
            "findings": [f.to_dict() for f in self.findings],
            "subsections": [s.to_dict() for s in self.subsections],
            "visualizations": self.visualizations,
        }


@dataclass
class ReportMetadata:
    """Metadata for a report."""

    id: UUID = field(default_factory=uuid4)
    report_type: ReportType = ReportType.STRUCTURAL_RISK
    title: str = ""
    subtitle: str = ""
    generated_at: datetime = field(default_factory=datetime.utcnow)
    generated_by: str = "MITDS"
    version: str = "1.0"

    # Scope
    entity_ids: list[UUID] = field(default_factory=list)
    date_range_start: datetime | None = None
    date_range_end: datetime | None = None

    # Status
    status: ReportStatus = ReportStatus.PENDING

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "report_type": self.report_type.value,
            "title": self.title,
            "subtitle": self.subtitle,
            "generated_at": self.generated_at.isoformat(),
            "generated_by": self.generated_by,
            "version": self.version,
            "entity_ids": [str(eid) for eid in self.entity_ids],
            "date_range_start": self.date_range_start.isoformat() if self.date_range_start else None,
            "date_range_end": self.date_range_end.isoformat() if self.date_range_end else None,
            "status": self.status.value,
        }


# =============================================================================
# Report Base Class
# =============================================================================


@dataclass
class ReportBase:
    """Base class for all report types."""

    metadata: ReportMetadata = field(default_factory=ReportMetadata)
    executive_summary: str = ""
    methodology: str = ""
    sections: list[ReportSection] = field(default_factory=list)
    conclusions: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    # Aggregate stats
    total_findings: int = 0
    high_severity_count: int = 0
    entities_analyzed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "executive_summary": self.executive_summary,
            "methodology": self.methodology,
            "sections": [s.to_dict() for s in self.sections],
            "conclusions": self.conclusions,
            "limitations": self.limitations,
            "recommendations": self.recommendations,
            "statistics": {
                "total_findings": self.total_findings,
                "high_severity_count": self.high_severity_count,
                "entities_analyzed": self.entities_analyzed,
            },
        }


# =============================================================================
# Structural Risk Report Template
# =============================================================================


class StructuralRiskReport(ReportBase):
    """Comprehensive structural risk assessment report.

    Analyzes funding patterns, ownership structures, board interlocks,
    and other structural indicators of coordinated influence.
    """

    def __init__(
        self,
        entity_ids: list[UUID],
        title: str = "Structural Risk Assessment",
        date_range_start: datetime | None = None,
        date_range_end: datetime | None = None,
    ):
        super().__init__()
        self.metadata = ReportMetadata(
            report_type=ReportType.STRUCTURAL_RISK,
            title=title,
            subtitle="Analysis of organizational relationships and influence patterns",
            entity_ids=entity_ids,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )

        self.methodology = """
This report analyzes structural relationships between entities using publicly
available data sources including IRS Form 990 filings, corporate registries,
and documented organizational relationships. The analysis identifies patterns
that may indicate coordinated activity while acknowledging limitations in
data completeness and potential alternative explanations.
""".strip()

        self.limitations = [
            "Analysis limited to publicly available data sources",
            "Funding relationships may have legitimate explanations not captured",
            "Board member overlaps are common in some sectors",
            "Historical data may not reflect current relationships",
        ]

        # Initialize standard sections
        self._init_sections()

    def _init_sections(self) -> None:
        """Initialize standard report sections."""
        self.sections = [
            ReportSection(
                title="Funding Network Analysis",
                content="Analysis of financial flows and funding relationships.",
            ),
            ReportSection(
                title="Board and Leadership Overlaps",
                content="Examination of shared personnel across organizations.",
            ),
            ReportSection(
                title="Ownership Structure",
                content="Review of corporate ownership and control relationships.",
            ),
            ReportSection(
                title="Temporal Patterns",
                content="Analysis of timing and coordination indicators.",
            ),
            ReportSection(
                title="Infrastructure Overlap",
                content="Technical infrastructure sharing analysis.",
            ),
        ]

    def add_funding_finding(
        self,
        funder: EntityReference,
        recipients: list[EntityReference],
        total_amount: float,
        concentration: float,
        evidence: list[EvidenceCitation],
        why_flagged: str,
    ) -> Finding:
        """Add a funding-related finding to the report."""
        severity = self._calculate_funding_severity(concentration)
        confidence, conf_lower, conf_upper = self._calculate_confidence_band(
            evidence, concentration
        )

        finding = Finding(
            title=f"Funding concentration pattern involving {funder.name}",
            description=(
                f"Analysis identified {len(recipients)} entities receiving funding "
                f"from {funder.name}, with a concentration score of {concentration:.2f}. "
                f"Total funding observed: ${total_amount:,.2f}."
            ),
            severity=severity,
            confidence=confidence,
            confidence_lower=conf_lower,
            confidence_upper=conf_upper,
            entities=[funder] + recipients,
            evidence=evidence,
            why_flagged=why_flagged,
            limitations=[
                "Funding may support legitimate shared objectives",
                "Not all funding relationships may be captured in public records",
            ],
            signal_types=["funding_concentration", "shared_funder"],
        )

        # Add to funding section
        if self.sections:
            self.sections[0].findings.append(finding)
        self.total_findings += 1
        if severity in (FindingSeverity.HIGH, FindingSeverity.CRITICAL):
            self.high_severity_count += 1

        return finding

    def add_board_overlap_finding(
        self,
        person: EntityReference,
        organizations: list[EntityReference],
        overlap_count: int,
        evidence: list[EvidenceCitation],
        why_flagged: str,
    ) -> Finding:
        """Add a board overlap finding to the report."""
        severity = self._calculate_overlap_severity(overlap_count)
        confidence, conf_lower, conf_upper = self._calculate_confidence_band(
            evidence, overlap_count / 10.0
        )

        finding = Finding(
            title=f"Board position overlap: {person.name}",
            description=(
                f"{person.name} holds positions at {overlap_count} organizations "
                f"included in this analysis. While board service across multiple "
                f"organizations is common, the specific combination may warrant review."
            ),
            severity=severity,
            confidence=confidence,
            confidence_lower=conf_lower,
            confidence_upper=conf_upper,
            entities=[person] + organizations,
            evidence=evidence,
            why_flagged=why_flagged,
            limitations=[
                "Board service at multiple organizations is common in many sectors",
                "Positions may be advisory with limited influence",
            ],
            signal_types=["board_overlap", "personnel_interlock"],
        )

        # Add to board section
        if len(self.sections) > 1:
            self.sections[1].findings.append(finding)
        self.total_findings += 1
        if severity in (FindingSeverity.HIGH, FindingSeverity.CRITICAL):
            self.high_severity_count += 1

        return finding

    def _calculate_funding_severity(self, concentration: float) -> FindingSeverity:
        """Calculate severity based on funding concentration."""
        if concentration >= 0.8:
            return FindingSeverity.CRITICAL
        elif concentration >= 0.6:
            return FindingSeverity.HIGH
        elif concentration >= 0.4:
            return FindingSeverity.MEDIUM
        return FindingSeverity.LOW

    def _calculate_overlap_severity(self, overlap_count: int) -> FindingSeverity:
        """Calculate severity based on board overlap count."""
        if overlap_count >= 5:
            return FindingSeverity.HIGH
        elif overlap_count >= 3:
            return FindingSeverity.MEDIUM
        return FindingSeverity.LOW

    def _calculate_confidence_band(
        self,
        evidence: list[EvidenceCitation],
        signal_strength: float,
    ) -> tuple[float, float, float]:
        """Calculate confidence with upper/lower bounds."""
        base_confidence = min(0.95, signal_strength * 0.6 + len(evidence) * 0.1)

        # Add uncertainty margins
        margin = 0.15 - (len(evidence) * 0.02)
        margin = max(0.05, margin)

        return (
            base_confidence,
            max(0.0, base_confidence - margin),
            min(1.0, base_confidence + margin),
        )

    def finalize(self) -> None:
        """Finalize the report with summary and conclusions."""
        self.entities_analyzed = len(self.metadata.entity_ids)

        # Generate executive summary
        self.executive_summary = f"""
This structural risk assessment analyzed {self.entities_analyzed} entities and identified
{self.total_findings} patterns of potential interest. Of these, {self.high_severity_count}
were classified as high or critical severity based on the strength of observed signals
and supporting evidence.

The analysis examined funding relationships, board and personnel overlaps, ownership
structures, temporal coordination patterns, and shared infrastructure. All findings
include confidence bands and link to supporting evidence.

This report presents structural observations and does not make claims about intent
or wrongdoing. The patterns identified may have legitimate explanations not captured
in the available data.
""".strip()

        self.conclusions = [
            f"Identified {self.total_findings} structural patterns warranting attention",
            "Multiple independent signals corroborate key findings",
            "Temporal analysis reveals coordinated timing in some relationships",
        ]

        self.recommendations = [
            "Review high-severity findings with relevant domain expertise",
            "Consider additional data sources to validate observations",
            "Examine temporal evolution of key relationships",
        ]

        self.metadata.status = ReportStatus.COMPLETED


# =============================================================================
# Topology Summary Template
# =============================================================================


class TopologySummaryReport(ReportBase):
    """Visual summary of entity connections and network structure.

    Provides a condensed overview of the influence topology with
    key metrics and visualization data.
    """

    def __init__(
        self,
        entity_ids: list[UUID],
        title: str = "Influence Topology Summary",
    ):
        super().__init__()
        self.metadata = ReportMetadata(
            report_type=ReportType.TOPOLOGY_SUMMARY,
            title=title,
            subtitle="Network structure and connectivity overview",
            entity_ids=entity_ids,
        )

        self.methodology = """
This summary presents the network topology of relationships between analyzed entities.
Network metrics include centrality measures, clustering coefficients, and path lengths.
The visualization highlights key nodes and dense connection clusters.
""".strip()

        # Network metrics
        self.network_metrics: dict[str, Any] = {
            "node_count": 0,
            "edge_count": 0,
            "density": 0.0,
            "avg_clustering": 0.0,
            "connected_components": 0,
            "diameter": 0,
        }

        # Central entities
        self.central_entities: list[dict[str, Any]] = []

        # Clusters
        self.clusters: list[dict[str, Any]] = []

        # Graph data for visualization
        self.graph_data: dict[str, Any] = {"nodes": [], "edges": []}

    def set_network_metrics(
        self,
        node_count: int,
        edge_count: int,
        density: float,
        avg_clustering: float,
        connected_components: int,
        diameter: int,
    ) -> None:
        """Set network-level metrics."""
        self.network_metrics = {
            "node_count": node_count,
            "edge_count": edge_count,
            "density": density,
            "avg_clustering": avg_clustering,
            "connected_components": connected_components,
            "diameter": diameter,
        }

    def add_central_entity(
        self,
        entity: EntityReference,
        degree_centrality: float,
        betweenness_centrality: float,
        eigenvector_centrality: float,
    ) -> None:
        """Add a central entity to the summary."""
        self.central_entities.append({
            "entity": entity.to_dict(),
            "centrality": {
                "degree": degree_centrality,
                "betweenness": betweenness_centrality,
                "eigenvector": eigenvector_centrality,
            },
        })

    def add_cluster(
        self,
        cluster_id: str,
        members: list[EntityReference],
        internal_density: float,
        label: str = "",
    ) -> None:
        """Add a cluster to the summary."""
        self.clusters.append({
            "cluster_id": cluster_id,
            "members": [m.to_dict() for m in members],
            "member_count": len(members),
            "internal_density": internal_density,
            "label": label,
        })

    def to_dict(self) -> dict[str, Any]:
        base = super().to_dict()
        base["network_metrics"] = self.network_metrics
        base["central_entities"] = self.central_entities
        base["clusters"] = self.clusters
        base["graph_data"] = self.graph_data
        return base


# =============================================================================
# Timeline Narrative Template
# =============================================================================


class TimelineNarrativeReport(ReportBase):
    """Temporal analysis of relationship changes over time.

    Generates a narrative describing how relationships evolved,
    highlighting key events and pattern changes.
    """

    def __init__(
        self,
        entity_ids: list[UUID],
        title: str = "Timeline Narrative",
        date_range_start: datetime | None = None,
        date_range_end: datetime | None = None,
    ):
        super().__init__()
        self.metadata = ReportMetadata(
            report_type=ReportType.TIMELINE_NARRATIVE,
            title=title,
            subtitle="Temporal evolution of organizational relationships",
            entity_ids=entity_ids,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )

        self.methodology = """
This timeline narrative tracks the evolution of relationships over time using
dated records from corporate filings, funding disclosures, and other public
documents. Events are presented chronologically with supporting evidence.
""".strip()

        # Timeline events
        self.events: list[dict[str, Any]] = []

        # Phases identified
        self.phases: list[dict[str, Any]] = []

    def add_event(
        self,
        date: datetime,
        event_type: str,
        description: str,
        entities: list[EntityReference],
        evidence: list[EvidenceCitation],
        significance: str = "normal",
    ) -> None:
        """Add an event to the timeline."""
        self.events.append({
            "date": date.isoformat(),
            "event_type": event_type,
            "description": description,
            "entities": [e.to_dict() for e in entities],
            "evidence": [e.to_dict() for e in evidence],
            "significance": significance,
        })

        # Keep events sorted
        self.events.sort(key=lambda e: e["date"])

    def add_phase(
        self,
        start_date: datetime,
        end_date: datetime | None,
        phase_name: str,
        description: str,
        key_developments: list[str],
    ) -> None:
        """Add a phase to the timeline narrative."""
        self.phases.append({
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat() if end_date else None,
            "phase_name": phase_name,
            "description": description,
            "key_developments": key_developments,
        })

    def generate_narrative(self) -> str:
        """Generate a prose narrative from events and phases."""
        if not self.events:
            return "No events recorded in the analysis period."

        narrative_parts = []

        # Introduction
        if self.metadata.date_range_start and self.metadata.date_range_end:
            narrative_parts.append(
                f"This narrative covers the period from "
                f"{self.metadata.date_range_start.strftime('%B %Y')} to "
                f"{self.metadata.date_range_end.strftime('%B %Y')}."
            )

        # Phase summaries
        for phase in self.phases:
            narrative_parts.append(
                f"\n**{phase['phase_name']}** "
                f"({phase['start_date'][:7]} - {phase['end_date'][:7] if phase['end_date'] else 'present'}): "
                f"{phase['description']}"
            )

        # Key events
        significant_events = [e for e in self.events if e["significance"] == "high"]
        if significant_events:
            narrative_parts.append("\n**Key Events:**")
            for event in significant_events:
                narrative_parts.append(
                    f"- {event['date'][:10]}: {event['description']}"
                )

        return "\n".join(narrative_parts)

    def to_dict(self) -> dict[str, Any]:
        base = super().to_dict()
        base["events"] = self.events
        base["phases"] = self.phases
        base["narrative"] = self.generate_narrative()
        return base


# =============================================================================
# Report Factory
# =============================================================================


def create_report(
    report_type: ReportType,
    entity_ids: list[UUID],
    title: str | None = None,
    date_range_start: datetime | None = None,
    date_range_end: datetime | None = None,
) -> ReportBase:
    """Factory function to create reports by type."""
    if report_type == ReportType.STRUCTURAL_RISK:
        return StructuralRiskReport(
            entity_ids=entity_ids,
            title=title or "Structural Risk Assessment",
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
    elif report_type == ReportType.TOPOLOGY_SUMMARY:
        return TopologySummaryReport(
            entity_ids=entity_ids,
            title=title or "Influence Topology Summary",
        )
    elif report_type == ReportType.TIMELINE_NARRATIVE:
        return TimelineNarrativeReport(
            entity_ids=entity_ids,
            title=title or "Timeline Narrative",
            date_range_start=date_range_start,
            date_range_end=date_range_end,
        )
    else:
        raise ValueError(f"Unknown report type: {report_type}")


# =============================================================================
# Template Registry
# =============================================================================


REPORT_TEMPLATES = {
    ReportType.STRUCTURAL_RISK: {
        "name": "Structural Risk Assessment",
        "description": "Comprehensive analysis of funding, ownership, and personnel relationships",
        "sections": [
            "Funding Network Analysis",
            "Board and Leadership Overlaps",
            "Ownership Structure",
            "Temporal Patterns",
            "Infrastructure Overlap",
        ],
        "required_data": ["entities", "relationships", "funding_records"],
        "output_formats": ["json", "pdf", "html"],
    },
    ReportType.TOPOLOGY_SUMMARY: {
        "name": "Influence Topology Summary",
        "description": "Visual network summary with centrality metrics and clusters",
        "sections": ["Network Overview", "Central Entities", "Cluster Analysis"],
        "required_data": ["entities", "relationships"],
        "output_formats": ["json", "pdf", "html"],
    },
    ReportType.TIMELINE_NARRATIVE: {
        "name": "Timeline Narrative",
        "description": "Chronological narrative of relationship evolution",
        "sections": ["Timeline Overview", "Phase Analysis", "Key Events"],
        "required_data": ["entities", "relationships", "temporal_data"],
        "output_formats": ["json", "pdf", "html"],
    },
}
