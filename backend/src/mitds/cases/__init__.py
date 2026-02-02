"""Cases module for MITDS - Autonomous Case Intake System.

Provides a universal research framework that transforms MITDS from
a CLI-driven tool into an autonomous research platform. Supports
multiple entry points (Meta Ads, corporation names, URLs, text)
and produces evidence-backed case reports with ranked findings.

Main components:
- CaseManager: Manages case lifecycle (create, pause, resume, complete)
- Entry point adapters: Normalize diverse inputs into leads
- Entity extraction: Deterministic + LLM-based extraction from text
- Sponsor resolution: Match sponsors to known organizations
- Report generator: Produce ranked findings with evidence links

Usage:
    from mitds.cases import (
        get_case_manager,
        CaseStatus,
        EntryPointType,
    )

    # Create a new case from a Meta Ad sponsor
    manager = get_case_manager()
    case = await manager.create_case(
        name="PAC Investigation",
        entry_point_type=EntryPointType.META_AD,
        entry_point_value="Americans for Prosperity",
    )

    # Get case status and report
    case = await manager.get_case(case.id)
    report = await manager.get_report(case.id)
"""

from .models import (
    Case,
    CaseConfig,
    CaseReport,
    CaseStatus,
    EntryPointType,
    EntityMatch,
    Evidence,
    EvidenceType,
    ExtractedLead,
    ExtractionMethod,
    MatchStatus,
)

__all__ = [
    # Core models
    "Case",
    "CaseConfig",
    "CaseReport",
    "CaseStatus",
    "EntryPointType",
    "EntityMatch",
    "Evidence",
    "EvidenceType",
    "ExtractedLead",
    "ExtractionMethod",
    "MatchStatus",
]
