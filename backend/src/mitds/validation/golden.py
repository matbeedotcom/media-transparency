"""Golden dataset schema and loader for MITDS validation.

The golden dataset contains documented influence operations used to
validate detection algorithm accuracy.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


class GoldenCaseType(str, Enum):
    """Types of golden cases."""

    KNOWN_COORDINATION = "known_coordination"
    KNOWN_FUNDING = "known_funding"
    KNOWN_INFRASTRUCTURE = "known_infrastructure"
    HARD_NEGATIVE = "hard_negative"  # Known non-coordination


class CaseLabel(str, Enum):
    """Ground truth labels for validation."""

    POSITIVE = "positive"  # Should be flagged
    NEGATIVE = "negative"  # Should NOT be flagged


@dataclass
class ExpectedSignal:
    """Expected signal that should be detected in a golden case."""

    signal_type: str
    description: str
    min_confidence: float = 0.5
    required: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class GoldenCase:
    """A single golden dataset case for validation."""

    id: UUID
    name: str
    description: str
    case_type: GoldenCaseType
    label: CaseLabel
    source_documentation: str
    date_documented: datetime

    # Entities involved
    entity_ids: list[UUID] = field(default_factory=list)
    entity_names: list[str] = field(default_factory=list)

    # Expected detection results
    expected_signals: list[ExpectedSignal] = field(default_factory=list)
    expected_min_score: float | None = None
    expected_max_score: float | None = None

    # Time period of the operation
    period_start: datetime | None = None
    period_end: datetime | None = None

    # Additional metadata
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": str(self.id),
            "name": self.name,
            "description": self.description,
            "case_type": self.case_type.value,
            "label": self.label.value,
            "source_documentation": self.source_documentation,
            "date_documented": self.date_documented.isoformat(),
            "entity_ids": [str(eid) for eid in self.entity_ids],
            "entity_names": self.entity_names,
            "expected_signals": [
                {
                    "signal_type": s.signal_type,
                    "description": s.description,
                    "min_confidence": s.min_confidence,
                    "required": s.required,
                    "metadata": s.metadata,
                }
                for s in self.expected_signals
            ],
            "expected_min_score": self.expected_min_score,
            "expected_max_score": self.expected_max_score,
            "period_start": self.period_start.isoformat() if self.period_start else None,
            "period_end": self.period_end.isoformat() if self.period_end else None,
            "tags": self.tags,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GoldenCase:
        """Create from dictionary."""
        expected_signals = [
            ExpectedSignal(
                signal_type=s["signal_type"],
                description=s["description"],
                min_confidence=s.get("min_confidence", 0.5),
                required=s.get("required", True),
                metadata=s.get("metadata", {}),
            )
            for s in data.get("expected_signals", [])
        ]

        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            name=data["name"],
            description=data["description"],
            case_type=GoldenCaseType(data["case_type"]),
            label=CaseLabel(data["label"]),
            source_documentation=data["source_documentation"],
            date_documented=datetime.fromisoformat(data["date_documented"]),
            entity_ids=[
                UUID(eid) if isinstance(eid, str) else eid
                for eid in data.get("entity_ids", [])
            ],
            entity_names=data.get("entity_names", []),
            expected_signals=expected_signals,
            expected_min_score=data.get("expected_min_score"),
            expected_max_score=data.get("expected_max_score"),
            period_start=(
                datetime.fromisoformat(data["period_start"])
                if data.get("period_start")
                else None
            ),
            period_end=(
                datetime.fromisoformat(data["period_end"])
                if data.get("period_end")
                else None
            ),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


@dataclass
class GoldenDataset:
    """Collection of golden cases for validation."""

    name: str
    version: str
    description: str
    cases: list[GoldenCase] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def positive_cases(self) -> list[GoldenCase]:
        """Get cases that should be flagged."""
        return [c for c in self.cases if c.label == CaseLabel.POSITIVE]

    @property
    def negative_cases(self) -> list[GoldenCase]:
        """Get hard negative cases (should NOT be flagged)."""
        return [c for c in self.cases if c.label == CaseLabel.NEGATIVE]

    @property
    def coordination_cases(self) -> list[GoldenCase]:
        """Get known coordination cases."""
        return [c for c in self.cases if c.case_type == GoldenCaseType.KNOWN_COORDINATION]

    @property
    def funding_cases(self) -> list[GoldenCase]:
        """Get known funding cases."""
        return [c for c in self.cases if c.case_type == GoldenCaseType.KNOWN_FUNDING]

    @property
    def infrastructure_cases(self) -> list[GoldenCase]:
        """Get known infrastructure cases."""
        return [c for c in self.cases if c.case_type == GoldenCaseType.KNOWN_INFRASTRUCTURE]

    @property
    def hard_negatives(self) -> list[GoldenCase]:
        """Get hard negative cases."""
        return [c for c in self.cases if c.case_type == GoldenCaseType.HARD_NEGATIVE]

    def get_case_by_id(self, case_id: UUID) -> GoldenCase | None:
        """Get a case by ID."""
        for case in self.cases:
            if case.id == case_id:
                return case
        return None

    def get_cases_by_tag(self, tag: str) -> list[GoldenCase]:
        """Get cases with a specific tag."""
        return [c for c in self.cases if tag in c.tags]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "cases": [c.to_dict() for c in self.cases],
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GoldenDataset:
        """Create from dictionary."""
        return cls(
            name=data["name"],
            version=data["version"],
            description=data["description"],
            cases=[GoldenCase.from_dict(c) for c in data.get("cases", [])],
            created_at=datetime.fromisoformat(data["created_at"]),
            metadata=data.get("metadata", {}),
        )

    def save(self, path: Path) -> None:
        """Save dataset to JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> GoldenDataset:
        """Load dataset from JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls.from_dict(data)


def load_golden_dataset(path: str | Path) -> GoldenDataset:
    """Load a golden dataset from file.

    Args:
        path: Path to the JSON dataset file

    Returns:
        Loaded GoldenDataset instance

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Golden dataset not found: {path}")

    try:
        return GoldenDataset.load(path)
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Invalid golden dataset format: {e}") from e


@dataclass
class ValidationResult:
    """Result of validating a detection against a golden case."""

    case_id: UUID
    detected: bool
    expected_label: CaseLabel
    score: float | None = None
    signals_found: list[str] = field(default_factory=list)
    signals_missing: list[str] = field(default_factory=list)
    passed: bool = False
    details: dict[str, Any] = field(default_factory=dict)


def validate_golden_case(
    case: GoldenCase,
    detection_result: dict[str, Any],
    threshold: float = 0.45,
) -> ValidationResult:
    """Validate a detection result against a golden case.

    Args:
        case: The golden case to validate against
        detection_result: The detection output to validate
        threshold: Score threshold for considering something detected

    Returns:
        ValidationResult with pass/fail and details
    """
    score = detection_result.get("score", 0.0)
    detected = score >= threshold

    # Check which signals were found
    detected_signals = detection_result.get("signals", [])
    detected_types = {s.get("type", s) for s in detected_signals}

    signals_found = []
    signals_missing = []

    for expected in case.expected_signals:
        if expected.signal_type in detected_types:
            signals_found.append(expected.signal_type)
        elif expected.required:
            signals_missing.append(expected.signal_type)

    # Determine if validation passed
    if case.label == CaseLabel.POSITIVE:
        # Should have been detected
        passed = detected and len(signals_missing) == 0

        # Check score bounds if specified
        if passed and case.expected_min_score is not None:
            passed = passed and score >= case.expected_min_score
        if passed and case.expected_max_score is not None:
            passed = passed and score <= case.expected_max_score

    else:  # NEGATIVE - hard negative
        # Should NOT have been detected
        passed = not detected

    return ValidationResult(
        case_id=case.id,
        detected=detected,
        expected_label=case.label,
        score=score,
        signals_found=signals_found,
        signals_missing=signals_missing,
        passed=passed,
        details={
            "case_name": case.name,
            "case_type": case.case_type.value,
            "threshold": threshold,
            "detected_signal_count": len(detected_signals),
        },
    )


# =========================
# Sample Golden Cases
# =========================


def create_sample_golden_dataset() -> GoldenDataset:
    """Create a sample golden dataset for testing.

    This creates example cases that can be used for initial validation.
    In production, replace with documented influence operations.
    """
    cases = [
        GoldenCase(
            id=uuid4(),
            name="Coordinated Climate Disinformation Network",
            description=(
                "Network of websites and social accounts coordinating to spread "
                "climate change denial content, sharing infrastructure and timing."
            ),
            case_type=GoldenCaseType.KNOWN_COORDINATION,
            label=CaseLabel.POSITIVE,
            source_documentation="https://example.org/research/climate-disinfo-2024",
            date_documented=datetime(2024, 3, 15),
            expected_signals=[
                ExpectedSignal(
                    signal_type="temporal_coordination",
                    description="Synchronized publication within 30-minute windows",
                    min_confidence=0.7,
                ),
                ExpectedSignal(
                    signal_type="shared_infrastructure",
                    description="Common Google Analytics ID across domains",
                    min_confidence=0.8,
                ),
                ExpectedSignal(
                    signal_type="funding_concentration",
                    description="Common funder identified in 990 filings",
                    min_confidence=0.6,
                ),
            ],
            expected_min_score=0.5,
            tags=["climate", "disinformation", "infrastructure"],
        ),
        GoldenCase(
            id=uuid4(),
            name="Legitimate Wire Service Coverage",
            description=(
                "Multiple outlets publishing similar content because they use "
                "the same wire service (AP, Reuters). Should NOT be flagged."
            ),
            case_type=GoldenCaseType.HARD_NEGATIVE,
            label=CaseLabel.NEGATIVE,
            source_documentation="Internal validation case",
            date_documented=datetime(2024, 1, 1),
            expected_signals=[],
            expected_max_score=0.3,
            tags=["wire-service", "legitimate", "hard-negative"],
        ),
        GoldenCase(
            id=uuid4(),
            name="Think Tank Funding Network",
            description=(
                "Network of think tanks receiving funding from common donors "
                "and producing aligned policy recommendations."
            ),
            case_type=GoldenCaseType.KNOWN_FUNDING,
            label=CaseLabel.POSITIVE,
            source_documentation="https://example.org/research/think-tank-funding",
            date_documented=datetime(2024, 2, 20),
            expected_signals=[
                ExpectedSignal(
                    signal_type="funding_concentration",
                    description="80%+ funding from 3 or fewer sources",
                    min_confidence=0.7,
                ),
                ExpectedSignal(
                    signal_type="board_overlap",
                    description="Shared board members across organizations",
                    min_confidence=0.5,
                    required=False,
                ),
            ],
            expected_min_score=0.45,
            tags=["funding", "think-tank", "policy"],
        ),
        GoldenCase(
            id=uuid4(),
            name="Breaking News Event Coverage",
            description=(
                "Multiple outlets covering same breaking news event. "
                "Temporal clustering is expected and legitimate."
            ),
            case_type=GoldenCaseType.HARD_NEGATIVE,
            label=CaseLabel.NEGATIVE,
            source_documentation="Internal validation case - breaking news filter",
            date_documented=datetime(2024, 1, 15),
            expected_signals=[],
            expected_max_score=0.25,
            tags=["breaking-news", "legitimate", "hard-negative"],
        ),
    ]

    return GoldenDataset(
        name="MITDS Sample Golden Dataset",
        version="1.0.0",
        description=(
            "Sample golden dataset for MITDS validation. Contains example "
            "documented influence operations and hard negative cases."
        ),
        cases=cases,
        metadata={
            "created_by": "MITDS Development Team",
            "purpose": "Initial validation testing",
        },
    )
