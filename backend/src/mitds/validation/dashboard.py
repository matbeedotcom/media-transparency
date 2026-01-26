"""Dashboard data aggregation for MITDS validation metrics.

Provides aggregated views of validation metrics for display on dashboards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from .metrics import ValidationMetrics, MetricHistory


@dataclass
class TimeSeriesMetric:
    """A time series data point for a metric."""

    timestamp: datetime
    value: float
    label: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "label": self.label,
            "metadata": self.metadata,
        }


@dataclass
class MetricsSummary:
    """Summary of validation metrics for dashboard display."""

    # Current metrics
    current_recall: float = 0.0
    current_precision: float = 0.0
    current_f1: float = 0.0
    current_fpr: float = 0.0

    # Targets
    target_recall: float = 0.85
    target_max_fpr: float = 0.05

    # Status
    meets_targets: bool = False
    status_message: str = ""

    # Change indicators
    recall_change: float = 0.0
    precision_change: float = 0.0
    fpr_change: float = 0.0

    # Case counts
    total_cases: int = 0
    positive_cases: int = 0
    negative_cases: int = 0
    passed_cases: int = 0
    failed_cases: int = 0

    # Last run info
    last_run_at: datetime | None = None
    last_run_id: UUID | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "current": {
                "recall": self.current_recall,
                "precision": self.current_precision,
                "f1": self.current_f1,
                "false_positive_rate": self.current_fpr,
            },
            "targets": {
                "recall": self.target_recall,
                "max_fpr": self.target_max_fpr,
            },
            "status": {
                "meets_targets": self.meets_targets,
                "message": self.status_message,
            },
            "changes": {
                "recall": self.recall_change,
                "precision": self.precision_change,
                "fpr": self.fpr_change,
            },
            "cases": {
                "total": self.total_cases,
                "positive": self.positive_cases,
                "negative": self.negative_cases,
                "passed": self.passed_cases,
                "failed": self.failed_cases,
            },
            "last_run": {
                "at": self.last_run_at.isoformat() if self.last_run_at else None,
                "id": str(self.last_run_id) if self.last_run_id else None,
            },
        }


@dataclass
class MetricsDashboard:
    """Dashboard data aggregation for validation metrics."""

    # Summary
    summary: MetricsSummary = field(default_factory=MetricsSummary)

    # Time series data
    recall_history: list[TimeSeriesMetric] = field(default_factory=list)
    precision_history: list[TimeSeriesMetric] = field(default_factory=list)
    fpr_history: list[TimeSeriesMetric] = field(default_factory=list)
    f1_history: list[TimeSeriesMetric] = field(default_factory=list)

    # Breakdown by case type
    metrics_by_type: dict[str, dict[str, float]] = field(default_factory=dict)

    # Signal analysis
    signal_performance: dict[str, dict[str, float]] = field(default_factory=dict)

    # Recent failures
    recent_failures: list[dict[str, Any]] = field(default_factory=list)

    # Health indicators
    health_checks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "summary": self.summary.to_dict(),
            "history": {
                "recall": [m.to_dict() for m in self.recall_history],
                "precision": [m.to_dict() for m in self.precision_history],
                "false_positive_rate": [m.to_dict() for m in self.fpr_history],
                "f1": [m.to_dict() for m in self.f1_history],
            },
            "by_case_type": self.metrics_by_type,
            "signal_performance": self.signal_performance,
            "recent_failures": self.recent_failures,
            "health_checks": self.health_checks,
        }


def aggregate_metrics(
    history: MetricHistory,
    target_recall: float = 0.85,
    target_max_fpr: float = 0.05,
) -> MetricsDashboard:
    """Aggregate validation metrics for dashboard display.

    Args:
        history: History of validation runs
        target_recall: Target minimum recall
        target_max_fpr: Target maximum false positive rate

    Returns:
        MetricsDashboard with aggregated data
    """
    dashboard = MetricsDashboard()

    if not history.entries:
        dashboard.summary.status_message = "No validation runs yet"
        return dashboard

    # Get latest metrics
    latest = history.get_latest()
    if not latest:
        return dashboard

    # Previous run for change calculation
    sorted_entries = sorted(history.entries, key=lambda m: m.run_at)
    previous = sorted_entries[-2] if len(sorted_entries) >= 2 else None

    # Build summary
    summary = MetricsSummary(
        current_recall=latest.recall,
        current_precision=latest.precision,
        current_f1=latest.f1_score,
        current_fpr=latest.false_positive_rate,
        target_recall=target_recall,
        target_max_fpr=target_max_fpr,
        meets_targets=latest.meets_targets(target_recall, target_max_fpr),
        total_cases=len(latest.case_results),
        passed_cases=len(latest.passed_cases()),
        failed_cases=len(latest.failed_cases()),
        last_run_at=latest.run_at,
        last_run_id=latest.id,
    )

    # Count positive/negative cases
    from .golden import CaseLabel
    summary.positive_cases = sum(
        1 for r in latest.case_results if r.expected_label == CaseLabel.POSITIVE
    )
    summary.negative_cases = summary.total_cases - summary.positive_cases

    # Calculate changes from previous run
    if previous:
        summary.recall_change = latest.recall - previous.recall
        summary.precision_change = latest.precision - previous.precision
        summary.fpr_change = latest.false_positive_rate - previous.false_positive_rate

    # Set status message
    if summary.meets_targets:
        summary.status_message = "All targets met"
    else:
        issues = []
        if latest.recall < target_recall:
            issues.append(f"Recall {latest.recall:.1%} < {target_recall:.0%} target")
        if latest.false_positive_rate > target_max_fpr:
            issues.append(f"FPR {latest.false_positive_rate:.1%} > {target_max_fpr:.0%} target")
        summary.status_message = "; ".join(issues)

    dashboard.summary = summary

    # Build time series
    for entry in sorted_entries[-30:]:  # Last 30 runs
        dashboard.recall_history.append(
            TimeSeriesMetric(timestamp=entry.run_at, value=entry.recall)
        )
        dashboard.precision_history.append(
            TimeSeriesMetric(timestamp=entry.run_at, value=entry.precision)
        )
        dashboard.fpr_history.append(
            TimeSeriesMetric(timestamp=entry.run_at, value=entry.false_positive_rate)
        )
        dashboard.f1_history.append(
            TimeSeriesMetric(timestamp=entry.run_at, value=entry.f1_score)
        )

    # Aggregate metrics by case type
    dashboard.metrics_by_type = _aggregate_by_case_type(latest)

    # Analyze signal performance
    dashboard.signal_performance = _analyze_signal_performance(latest)

    # Get recent failures
    dashboard.recent_failures = _get_recent_failures(latest, limit=10)

    # Run health checks
    dashboard.health_checks = _run_health_checks(dashboard.summary, history)

    return dashboard


def _aggregate_by_case_type(metrics: ValidationMetrics) -> dict[str, dict[str, float]]:
    """Aggregate metrics by golden case type."""
    from .golden import CaseLabel

    type_results: dict[str, dict[str, list[bool]]] = {}

    for result in metrics.case_results:
        case_type = result.details.get("case_type", "unknown")
        if case_type not in type_results:
            type_results[case_type] = {"detected": [], "expected_positive": []}

        type_results[case_type]["detected"].append(result.detected)
        type_results[case_type]["expected_positive"].append(
            result.expected_label == CaseLabel.POSITIVE
        )

    aggregated = {}
    for case_type, data in type_results.items():
        detected = data["detected"]
        expected_positive = data["expected_positive"]

        tp = sum(1 for d, e in zip(detected, expected_positive) if d and e)
        fn = sum(1 for d, e in zip(detected, expected_positive) if not d and e)
        fp = sum(1 for d, e in zip(detected, expected_positive) if d and not e)
        tn = sum(1 for d, e in zip(detected, expected_positive) if not d and not e)

        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        aggregated[case_type] = {
            "count": len(detected),
            "recall": recall,
            "precision": precision,
            "false_positive_rate": fpr,
        }

    return aggregated


def _analyze_signal_performance(metrics: ValidationMetrics) -> dict[str, dict[str, float]]:
    """Analyze which signals are performing well/poorly."""
    signal_stats: dict[str, dict[str, int]] = {}

    for result in metrics.case_results:
        for signal in result.signals_found:
            if signal not in signal_stats:
                signal_stats[signal] = {"found": 0, "in_tp": 0, "in_fp": 0}
            signal_stats[signal]["found"] += 1

            from .golden import CaseLabel
            if result.expected_label == CaseLabel.POSITIVE:
                signal_stats[signal]["in_tp"] += 1
            else:
                signal_stats[signal]["in_fp"] += 1

        for signal in result.signals_missing:
            if signal not in signal_stats:
                signal_stats[signal] = {"found": 0, "in_tp": 0, "in_fp": 0, "missed": 0}
            signal_stats[signal].setdefault("missed", 0)
            signal_stats[signal]["missed"] += 1

    # Calculate performance ratios
    performance = {}
    for signal, stats in signal_stats.items():
        found = stats["found"]
        in_tp = stats["in_tp"]
        in_fp = stats["in_fp"]
        missed = stats.get("missed", 0)

        performance[signal] = {
            "detection_rate": found / (found + missed) if (found + missed) > 0 else 0.0,
            "true_positive_rate": in_tp / found if found > 0 else 0.0,
            "false_positive_rate": in_fp / found if found > 0 else 0.0,
            "total_found": found,
            "total_missed": missed,
        }

    return performance


def _get_recent_failures(metrics: ValidationMetrics, limit: int = 10) -> list[dict[str, Any]]:
    """Get details of recent validation failures."""
    failures = []

    for result in metrics.failed_cases()[:limit]:
        from .golden import CaseLabel
        failure_type = (
            "false_negative"
            if result.expected_label == CaseLabel.POSITIVE
            else "false_positive"
        )

        failures.append({
            "case_id": str(result.case_id),
            "case_name": result.details.get("case_name", "Unknown"),
            "failure_type": failure_type,
            "score": result.score,
            "expected_label": result.expected_label.value,
            "detected": result.detected,
            "signals_found": result.signals_found,
            "signals_missing": result.signals_missing,
        })

    return failures


def _run_health_checks(
    summary: MetricsSummary,
    history: MetricHistory,
) -> list[dict[str, Any]]:
    """Run health checks on validation metrics."""
    checks = []

    # Check 1: Recall target
    checks.append({
        "name": "Recall Target",
        "status": "pass" if summary.current_recall >= summary.target_recall else "fail",
        "message": (
            f"Recall {summary.current_recall:.1%} "
            f"{'meets' if summary.current_recall >= summary.target_recall else 'below'} "
            f"{summary.target_recall:.0%} target"
        ),
        "value": summary.current_recall,
        "target": summary.target_recall,
    })

    # Check 2: FPR target
    checks.append({
        "name": "False Positive Rate",
        "status": "pass" if summary.current_fpr <= summary.target_max_fpr else "fail",
        "message": (
            f"FPR {summary.current_fpr:.1%} "
            f"{'within' if summary.current_fpr <= summary.target_max_fpr else 'exceeds'} "
            f"{summary.target_max_fpr:.0%} limit"
        ),
        "value": summary.current_fpr,
        "target": summary.target_max_fpr,
    })

    # Check 3: Recall regression
    if history.detect_regression("recall", threshold=0.05):
        checks.append({
            "name": "Recall Regression",
            "status": "warning",
            "message": f"Recall dropped by {abs(summary.recall_change):.1%} from previous run",
            "value": summary.recall_change,
        })
    else:
        checks.append({
            "name": "Recall Stability",
            "status": "pass",
            "message": "No significant recall regression detected",
            "value": summary.recall_change,
        })

    # Check 4: Precision health
    if summary.current_precision < 0.5:
        checks.append({
            "name": "Precision Warning",
            "status": "warning",
            "message": f"Low precision {summary.current_precision:.1%} - many false positives",
            "value": summary.current_precision,
        })

    # Check 5: Test coverage
    if summary.total_cases < 20:
        checks.append({
            "name": "Test Coverage",
            "status": "warning",
            "message": f"Only {summary.total_cases} test cases - consider adding more",
            "value": summary.total_cases,
        })
    else:
        checks.append({
            "name": "Test Coverage",
            "status": "pass",
            "message": f"{summary.total_cases} test cases",
            "value": summary.total_cases,
        })

    return checks


def create_empty_dashboard() -> MetricsDashboard:
    """Create an empty dashboard for when no data is available."""
    dashboard = MetricsDashboard()
    dashboard.summary.status_message = "No validation data available"
    dashboard.health_checks = [
        {
            "name": "Validation Status",
            "status": "warning",
            "message": "No validation runs have been performed yet",
        }
    ]
    return dashboard
