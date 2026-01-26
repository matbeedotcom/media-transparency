"""Validation metrics calculation for MITDS.

Implements recall, precision, F1, and false positive rate tracking
for validating detection algorithm accuracy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from .golden import CaseLabel, GoldenCase, ValidationResult


@dataclass
class ConfusionMatrix:
    """Confusion matrix for binary classification."""

    true_positives: int = 0
    true_negatives: int = 0
    false_positives: int = 0
    false_negatives: int = 0

    @property
    def total(self) -> int:
        """Total number of cases."""
        return (
            self.true_positives
            + self.true_negatives
            + self.false_positives
            + self.false_negatives
        )

    @property
    def total_positives(self) -> int:
        """Total actual positive cases."""
        return self.true_positives + self.false_negatives

    @property
    def total_negatives(self) -> int:
        """Total actual negative cases."""
        return self.true_negatives + self.false_positives

    @property
    def total_predicted_positives(self) -> int:
        """Total predicted positive cases."""
        return self.true_positives + self.false_positives

    @property
    def total_predicted_negatives(self) -> int:
        """Total predicted negative cases."""
        return self.true_negatives + self.false_negatives

    def add_result(self, actual_positive: bool, predicted_positive: bool) -> None:
        """Add a single result to the matrix."""
        if actual_positive and predicted_positive:
            self.true_positives += 1
        elif actual_positive and not predicted_positive:
            self.false_negatives += 1
        elif not actual_positive and predicted_positive:
            self.false_positives += 1
        else:
            self.true_negatives += 1

    def to_dict(self) -> dict[str, int]:
        """Convert to dictionary."""
        return {
            "true_positives": self.true_positives,
            "true_negatives": self.true_negatives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
        }


@dataclass
class ValidationMetrics:
    """Comprehensive validation metrics."""

    id: UUID = field(default_factory=uuid4)
    run_at: datetime = field(default_factory=datetime.utcnow)
    threshold: float = 0.45

    # Confusion matrix
    confusion_matrix: ConfusionMatrix = field(default_factory=ConfusionMatrix)

    # Per-case results
    case_results: list[ValidationResult] = field(default_factory=list)

    # Metadata
    dataset_name: str = ""
    dataset_version: str = ""
    algorithm_version: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def recall(self) -> float:
        """Calculate recall (sensitivity, true positive rate).

        Recall = TP / (TP + FN)
        """
        return calculate_recall(self.confusion_matrix)

    @property
    def precision(self) -> float:
        """Calculate precision (positive predictive value).

        Precision = TP / (TP + FP)
        """
        return calculate_precision(self.confusion_matrix)

    @property
    def f1_score(self) -> float:
        """Calculate F1 score (harmonic mean of precision and recall)."""
        return calculate_f1(self.confusion_matrix)

    @property
    def false_positive_rate(self) -> float:
        """Calculate false positive rate.

        FPR = FP / (FP + TN)
        """
        return calculate_false_positive_rate(self.confusion_matrix)

    @property
    def false_negative_rate(self) -> float:
        """Calculate false negative rate.

        FNR = FN / (FN + TP)
        """
        return calculate_false_negative_rate(self.confusion_matrix)

    @property
    def accuracy(self) -> float:
        """Calculate accuracy.

        Accuracy = (TP + TN) / Total
        """
        return calculate_accuracy(self.confusion_matrix)

    @property
    def specificity(self) -> float:
        """Calculate specificity (true negative rate).

        Specificity = TN / (TN + FP)
        """
        return calculate_specificity(self.confusion_matrix)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result and update metrics."""
        self.case_results.append(result)

        actual_positive = result.expected_label == CaseLabel.POSITIVE
        predicted_positive = result.detected

        self.confusion_matrix.add_result(actual_positive, predicted_positive)

    def passed_cases(self) -> list[ValidationResult]:
        """Get cases that passed validation."""
        return [r for r in self.case_results if r.passed]

    def failed_cases(self) -> list[ValidationResult]:
        """Get cases that failed validation."""
        return [r for r in self.case_results if not r.passed]

    def false_positive_cases(self) -> list[ValidationResult]:
        """Get false positive cases (negative cases that were detected)."""
        return [
            r
            for r in self.case_results
            if r.expected_label == CaseLabel.NEGATIVE and r.detected
        ]

    def false_negative_cases(self) -> list[ValidationResult]:
        """Get false negative cases (positive cases that were missed)."""
        return [
            r
            for r in self.case_results
            if r.expected_label == CaseLabel.POSITIVE and not r.detected
        ]

    def meets_targets(
        self,
        min_recall: float = 0.85,
        max_fpr: float = 0.05,
    ) -> bool:
        """Check if metrics meet target thresholds.

        Args:
            min_recall: Minimum required recall (default 85%)
            max_fpr: Maximum allowed false positive rate (default 5%)

        Returns:
            True if both targets are met
        """
        return self.recall >= min_recall and self.false_positive_rate <= max_fpr

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": str(self.id),
            "run_at": self.run_at.isoformat(),
            "threshold": self.threshold,
            "confusion_matrix": self.confusion_matrix.to_dict(),
            "metrics": {
                "recall": self.recall,
                "precision": self.precision,
                "f1_score": self.f1_score,
                "false_positive_rate": self.false_positive_rate,
                "false_negative_rate": self.false_negative_rate,
                "accuracy": self.accuracy,
                "specificity": self.specificity,
            },
            "case_count": len(self.case_results),
            "passed_count": len(self.passed_cases()),
            "failed_count": len(self.failed_cases()),
            "dataset_name": self.dataset_name,
            "dataset_version": self.dataset_version,
            "algorithm_version": self.algorithm_version,
            "metadata": self.metadata,
        }


# =========================
# Metric Calculation Functions
# =========================


def calculate_recall(cm: ConfusionMatrix) -> float:
    """Calculate recall (sensitivity, true positive rate).

    Recall = TP / (TP + FN)

    Interpretation: Of all actual positives, what fraction did we detect?
    Target: >= 85% for MITDS
    """
    denominator = cm.true_positives + cm.false_negatives
    if denominator == 0:
        return 0.0
    return cm.true_positives / denominator


def calculate_precision(cm: ConfusionMatrix) -> float:
    """Calculate precision (positive predictive value).

    Precision = TP / (TP + FP)

    Interpretation: Of all predicted positives, what fraction were correct?
    """
    denominator = cm.true_positives + cm.false_positives
    if denominator == 0:
        return 0.0
    return cm.true_positives / denominator


def calculate_f1(cm: ConfusionMatrix) -> float:
    """Calculate F1 score (harmonic mean of precision and recall).

    F1 = 2 * (precision * recall) / (precision + recall)
    """
    prec = calculate_precision(cm)
    rec = calculate_recall(cm)
    if prec + rec == 0:
        return 0.0
    return 2 * (prec * rec) / (prec + rec)


def calculate_false_positive_rate(cm: ConfusionMatrix) -> float:
    """Calculate false positive rate.

    FPR = FP / (FP + TN)

    Interpretation: Of all actual negatives, what fraction did we incorrectly flag?
    Target: <= 5% for MITDS
    """
    denominator = cm.false_positives + cm.true_negatives
    if denominator == 0:
        return 0.0
    return cm.false_positives / denominator


def calculate_false_negative_rate(cm: ConfusionMatrix) -> float:
    """Calculate false negative rate.

    FNR = FN / (FN + TP)

    Interpretation: Of all actual positives, what fraction did we miss?
    """
    denominator = cm.false_negatives + cm.true_positives
    if denominator == 0:
        return 0.0
    return cm.false_negatives / denominator


def calculate_accuracy(cm: ConfusionMatrix) -> float:
    """Calculate accuracy.

    Accuracy = (TP + TN) / Total

    Note: Accuracy can be misleading for imbalanced datasets.
    Prefer recall + FPR for MITDS validation.
    """
    if cm.total == 0:
        return 0.0
    return (cm.true_positives + cm.true_negatives) / cm.total


def calculate_specificity(cm: ConfusionMatrix) -> float:
    """Calculate specificity (true negative rate).

    Specificity = TN / (TN + FP)

    Interpretation: Of all actual negatives, what fraction did we correctly reject?
    """
    denominator = cm.true_negatives + cm.false_positives
    if denominator == 0:
        return 0.0
    return cm.true_negatives / denominator


def calculate_metrics(
    results: list[ValidationResult],
    threshold: float = 0.45,
    dataset_name: str = "",
    dataset_version: str = "",
) -> ValidationMetrics:
    """Calculate comprehensive metrics from validation results.

    Args:
        results: List of validation results
        threshold: Detection threshold used
        dataset_name: Name of the validation dataset
        dataset_version: Version of the validation dataset

    Returns:
        ValidationMetrics with all calculated metrics
    """
    metrics = ValidationMetrics(
        threshold=threshold,
        dataset_name=dataset_name,
        dataset_version=dataset_version,
    )

    for result in results:
        metrics.add_result(result)

    return metrics


# =========================
# Metric Tracking Over Time
# =========================


@dataclass
class MetricHistory:
    """Tracks metrics over multiple validation runs."""

    entries: list[ValidationMetrics] = field(default_factory=list)

    def add_run(self, metrics: ValidationMetrics) -> None:
        """Add a validation run to history."""
        self.entries.append(metrics)

    def get_latest(self) -> ValidationMetrics | None:
        """Get the most recent validation run."""
        if not self.entries:
            return None
        return max(self.entries, key=lambda m: m.run_at)

    def get_trend(self, metric_name: str, last_n: int = 10) -> list[tuple[datetime, float]]:
        """Get trend data for a specific metric.

        Args:
            metric_name: Name of metric (recall, precision, f1_score, etc.)
            last_n: Number of most recent runs to include

        Returns:
            List of (timestamp, value) tuples
        """
        sorted_entries = sorted(self.entries, key=lambda m: m.run_at)[-last_n:]

        return [
            (entry.run_at, getattr(entry, metric_name, 0.0))
            for entry in sorted_entries
        ]

    def detect_regression(
        self,
        metric_name: str,
        threshold: float = 0.05,
    ) -> bool:
        """Detect if a metric has regressed from previous run.

        Args:
            metric_name: Name of metric to check
            threshold: Minimum decrease to count as regression

        Returns:
            True if metric has decreased by more than threshold
        """
        if len(self.entries) < 2:
            return False

        sorted_entries = sorted(self.entries, key=lambda m: m.run_at)
        current = getattr(sorted_entries[-1], metric_name, 0.0)
        previous = getattr(sorted_entries[-2], metric_name, 0.0)

        return (previous - current) > threshold

    def summary(self) -> dict[str, Any]:
        """Get summary statistics across all runs."""
        if not self.entries:
            return {}

        recalls = [e.recall for e in self.entries]
        precisions = [e.precision for e in self.entries]
        fprs = [e.false_positive_rate for e in self.entries]

        return {
            "total_runs": len(self.entries),
            "first_run": min(e.run_at for e in self.entries).isoformat(),
            "last_run": max(e.run_at for e in self.entries).isoformat(),
            "recall": {
                "min": min(recalls),
                "max": max(recalls),
                "avg": sum(recalls) / len(recalls),
                "latest": recalls[-1] if recalls else 0.0,
            },
            "precision": {
                "min": min(precisions),
                "max": max(precisions),
                "avg": sum(precisions) / len(precisions),
                "latest": precisions[-1] if precisions else 0.0,
            },
            "false_positive_rate": {
                "min": min(fprs),
                "max": max(fprs),
                "avg": sum(fprs) / len(fprs),
                "latest": fprs[-1] if fprs else 0.0,
            },
        }


@dataclass
class FalsePositiveTracker:
    """Tracks false positive cases for analysis and improvement."""

    cases: list[dict[str, Any]] = field(default_factory=list)

    def add_false_positive(
        self,
        case_id: UUID,
        case_name: str,
        score: float,
        signals: list[str],
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record a false positive case."""
        self.cases.append({
            "case_id": str(case_id),
            "case_name": case_name,
            "score": score,
            "signals": signals,
            "details": details or {},
            "recorded_at": datetime.utcnow().isoformat(),
        })

    def get_common_signals(self, top_n: int = 5) -> list[tuple[str, int]]:
        """Get most common signals in false positives.

        Helps identify which signals are causing false positives.
        """
        signal_counts: dict[str, int] = {}
        for case in self.cases:
            for signal in case.get("signals", []):
                signal_counts[signal] = signal_counts.get(signal, 0) + 1

        sorted_signals = sorted(
            signal_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return sorted_signals[:top_n]

    def get_score_distribution(self) -> dict[str, int]:
        """Get distribution of false positive scores by range."""
        ranges = {
            "0.45-0.50": 0,
            "0.50-0.60": 0,
            "0.60-0.70": 0,
            "0.70-0.80": 0,
            "0.80-0.90": 0,
            "0.90-1.00": 0,
        }

        for case in self.cases:
            score = case.get("score", 0)
            if 0.45 <= score < 0.50:
                ranges["0.45-0.50"] += 1
            elif 0.50 <= score < 0.60:
                ranges["0.50-0.60"] += 1
            elif 0.60 <= score < 0.70:
                ranges["0.60-0.70"] += 1
            elif 0.70 <= score < 0.80:
                ranges["0.70-0.80"] += 1
            elif 0.80 <= score < 0.90:
                ranges["0.80-0.90"] += 1
            elif 0.90 <= score <= 1.00:
                ranges["0.90-1.00"] += 1

        return ranges

    def summary(self) -> dict[str, Any]:
        """Get summary of false positives."""
        if not self.cases:
            return {"total": 0}

        scores = [c.get("score", 0) for c in self.cases]

        return {
            "total": len(self.cases),
            "common_signals": self.get_common_signals(),
            "score_distribution": self.get_score_distribution(),
            "avg_score": sum(scores) / len(scores),
            "max_score": max(scores),
            "min_score": min(scores),
        }
