"""Validation framework for MITDS detection algorithms."""

from .golden import (
    GoldenDataset,
    GoldenCase,
    GoldenCaseType,
    CaseLabel,
    ExpectedSignal,
    ValidationResult,
    load_golden_dataset,
    validate_golden_case,
    create_sample_golden_dataset,
)
from .synthetic import (
    SyntheticGenerator,
    CoordinationPattern,
    PatternType,
    SyntheticEntity,
    SyntheticEvent,
    SyntheticRelationship,
    generate_synthetic_case,
    generate_validation_suite,
)
from .metrics import (
    ValidationMetrics,
    ConfusionMatrix,
    MetricHistory,
    FalsePositiveTracker,
    calculate_metrics,
    calculate_recall,
    calculate_precision,
    calculate_f1,
    calculate_false_positive_rate,
    calculate_accuracy,
)
from .dashboard import (
    MetricsDashboard,
    MetricsSummary,
    TimeSeriesMetric,
    aggregate_metrics,
    create_empty_dashboard,
)

__all__ = [
    # Golden dataset
    "GoldenDataset",
    "GoldenCase",
    "GoldenCaseType",
    "CaseLabel",
    "ExpectedSignal",
    "ValidationResult",
    "load_golden_dataset",
    "validate_golden_case",
    "create_sample_golden_dataset",
    # Synthetic generation
    "SyntheticGenerator",
    "CoordinationPattern",
    "PatternType",
    "SyntheticEntity",
    "SyntheticEvent",
    "SyntheticRelationship",
    "generate_synthetic_case",
    "generate_validation_suite",
    # Metrics
    "ValidationMetrics",
    "ConfusionMatrix",
    "MetricHistory",
    "FalsePositiveTracker",
    "calculate_metrics",
    "calculate_recall",
    "calculate_precision",
    "calculate_f1",
    "calculate_false_positive_rate",
    "calculate_accuracy",
    # Dashboard
    "MetricsDashboard",
    "MetricsSummary",
    "TimeSeriesMetric",
    "aggregate_metrics",
    "create_empty_dashboard",
]
