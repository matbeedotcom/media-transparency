"""Data quality metrics tracking for MITDS.

Implements FR-028: Track data freshness and completeness.
Provides monitoring of ingestion quality across all data sources.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from ..db import get_db_session
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions measured by the system."""

    COMPLETENESS = "completeness"  # Are all expected fields present?
    CONSISTENCY = "consistency"  # Do values conform to expected formats?
    TIMELINESS = "timeliness"  # Is data within expected freshness window?
    ACCURACY = "accuracy"  # Do values pass validation rules?
    UNIQUENESS = "uniqueness"  # Are there duplicate records?


@dataclass
class QualityMetric:
    """A single quality metric measurement."""

    id: UUID
    source: str
    dimension: QualityDimension
    metric_name: str
    value: float
    threshold: float
    passed: bool
    measured_at: datetime
    details: dict[str, Any] | None = None


@dataclass
class QualityReport:
    """Quality report for a data source or ingestion run."""

    source: str
    measured_at: datetime
    metrics: list[QualityMetric]
    overall_score: float
    passed: bool


class DataQualityTracker:
    """Tracks and reports data quality metrics.

    Measures quality across dimensions:
    - Completeness: Percentage of non-null required fields
    - Consistency: Percentage of values matching expected patterns
    - Timeliness: Data freshness relative to expected update frequency
    - Accuracy: Percentage passing validation rules
    - Uniqueness: Duplicate rate within source
    """

    # Default quality thresholds by dimension
    DEFAULT_THRESHOLDS = {
        QualityDimension.COMPLETENESS: 0.95,
        QualityDimension.CONSISTENCY: 0.98,
        QualityDimension.TIMELINESS: 0.90,
        QualityDimension.ACCURACY: 0.95,
        QualityDimension.UNIQUENESS: 0.99,
    }

    # Expected refresh intervals by source
    FRESHNESS_WINDOWS = {
        "irs990": timedelta(days=7),
        "cra": timedelta(days=7),
        "sec_edgar": timedelta(days=7),
        "canada_corps": timedelta(days=7),
        "opencorporates": timedelta(days=7),
        "meta_ads": timedelta(days=1),
    }

    def __init__(self, source: str):
        """Initialize tracker for a data source.

        Args:
            source: Data source identifier
        """
        self.source = source
        self._metrics: list[QualityMetric] = []

    def measure_completeness(
        self,
        records: list[dict[str, Any]],
        required_fields: list[str],
    ) -> QualityMetric:
        """Measure completeness of required fields.

        Args:
            records: List of records to measure
            required_fields: Fields that must be non-null

        Returns:
            Completeness metric
        """
        if not records:
            return self._create_metric(
                QualityDimension.COMPLETENESS,
                "field_completeness",
                1.0,
                details={"total_records": 0},
            )

        total_checks = len(records) * len(required_fields)
        complete_checks = 0

        for record in records:
            for field in required_fields:
                if record.get(field) is not None:
                    complete_checks += 1

        score = complete_checks / total_checks if total_checks > 0 else 1.0

        metric = self._create_metric(
            QualityDimension.COMPLETENESS,
            "field_completeness",
            score,
            details={
                "total_records": len(records),
                "required_fields": required_fields,
                "complete_checks": complete_checks,
                "total_checks": total_checks,
            },
        )
        self._metrics.append(metric)
        return metric

    def measure_consistency(
        self,
        records: list[dict[str, Any]],
        field_patterns: dict[str, str],
    ) -> QualityMetric:
        """Measure consistency of field values against patterns.

        Args:
            records: List of records to measure
            field_patterns: Dict mapping field names to regex patterns

        Returns:
            Consistency metric
        """
        import re

        if not records or not field_patterns:
            return self._create_metric(
                QualityDimension.CONSISTENCY,
                "pattern_consistency",
                1.0,
                details={"total_records": 0},
            )

        total_checks = 0
        consistent_checks = 0

        for record in records:
            for field, pattern in field_patterns.items():
                value = record.get(field)
                if value is not None:
                    total_checks += 1
                    if re.match(pattern, str(value)):
                        consistent_checks += 1

        score = consistent_checks / total_checks if total_checks > 0 else 1.0

        metric = self._create_metric(
            QualityDimension.CONSISTENCY,
            "pattern_consistency",
            score,
            details={
                "total_records": len(records),
                "field_patterns": field_patterns,
                "consistent_checks": consistent_checks,
                "total_checks": total_checks,
            },
        )
        self._metrics.append(metric)
        return metric

    def measure_timeliness(
        self,
        last_update: datetime | None,
        freshness_window: timedelta | None = None,
    ) -> QualityMetric:
        """Measure data timeliness against expected freshness window.

        Args:
            last_update: Timestamp of last successful update
            freshness_window: Expected update frequency (defaults to source-specific)

        Returns:
            Timeliness metric
        """
        if freshness_window is None:
            freshness_window = self.FRESHNESS_WINDOWS.get(
                self.source, timedelta(days=7)
            )

        now = datetime.utcnow()

        if last_update is None:
            score = 0.0
            age_hours = None
        else:
            age = now - last_update
            # Score decreases linearly from 1.0 at 0 age to 0.0 at 2x freshness window
            max_age = freshness_window * 2
            score = max(0.0, 1.0 - (age.total_seconds() / max_age.total_seconds()))
            age_hours = age.total_seconds() / 3600

        metric = self._create_metric(
            QualityDimension.TIMELINESS,
            "data_freshness",
            score,
            details={
                "last_update": last_update.isoformat() if last_update else None,
                "age_hours": age_hours,
                "freshness_window_hours": freshness_window.total_seconds() / 3600,
            },
        )
        self._metrics.append(metric)
        return metric

    def measure_uniqueness(
        self,
        records: list[dict[str, Any]],
        key_fields: list[str],
    ) -> QualityMetric:
        """Measure uniqueness based on key fields.

        Args:
            records: List of records to measure
            key_fields: Fields that should form a unique key

        Returns:
            Uniqueness metric
        """
        if not records:
            return self._create_metric(
                QualityDimension.UNIQUENESS,
                "record_uniqueness",
                1.0,
                details={"total_records": 0},
            )

        seen_keys: set[tuple] = set()
        duplicates = 0

        for record in records:
            key = tuple(record.get(f) for f in key_fields)
            if key in seen_keys:
                duplicates += 1
            else:
                seen_keys.add(key)

        score = 1.0 - (duplicates / len(records)) if records else 1.0

        metric = self._create_metric(
            QualityDimension.UNIQUENESS,
            "record_uniqueness",
            score,
            details={
                "total_records": len(records),
                "key_fields": key_fields,
                "duplicates_found": duplicates,
                "unique_records": len(seen_keys),
            },
        )
        self._metrics.append(metric)
        return metric

    def measure_accuracy(
        self,
        records: list[dict[str, Any]],
        validation_rules: dict[str, callable],
    ) -> QualityMetric:
        """Measure accuracy using validation rules.

        Args:
            records: List of records to measure
            validation_rules: Dict mapping field names to validation functions

        Returns:
            Accuracy metric
        """
        if not records or not validation_rules:
            return self._create_metric(
                QualityDimension.ACCURACY,
                "validation_accuracy",
                1.0,
                details={"total_records": 0},
            )

        total_checks = 0
        valid_checks = 0

        for record in records:
            for field, validator in validation_rules.items():
                value = record.get(field)
                if value is not None:
                    total_checks += 1
                    try:
                        if validator(value):
                            valid_checks += 1
                    except Exception:
                        pass  # Validation failed

        score = valid_checks / total_checks if total_checks > 0 else 1.0

        metric = self._create_metric(
            QualityDimension.ACCURACY,
            "validation_accuracy",
            score,
            details={
                "total_records": len(records),
                "validated_fields": list(validation_rules.keys()),
                "valid_checks": valid_checks,
                "total_checks": total_checks,
            },
        )
        self._metrics.append(metric)
        return metric

    def _create_metric(
        self,
        dimension: QualityDimension,
        metric_name: str,
        value: float,
        details: dict[str, Any] | None = None,
    ) -> QualityMetric:
        """Create a quality metric with threshold comparison.

        Args:
            dimension: Quality dimension being measured
            metric_name: Name of the specific metric
            value: Measured value (0.0 to 1.0)
            details: Additional measurement details

        Returns:
            Quality metric with pass/fail status
        """
        threshold = self.DEFAULT_THRESHOLDS.get(dimension, 0.95)
        return QualityMetric(
            id=uuid4(),
            source=self.source,
            dimension=dimension,
            metric_name=metric_name,
            value=value,
            threshold=threshold,
            passed=value >= threshold,
            measured_at=datetime.utcnow(),
            details=details,
        )

    def generate_report(self) -> QualityReport:
        """Generate quality report from collected metrics.

        Returns:
            Quality report with overall score
        """
        if not self._metrics:
            return QualityReport(
                source=self.source,
                measured_at=datetime.utcnow(),
                metrics=[],
                overall_score=1.0,
                passed=True,
            )

        overall_score = sum(m.value for m in self._metrics) / len(self._metrics)
        all_passed = all(m.passed for m in self._metrics)

        report = QualityReport(
            source=self.source,
            measured_at=datetime.utcnow(),
            metrics=self._metrics.copy(),
            overall_score=overall_score,
            passed=all_passed,
        )

        logger.info(
            "quality_report_generated",
            source=self.source,
            overall_score=overall_score,
            passed=all_passed,
            metric_count=len(self._metrics),
        )

        return report

    def clear_metrics(self) -> None:
        """Clear collected metrics for new measurement cycle."""
        self._metrics.clear()


async def store_quality_metrics(report: QualityReport) -> None:
    """Store quality metrics in the database.

    Args:
        report: Quality report to store
    """
    from sqlalchemy import text

    async with get_db_session() as db:
        for metric in report.metrics:
            await db.execute(
                text("""
                    INSERT INTO quality_metrics (
                        id, source, dimension, metric_name, value,
                        threshold, passed, measured_at, details
                    ) VALUES (
                        :id, :source, :dimension, :metric_name, :value,
                        :threshold, :passed, :measured_at, :details
                    )
                """),
                {
                    "id": metric.id,
                    "source": metric.source,
                    "dimension": metric.dimension.value,
                    "metric_name": metric.metric_name,
                    "value": metric.value,
                    "threshold": metric.threshold,
                    "passed": metric.passed,
                    "measured_at": metric.measured_at,
                    "details": str(metric.details) if metric.details else None,
                },
            )
        await db.commit()

        logger.info(
            "quality_metrics_stored",
            source=report.source,
            metric_count=len(report.metrics),
        )


async def get_latest_quality_report(source: str) -> QualityReport | None:
    """Get the latest quality report for a source.

    Args:
        source: Data source identifier

    Returns:
        Latest quality report or None if no metrics exist
    """
    from sqlalchemy import text

    async with get_db_session() as db:
        result = await db.execute(
            text("""
                SELECT dimension, metric_name, value, threshold, passed, measured_at, details
                FROM quality_metrics
                WHERE source = :source
                AND measured_at = (
                    SELECT MAX(measured_at) FROM quality_metrics WHERE source = :source
                )
            """),
            {"source": source},
        )
        rows = result.fetchall()

        if not rows:
            return None

        metrics = [
            QualityMetric(
                id=uuid4(),
                source=source,
                dimension=QualityDimension(row.dimension),
                metric_name=row.metric_name,
                value=row.value,
                threshold=row.threshold,
                passed=row.passed,
                measured_at=row.measured_at,
                details=eval(row.details) if row.details else None,
            )
            for row in rows
        ]

        overall_score = sum(m.value for m in metrics) / len(metrics)
        all_passed = all(m.passed for m in metrics)

        return QualityReport(
            source=source,
            measured_at=metrics[0].measured_at if metrics else datetime.utcnow(),
            metrics=metrics,
            overall_score=overall_score,
            passed=all_passed,
        )
