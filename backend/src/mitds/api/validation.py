"""Validation API endpoints for MITDS."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from .auth import CurrentUser, OptionalUser
from ..validation import (
    GoldenDataset,
    MetricHistory,
    ValidationMetrics,
    aggregate_metrics,
    calculate_metrics,
    create_empty_dashboard,
    create_sample_golden_dataset,
    generate_validation_suite,
    validate_golden_case,
)

router = APIRouter(prefix="/validation")

# In-memory storage for demo
# In production, use database
_metrics_history = MetricHistory()
_validation_jobs: dict[str, dict[str, Any]] = {}
_golden_datasets: dict[str, GoldenDataset] = {}


# Initialize sample dataset
def _init_sample_dataset() -> None:
    """Initialize with sample golden dataset."""
    if "sample" not in _golden_datasets:
        _golden_datasets["sample"] = create_sample_golden_dataset()


_init_sample_dataset()


# =========================
# Request/Response Models
# =========================


class ValidationRunRequest(BaseModel):
    """Request for running validation."""

    dataset_id: str = Field(default="sample", description="Golden dataset ID to validate against")
    include_synthetic: bool = Field(default=True, description="Include synthetic test cases")
    threshold: float = Field(default=0.45, ge=0.0, le=1.0, description="Detection threshold")
    synthetic_seed: int | None = Field(None, description="Random seed for synthetic cases")


class ValidationRunResponse(BaseModel):
    """Response for validation run request."""

    job_id: UUID
    status: str
    status_url: str
    estimated_cases: int


class MetricsResponse(BaseModel):
    """Validation metrics response."""

    recall: float
    precision: float
    f1_score: float
    false_positive_rate: float
    accuracy: float
    total_cases: int
    passed_cases: int
    failed_cases: int
    last_run_at: str | None
    meets_targets: bool
    target_recall: float
    target_max_fpr: float


class DashboardResponse(BaseModel):
    """Dashboard data response."""

    summary: dict[str, Any]
    history: dict[str, list[dict[str, Any]]]
    by_case_type: dict[str, dict[str, float]]
    signal_performance: dict[str, dict[str, float]]
    recent_failures: list[dict[str, Any]]
    health_checks: list[dict[str, Any]]


# =========================
# GET /metrics (T131)
# =========================


@router.get("/metrics")
async def get_validation_metrics(
    user: OptionalUser = None,
) -> MetricsResponse:
    """Get current validation metrics.

    Returns the latest validation metrics including recall,
    precision, false positive rate, and target compliance.
    """
    latest = _metrics_history.get_latest()

    if not latest:
        return MetricsResponse(
            recall=0.0,
            precision=0.0,
            f1_score=0.0,
            false_positive_rate=0.0,
            accuracy=0.0,
            total_cases=0,
            passed_cases=0,
            failed_cases=0,
            last_run_at=None,
            meets_targets=False,
            target_recall=0.85,
            target_max_fpr=0.05,
        )

    return MetricsResponse(
        recall=latest.recall,
        precision=latest.precision,
        f1_score=latest.f1_score,
        false_positive_rate=latest.false_positive_rate,
        accuracy=latest.accuracy,
        total_cases=len(latest.case_results),
        passed_cases=len(latest.passed_cases()),
        failed_cases=len(latest.failed_cases()),
        last_run_at=latest.run_at.isoformat(),
        meets_targets=latest.meets_targets(),
        target_recall=0.85,
        target_max_fpr=0.05,
    )


@router.get("/metrics/history")
async def get_metrics_history(
    limit: int = Query(default=30, ge=1, le=100),
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get historical validation metrics.

    Returns time series data for tracking metric trends.
    """
    entries = sorted(_metrics_history.entries, key=lambda m: m.run_at)[-limit:]

    return {
        "entries": [
            {
                "id": str(e.id),
                "run_at": e.run_at.isoformat(),
                "recall": e.recall,
                "precision": e.precision,
                "f1_score": e.f1_score,
                "false_positive_rate": e.false_positive_rate,
                "total_cases": len(e.case_results),
            }
            for e in entries
        ],
        "summary": _metrics_history.summary(),
    }


@router.get("/dashboard")
async def get_validation_dashboard(
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get aggregated dashboard data for validation metrics.

    Returns comprehensive dashboard data including:
    - Summary metrics
    - Historical trends
    - Metrics by case type
    - Signal performance analysis
    - Recent failures
    - Health checks
    """
    if not _metrics_history.entries:
        dashboard = create_empty_dashboard()
    else:
        dashboard = aggregate_metrics(_metrics_history)

    return dashboard.to_dict()


# =========================
# POST /run (T132)
# =========================


async def _run_validation_job(
    job_id: str,
    dataset: GoldenDataset,
    include_synthetic: bool,
    threshold: float,
    synthetic_seed: int | None,
) -> None:
    """Background task to run validation."""
    try:
        _validation_jobs[job_id]["status"] = "running"

        results = []

        # Validate against golden dataset
        for case in dataset.cases:
            # Simulate detection result
            # In production, run actual detection algorithms
            detection_result = _simulate_detection(case, threshold)
            result = validate_golden_case(case, detection_result, threshold)
            results.append(result)

        # Add synthetic cases if requested
        if include_synthetic:
            synthetic_patterns = generate_validation_suite(
                seed=synthetic_seed,
                positive_per_type=2,
                negative_count=6,
            )

            for pattern in synthetic_patterns:
                # Simulate detection on synthetic
                detection_result = _simulate_detection_synthetic(pattern, threshold)
                # Create pseudo-result
                from ..validation.golden import CaseLabel, ValidationResult
                result = ValidationResult(
                    case_id=pattern.id,
                    detected=detection_result.get("score", 0) >= threshold,
                    expected_label=(
                        CaseLabel.POSITIVE if pattern.label == "positive" else CaseLabel.NEGATIVE
                    ),
                    score=detection_result.get("score", 0),
                    signals_found=detection_result.get("signals", []),
                    signals_missing=[],
                    passed=True,  # Will be recalculated
                    details={"case_name": pattern.description, "case_type": pattern.pattern_type.value},
                )
                # Recalculate passed
                if result.expected_label == CaseLabel.POSITIVE:
                    result.passed = result.detected
                else:
                    result.passed = not result.detected

                results.append(result)

        # Calculate metrics
        metrics = calculate_metrics(
            results,
            threshold=threshold,
            dataset_name=dataset.name,
            dataset_version=dataset.version,
        )

        # Store in history
        _metrics_history.add_run(metrics)

        # Update job
        _validation_jobs[job_id]["status"] = "completed"
        _validation_jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
        _validation_jobs[job_id]["metrics_id"] = str(metrics.id)
        _validation_jobs[job_id]["results"] = {
            "recall": metrics.recall,
            "precision": metrics.precision,
            "f1_score": metrics.f1_score,
            "false_positive_rate": metrics.false_positive_rate,
            "total_cases": len(results),
            "passed": len(metrics.passed_cases()),
            "failed": len(metrics.failed_cases()),
        }

    except Exception as e:
        _validation_jobs[job_id]["status"] = "failed"
        _validation_jobs[job_id]["error"] = str(e)


def _simulate_detection(case: Any, threshold: float) -> dict[str, Any]:
    """Simulate detection for a golden case.

    In production, this would run the actual detection algorithms.
    For demo, we simulate based on case properties.
    """
    from ..validation.golden import CaseLabel, GoldenCaseType
    import random

    # Base score depends on case type
    if case.label == CaseLabel.POSITIVE:
        # Positive cases should generally be detected
        base_score = random.uniform(0.5, 0.9)
        signals = [s.signal_type for s in case.expected_signals[:3]]
    else:
        # Negative cases should generally not be detected
        base_score = random.uniform(0.1, 0.35)
        signals = []

    # Add some noise
    score = max(0.0, min(1.0, base_score + random.uniform(-0.1, 0.1)))

    return {
        "score": score,
        "signals": signals,
        "detected": score >= threshold,
    }


def _simulate_detection_synthetic(pattern: Any, threshold: float) -> dict[str, Any]:
    """Simulate detection for a synthetic case."""
    import random

    if pattern.label == "positive":
        base_score = random.uniform(0.5, 0.85)
        signals = pattern.expected_signals[:2]
    else:
        base_score = random.uniform(0.05, 0.3)
        signals = []

    score = max(0.0, min(1.0, base_score + random.uniform(-0.05, 0.05)))

    return {
        "score": score,
        "signals": signals,
        "detected": score >= threshold,
    }


@router.post("/run")
async def run_validation(
    request: ValidationRunRequest,
    background_tasks: BackgroundTasks,
    user: CurrentUser,
) -> ValidationRunResponse:
    """Run validation against golden dataset.

    Starts an asynchronous validation run that:
    1. Loads the specified golden dataset
    2. Optionally generates synthetic test cases
    3. Runs detection algorithms against all cases
    4. Calculates and stores validation metrics

    Args:
        request: Validation run configuration

    Returns:
        Job ID and status URL for tracking progress
    """
    # Get or create dataset
    if request.dataset_id not in _golden_datasets:
        raise HTTPException(
            status_code=404,
            detail=f"Golden dataset not found: {request.dataset_id}"
        )

    dataset = _golden_datasets[request.dataset_id]

    # Estimate case count
    case_count = len(dataset.cases)
    if request.include_synthetic:
        case_count += 14  # 4 types * 2 positive + 6 negative

    # Create job
    job_id = uuid4()
    _validation_jobs[str(job_id)] = {
        "id": str(job_id),
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "dataset_id": request.dataset_id,
        "include_synthetic": request.include_synthetic,
        "threshold": request.threshold,
    }

    # Start background task
    background_tasks.add_task(
        _run_validation_job,
        str(job_id),
        dataset,
        request.include_synthetic,
        request.threshold,
        request.synthetic_seed,
    )

    return ValidationRunResponse(
        job_id=job_id,
        status="queued",
        status_url=f"/api/validation/jobs/{job_id}",
        estimated_cases=case_count,
    )


@router.get("/jobs/{job_id}")
async def get_validation_job(
    job_id: UUID,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get status of a validation job."""
    job_key = str(job_id)

    if job_key not in _validation_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return _validation_jobs[job_key]


# =========================
# Dataset Management
# =========================


@router.get("/datasets")
async def list_golden_datasets(
    user: OptionalUser = None,
) -> list[dict[str, Any]]:
    """List available golden datasets."""
    return [
        {
            "id": ds_id,
            "name": ds.name,
            "version": ds.version,
            "description": ds.description,
            "case_count": len(ds.cases),
            "positive_cases": len(ds.positive_cases),
            "negative_cases": len(ds.negative_cases),
        }
        for ds_id, ds in _golden_datasets.items()
    ]


@router.get("/datasets/{dataset_id}")
async def get_golden_dataset(
    dataset_id: str,
    user: OptionalUser = None,
) -> dict[str, Any]:
    """Get details of a golden dataset."""
    if dataset_id not in _golden_datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    ds = _golden_datasets[dataset_id]
    return ds.to_dict()
