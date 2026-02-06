"""Integration tests for political funding features.

Contains the BaseVerificationTest framework and integration tests
for each data source in the political ad funding tracker.

Verification tests compare system output against manually verified
reference data stored in backend/tests/fixtures/verification/.
"""

import json
from pathlib import Path
from typing import Any

import pytest


FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "verification"


class BaseVerificationTest:
    """Base class for real-data verification tests.

    Provides infrastructure for loading reference JSON fixtures,
    comparing system output against expected data, and reporting
    accuracy metrics.

    Subclasses should:
    1. Set `fixture_file` to the name of the reference JSON fixture
    2. Implement `get_system_output()` to fetch actual system results
    3. Implement `compare_results()` for source-specific comparison logic
    4. Optionally override `accuracy_threshold` (default: 0.95)

    Usage in tests:
        class TestElectionsVerification(BaseVerificationTest):
            fixture_file = "elections_canada_reference.json"
            accuracy_threshold = 0.95

            async def get_system_output(self):
                return await run_ingestion_and_query()

            def compare_results(self, expected, actual):
                return self._compare_entities(expected, actual)
    """

    fixture_file: str = ""
    accuracy_threshold: float = 0.95

    def load_reference_data(self) -> dict[str, Any]:
        """Load reference data from fixture file.

        Returns:
            Dictionary containing manually verified reference data.

        Raises:
            FileNotFoundError: If fixture file doesn't exist.
        """
        fixture_path = FIXTURES_DIR / self.fixture_file
        if not fixture_path.exists():
            pytest.skip(f"Reference fixture not found: {fixture_path}")

        with open(fixture_path) as f:
            return json.load(f)

    def calculate_accuracy(
        self,
        expected_items: list[dict[str, Any]],
        actual_items: list[dict[str, Any]],
        match_key: str = "name",
    ) -> dict[str, Any]:
        """Calculate accuracy metrics comparing expected vs actual items.

        Args:
            expected_items: Manually verified expected data
            actual_items: System-produced data
            match_key: Key field to use for matching items

        Returns:
            Dict with precision, recall, f1, matched, missing, extra counts
        """
        expected_keys = {item[match_key] for item in expected_items if match_key in item}
        actual_keys = {item[match_key] for item in actual_items if match_key in item}

        matched = expected_keys & actual_keys
        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys

        precision = len(matched) / len(actual_keys) if actual_keys else 0.0
        recall = len(matched) / len(expected_keys) if expected_keys else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "matched_count": len(matched),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "matched": list(matched),
            "missing": list(missing),
            "extra": list(extra),
            "total_expected": len(expected_keys),
            "total_actual": len(actual_keys),
        }

    def compare_entity_fields(
        self,
        expected: dict[str, Any],
        actual: dict[str, Any],
        fields: list[str],
    ) -> dict[str, Any]:
        """Compare specific fields between expected and actual entities.

        Args:
            expected: Expected entity data
            actual: Actual entity data
            fields: List of field names to compare

        Returns:
            Dict with field-level comparison results
        """
        results: dict[str, Any] = {"matches": [], "mismatches": []}

        for field in fields:
            expected_val = expected.get(field)
            actual_val = actual.get(field)

            if expected_val == actual_val:
                results["matches"].append(field)
            else:
                results["mismatches"].append(
                    {
                        "field": field,
                        "expected": expected_val,
                        "actual": actual_val,
                    }
                )

        total = len(fields)
        matched = len(results["matches"])
        results["accuracy"] = matched / total if total > 0 else 0.0

        return results

    def assert_accuracy(
        self,
        metrics: dict[str, Any],
        threshold: float | None = None,
        metric_name: str = "recall",
    ) -> None:
        """Assert that accuracy meets the threshold.

        Args:
            metrics: Accuracy metrics from calculate_accuracy()
            threshold: Override default accuracy_threshold
            metric_name: Which metric to check (precision, recall, f1)
        """
        threshold = threshold or self.accuracy_threshold
        actual = metrics.get(metric_name, 0.0)

        assert actual >= threshold, (
            f"{metric_name} {actual:.2%} below threshold {threshold:.2%}. "
            f"Matched: {metrics['matched_count']}/{metrics['total_expected']}. "
            f"Missing: {metrics['missing']}. "
            f"Extra: {metrics['extra']}."
        )

    def report_accuracy(
        self,
        metrics: dict[str, Any],
        source_name: str,
        verbose: bool = False,
    ) -> str:
        """Generate a human-readable accuracy report.

        Args:
            metrics: Accuracy metrics from calculate_accuracy()
            source_name: Name of the data source being verified
            verbose: If True, include detailed match/mismatch info

        Returns:
            Formatted report string
        """
        lines = [
            f"=== Verification Report: {source_name} ===",
            f"Precision: {metrics['precision']:.2%}",
            f"Recall:    {metrics['recall']:.2%}",
            f"F1 Score:  {metrics['f1']:.2%}",
            f"Matched:   {metrics['matched_count']}/{metrics['total_expected']}",
            f"Missing:   {metrics['missing_count']}",
            f"Extra:     {metrics['extra_count']}",
        ]

        if verbose and metrics.get("missing"):
            lines.append(f"\nMissing items:")
            for item in metrics["missing"]:
                lines.append(f"  - {item}")

        if verbose and metrics.get("extra"):
            lines.append(f"\nExtra items (not in reference):")
            for item in metrics["extra"]:
                lines.append(f"  - {item}")

        passed = metrics["recall"] >= self.accuracy_threshold
        status = "PASS" if passed else "FAIL"
        lines.append(f"\nResult: {status} (threshold: {self.accuracy_threshold:.0%})")

        return "\n".join(lines)
