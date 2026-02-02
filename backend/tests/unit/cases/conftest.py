"""Pytest fixtures for Case Intake unit tests."""

import pytest
from datetime import datetime
from uuid import uuid4

from mitds.cases.models import (
    CaseConfig,
    CaseStats,
    MatchSignals,
)


@pytest.fixture
def sample_case_config() -> CaseConfig:
    """Create a sample case configuration."""
    return CaseConfig(
        max_depth=2,
        max_entities=100,
        max_relationships=500,
        jurisdictions=["US", "CA"],
        min_confidence=0.7,
        auto_merge_threshold=0.9,
        review_threshold=0.7,
        enable_llm_extraction=False,
    )


@pytest.fixture
def sample_match_signals() -> MatchSignals:
    """Create sample match signals."""
    return MatchSignals(
        name_similarity=0.92,
        identifier_match={"type": "ein", "matched": True},
        jurisdiction_match=True,
        address_overlap={"city": True, "postal_fsa": False},
        shared_directors=["John Smith"],
    )
