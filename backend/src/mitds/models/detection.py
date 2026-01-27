"""Detection finding and job models for MITDS.

Provides Pydantic models for:
- Detection findings (persisted analysis results)
- Background jobs (async analysis tracking)
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DetectionFinding(BaseModel):
    """A persisted detection analysis result."""

    id: UUID = Field(default_factory=uuid4)
    finding_type: str = Field(..., description="Type: temporal, funding, infrastructure, composite")
    entity_ids: list[UUID] = Field(..., description="Analyzed entity IDs")
    score: float = Field(..., ge=0.0, le=1.0, description="Overall score")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence level")
    flagged: bool = Field(default=False, description="Whether the finding was flagged")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Full result details")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str | None = Field(default=None, description="User ID or 'system'")

    model_config = ConfigDict(
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat(),
        },
    )


class DetectionFindingCreate(BaseModel):
    """Input model for creating a detection finding."""

    finding_type: str
    entity_ids: list[UUID]
    score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    flagged: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None


class Job(BaseModel):
    """A background job for async analysis."""

    id: UUID = Field(default_factory=uuid4)
    job_type: str = Field(..., description="Type: temporal_analysis, funding_analysis, etc.")
    status: str = Field(default="pending", description="pending, running, completed, failed")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat(),
        },
    )
