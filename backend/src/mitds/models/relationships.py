"""Relationship model for MITDS.

All relationships are temporal and evidence-linked, supporting
point-in-time queries and full provenance tracking.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from .evidence import EvidenceRef


class RelationType(str, Enum):
    """Types of relationships between entities."""

    FUNDED_BY = "FUNDED_BY"  # org/outlet <- sponsor/org
    DIRECTOR_OF = "DIRECTOR_OF"  # person -> org
    EMPLOYED_BY = "EMPLOYED_BY"  # person -> org/outlet
    SPONSORED_BY = "SPONSORED_BY"  # ad/content <- sponsor
    OWNS = "OWNS"  # org -> org/outlet
    CITED = "CITED"  # content -> content
    AMPLIFIED = "AMPLIFIED"  # account -> content
    SHARED_INFRA = "SHARED_INFRA"  # outlet <-> outlet (via vendor)


class Relationship(BaseModel):
    """A relationship between two entities in the MITDS graph.

    All relationships support:
    - Temporal bounds (valid_from, valid_to)
    - Confidence scoring
    - Evidence linking
    - Type-specific properties
    """

    id: UUID = Field(default_factory=uuid4)
    rel_type: RelationType

    source_entity_id: UUID = Field(..., description="ID of the source entity")
    target_entity_id: UUID = Field(..., description="ID of the target entity")

    # Temporal bounds
    valid_from: datetime | None = Field(
        default=None, description="When relationship started"
    )
    valid_to: datetime | None = Field(
        default=None, description="When relationship ended (None = current)"
    )

    # Evidence
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence in this relationship",
    )
    evidence_refs: list[UUID] = Field(
        default_factory=list, description="Evidence IDs supporting this relationship"
    )

    # Type-specific metadata
    properties: dict[str, Any] = Field(
        default_factory=dict, description="Relationship-specific properties"
    )

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat() if v else None,
        },
    )

    @property
    def is_current(self) -> bool:
        """Check if this relationship is currently active."""
        return self.valid_to is None or self.valid_to > datetime.utcnow()

    def is_valid_at(self, point_in_time: datetime) -> bool:
        """Check if this relationship was valid at a specific point in time."""
        if self.valid_from and point_in_time < self.valid_from:
            return False
        if self.valid_to and point_in_time > self.valid_to:
            return False
        return True

    def end_relationship(self, end_date: datetime | None = None) -> None:
        """Mark this relationship as ended."""
        self.valid_to = end_date or datetime.utcnow()


# =========================
# Relationship Property Schemas
# =========================


class FundedByProperties(BaseModel):
    """Properties specific to FUNDED_BY relationships."""

    amount: float | None = None  # USD (normalized)
    amount_currency: str = "USD"  # Original currency
    fiscal_year: int | None = None
    grant_purpose: str | None = None  # From Schedule I


class DirectorOfProperties(BaseModel):
    """Properties specific to DIRECTOR_OF relationships."""

    title: str | None = None
    compensation: float | None = None  # Annual, USD
    hours_per_week: float | None = None


class EmployedByProperties(BaseModel):
    """Properties specific to EMPLOYED_BY relationships."""

    title: str | None = None
    department: str | None = None
    compensation: float | None = None


class SharedInfraProperties(BaseModel):
    """Properties specific to SHARED_INFRA relationships."""

    shared_vendor_id: UUID
    service_type: str  # VendorType value


class OwnsProperties(BaseModel):
    """Properties specific to OWNS relationships."""

    ownership_percentage: float | None = None
    ownership_type: str | None = None  # "direct", "indirect", "beneficial"


class RoleProperties(BaseModel):
    """Properties for role-based relationships (DIRECTOR_OF, EMPLOYED_BY)."""

    title: str | None = None
    compensation: float | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class RelationshipCreate(BaseModel):
    """Schema for creating a new relationship."""

    rel_type: RelationType
    source_entity_id: UUID
    target_entity_id: UUID
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    properties: dict[str, Any] = Field(default_factory=dict)
    evidence_refs: list[UUID] = Field(default_factory=list)