"""Base models and enums for MITDS entities."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EntityType(str, Enum):
    """Types of entities in the MITDS graph."""

    PERSON = "PERSON"
    ORGANIZATION = "ORGANIZATION"
    OUTLET = "OUTLET"
    DOMAIN = "DOMAIN"
    PLATFORM_ACCOUNT = "PLATFORM_ACCOUNT"
    SPONSOR = "SPONSOR"
    VENDOR = "VENDOR"


class SourceRef(BaseModel):
    """Reference to an evidence source."""

    source: str
    identifier: str
    evidence_id: UUID | None = None

    model_config = ConfigDict(frozen=True)


class EntityBase(BaseModel):
    """Base class for all MITDS entities.

    All entities share common provenance fields for tracking
    origin, confidence, and audit trail.
    """

    id: UUID = Field(default_factory=uuid4, description="Internal stable identifier")
    entity_type: EntityType = Field(..., description="Entity type discriminator")
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="First ingestion timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Last update timestamp"
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Resolution confidence (0.0-1.0)",
    )
    source_ids: list[SourceRef] = Field(
        default_factory=list, description="Evidence references"
    )

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat(),
        },
    )

    def add_source(self, source: str, identifier: str, evidence_id: UUID | None = None) -> None:
        """Add a source reference to this entity."""
        ref = SourceRef(source=source, identifier=identifier, evidence_id=evidence_id)
        if ref not in self.source_ids:
            self.source_ids.append(ref)

    def merge_sources(self, other: "EntityBase") -> None:
        """Merge source references from another entity."""
        for ref in other.source_ids:
            if ref not in self.source_ids:
                self.source_ids.append(ref)


class EntitySummary(BaseModel):
    """Lightweight entity representation for API responses."""

    id: UUID
    entity_type: EntityType
    name: str

    model_config = ConfigDict(use_enum_values=True)


class Address(BaseModel):
    """Physical address for organizations and persons."""

    street: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str = "US"

    def __str__(self) -> str:
        """Format address as a single line."""
        parts = [p for p in [self.street, self.city, self.state, self.postal_code] if p]
        if parts:
            return ", ".join(parts)
        return ""


class RoleAssignment(BaseModel):
    """Time-bounded role assignment for a person."""

    organization_id: UUID
    title: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    compensation: float | None = None
    hours_per_week: float | None = None

    @property
    def is_current(self) -> bool:
        """Check if this role is currently active."""
        return self.end_date is None or self.end_date > datetime.utcnow()
