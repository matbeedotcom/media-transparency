"""Event model for MITDS.

Time-stamped occurrences for audit and temporal analysis.
Events are immutable once created.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EventType(str, Enum):
    """Types of events tracked in MITDS."""

    # Funding events
    GRANT_RECEIVED = "grant_received"
    GRANT_MADE = "grant_made"
    SPONSORSHIP_STARTED = "sponsorship_started"
    SPONSORSHIP_ENDED = "sponsorship_ended"

    # Organizational events
    BOARD_APPOINTMENT = "board_appointment"
    BOARD_DEPARTURE = "board_departure"
    INCORPORATION = "incorporation"
    DISSOLUTION = "dissolution"
    MERGER = "merger"
    NAME_CHANGE = "name_change"

    # Publication events
    ARTICLE_PUBLISHED = "article_published"
    AD_LAUNCHED = "ad_launched"
    AD_ENDED = "ad_ended"

    # Infrastructure events
    HOSTING_CHANGED = "hosting_changed"
    DOMAIN_REGISTERED = "domain_registered"

    # System events
    ENTITY_CREATED = "entity_created"
    ENTITY_UPDATED = "entity_updated"
    ENTITY_MERGED = "entity_merged"
    RELATIONSHIP_CREATED = "relationship_created"
    RELATIONSHIP_ENDED = "relationship_ended"


class Event(BaseModel):
    """An immutable event in the MITDS timeline.

    Events represent discrete occurrences that can be queried
    for temporal analysis and audit trails.
    """

    id: UUID = Field(default_factory=uuid4)
    event_type: EventType

    # When it happened
    occurred_at: datetime = Field(..., description="When the event occurred")

    # Involved entities
    entity_ids: list[UUID] = Field(
        default_factory=list, description="Entities involved in this event"
    )
    relationship_id: UUID | None = Field(
        default=None, description="Related relationship if applicable"
    )

    # Details
    description: str | None = Field(
        default=None, description="Human-readable description"
    )
    properties: dict[str, Any] = Field(
        default_factory=dict, description="Event-specific properties"
    )

    # Provenance
    evidence_ref: UUID = Field(..., description="Evidence supporting this event")
    detected_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the system detected this event",
    )

    model_config = ConfigDict(
        use_enum_values=True,
        frozen=True,  # Events are immutable
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat(),
        },
    )


class EventSummary(BaseModel):
    """Lightweight event representation for API responses."""

    id: UUID
    event_type: EventType
    occurred_at: datetime
    description: str | None

    model_config = ConfigDict(use_enum_values=True)

    @classmethod
    def from_event(cls, event: Event) -> "EventSummary":
        """Create a summary from a full Event object."""
        return cls(
            id=event.id,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            description=event.description,
        )
