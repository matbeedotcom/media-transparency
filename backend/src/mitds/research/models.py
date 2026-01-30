"""Pydantic models for the research system.

This module defines the domain models for research sessions,
leads, and related types used by the "follow the leads" feature.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EntryPointType(str, Enum):
    """Types of entry points for research sessions."""

    META_ADS = "meta_ads"
    COMPANY = "company"
    EIN = "ein"
    BN = "bn"
    NONPROFIT = "nonprofit"
    ENTITY_ID = "entity_id"


class SessionStatus(str, Enum):
    """Status of a research session."""

    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class LeadType(str, Enum):
    """Types of leads that can be followed."""

    OWNERSHIP = "ownership"
    FUNDING = "funding"
    SPONSORSHIP = "sponsorship"
    BOARD_INTERLOCK = "board_interlock"
    CROSS_BORDER = "cross_border"
    INFRASTRUCTURE = "infrastructure"


class IdentifierType(str, Enum):
    """Types of identifiers for lead targets."""

    NAME = "name"
    EIN = "ein"
    BN = "bn"
    CIK = "cik"
    SEDAR_PROFILE = "sedar_profile"
    META_PAGE_ID = "meta_page_id"
    OPENCORP_ID = "opencorp_id"
    LITTLESIS_ID = "littlesis_id"
    ENTITY_ID = "entity_id"


class LeadStatus(str, Enum):
    """Status of a lead in the queue."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


class ResearchSessionConfig(BaseModel):
    """Configuration for a research session."""

    # Depth control
    max_depth: int = Field(default=3, ge=1, le=10, description="Maximum hops from entry point")
    max_entities: int = Field(default=500, ge=1, description="Maximum entities to discover")
    max_relationships: int = Field(default=2000, ge=1, description="Maximum relationships")

    # Lead filtering
    enabled_lead_types: list[LeadType] = Field(
        default_factory=lambda: list(LeadType),
        description="Which lead types to follow",
    )
    min_confidence: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Minimum confidence to pursue a lead"
    )
    min_funding_amount: float | None = Field(
        default=None, description="Minimum funding amount to follow (USD)"
    )

    # Geographic scope
    jurisdictions: list[str] = Field(
        default_factory=lambda: ["US", "CA"],
        description="Jurisdictions to include (country codes)",
    )

    # Source filtering
    enabled_sources: list[str] = Field(
        default_factory=list,
        description="Which ingestors to use (empty = all)",
    )

    # Priority weighting
    ownership_priority_boost: int = Field(default=0, ge=-2, le=2)
    funding_priority_boost: int = Field(default=0, ge=-2, le=2)

    # Rate limiting
    max_api_calls_per_minute: int = Field(default=30, ge=1)

    # Auto-pause conditions
    pause_on_high_value_entity: bool = Field(default=False)
    high_value_threshold: float = Field(default=1000000.0)

    model_config = ConfigDict(use_enum_values=True)


class Lead(BaseModel):
    """A lead to investigate."""

    id: UUID = Field(default_factory=uuid4)
    lead_type: LeadType
    target_identifier: str = Field(..., description="Name, ID, or query to investigate")
    target_identifier_type: IdentifierType
    priority: int = Field(default=3, ge=1, le=5, description="1=highest, 5=lowest")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    context: dict[str, Any] = Field(
        default_factory=dict, description="Additional context (relationship type, amount, etc.)"
    )
    source_relationship_type: str | None = Field(
        default=None, description="Type of relationship that generated this lead"
    )

    model_config = ConfigDict(use_enum_values=True)


class QueuedLead(Lead):
    """A lead in the queue with session context."""

    session_id: UUID
    source_entity_id: UUID | None = Field(
        default=None, description="Entity that generated this lead"
    )
    depth: int = Field(default=0, ge=0, description="Hops from entry point")
    status: LeadStatus = LeadStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: datetime | None = None
    result: dict[str, Any] | None = Field(
        default=None, description="Processing result summary"
    )
    skip_reason: str | None = None


class SessionStats(BaseModel):
    """Statistics for a research session."""

    # Counts
    total_entities: int = 0
    total_relationships: int = 0
    entities_by_type: dict[str, int] = Field(default_factory=dict)
    relationships_by_type: dict[str, int] = Field(default_factory=dict)
    entities_by_depth: dict[int, int] = Field(default_factory=dict)

    # Lead queue
    leads_total: int = 0
    leads_pending: int = 0
    leads_completed: int = 0
    leads_skipped: int = 0
    leads_failed: int = 0

    # Sources
    sources_queried: dict[str, int] = Field(default_factory=dict)
    api_calls_made: int = 0

    # Timing
    processing_time_seconds: float = 0.0
    estimated_remaining_seconds: float | None = None

    # Notable findings
    high_value_entities: list[UUID] = Field(default_factory=list)
    cross_border_connections: int = 0
    board_interlocks_found: int = 0


class ResearchSession(BaseModel):
    """A research investigation session."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, description="User-provided name")
    description: str | None = None

    entry_point_type: EntryPointType
    entry_point_value: str
    entry_point_entity_id: UUID | None = Field(
        default=None, description="Resolved entry point entity"
    )

    status: SessionStatus = SessionStatus.INITIALIZING
    config: ResearchSessionConfig = Field(default_factory=ResearchSessionConfig)
    stats: SessionStats = Field(default_factory=SessionStats)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    paused_at: datetime | None = None

    created_by: str | None = None

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat() if v else None,
        },
    )


class LeadResult(BaseModel):
    """Result of processing a lead."""

    lead_id: UUID
    success: bool
    entity_id: UUID | None = Field(default=None, description="Discovered/resolved entity")
    is_new_entity: bool = False
    relationships_created: int = 0
    new_leads_generated: int = 0
    error_message: str | None = None


class SingleIngestionResult(BaseModel):
    """Result of ingesting a single entity."""

    entity_id: UUID | None = None
    entity_type: str | None = None
    entity_name: str | None = None
    is_new: bool = False
    relationships_created: list[UUID] = Field(default_factory=list)
    error: str | None = None


# =============================================================================
# API Request/Response Models
# =============================================================================


class CreateSessionRequest(BaseModel):
    """Request to create a new research session."""

    name: str = Field(..., min_length=1)
    description: str | None = None
    entry_point_type: EntryPointType
    entry_point_value: str
    config: ResearchSessionConfig | None = None


class SessionResponse(BaseModel):
    """Response with session details."""

    id: UUID
    name: str
    description: str | None
    entry_point_type: EntryPointType
    entry_point_value: str
    status: SessionStatus
    stats: SessionStats
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = ConfigDict(use_enum_values=True)


class LeadSummary(BaseModel):
    """Summary of a lead for API responses."""

    id: UUID
    lead_type: LeadType
    target_identifier: str
    target_identifier_type: IdentifierType
    priority: int
    confidence: float
    depth: int
    status: LeadStatus
    created_at: datetime
    processed_at: datetime | None

    model_config = ConfigDict(use_enum_values=True)


class EntitySummary(BaseModel):
    """Summary of an entity discovered in a session."""

    id: UUID
    name: str
    entity_type: str
    depth: int
    relevance_score: float
    discovered_via: str | None = None


class GraphNode(BaseModel):
    """Node in a graph visualization."""

    id: str
    label: str
    type: str
    properties: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    """Edge in a graph visualization."""

    source: str
    target: str
    type: str
    properties: dict[str, Any] = Field(default_factory=dict)


class SessionGraph(BaseModel):
    """Graph representation of a session's discoveries."""

    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)


class QueueStats(BaseModel):
    """Statistics about the lead queue."""

    total: int = 0
    pending: int = 0
    in_progress: int = 0
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    by_type: dict[str, int] = Field(default_factory=dict)
    by_priority: dict[int, int] = Field(default_factory=dict)
    average_confidence: float = 0.0
