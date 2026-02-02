"""Pydantic models for the Case Intake System.

This module defines the domain models for cases, evidence, entity matches,
and case reports used by the autonomous research framework.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


# =============================================================================
# Enumerations (T013)
# =============================================================================


class EntryPointType(str, Enum):
    """Types of entry points for case creation."""

    META_AD = "meta_ad"
    CORPORATION = "corporation"
    URL = "url"
    TEXT = "text"


class CaseStatus(str, Enum):
    """Status of a case investigation."""

    INITIALIZING = "initializing"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class EvidenceType(str, Enum):
    """Types of evidence records."""

    ENTRY_POINT = "entry_point"  # Original user input
    URL_FETCH = "url_fetch"  # Fetched webpage
    API_RESPONSE = "api_response"  # Meta Ad Library, etc.
    UPLOADED = "uploaded"  # User-uploaded file


class ExtractionMethod(str, Enum):
    """Methods used for entity extraction."""

    DETERMINISTIC = "deterministic"
    LLM = "llm"
    HYBRID = "hybrid"


class MatchStatus(str, Enum):
    """Status of an entity match in the review queue."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    DEFERRED = "deferred"


# =============================================================================
# Configuration Models
# =============================================================================


class CaseConfig(BaseModel):
    """Configuration for case processing."""

    # Depth control (inherited from research session)
    max_depth: int = Field(default=2, ge=1, le=5, description="Maximum hops from entry point")
    max_entities: int = Field(default=100, ge=1, description="Maximum entities to discover")
    max_relationships: int = Field(default=500, ge=1, description="Maximum relationships")

    # Geographic scope
    jurisdictions: list[str] = Field(
        default_factory=lambda: ["US", "CA"],
        description="Jurisdictions to include (country codes)",
    )

    # Confidence thresholds
    min_confidence: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Minimum confidence to pursue a lead"
    )
    auto_merge_threshold: float = Field(
        default=0.9, ge=0.0, le=1.0, description="Auto-merge entity matches above this"
    )
    review_threshold: float = Field(
        default=0.7, ge=0.0, le=1.0, description="Queue for review above this"
    )

    # Extraction options
    enable_llm_extraction: bool = Field(
        default=False, description="Use LLM for entity extraction from text"
    )

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# Core Entity Models
# =============================================================================


class Evidence(BaseModel):
    """A record of input or fetched content with provenance.

    Evidence records store the raw data that supports case findings,
    including original inputs, fetched content, and API responses.
    """

    id: UUID = Field(default_factory=uuid4)
    case_id: UUID
    evidence_type: EvidenceType
    source_url: str | None = Field(default=None, description="Original URL if applicable")
    source_archive_url: str | None = Field(default=None, description="Archive.org link if archived")
    content_ref: str = Field(..., description="S3 path to raw content")
    content_hash: str = Field(..., description="SHA-256 of content")
    content_type: str = Field(default="application/octet-stream", description="MIME type")
    extractor: str | None = Field(default=None, description="Module that processed this")
    extractor_version: str | None = Field(default=None, description="Extractor version")
    extraction_result: dict[str, Any] | None = Field(
        default=None, description="Extracted entities, confidence"
    )
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(use_enum_values=True)


class ExtractedLead(BaseModel):
    """An entity mention extracted from evidence.

    Represents a potential entity discovered through extraction,
    before conversion to a research Lead.
    """

    id: UUID = Field(default_factory=uuid4)
    evidence_id: UUID
    entity_type: str = Field(..., description="organization, person, identifier")
    extracted_value: str = Field(..., description="The extracted text")
    identifier_type: str | None = Field(default=None, description="ein, bn, domain, name, etc.")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Extraction confidence")
    extraction_method: ExtractionMethod
    context: str | None = Field(default=None, description="Surrounding text context")
    converted_to_lead_id: UUID | None = Field(default=None, description="If converted to Lead")
    created_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(use_enum_values=True)


class MatchSignals(BaseModel):
    """Signals that contributed to an entity match confidence score."""

    name_similarity: float | None = Field(default=None, description="Fuzzy name match score")
    identifier_match: dict[str, Any] | None = Field(
        default=None, description="Identifier match details"
    )
    jurisdiction_match: bool = Field(default=False)
    address_overlap: dict[str, bool] | None = Field(
        default=None, description="City, postal FSA overlap"
    )
    shared_directors: list[str] | None = Field(
        default=None, description="Names of shared directors"
    )


class EntityMatch(BaseModel):
    """A proposed link between two entities awaiting review.

    When the system finds a potential match between an entry point
    entity (e.g., Meta Ad Sponsor) and a known organization, it
    creates an EntityMatch for human review or auto-merge.
    """

    id: UUID = Field(default_factory=uuid4)
    case_id: UUID
    source_entity_id: UUID = Field(..., description="Entity from case (e.g., Sponsor)")
    target_entity_id: UUID = Field(..., description="Candidate match (e.g., Organization)")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Match confidence")
    match_signals: MatchSignals = Field(
        default_factory=MatchSignals, description="Signals that contributed to score"
    )
    status: MatchStatus = MatchStatus.PENDING
    reviewed_by: str | None = Field(default=None, description="Reviewer identifier")
    reviewed_at: datetime | None = Field(default=None, description="Review timestamp")
    review_notes: str | None = Field(default=None, description="Reviewer notes")
    created_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# Case Report Models
# =============================================================================


class RankedEntity(BaseModel):
    """An entity ranked by relevance in a case report."""

    entity_id: UUID
    name: str
    entity_type: str
    relevance_score: float = Field(..., description="Computed relevance score")
    depth: int = Field(..., description="Hops from entry point")
    key_relationships: list[str] = Field(default_factory=list)
    jurisdiction: str | None = None


class AdMetadata(BaseModel):
    """Metadata for an ad relationship (Meta Ads specific)."""
    
    ad_id: str | None = Field(default=None, description="Meta Ad ID")
    creative_body: str | None = Field(default=None, description="Ad text content")
    creative_title: str | None = Field(default=None, description="Ad title")
    ad_snapshot_url: str | None = Field(default=None, description="URL to view the ad")
    impressions_lower: int | None = None
    impressions_upper: int | None = None
    spend_lower: float | None = None
    spend_upper: float | None = None
    currency: str | None = "USD"
    delivery_start: str | None = None
    delivery_stop: str | None = None
    publisher_platforms: list[str] = Field(default_factory=list)
    target_regions: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)


class RankedRelationship(BaseModel):
    """A relationship ranked by significance in a case report."""

    source_entity_id: UUID
    source_name: str
    target_entity_id: UUID
    target_name: str
    relationship_type: str
    significance_score: float = Field(..., description="Computed significance score")
    amount: float | None = Field(default=None, description="Funding amount if applicable")
    evidence_ids: list[UUID] = Field(default_factory=list)
    ad_metadata: AdMetadata | None = Field(default=None, description="Ad details if SPONSORED_BY relationship")


class CrossBorderFlag(BaseModel):
    """A flagged US-CA connection in a case report."""

    us_entity_id: UUID
    us_entity_name: str
    ca_entity_id: UUID
    ca_entity_name: str
    relationship_type: str
    amount: float | None = None
    evidence_ids: list[UUID] = Field(default_factory=list)


class Unknown(BaseModel):
    """An entity or relationship that could not be fully traced."""

    entity_name: str
    reason: str
    attempted_sources: list[str] = Field(default_factory=list)


class EvidenceCitation(BaseModel):
    """A citation to evidence in a case report."""

    evidence_id: UUID
    source_type: str
    source_url: str | None = None
    retrieved_at: datetime | None = None


class ReportSummary(BaseModel):
    """Quick stats and flags for a case report."""

    entry_point: str
    processing_time_seconds: float
    entity_count: int
    relationship_count: int
    cross_border_count: int
    has_unresolved_matches: bool


class AdSummary(BaseModel):
    """Aggregated summary of Meta Ads data in a case."""
    
    total_ads: int = 0
    total_spend_lower: float | None = None
    total_spend_upper: float | None = None
    total_impressions_lower: int | None = None
    total_impressions_upper: int | None = None
    currencies: list[str] = Field(default_factory=list)
    date_range_start: str | None = None
    date_range_end: str | None = None
    publisher_platforms: list[str] = Field(default_factory=list, description="All platforms used")
    target_countries: list[str] = Field(default_factory=list, description="All countries targeted")
    top_creative_themes: list[str] = Field(default_factory=list, description="Common words/themes in ad content")
    sponsors: list[str] = Field(default_factory=list, description="All sponsor names")


class SimilarityLead(BaseModel):
    """A lead for further investigation based on similarity patterns."""
    
    lead_type: str = Field(..., description="Type: shared_sponsor, similar_content, shared_region, shared_platform")
    description: str = Field(..., description="Human-readable description of the similarity")
    target_value: str = Field(..., description="The value to search for (sponsor name, keyword, etc.)")
    confidence: float = Field(default=0.7, description="Confidence this is worth investigating")
    source_ads: list[str] = Field(default_factory=list, description="Ad IDs that suggest this lead")
    suggested_search: str | None = Field(default=None, description="Suggested search query for Meta Ad Library")


class CaseReport(BaseModel):
    """Generated summary of case findings.

    A structured report containing ranked entities, relationships,
    cross-border flags, and evidence citations.
    """

    id: UUID = Field(default_factory=uuid4)
    case_id: UUID
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    report_version: int = Field(default=1)
    summary: ReportSummary
    top_entities: list[RankedEntity] = Field(
        default_factory=list, description="Max 20 ranked entities"
    )
    top_relationships: list[RankedRelationship] = Field(
        default_factory=list, description="Max 30 ranked relationships"
    )
    cross_border_flags: list[CrossBorderFlag] = Field(default_factory=list)
    unknowns: list[Unknown] = Field(default_factory=list)
    evidence_index: list[EvidenceCitation] = Field(default_factory=list)
    ads_summary: AdSummary | None = Field(default=None, description="Aggregated Meta Ads data")
    similarity_leads: list[SimilarityLead] = Field(
        default_factory=list, description="Suggested leads based on ad content similarity"
    )

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat() if v else None,
        },
    )


# =============================================================================
# Case Model
# =============================================================================


class CaseStats(BaseModel):
    """Statistics for a case investigation."""

    entity_count: int = 0
    relationship_count: int = 0
    evidence_count: int = 0
    pending_matches: int = 0
    leads_processed: int = 0
    leads_pending: int = 0


class ProcessingDetails(BaseModel):
    """Detailed processing information for an active case."""

    is_processing: bool = False
    current_phase: str = Field(default="idle", description="Current processing phase")
    progress_percent: float = Field(default=0.0, description="Estimated progress 0-100")
    leads_total: int = 0
    leads_pending: int = 0
    leads_completed: int = 0
    leads_failed: int = 0
    leads_skipped: int = 0
    recent_entities: list[str] = Field(default_factory=list, description="Recently discovered entity names")
    recent_leads: list[str] = Field(default_factory=list, description="Recent lead targets being processed")
    started_at: datetime | None = None
    elapsed_seconds: float = 0.0


class Case(BaseModel):
    """A research investigation started from an entry point.

    Cases are the top-level container for autonomous research,
    containing configuration, status, discovered entities/relationships,
    and a final report.
    """

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    entry_point_type: EntryPointType
    entry_point_value: str = Field(..., min_length=1, max_length=10000)
    status: CaseStatus = CaseStatus.INITIALIZING
    config: CaseConfig = Field(default_factory=CaseConfig)
    stats: CaseStats = Field(default_factory=CaseStats)
    research_session_id: UUID | None = Field(
        default=None, description="Link to underlying research session"
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    created_by: str | None = None

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat() if v else None,
        },
    )


# =============================================================================
# API Request/Response Models
# =============================================================================


class CreateCaseRequest(BaseModel):
    """Request to create a new case."""

    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    entry_point_type: EntryPointType
    entry_point_value: str = Field(..., min_length=1, max_length=10000)
    config: CaseConfig | None = None


class CaseSummary(BaseModel):
    """Summary of a case for list views."""

    id: UUID
    name: str
    status: CaseStatus
    entry_point_type: EntryPointType
    entity_count: int
    created_at: datetime

    model_config = ConfigDict(use_enum_values=True)


class CaseResponse(BaseModel):
    """Full case details for API responses."""

    id: UUID
    name: str
    description: str | None
    entry_point_type: EntryPointType
    entry_point_value: str
    status: CaseStatus
    config: CaseConfig
    stats: CaseStats
    research_session_id: UUID | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = ConfigDict(use_enum_values=True)


class EntitySummary(BaseModel):
    """Summary of an entity for match review."""

    id: UUID
    name: str
    entity_type: str
    jurisdiction: str | None
    identifiers: dict[str, str] = Field(default_factory=dict)


class EntityMatchResponse(BaseModel):
    """Entity match details for review."""

    id: UUID
    source_entity: EntitySummary
    target_entity: EntitySummary
    confidence: float
    match_signals: MatchSignals
    status: MatchStatus
    reviewed_by: str | None
    reviewed_at: datetime | None
    review_notes: str | None

    model_config = ConfigDict(use_enum_values=True)
