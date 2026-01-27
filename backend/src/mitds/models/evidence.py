"""Evidence model for MITDS.

Every relationship and entity must have evidence linking it to
source documents for full provenance tracking.
"""

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EvidenceType(str, Enum):
    """Types of evidence sources."""

    IRS_990_FILING = "irs_990_filing"
    CRA_T3010 = "cra_t3010"
    OPENCORP_RECORD = "opencorp_record"
    SEC_EDGAR_FILING = "sec_edgar_filing"
    CANADA_CORP_RECORD = "canada_corp_record"
    META_AD = "meta_ad"
    WHOIS_RECORD = "whois_record"
    DNS_LOOKUP = "dns_lookup"
    PAGE_ANALYSIS = "page_analysis"
    MANUAL_RESEARCH = "manual_research"


class Evidence(BaseModel):
    """Evidence record linking entities and relationships to source documents.

    All data in MITDS must be traceable back to original source documents
    for transparency and reproducibility.
    """

    id: UUID = Field(default_factory=uuid4)
    evidence_type: EvidenceType

    # Source reference
    source_url: str = Field(..., description="Original URL")
    source_archive_url: str | None = Field(
        default=None, description="Archive.org or local archive URL"
    )
    retrieved_at: datetime = Field(
        default_factory=datetime.utcnow, description="When the source was retrieved"
    )

    # Extraction metadata
    extractor: str = Field(
        ..., description="Module that extracted (e.g., 'irs990.schedule_i')"
    )
    extractor_version: str = Field(..., description="Version of the extractor")
    raw_data_ref: str = Field(..., description="S3 path to raw source file")

    # Confidence
    extraction_confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="How confident in extraction",
    )

    # Content integrity
    content_hash: str = Field(..., description="SHA-256 of source document")

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            UUID: str,
            datetime: lambda v: v.isoformat(),
        },
    )


class EvidenceRef(BaseModel):
    """Lightweight reference to evidence for API responses."""

    id: UUID
    evidence_type: EvidenceType
    source_url: str
    archive_url: str | None = None
    retrieved_at: datetime

    model_config = ConfigDict(use_enum_values=True)

    @classmethod
    def from_evidence(cls, evidence: Evidence) -> "EvidenceRef":
        """Create a reference from a full Evidence object."""
        return cls(
            id=evidence.id,
            evidence_type=evidence.evidence_type,
            source_url=evidence.source_url,
            archive_url=evidence.source_archive_url,
            retrieved_at=evidence.retrieved_at,
        )


class SourceSnapshot(BaseModel):
    """Archived snapshot of a source for dead link protection."""

    id: UUID = Field(default_factory=uuid4)
    evidence_id: UUID
    snapshot_url: str = Field(..., description="Local/S3 archive path")
    snapshot_at: datetime = Field(default_factory=datetime.utcnow)
    content_type: str | None = None
    size_bytes: int | None = None
