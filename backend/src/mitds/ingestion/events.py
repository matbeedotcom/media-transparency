"""Ingestion event schemas for MITDS.

Defines the structure of events emitted during data ingestion.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseIngestionEvent(BaseModel):
    """Base class for all ingestion events."""

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IRS990FilingIngestedEvent(BaseIngestionEvent):
    """Event emitted when an IRS 990 filing is ingested."""

    event_type: str = "IRS990FilingIngested"

    class Payload(BaseModel):
        ein: str
        tax_period: str
        form_type: str
        object_id: str
        filing_url: str
        organization_name: str
        total_revenue: float | None = None
        total_expenses: float | None = None
        officers: list[dict[str, Any]] = []
        grants_made: list[dict[str, Any]] = []
        raw_file_path: str
        content_hash: str

    payload: Payload


class CRACharityIngestedEvent(BaseIngestionEvent):
    """Event emitted when a CRA charity record is ingested."""

    event_type: str = "CRACharityIngested"

    class Payload(BaseModel):
        bn: str
        legal_name: str
        operating_name: str | None = None
        charity_status: str
        effective_date: str | None = None
        category_code: str | None = None
        address: dict[str, str] = {}
        directors: list[dict[str, Any]] = []
        gifts_to_qualified_donees: list[dict[str, Any]] = []
        raw_file_path: str
        content_hash: str

    payload: Payload


class OpenCorporatesCompanyIngestedEvent(BaseIngestionEvent):
    """Event emitted when an OpenCorporates company record is ingested."""

    event_type: str = "OpenCorporatesCompanyIngested"

    class Payload(BaseModel):
        opencorporates_url: str
        company_number: str
        jurisdiction_code: str
        name: str
        company_type: str | None = None
        incorporation_date: str | None = None
        current_status: str | None = None
        registered_address: dict[str, str] = {}
        officers: list[dict[str, Any]] = []
        raw_file_path: str
        content_hash: str

    payload: Payload


class MetaAdIngestedEvent(BaseIngestionEvent):
    """Event emitted when a Meta Ad Library ad is ingested."""

    event_type: str = "MetaAdIngested"

    class Payload(BaseModel):
        ad_archive_id: str
        page_id: str
        page_name: str
        funding_entity: str | None = None
        ad_creation_time: datetime
        ad_delivery_start_time: datetime | None = None
        ad_delivery_stop_time: datetime | None = None
        ad_creative_body: str | None = None
        currency: str = "USD"
        spend: dict[str, float] = {}  # lower_bound, upper_bound
        impressions: dict[str, int] = {}  # lower_bound, upper_bound
        demographic_distribution: list[dict[str, Any]] = []
        delivery_by_region: list[dict[str, Any]] = []
        raw_file_path: str
        content_hash: str

    payload: Payload


class IngestionRunStartedEvent(BaseIngestionEvent):
    """Event emitted when an ingestion run starts."""

    event_type: str = "IngestionRunStarted"

    class Payload(BaseModel):
        run_id: UUID
        source: str
        incremental: bool = True
        triggered_by: str = "scheduler"

    payload: Payload


class IngestionRunCompletedEvent(BaseIngestionEvent):
    """Event emitted when an ingestion run completes."""

    event_type: str = "IngestionRunCompleted"

    class Payload(BaseModel):
        run_id: UUID
        source: str
        duration_seconds: float
        records_processed: int
        records_created: int
        records_updated: int
        duplicates_found: int
        errors: list[dict[str, Any]] = []

    payload: Payload


class IngestionRunFailedEvent(BaseIngestionEvent):
    """Event emitted when an ingestion run fails."""

    event_type: str = "IngestionRunFailed"

    class Payload(BaseModel):
        run_id: UUID
        source: str
        failure_reason: str
        retry_count: int = 0
        next_retry_at: datetime | None = None
        error_details: str | None = None

    payload: Payload
