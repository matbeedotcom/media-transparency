# Event Schemas: MITDS Ingestion Pipeline

**Branch**: `001-media-influence-detection` | **Date**: 2026-01-26
**Purpose**: Define event schemas for data ingestion and internal messaging

## Ingestion Events

All events are published to internal message queues (Redis/Celery) for processing.

### Raw Source Events

These events represent raw data retrieved from external sources before entity resolution.

#### IRS990FilingIngested

Triggered when an IRS 990 filing is successfully downloaded and parsed.

```json
{
  "event_type": "IRS990FilingIngested",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "ein": "12-3456789",
    "tax_period": "202312",
    "form_type": "990",
    "object_id": "202341234567890123",
    "filing_url": "s3://irs-form-990/2023/...",
    "organization_name": "Example Foundation",
    "total_revenue": 5000000,
    "total_expenses": 4500000,
    "officers": [
      {
        "name": "John Smith",
        "title": "Executive Director",
        "compensation": 150000,
        "hours_per_week": 40
      }
    ],
    "grants_made": [
      {
        "recipient_name": "Another Nonprofit",
        "recipient_ein": "98-7654321",
        "amount": 50000,
        "purpose": "General operations"
      }
    ],
    "raw_file_path": "s3://mitds-raw/irs990/2023/12-3456789_202312.xml",
    "content_hash": "sha256:abc123..."
  },
  "metadata": {
    "source": "irs990",
    "extractor_version": "1.0.0",
    "ingestion_run_id": "uuid"
  }
}
```

#### CRACharityIngested

Triggered when a CRA charity record is processed.

```json
{
  "event_type": "CRACharityIngested",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "bn": "123456789RR0001",
    "legal_name": "Canadian Charity Example",
    "operating_name": "CCE",
    "charity_status": "Registered",
    "effective_date": "2010-05-15",
    "category_code": "C",
    "designation_code": "1",
    "address": {
      "city": "Toronto",
      "province": "ON",
      "postal_code": "M5V 1A1"
    },
    "directors": [
      {
        "name": "Jane Doe",
        "position": "Chair"
      }
    ],
    "gifts_to_qualified_donees": [
      {
        "donee_name": "US Foundation",
        "donee_bn": null,
        "amount": 100000
      }
    ],
    "raw_file_path": "s3://mitds-raw/cra/2023/123456789RR0001.json",
    "content_hash": "sha256:def456..."
  },
  "metadata": {
    "source": "cra",
    "extractor_version": "1.0.0",
    "ingestion_run_id": "uuid"
  }
}
```

#### OpenCorporatesCompanyIngested

Triggered when an OpenCorporates company record is fetched.

```json
{
  "event_type": "OpenCorporatesCompanyIngested",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "opencorporates_url": "https://opencorporates.com/companies/us_de/1234567",
    "company_number": "1234567",
    "jurisdiction_code": "us_de",
    "name": "Media Holdings Inc",
    "company_type": "Corporation",
    "incorporation_date": "2015-03-20",
    "current_status": "Active",
    "registered_address": {
      "street": "1209 Orange St",
      "city": "Wilmington",
      "state": "DE",
      "postal_code": "19801",
      "country": "US"
    },
    "officers": [
      {
        "name": "Robert Johnson",
        "position": "Director",
        "start_date": "2015-03-20",
        "end_date": null
      }
    ],
    "raw_file_path": "s3://mitds-raw/opencorp/us_de/1234567.json",
    "content_hash": "sha256:ghi789..."
  },
  "metadata": {
    "source": "opencorporates",
    "extractor_version": "1.0.0",
    "ingestion_run_id": "uuid",
    "api_call_count": 1
  }
}
```

#### MetaAdIngested

Triggered when a Meta Ad Library ad is fetched.

```json
{
  "event_type": "MetaAdIngested",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "ad_archive_id": "123456789012345",
    "page_id": "987654321",
    "page_name": "Political Action Committee",
    "funding_entity": "PAC For Change",
    "ad_creation_time": "2026-01-15T10:00:00Z",
    "ad_delivery_start_time": "2026-01-16T00:00:00Z",
    "ad_delivery_stop_time": null,
    "ad_creative_body": "Vote for change!",
    "ad_creative_link_caption": "Learn more",
    "ad_creative_link_title": "Our Platform",
    "currency": "USD",
    "spend": {
      "lower_bound": 1000,
      "upper_bound": 5000
    },
    "impressions": {
      "lower_bound": 50000,
      "upper_bound": 100000
    },
    "demographic_distribution": [
      {
        "age": "25-34",
        "gender": "male",
        "percentage": 0.35
      }
    ],
    "delivery_by_region": [
      {
        "region": "California",
        "percentage": 0.15
      }
    ],
    "raw_file_path": "s3://mitds-raw/meta_ads/2026-01/123456789012345.json",
    "content_hash": "sha256:jkl012..."
  },
  "metadata": {
    "source": "meta_ads",
    "extractor_version": "1.0.0",
    "ingestion_run_id": "uuid"
  }
}
```

---

## Entity Resolution Events

### EntityResolutionCandidate

Triggered when a potential entity match is identified.

```json
{
  "event_type": "EntityResolutionCandidate",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "candidate_id": "uuid",
    "source_entity": {
      "source": "irs990",
      "identifier": "12-3456789",
      "name": "Example Foundation Inc"
    },
    "target_entity": {
      "source": "opencorporates",
      "identifier": "us_de/1234567",
      "name": "Example Foundation"
    },
    "match_method": "fuzzy_name",
    "confidence": 0.75,
    "matching_factors": [
      {
        "factor": "normalized_name",
        "value": "example foundation",
        "score": 0.95
      },
      {
        "factor": "jurisdiction",
        "value": "US",
        "score": 1.0
      },
      {
        "factor": "address_city",
        "value": "Wilmington",
        "score": 0.5
      }
    ],
    "requires_review": true
  }
}
```

### EntityResolved

Triggered when entities are merged.

```json
{
  "event_type": "EntityResolved",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "canonical_entity_id": "uuid",
    "merged_source_ids": [
      {"source": "irs990", "identifier": "12-3456789"},
      {"source": "opencorporates", "identifier": "us_de/1234567"}
    ],
    "resolution_method": "deterministic",
    "confidence": 0.95,
    "resolved_by": "system"
  }
}
```

### EntityResolutionRejected

Triggered when a candidate match is rejected.

```json
{
  "event_type": "EntityResolutionRejected",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "candidate_id": "uuid",
    "reason": "Different legal entities confirmed",
    "rejected_by": "analyst@example.com"
  }
}
```

---

## Graph Events

### RelationshipCreated

Triggered when a new relationship is added to the graph.

```json
{
  "event_type": "RelationshipCreated",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "relationship_id": "uuid",
    "rel_type": "FUNDED_BY",
    "source_entity_id": "uuid",
    "target_entity_id": "uuid",
    "valid_from": "2023-01-01T00:00:00Z",
    "valid_to": null,
    "confidence": 0.9,
    "properties": {
      "amount": 50000,
      "fiscal_year": 2023
    },
    "evidence_refs": ["uuid1", "uuid2"]
  }
}
```

### RelationshipEnded

Triggered when a relationship is marked as ended.

```json
{
  "event_type": "RelationshipEnded",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "relationship_id": "uuid",
    "valid_to": "2025-12-31T23:59:59Z",
    "reason": "No funding in latest filing",
    "evidence_refs": ["uuid"]
  }
}
```

---

## Detection Events

### CoordinationDetected

Triggered when a coordination pattern is identified.

```json
{
  "event_type": "CoordinationDetected",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "finding_id": "uuid",
    "detection_type": "temporal_coordination",
    "outlet_ids": ["uuid1", "uuid2", "uuid3"],
    "composite_score": 0.72,
    "signal_scores": {
      "temporal": 0.85,
      "funding": 0.65,
      "infrastructure": 0.40
    },
    "time_range": {
      "start": "2026-01-01T00:00:00Z",
      "end": "2026-01-15T23:59:59Z"
    },
    "flagged": true,
    "explanation_summary": "Three outlets showed statistically significant publication synchronization during period, sharing two common funders."
  }
}
```

### HardNegativeFiltered

Triggered when a potential detection is filtered as a hard negative.

```json
{
  "event_type": "HardNegativeFiltered",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "candidate_finding_id": "uuid",
    "filter_reason": "breaking_news",
    "filter_details": "AP wire story published 2026-01-10T08:00:00Z triggered legitimate news cycle",
    "outlets_affected": ["uuid1", "uuid2"],
    "original_temporal_score": 0.92,
    "adjusted_score": 0.15
  }
}
```

---

## System Events

### IngestionRunStarted

```json
{
  "event_type": "IngestionRunStarted",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "run_id": "uuid",
    "source": "irs990",
    "incremental": true,
    "triggered_by": "scheduler"
  }
}
```

### IngestionRunCompleted

```json
{
  "event_type": "IngestionRunCompleted",
  "event_id": "uuid",
  "timestamp": "2026-01-26T14:00:00Z",
  "payload": {
    "run_id": "uuid",
    "source": "irs990",
    "duration_seconds": 7200,
    "records_processed": 15000,
    "records_created": 500,
    "records_updated": 2000,
    "duplicates_found": 50,
    "errors": []
  }
}
```

### IngestionRunFailed

```json
{
  "event_type": "IngestionRunFailed",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:30:00Z",
  "payload": {
    "run_id": "uuid",
    "source": "meta_ads",
    "failure_reason": "api_rate_limit",
    "retry_count": 3,
    "next_retry_at": "2026-01-26T13:30:00Z",
    "error_details": "HTTP 429: Rate limit exceeded"
  }
}
```

### AlertGenerated

```json
{
  "event_type": "AlertGenerated",
  "event_id": "uuid",
  "timestamp": "2026-01-26T12:00:00Z",
  "payload": {
    "alert_type": "ingestion_failure",
    "severity": "warning",
    "source": "opencorporates",
    "message": "OpenCorporates API failed 3 consecutive times. Serving stale data.",
    "last_successful_run": "2026-01-25T12:00:00Z",
    "data_age_hours": 24
  }
}
```

---

## Event Delivery Guarantees

| Event Category | Delivery | Ordering |
|----------------|----------|----------|
| Ingestion Events | At-least-once | Per source |
| Resolution Events | At-least-once | Per entity |
| Graph Events | At-least-once | Per relationship |
| Detection Events | At-least-once | Per finding |
| System Events | Best-effort | None |

**Idempotency**: All events include `event_id`. Consumers must deduplicate by event ID.

**Retention**: Events retained in PostgreSQL event store for 2 years minimum.
