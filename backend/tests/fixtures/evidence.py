"""Evidence fixtures for integration tests."""

from datetime import datetime
from uuid import UUID, uuid4

import pytest

# =========================
# Evidence Fixtures
# =========================

SAMPLE_EVIDENCE = [
    {
        "id": UUID("770e8400-e29b-41d4-a716-446655440001"),
        "evidence_type": "irs_990_filing",
        "source_url": "https://s3.amazonaws.com/irs-form-990/202312349349301234_public.xml",
        "source_archive_url": "s3://mitds-archives/irs-990/2023/12349349301234.xml",
        "retrieved_at": datetime(2024, 1, 15, 10, 30),
        "extractor": "irs990.schedule_i",
        "extractor_version": "1.0.0",
        "raw_data_ref": "s3://mitds-raw/irs-990/2023/12349349301234.xml",
        "extraction_confidence": 0.95,
        "content_hash": "sha256:abc123def456...",
    },
    {
        "id": UUID("770e8400-e29b-41d4-a716-446655440002"),
        "evidence_type": "cra_t3010",
        "source_url": "https://apps.cra-arc.gc.ca/ebci/hacc/cirs/requestT3010/123456789RR0001/2023",
        "source_archive_url": None,
        "retrieved_at": datetime(2024, 2, 1, 14, 0),
        "extractor": "cra_t3010.parser",
        "extractor_version": "1.0.0",
        "raw_data_ref": "s3://mitds-raw/cra-t3010/2023/123456789RR0001.json",
        "extraction_confidence": 0.92,
        "content_hash": "sha256:xyz789abc012...",
    },
    {
        "id": UUID("770e8400-e29b-41d4-a716-446655440003"),
        "evidence_type": "sec_edgar_filing",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001234567",
        "source_archive_url": None,
        "retrieved_at": datetime(2024, 3, 5, 9, 15),
        "extractor": "sec_edgar_ingester",
        "extractor_version": "1.0.0",
        "raw_data_ref": "s3://mitds-raw/sec-edgar/0001234567.json",
        "extraction_confidence": 0.95,
        "content_hash": "sha256:111222333444...",
    },
    {
        "id": UUID("770e8400-e29b-41d4-a716-446655440004"),
        "evidence_type": "canada_corp_record",
        "source_url": "https://corporationscanada.ic.gc.ca/api/v1/123456789",
        "source_archive_url": None,
        "retrieved_at": datetime(2024, 3, 10, 11, 30),
        "extractor": "canada_corps_ingester",
        "extractor_version": "1.0.0",
        "raw_data_ref": "s3://mitds-raw/canada-corps/123456789.json",
        "extraction_confidence": 0.93,
        "content_hash": "sha256:555666777888...",
    },
    {
        "id": UUID("770e8400-e29b-41d4-a716-446655440005"),
        "evidence_type": "whois_record",
        "source_url": "whois://example-news.com",
        "source_archive_url": None,
        "retrieved_at": datetime(2024, 1, 20, 8, 45),
        "extractor": "infrastructure_detector.whois",
        "extractor_version": "1.0.0",
        "raw_data_ref": "s3://mitds-raw/whois/example-news.com.json",
        "extraction_confidence": 0.85,
        "content_hash": "sha256:aabbccddee...",
    },
]


# =========================
# Source Snapshot Fixtures
# =========================

SAMPLE_SNAPSHOTS = [
    {
        "id": UUID("880e8400-e29b-41d4-a716-446655440001"),
        "evidence_id": UUID("770e8400-e29b-41d4-a716-446655440001"),
        "snapshot_url": "s3://mitds-archives/irs-990/2023/12349349301234.xml",
        "snapshot_at": datetime(2024, 1, 15, 10, 35),
        "content_type": "application/xml",
        "size_bytes": 245678,
    },
    {
        "id": UUID("880e8400-e29b-41d4-a716-446655440002"),
        "evidence_id": UUID("770e8400-e29b-41d4-a716-446655440005"),
        "snapshot_url": "s3://mitds-archives/pages/example-news.com/2024-01-20.html",
        "snapshot_at": datetime(2024, 1, 20, 8, 50),
        "content_type": "text/html",
        "size_bytes": 56789,
    },
]


# =========================
# Pytest Fixtures
# =========================


@pytest.fixture
def sample_evidence():
    """Return a single sample evidence record."""
    return SAMPLE_EVIDENCE[0].copy()


@pytest.fixture
def sample_all_evidence():
    """Return all sample evidence records."""
    return [ev.copy() for ev in SAMPLE_EVIDENCE]


@pytest.fixture
def sample_snapshots():
    """Return all sample snapshots."""
    return [snap.copy() for snap in SAMPLE_SNAPSHOTS]


def create_evidence(
    evidence_type: str = "irs_990_filing",
    source_url: str = "https://example.com/source",
    extractor: str = "test_extractor",
    **kwargs,
) -> dict:
    """Factory function to create evidence fixtures.

    Args:
        evidence_type: Type of evidence
        source_url: Original source URL
        extractor: Extractor module name
        **kwargs: Additional properties

    Returns:
        Evidence dictionary
    """
    import hashlib

    content = kwargs.pop("content", f"test-content-{uuid4()}")
    content_hash = hashlib.sha256(content.encode()).hexdigest()

    return {
        "id": uuid4(),
        "evidence_type": evidence_type,
        "source_url": source_url,
        "source_archive_url": kwargs.pop("archive_url", None),
        "retrieved_at": datetime.utcnow(),
        "extractor": extractor,
        "extractor_version": kwargs.pop("extractor_version", "1.0.0"),
        "raw_data_ref": kwargs.pop("raw_data_ref", f"s3://mitds-raw/test/{uuid4()}.json"),
        "extraction_confidence": kwargs.pop("extraction_confidence", 0.90),
        "content_hash": f"sha256:{content_hash}",
        **kwargs,
    }
