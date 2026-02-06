"""PPSA (Personal Property Security Act) ingester.

Ingests secured interest data from PPSACanada SOAP API to identify
financial relationships between corporations.

Data source:
- PPSACanada SOAP API (requires API key)
- Cost: $8-20 per search (gated behind API key)

Key features:
- Targeted lookups via SOAP API
- SECURED_BY relationships in Neo4j
- Cost tracking per search
- Evidence records for secured interests
"""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session, get_neo4j_session
from ..logging import get_context_logger
from .base import BaseIngester, IngestionConfig, Neo4jHelper, PostgresHelper

logger = get_context_logger(__name__)

# PPSA cost per search (CAD)
PPSA_COST_PER_SEARCH_MIN = Decimal("8.00")
PPSA_COST_PER_SEARCH_MAX = Decimal("20.00")


class PPSASecuredInterestRecord(BaseModel):
    """A PPSA secured interest record."""

    registration_number: str = Field(..., description="PPSA registration number")
    debtor_name: str = Field(..., description="Debtor organization name")
    debtor_registration_number: str | None = Field(
        default=None, description="Debtor corporate registration number"
    )
    creditor_name: str = Field(..., description="Creditor organization name")
    creditor_registration_number: str | None = Field(
        default=None, description="Creditor corporate registration number"
    )
    registration_date: datetime | None = Field(
        default=None, description="Registration date"
    )
    expiry_date: datetime | None = Field(default=None, description="Expiry date")
    collateral_description: str | None = Field(
        default=None, description="Description of collateral"
    )
    amount: Decimal | None = Field(default=None, description="Secured amount (CAD)")
    jurisdiction: str | None = Field(default=None, description="Jurisdiction (province)")


class PPSAIngester(BaseIngester[PPSASecuredInterestRecord]):
    """Ingester for PPSA secured interest data.

    Queries PPSACanada SOAP API for secured interests and creates
    SECURED_BY relationships between debtor and creditor organizations.
    """

    def __init__(self):
        super().__init__("ppsa")
        self._http_client: httpx.AsyncClient | None = None
        self._neo4j = Neo4jHelper(self.logger)
        self._postgres = PostgresHelper(self.logger)
        self._settings = get_settings()
        self._total_cost = Decimal("0.00")
        self._search_count = 0

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            api_key = self._settings.ppsa_canada_api_key
            if not api_key:
                raise ValueError(
                    "PPSA_CANADA_API_KEY not configured. "
                    "PPSA searches require an API key."
                )

            # SOAP API typically uses XML
            headers = {
                "User-Agent": "MITDS/1.0 (Media Influence Transparency; research)",
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": "Search",
            }

            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=60.0),
                follow_redirects=True,
                headers=headers,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    @property
    def total_cost(self) -> Decimal:
        """Get total cost of searches."""
        return self._total_cost

    @property
    def search_count(self) -> int:
        """Get number of searches performed."""
        return self._search_count

    async def get_last_sync_time(self) -> datetime | None:
        """Get last successful sync timestamp."""
        async with get_db_session() as db:
            result = await db.execute(
                text("""
                    SELECT MAX(retrieved_at) as last_sync
                    FROM evidence
                    WHERE evidence_type = 'PPSA_SECURED_INTEREST'
                    AND extractor = 'ppsa_ingester'
                """),
            )
            row = result.fetchone()
            return row.last_sync if row else None

    async def save_sync_time(self, timestamp: datetime) -> None:
        """Save sync time (handled by base class)."""
        pass

    async def fetch_records(
        self, config: IngestionConfig
    ) -> AsyncIterator[PPSASecuredInterestRecord]:
        """Fetch PPSA secured interest records.

        Requires target entities to search for. Each search costs $8-20.
        """
        api_key = self._settings.ppsa_canada_api_key
        if not api_key:
            self.logger.error(
                "PPSA_CANADA_API_KEY not configured. "
                "Cannot perform PPSA searches."
            )
            return

        # Get target entities to search
        target_entities = config.target_entities or []
        if not target_entities:
            self.logger.warning(
                "No target entities specified. "
                "PPSA searches require specific entity names."
            )
            return

        # Warn about cost
        estimated_cost = len(target_entities) * PPSA_COST_PER_SEARCH_MAX
        self.logger.warning(
            f"PPSA searches will cost approximately "
            f"${estimated_cost:.2f} CAD ({len(target_entities)} searches × "
            f"${PPSA_COST_PER_SEARCH_MAX:.2f} per search)"
        )

        # Search for each target entity
        for entity_name in target_entities:
            try:
                async for record in self._search_entity(entity_name, config):
                    yield record
            except Exception as e:
                self.logger.error(f"PPSA search failed for {entity_name}: {e}")
                continue

    async def _search_entity(
        self, entity_name: str, config: IngestionConfig
    ) -> AsyncIterator[PPSASecuredInterestRecord]:
        """Search PPSA for a specific entity."""
        api_key = self._settings.ppsa_canada_api_key
        api_url = self._settings.ppsa_canada_api_url

        # Build SOAP request
        # Note: Actual SOAP structure depends on PPSACanada API specification
        # This is a template - adjust based on actual API docs
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
              xmlns:ns="http://api.ppsacanada.com/">
    <soap:Header>
        <ns:APIKey>{api_key}</ns:APIKey>
    </soap:Header>
    <soap:Body>
        <ns:SearchRequest>
            <ns:DebtorName>{entity_name}</ns:DebtorName>
            <ns:SearchType>DEBTOR</ns:SearchType>
            <ns:IncludeExpired>false</ns:IncludeExpired>
        </ns:SearchRequest>
    </soap:Body>
</soap:Envelope>"""

        try:
            response = await self.http_client.post(
                api_url,
                content=soap_body,
            )
            response.raise_for_status()

            # Parse SOAP response
            # Note: Use proper XML parsing - this is simplified
            records = self._parse_soap_response(response.text, entity_name)

            # Track cost
            self._search_count += 1
            self._total_cost += PPSA_COST_PER_SEARCH_MAX  # Use max for safety

            for record in records:
                yield record

        except httpx.HTTPStatusError as e:
            self.logger.error(f"PPSA API error: {e.response.status_code}")
            if e.response.status_code == 401:
                self.logger.error("Invalid API key or unauthorized")
            # Still charge for failed search
            self._search_count += 1
            self._total_cost += PPSA_COST_PER_SEARCH_MAX
        except Exception as e:
            self.logger.error(f"PPSA search failed: {e}")
            # Still charge for failed search
            self._search_count += 1
            self._total_cost += PPSA_COST_PER_SEARCH_MAX

    def _parse_soap_response(
        self, xml_content: str, debtor_name: str
    ) -> list[PPSASecuredInterestRecord]:
        """Parse SOAP XML response into records."""
        try:
            from xml.etree import ElementTree as ET

            root = ET.fromstring(xml_content)

            records = []

            # Navigate SOAP response structure
            # Adjust namespaces and element names based on actual API
            namespaces = {
                "soap": "http://schemas.xmlsoap.org/soap/envelope/",
                "ns": "http://api.ppsacanada.com/",
            }

            # Find search results
            results = root.findall(".//ns:SearchResult", namespaces) or root.findall(
                ".//SearchResult"
            )

            for result in results:
                try:
                    # Extract fields - adjust based on actual API structure
                    reg_num = (
                        result.findtext("ns:RegistrationNumber", namespaces)
                        or result.findtext("RegistrationNumber")
                        or ""
                    )
                    if not reg_num:
                        continue

                    creditor_name = (
                        result.findtext("ns:CreditorName", namespaces)
                        or result.findtext("CreditorName")
                        or ""
                    )

                    # Parse dates
                    reg_date = None
                    reg_date_str = (
                        result.findtext("ns:RegistrationDate", namespaces)
                        or result.findtext("RegistrationDate")
                    )
                    if reg_date_str:
                        try:
                            reg_date = datetime.fromisoformat(reg_date_str.replace("Z", "+00:00"))
                        except Exception:
                            pass

                    expiry_date = None
                    expiry_str = (
                        result.findtext("ns:ExpiryDate", namespaces)
                        or result.findtext("ExpiryDate")
                    )
                    if expiry_str:
                        try:
                            expiry_date = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                        except Exception:
                            pass

                    # Parse amount
                    amount = None
                    amount_str = (
                        result.findtext("ns:Amount", namespaces) or result.findtext("Amount")
                    )
                    if amount_str:
                        try:
                            amount = Decimal(amount_str.replace("$", "").replace(",", ""))
                        except Exception:
                            pass

                    collateral = (
                        result.findtext("ns:CollateralDescription", namespaces)
                        or result.findtext("CollateralDescription")
                        or None
                    )

                    jurisdiction = (
                        result.findtext("ns:Jurisdiction", namespaces)
                        or result.findtext("Jurisdiction")
                        or None
                    )

                    creditor_reg = (
                        result.findtext("ns:CreditorRegistrationNumber", namespaces)
                        or result.findtext("CreditorRegistrationNumber")
                        or None
                    )

                    debtor_reg = (
                        result.findtext("ns:DebtorRegistrationNumber", namespaces)
                        or result.findtext("DebtorRegistrationNumber")
                        or None
                    )

                    record = PPSASecuredInterestRecord(
                        registration_number=reg_num,
                        debtor_name=debtor_name,
                        debtor_registration_number=debtor_reg,
                        creditor_name=creditor_name,
                        creditor_registration_number=creditor_reg,
                        registration_date=reg_date,
                        expiry_date=expiry_date,
                        collateral_description=collateral,
                        amount=amount,
                        jurisdiction=jurisdiction,
                    )
                    records.append(record)

                except Exception as e:
                    self.logger.warning(f"Failed to parse PPSA result: {e}")
                    continue

            return records

        except Exception as e:
            self.logger.error(f"Failed to parse SOAP response: {e}")
            return []

    async def process_record(self, record: PPSASecuredInterestRecord) -> dict[str, Any]:
        """Process a PPSA secured interest record.

        Creates evidence record and SECURED_BY relationship
        between debtor and creditor.
        """
        result: dict[str, Any] = {"created": False}

        # Store evidence record
        evidence_id = uuid4()
        async with get_db_session() as db:
            # Check if registration already exists
            existing = await db.execute(
                text("""
                    SELECT id FROM evidence
                    WHERE evidence_type = 'PPSA_SECURED_INTEREST'
                    AND raw_data_ref = :reg_num
                    LIMIT 1
                """),
                {"reg_num": f"ppsa/{record.registration_number}"},
            )
            existing_row = existing.fetchone()

            if existing_row:
                evidence_id = existing_row[0]
                result = {"updated": True, "evidence_id": str(evidence_id)}
            else:
                # Create content hash
                content = json.dumps(
                    {
                        "registration_number": record.registration_number,
                        "debtor_name": record.debtor_name,
                        "creditor_name": record.creditor_name,
                        "registration_date": record.registration_date.isoformat()
                        if record.registration_date
                        else None,
                        "expiry_date": record.expiry_date.isoformat()
                        if record.expiry_date
                        else None,
                        "amount": str(record.amount) if record.amount else None,
                        "collateral": record.collateral_description,
                    },
                    sort_keys=True,
                )
                content_hash = hashlib.sha256(content.encode()).hexdigest()

                await db.execute(
                    text("""
                        INSERT INTO evidence (
                            id, evidence_type, source_url, retrieved_at,
                            extractor, extractor_version, raw_data_ref,
                            extraction_confidence, content_hash
                        )
                        VALUES (
                            :id, :evidence_type, :source_url, NOW(),
                            :extractor, :version, :raw_ref,
                            :confidence, :hash
                        )
                    """),
                    {
                        "id": evidence_id,
                        "evidence_type": "PPSA_SECURED_INTEREST",
                        "source_url": f"https://www.ppsacanada.com/search/{record.registration_number}",
                        "extractor": "ppsa_ingester",
                        "version": "1.0.0",
                        "raw_ref": f"ppsa/{record.registration_number}",
                        "confidence": 0.90,  # High confidence - official registry
                        "hash": content_hash,
                    },
                )
                result = {"created": True, "evidence_id": str(evidence_id)}

        # Create SECURED_BY relationship
        try:
            async with get_db_session() as db:
                # Find debtor entity
                debtor_id = await self._postgres.find_entity_by_name(db, record.debtor_name)
                if not debtor_id and record.debtor_registration_number:
                    debtor_id = await self._postgres.find_entity_by_external_id(
                        db, "canada_corp_num", record.debtor_registration_number
                    )

                # Find creditor entity
                creditor_id = await self._postgres.find_entity_by_name(db, record.creditor_name)
                if not creditor_id and record.creditor_registration_number:
                    creditor_id = await self._postgres.find_entity_by_external_id(
                        db, "canada_corp_num", record.creditor_registration_number
                    )

                # Create relationship if both entities found
                if debtor_id and creditor_id:
                    await self._create_secured_relationship(
                        debtor_id, creditor_id, record, evidence_id
                    )

        except Exception as e:
            self.logger.warning(f"Failed to create relationship: {e}")

        return result

    async def _create_secured_relationship(
        self,
        debtor_id: UUID,
        creditor_id: UUID,
        record: PPSASecuredInterestRecord,
        evidence_id: UUID,
    ):
        """Create SECURED_BY relationship in Neo4j."""
        try:
            async with get_neo4j_session() as session:
                await self._neo4j.create_relationship(
                    session,
                    "SECURED_BY",
                    "Organization",
                    "id",
                    str(debtor_id),
                    "Organization",
                    "id",
                    str(creditor_id),
                    properties={
                        "registration_number": record.registration_number,
                        "registration_date": record.registration_date.isoformat()
                        if record.registration_date
                        else None,
                        "expiry_date": record.expiry_date.isoformat()
                        if record.expiry_date
                        else None,
                        "amount": str(record.amount) if record.amount else None,
                        "collateral": record.collateral_description,
                        "jurisdiction": record.jurisdiction,
                        "evidence_id": str(evidence_id),
                    },
                    merge_on=["registration_number"],
                )

        except Exception as e:
            self.logger.warning(f"Neo4j relationship creation failed: {e}")


async def run_ppsa_ingestion(
    config: IngestionConfig | None = None,
) -> Any:
    """Run PPSA ingestion.

    Args:
        config: Ingestion configuration (must include target_entities)

    Returns:
        Ingestion result with cost information

    Warning:
        Each search costs $8-20 CAD. Total cost is tracked
        in result.metadata['total_cost'].
    """
    ingester = PPSAIngester()
    try:
        result = await ingester.run(config)
        # Add cost information to result
        if not hasattr(result, "metadata"):
            result.metadata = {}
        result.metadata["total_cost_cad"] = str(ingester.total_cost)
        result.metadata["search_count"] = ingester.search_count
        return result
    finally:
        await ingester.close()
