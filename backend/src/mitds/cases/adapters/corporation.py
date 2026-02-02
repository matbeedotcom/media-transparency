"""Corporation name entry point adapter.

Handles corporation names as entry points for case creation,
searching across multiple registries (EDGAR, SEDAR, ISED, CRA).
"""

import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from ...storage import store_evidence_content
from ..models import (
    EntryPointType,
    Evidence,
    EvidenceType,
    ExtractedLead,
    ExtractionMethod,
)
from .base import BaseEntryPointAdapter, SeedEntity, ValidationResult

logger = logging.getLogger(__name__)


class CorporationAdapter(BaseEntryPointAdapter):
    """Adapter for corporation name entry points.

    Searches multiple sources:
    - SEC EDGAR (US public companies)
    - SEDAR+ (Canadian public companies)
    - ISED Canada Corporations (Federal)
    - CRA Charities (Canadian registered charities)
    - Provincial registries (where available)
    """

    @property
    def entry_point_type(self) -> str:
        return EntryPointType.CORPORATION.value

    async def validate(self, input_value: str) -> ValidationResult:
        """Validate a corporation name.

        Performs quick validation:
        - Non-empty value
        - Minimum length (2 chars)
        - Maximum length (500 chars)
        """
        if not input_value or not input_value.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Corporation name is required",
            )

        value = input_value.strip()

        if len(value) < 2:
            return ValidationResult(
                is_valid=False,
                error_message="Corporation name must be at least 2 characters",
            )

        if len(value) > 500:
            return ValidationResult(
                is_valid=False,
                error_message="Corporation name must be less than 500 characters",
            )

        return ValidationResult(
            is_valid=True,
            normalized_value=value,
            metadata={
                "original_value": input_value,
            },
        )

    async def create_evidence(
        self,
        case_id: UUID,
        input_value: str,
        validation_result: ValidationResult,
    ) -> Evidence:
        """Create evidence by searching corporate registries.

        Searches multiple sources and stores aggregated results.
        """
        evidence_id = uuid4()
        now = datetime.utcnow()
        corp_name = validation_result.normalized_value

        search_results: dict[str, Any] = {
            "query": corp_name,
            "retrieved_at": now.isoformat(),
            "sources": {},
        }

        # Search SEC EDGAR
        try:
            edgar_results = await self._search_edgar(corp_name)
            search_results["sources"]["edgar"] = {
                "count": len(edgar_results),
                "results": edgar_results,
            }
        except Exception as e:
            logger.warning(f"EDGAR search failed: {e}")
            search_results["sources"]["edgar"] = {"error": str(e)}

        # Search SEDAR+ (Canadian)
        try:
            sedar_results = await self._search_sedar(corp_name)
            search_results["sources"]["sedar"] = {
                "count": len(sedar_results),
                "results": sedar_results,
            }
        except Exception as e:
            logger.warning(f"SEDAR search failed: {e}")
            search_results["sources"]["sedar"] = {"error": str(e)}

        # Search ISED Canada Corporations
        try:
            ised_results = await self._search_ised(corp_name)
            search_results["sources"]["ised"] = {
                "count": len(ised_results),
                "results": ised_results,
            }
        except Exception as e:
            logger.warning(f"ISED search failed: {e}")
            search_results["sources"]["ised"] = {"error": str(e)}

        # Search CRA Charities
        try:
            cra_results = await self._search_cra(corp_name)
            search_results["sources"]["cra"] = {
                "count": len(cra_results),
                "results": cra_results,
            }
        except Exception as e:
            logger.warning(f"CRA search failed: {e}")
            search_results["sources"]["cra"] = {"error": str(e)}

        # Store in S3
        content = json.dumps(search_results, indent=2).encode("utf-8")
        content_ref, content_hash = await store_evidence_content(
            case_id=str(case_id),
            evidence_id=str(evidence_id),
            content=content,
            content_type="application/json",
            filename="corporation_search",
            extension="json",
            metadata={"query": corp_name},
        )

        return Evidence(
            id=evidence_id,
            case_id=case_id,
            evidence_type=EvidenceType.API_RESPONSE,
            source_url=None,
            content_ref=content_ref,
            content_hash=content_hash,
            content_type="application/json",
            extractor="corporation_search",
            extractor_version="1.0.0",
            extraction_result={"query": corp_name},
            retrieved_at=now,
            created_at=now,
        )

    async def _search_edgar(self, name: str) -> list[dict[str, Any]]:
        """Search SEC EDGAR for company."""
        from ...ingestion.edgar import EdgarIngester

        try:
            ingester = EdgarIngester()
            results = await ingester.search_companies(name, limit=10)
            return [
                {
                    "cik": r.get("cik"),
                    "name": r.get("name"),
                    "ticker": r.get("ticker"),
                }
                for r in results
            ]
        except Exception as e:
            logger.debug(f"EDGAR search error: {e}")
            return []

    async def _search_sedar(self, name: str) -> list[dict[str, Any]]:
        """Search SEDAR+ for Canadian public companies."""
        from ...ingestion.sedar import SedarIngester

        try:
            ingester = SedarIngester()
            results = await ingester.search_issuers(name, limit=10)
            return [
                {
                    "sedar_id": r.get("sedar_id"),
                    "name": r.get("name"),
                    "jurisdiction": r.get("jurisdiction"),
                }
                for r in results
            ]
        except Exception as e:
            logger.debug(f"SEDAR search error: {e}")
            return []

    async def _search_ised(self, name: str) -> list[dict[str, Any]]:
        """Search ISED Canada Corporations database."""
        from ...ingestion.canada_corps import CanadaCorpsIngester

        try:
            ingester = CanadaCorpsIngester()
            results = await ingester.search_corporations(name, limit=10)
            return [
                {
                    "corporation_number": r.get("corporation_number"),
                    "name": r.get("name"),
                    "status": r.get("status"),
                    "bn": r.get("bn"),
                }
                for r in results
            ]
        except Exception as e:
            logger.debug(f"ISED search error: {e}")
            return []

    async def _search_cra(self, name: str) -> list[dict[str, Any]]:
        """Search CRA Charities database."""
        from ...ingestion.cra import CRAIngester

        try:
            ingester = CRAIngester()
            results = await ingester.search_charities(name, limit=10)
            return [
                {
                    "bn": r.get("bn"),
                    "name": r.get("name"),
                    "city": r.get("city"),
                    "province": r.get("province"),
                }
                for r in results
            ]
        except Exception as e:
            logger.debug(f"CRA search error: {e}")
            return []

    async def extract_leads(self, evidence: Evidence) -> list[ExtractedLead]:
        """Extract leads from corporation search results.

        Creates leads for each matching organization found.
        """
        leads: list[ExtractedLead] = []

        # Load evidence content
        from ...storage import retrieve_evidence_content

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to load evidence content: {e}")
            return leads

        sources = data.get("sources", {})
        seen: set[str] = set()

        # Process EDGAR results
        for result in sources.get("edgar", {}).get("results", []):
            name = result.get("name")
            cik = result.get("cik")
            if name and name not in seen:
                seen.add(name)
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="organization",
                    extracted_value=name,
                    identifier_type="cik" if cik else "name",
                    confidence=0.95,
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"SEC EDGAR: CIK {cik}" if cik else "SEC EDGAR",
                ))

        # Process SEDAR results
        for result in sources.get("sedar", {}).get("results", []):
            name = result.get("name")
            sedar_id = result.get("sedar_id")
            if name and name not in seen:
                seen.add(name)
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="organization",
                    extracted_value=name,
                    identifier_type="sedar_profile" if sedar_id else "name",
                    confidence=0.95,
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"SEDAR+: {sedar_id}" if sedar_id else "SEDAR+",
                ))

        # Process ISED results
        for result in sources.get("ised", {}).get("results", []):
            name = result.get("name")
            bn = result.get("bn")
            if name and name not in seen:
                seen.add(name)
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="organization",
                    extracted_value=name,
                    identifier_type="bn" if bn else "name",
                    confidence=0.9,
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"ISED: BN {bn}" if bn else "ISED Canada Corporations",
                ))

        # Process CRA results
        for result in sources.get("cra", {}).get("results", []):
            name = result.get("name")
            bn = result.get("bn")
            if name and name not in seen:
                seen.add(name)
                leads.append(ExtractedLead(
                    evidence_id=evidence.id,
                    entity_type="organization",
                    extracted_value=name,
                    identifier_type="bn" if bn else "name",
                    confidence=0.9,
                    extraction_method=ExtractionMethod.DETERMINISTIC,
                    context=f"CRA Charities: BN {bn}" if bn else "CRA Charities",
                ))

        return leads

    async def get_seed_entity(self, evidence: Evidence) -> SeedEntity | None:
        """Get the seed entity from corporation search results.

        Returns the best matching corporation from search results.
        """
        # Load evidence content
        from ...storage import retrieve_evidence_content

        try:
            content = await retrieve_evidence_content(evidence.content_ref)
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            logger.warning(f"Failed to get seed entity: {e}")
            return None

        query = data.get("query", "")
        sources = data.get("sources", {})

        # Priority: ISED > SEDAR > EDGAR > CRA
        # Check ISED first (federal corporations)
        ised_results = sources.get("ised", {}).get("results", [])
        if ised_results:
            result = ised_results[0]
            identifiers = {"name": result.get("name", query)}
            if result.get("bn"):
                identifiers["bn"] = result["bn"]
            return SeedEntity(
                entity_type="organization",
                name=result.get("name", query),
                identifiers=identifiers,
                is_new=True,
            )

        # Check SEDAR
        sedar_results = sources.get("sedar", {}).get("results", [])
        if sedar_results:
            result = sedar_results[0]
            identifiers = {"name": result.get("name", query)}
            if result.get("sedar_id"):
                identifiers["sedar_profile"] = result["sedar_id"]
            return SeedEntity(
                entity_type="organization",
                name=result.get("name", query),
                identifiers=identifiers,
                is_new=True,
            )

        # Check EDGAR
        edgar_results = sources.get("edgar", {}).get("results", [])
        if edgar_results:
            result = edgar_results[0]
            identifiers = {"name": result.get("name", query)}
            if result.get("cik"):
                identifiers["cik"] = result["cik"]
            return SeedEntity(
                entity_type="organization",
                name=result.get("name", query),
                identifiers=identifiers,
                is_new=True,
            )

        # Check CRA
        cra_results = sources.get("cra", {}).get("results", [])
        if cra_results:
            result = cra_results[0]
            identifiers = {"name": result.get("name", query)}
            if result.get("bn"):
                identifiers["bn"] = result["bn"]
            return SeedEntity(
                entity_type="organization",
                name=result.get("name", query),
                identifiers=identifiers,
                is_new=True,
            )

        # Fallback to query
        return SeedEntity(
            entity_type="organization",
            name=query,
            identifiers={"name": query},
            is_new=True,
        )
