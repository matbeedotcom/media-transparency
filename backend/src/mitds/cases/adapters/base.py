"""Base class for entry point adapters.

Entry point adapters normalize diverse inputs (Meta Ads, corporation names,
URLs, text) into a common format: evidence record + initial leads.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from ..models import Evidence, ExtractedLead


@dataclass
class ValidationResult:
    """Result of validating an entry point input."""

    is_valid: bool
    error_message: str | None = None
    normalized_value: str | None = None
    metadata: dict[str, Any] | None = None


class SeedEntity(BaseModel):
    """An entity discovered from the entry point that seeds the investigation."""

    entity_id: UUID | None = None
    entity_type: str
    name: str
    identifiers: dict[str, str] = {}
    is_new: bool = False


class BaseEntryPointAdapter(ABC):
    """Abstract base class for entry point adapters.

    Each adapter handles a specific entry point type (meta_ad, corporation,
    url, text) and normalizes the input into:
    1. An evidence record storing the original input
    2. Initial leads to begin investigation
    3. Optionally, a seed entity if one can be immediately resolved

    Subclasses must implement all abstract methods.
    """

    @property
    @abstractmethod
    def entry_point_type(self) -> str:
        """Return the entry point type this adapter handles."""
        ...

    @abstractmethod
    async def validate(self, input_value: str) -> ValidationResult:
        """Validate the entry point input.

        Args:
            input_value: The raw input value from the user

        Returns:
            ValidationResult indicating if the input is valid and any
            normalization or error details.

        Note:
            This should perform quick validation without making external
            API calls. Expensive validation happens in create_evidence.
        """
        ...

    @abstractmethod
    async def create_evidence(
        self, case_id: UUID, input_value: str, validation_result: ValidationResult
    ) -> Evidence:
        """Create an evidence record from the input.

        This method should:
        1. Fetch any required data (e.g., URL content, Meta Ad API)
        2. Store the raw content in S3
        3. Create and return an Evidence record

        Args:
            case_id: The ID of the case this evidence belongs to
            input_value: The validated input value
            validation_result: Result from validate() call

        Returns:
            Evidence record with content stored in S3
        """
        ...

    @abstractmethod
    async def extract_leads(self, evidence: Evidence) -> list[ExtractedLead]:
        """Extract potential leads from the evidence.

        This method should analyze the evidence content and extract
        entity mentions that can become leads for investigation.

        Args:
            evidence: The evidence record to analyze

        Returns:
            List of extracted leads with confidence scores
        """
        ...

    @abstractmethod
    async def get_seed_entity(self, evidence: Evidence) -> SeedEntity | None:
        """Get a seed entity from the evidence if one can be immediately resolved.

        For some entry points (e.g., Meta Ad Sponsor), we can immediately
        create an entity to seed the investigation. For others (e.g., text),
        there may be no obvious seed entity.

        Args:
            evidence: The evidence record

        Returns:
            SeedEntity if one can be immediately resolved, None otherwise
        """
        ...
